with avev as (
    select * from {{ ref('src_avev_pa__national_reporting_snapshot') }}
)

, persons as (
    select * from {{ ref('src_analytics_pa__person') }}
)

, scores as (
    select * from {{ ref('src_analytics_pa__all_scores') }}
)

-- join on persons table to retrieve demographic information and vanids 
-- and join on the support table for harris support score
, demos_plus_rejects as (
    select 
        ru.avev_id
        , ru.myv_van_id
        , ru.ballot_rejected -- ballot returned but has issues
        , ru.early_voted -- succesfully early voted
        , ru.ballot_curable -- ballot rejected but can be fixed
        , ru.ballot_mailed_not_returned
        , ru.reason_ballot_rejected
        , coalesce(upper(p.county_name), upper(ru.county_name)) as county_name_upper
        , coalesce(p.county_name, ru.county_name) as county_name_reg
        , p.gender_vf as gender
        , p.bfp_ethnicity_combined as ethnicity
        , p.bfp_cultural_religion_combined as religion
        , coalesce(ru.reg_party, ru.ballot_party, ru.party_combined) as political_party
        , p.bfp_education_combined as education_level
        , p.bfp_age_generation as generation
        , p.bfp_age_bucket_small as age_bucket
        , p.bfp_urbanicity_new as urbanacity
        , p.bfp_vote_history as voting_history
        , s.harris_support_score_targeting as harris_support_score
    from avev as ru
    left join persons as p
        on ru.person_id = p.person_id
    left join scores as s
        on ru.person_id = s.person_id
    where
        ballot_cancelled = 0 -- exclude cancelled ballots
)

-- clean up some of the column identifiers
, clean as (
    select
        avev_id
        , ballot_rejected
        , early_voted
        , ballot_curable
        , ballot_mailed_not_returned
        , case
            when
                (
                    ballot_rejected = 1 and reason_ballot_rejected = 'CANC - OTHER' 
                    or ballot_rejected = 1  and reason_ballot_rejected = 'PEND - OTHER'
                )
                then 'Other'
            when
                (
                    ballot_rejected = 1 and reason_ballot_rejected = 'CANC - NO SIGNATURE'
                    or ballot_rejected = 1 and reason_ballot_rejected = 'PEND - NO SIGNATURE'
                )
                then 'No Signature'
            when
                (
                    ballot_rejected = 1 and reason_ballot_rejected = 'CANC - INCORRECT DATE'
                    or ballot_rejected = 1 and reason_ballot_rejected = 'PEND - INCORRECT DATE'
                )
                then 'Incorrect Date'
            when
                (
                    ballot_rejected = 1 and reason_ballot_rejected = 'CANC - NO DATE'
                    or ballot_rejected = 1 and reason_ballot_rejected = 'PEND - NO DATE'
                )
                then 'No Date'
            when 
                (
                    ballot_rejected = 1 and reason_ballot_rejected = 'PEND - NO ID'
                    or ballot_rejected = 1 and reason_ballot_rejected = 'CANC - NO ID'
                )
                then 'No ID'
            when
                (
                    ballot_rejected = 1 and reason_ballot_rejected = 'PEND - NO SECRECY ENVELOPE'
                    or ballot_rejected = 1 and reason_ballot_rejected = 'CANC - NO SECRECY ENVELOPE'
                )
                then 'No Secrecy Envelope'
            when
                (
                    ballot_rejected = 1 and reason_ballot_rejected = 'CANC - VOTE CHALLENGED'
                )
                then 'Vote Challenged'
            when 
                (
                    ballot_rejected = 1 and reason_ballot_rejected = 'CANC - RETURNED AFTER DEADLINE'
                )
                then 'Returned After Deadline'
            else null
        end as reason_ballot_rejected
        , case
            when county_name_upper is null then 'UNKNOWN'
            else county_name_upper
        end as county_name_upper
        , case
            when county_name_reg is null then 'Unknown'
            else county_name_reg
        end as county_name_reg
        , case
            when gender = 'F' then 'Female'
            when gender = 'M' then 'Male'
            else 'Unknown'
        end as gender
        , case
            when ethnicity = 'W' then 'White'
            when ethnicity = 'H' then 'Hispanic'
            when ethnicity = 'B' then 'Black'
            when ethnicity = 'A' then 'Asian'
            else 'Unknown'
        end as ethnicity
        , case 
            when ethnicity = 'W' then 1 
            when ethnicity is null then null
            else 0
        end as white_yes_no
        , case
            when religion = 'not_religious' then 'Not Religous'
            when religion = 'catholic' then 'Catholic'
            when religion = 'evangelical_protestant' then 'Evengelical'
            when religion = 'mainline_protestant' then 'Protestant'
            when religion = 'jewish' then 'Jewish'
            when religion = 'buddhist' then 'Buddhist'
            when religion = 'hindu' then 'Hindu'
            when religion = 'muslim' then 'Muslim'
            when religion = 'latter_day_saint' or religion = 'other_christian' then 'Other Christian'
            else 'Unknown'
        end as religion
        , case
            when education_level = 'College' then 'College'
            when education_level = 'Non-college' then 'Non-College'
            else 'Unknown'
        end as education_level
        , case
            when generation = '1-Gen Z' then 'Gen Z' -- 18-29
            when generation = '6-Seniors' then 'Seniors' -- 65+
            when generation = '3-Xennials' then 'Xennials' -- 30-64
            when generation = '2-Millenials' then 'Millenials' -- 18-49
            when generation = '4-Reagan Gen X' then 'Gen X' -- 50-64
            when generation = '5-Baby Boomers' then 'Baby Boomers' -- 50+
            else 'Unknown'
        end as generation
        , case
            when age_bucket is null then 'Unknown'
            else age_bucket 
        end as age_bucket
        , case
            when urbanacity = '1. Rural' then 'Rural'
            when
                (
                    urbanacity = '3. Suburban Mid'
                    or urbanacity = '2. Suburban Low'
                    or urbanacity = '4. Suburban High'
                ) then 'Suburban'
            when urbanacity = '5. Urban' then 'Urban'
            else 'Unknown'
        end as urbanacity
        , case
            when voting_history = 'b Presidential voter' then 'Presidential Voter'
            when voting_history = 'c Newreg' then 'New Registered Voter'
            when voting_history = 'a Midterm voter' then 'Midterm Voter'
            when voting_history like 'd Nonvoter' then 'Non-Voter'
            else 'Unknown'
        end as voting_history
        , case
            when political_party = 'I' then 'Independent'
            when political_party = 'D' then 'Democrat'
            when political_party = 'R' then 'Republican'
            when political_party = 'L' then 'Libertarian'
            else 'Unknown'
        end as political_party
        , harris_support_score
        , case
            when harris_support_score <= 0.6 then 'Low Support'
            when harris_support_score <= 0.79 then 'Moderate Support'
            when harris_support_score <= 1 then 'High Support'
            else 'Unknown'
        end as harris_support_level
    from demos_plus_rejects
    where
        ballot_mailed_not_returned = 1
        or early_voted = 1
)

select * from clean