{{ config(
    materialized='table'
) }}

with cleaned_application_stats as (

    select
        year,
        month,
        TRIM(REGEXP_REPLACE(county, r'^Co\.?\s+', '')) as county,
        test_centre_raw as test_centre,
        test_category,
        statistic,
        total_applications as stat_value
    from {{ ref('stg_staging__driving_test_api_table') }}
    where 
        lower(county) not in ('ireland') and county is not null
        and lower(test_category) not in ('all test categories') and test_category is not null
        and test_centre_raw is not null

),

pivoted_stats as (

    select
        year,
        month,
        county,
        test_centre,
        test_category,

        MAX(CASE WHEN statistic = 'Driving Test Applications Received' THEN stat_value END) as applications_received,
        MAX(CASE WHEN statistic = 'Driving Test Applicants Waiting at Month End' THEN stat_value END) as applicants_waiting,
        MAX(CASE WHEN statistic = 'Driving Test Applicants Scheduled at Month End' THEN stat_value END) as applicants_scheduled,
        MAX(CASE WHEN statistic = 'Driving Test Applicants Paused at Month End' THEN stat_value END) as applicants_paused,
        MAX(CASE WHEN statistic = 'Driving Test Applicants Not Eligible at Month End' THEN stat_value END) as applicants_not_eligible

    from cleaned_application_stats
    group by year, month, county, test_centre, test_category

)

select * from pivoted_stats
