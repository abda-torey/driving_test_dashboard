{{ config(
    materialized='table'
) }}

with application_stats as (

    select
        year,
        month,
        REGEXP_REPLACE(TRIM(county), r'(?i)^Co\.\s*', '') as county,
        test_category,
        statistic,
        total_applications as stat_value
    from {{ ref('stg_staging__driving_test_api_table') }}
    where 
        lower(county) not in ('ireland') and county is not null
        and lower(test_category) not in ('all test categories') and test_category is not null

),

pivoted_applications as (

    select
        year,
        month,
        county,
        test_category,

        MAX(CASE WHEN statistic = 'Driving Test Applications Received' THEN stat_value END) as applications_received,
        MAX(CASE WHEN statistic = 'Driving Test Applicants Waiting at Month End' THEN stat_value END) as applicants_waiting,
        MAX(CASE WHEN statistic = 'Driving Test Applicants Scheduled at Month End' THEN stat_value END) as applicants_scheduled,
        MAX(CASE WHEN statistic = 'Driving Test Applicants Paused at Month End' THEN stat_value END) as applicants_paused,
        MAX(CASE WHEN statistic = 'Driving Test Applicants Not Eligible at Month End' THEN stat_value END) as applicants_not_eligible

    from application_stats
    group by year, month, county, test_category

),

local_stats as (

    select
        SAFE_CAST(SPLIT(month, ' ')[0] AS STRING) AS year,
        SAFE_CAST(SPLIT(month, ' ')[1] AS STRING) AS month,
        county,
        driving_test_categories as test_category,
        statistic_label,
        SAFE_CAST(value AS FLOAT64) as stat_value
    from {{ ref('stg_staging__driving_test_localfile_table') }}
    where 
        lower(county) not in ('ireland') and county is not null
        and lower(driving_test_categories) not in ('all test categories') and driving_test_categories is not null

),

pivoted_local as (

    select
        year,
        month,
        county,
        test_category,

        MAX(CASE WHEN statistic_label = 'Driving Tests Delivered' THEN stat_value END) as tests_delivered,
        MAX(CASE WHEN statistic_label = 'Driving Test Pass Rate' THEN stat_value END) as pass_rate,
        MAX(CASE WHEN statistic_label = 'Driving Test No-Shows' THEN stat_value END) as no_shows

    from local_stats
    group by year, month, county, test_category

)

select
    COALESCE(a.year, l.year) as year,
    COALESCE(a.month, l.month) as month,
    COALESCE(a.county, l.county) as county,
    COALESCE(a.test_category, l.test_category) as test_category,

    a.applications_received,
    a.applicants_waiting,
    a.applicants_scheduled,
    a.applicants_paused,
    a.applicants_not_eligible,

    l.tests_delivered,
    l.pass_rate,
    l.no_shows

from pivoted_applications a
full outer join pivoted_local l
on a.year = l.year
and a.month = l.month
and a.county = l.county
and a.test_category = l.test_category
