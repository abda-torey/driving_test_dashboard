{{ config(
    materialized='table'
) }}

with application_stats as (

    select
        year,
        month,
        TRIM(REGEXP_REPLACE(county, r'^Co\.?\s+', '')) AS county,
        test_category,
        statistic,
        unit as stat_value
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
        year,
        month,
        TRIM(REGEXP_REPLACE(county, r'^Co\.?\s+', '')) AS county,
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

),

combined_stats as (

    select
        COALESCE(a.year, l.year) as year,

        -- numeric month
        CASE 
            WHEN LOWER(COALESCE(a.month, l.month)) IN ('jan', 'january') THEN 1
            WHEN LOWER(COALESCE(a.month, l.month)) IN ('feb', 'february') THEN 2
            WHEN LOWER(COALESCE(a.month, l.month)) IN ('mar', 'march') THEN 3
            WHEN LOWER(COALESCE(a.month, l.month)) IN ('apr', 'april') THEN 4
            WHEN LOWER(COALESCE(a.month, l.month)) IN ('may') THEN 5
            WHEN LOWER(COALESCE(a.month, l.month)) IN ('jun', 'june') THEN 6
            WHEN LOWER(COALESCE(a.month, l.month)) IN ('jul', 'july') THEN 7
            WHEN LOWER(COALESCE(a.month, l.month)) IN ('aug', 'august') THEN 8
            WHEN LOWER(COALESCE(a.month, l.month)) IN ('sep', 'september') THEN 9
            WHEN LOWER(COALESCE(a.month, l.month)) IN ('oct', 'october') THEN 10
            WHEN LOWER(COALESCE(a.month, l.month)) IN ('nov', 'november') THEN 11
            WHEN LOWER(COALESCE(a.month, l.month)) IN ('dec', 'december') THEN 12
            ELSE SAFE_CAST(COALESCE(a.month, l.month) AS INT64)
        END as month,

        -- human-readable month name
        FORMAT_DATE('%B', DATE_FROM_UNIX_DATE(DATE_DIFF(DATE_TRUNC(CURRENT_DATE(), MONTH), DATE_TRUNC(DATE_TRUNC(CURRENT_DATE(), YEAR), MONTH), MONTH) + 
            CASE 
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('jan', 'january') THEN 0
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('feb', 'february') THEN 1
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('mar', 'march') THEN 2
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('apr', 'april') THEN 3
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('may') THEN 4
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('jun', 'june') THEN 5
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('jul', 'july') THEN 6
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('aug', 'august') THEN 7
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('sep', 'september') THEN 8
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('oct', 'october') THEN 9
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('nov', 'november') THEN 10
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('dec', 'december') THEN 11
                ELSE SAFE_CAST(COALESCE(a.month, l.month) AS INT64) - 1
            END
        )) as month_name,

        -- YYYY-MM formatted date
        PARSE_DATE('%Y-%m', CONCAT(CAST(COALESCE(a.year, l.year) AS STRING), '-', LPAD(CAST(
            CASE 
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('jan', 'january') THEN 1
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('feb', 'february') THEN 2
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('mar', 'march') THEN 3
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('apr', 'april') THEN 4
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('may') THEN 5
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('jun', 'june') THEN 6
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('jul', 'july') THEN 7
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('aug', 'august') THEN 8
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('sep', 'september') THEN 9
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('oct', 'october') THEN 10
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('nov', 'november') THEN 11
                WHEN LOWER(COALESCE(a.month, l.month)) IN ('dec', 'december') THEN 12
                ELSE SAFE_CAST(COALESCE(a.month, l.month) AS INT64)
            END AS STRING), 2, '0')
        )) as date_month,

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

)

select * from combined_stats
