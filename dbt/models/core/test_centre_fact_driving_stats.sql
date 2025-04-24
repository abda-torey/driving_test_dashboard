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
        unit as stat_value
    from {{ ref('stg_staging__driving_test_api_table') }}
    where 
        lower(county) not in ('ireland') and county is not null
        and lower(test_category) not in ('all test categories') and test_category is not null
        and test_centre_raw is not null

),

cleaned_r030_stats as (

    select
        year,
        month,
        TRIM(REGEXP_REPLACE(county, r'^Co\.?\s+', '')) as county,
        test_centre_raw as test_centre,
        test_category,
        statistic,
        unit as stat_value
    from {{ ref('stg_staging__driving_test_ro30_table') }}
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

),

pivoted_ro30_stats as (

    select
        year,
        month,
        county,
        test_centre,
        test_category,

        MAX(CASE WHEN statistic = 'Driving Tests Delivered' THEN stat_value END) as tests_delivered,
        MAX(CASE WHEN statistic = 'Driving Test Pass Rate' THEN stat_value END) as pass_rate,
        MAX(CASE WHEN statistic = 'Driving Test No-Shows' THEN stat_value END) as no_shows,
        MAX(CASE WHEN statistic = 'Driving Tests Not Conducted / Abandoned' THEN stat_value END) as notconducted_or_abandoned

    from cleaned_r030_stats
    group by year, month, county, test_centre, test_category

),

combined_stats as (

    select
    
        COALESCE(a.year, b.year) as year,
        -- Convert raw month to integer
    CASE 
        WHEN LOWER(COALESCE(a.month, b.month)) IN ('jan', 'january') THEN 1
        WHEN LOWER(COALESCE(a.month, b.month)) IN ('feb', 'february') THEN 2
        WHEN LOWER(COALESCE(a.month, b.month)) IN ('mar', 'march') THEN 3
        WHEN LOWER(COALESCE(a.month, b.month)) IN ('apr', 'april') THEN 4
        WHEN LOWER(COALESCE(a.month, b.month)) IN ('may') THEN 5
        WHEN LOWER(COALESCE(a.month, b.month)) IN ('jun', 'june') THEN 6
        WHEN LOWER(COALESCE(a.month, b.month)) IN ('jul', 'july') THEN 7
        WHEN LOWER(COALESCE(a.month, b.month)) IN ('aug', 'august') THEN 8
        WHEN LOWER(COALESCE(a.month, b.month)) IN ('sep', 'september') THEN 9
        WHEN LOWER(COALESCE(a.month, b.month)) IN ('oct', 'october') THEN 10
        WHEN LOWER(COALESCE(a.month, b.month)) IN ('nov', 'november') THEN 11
        WHEN LOWER(COALESCE(a.month, b.month)) IN ('dec', 'december') THEN 12
        ELSE SAFE_CAST(COALESCE(a.month, b.month) AS INT64)
    END as month,
    FORMAT_DATE('%B', DATE_FROM_UNIX_DATE(DATE_DIFF(DATE_TRUNC(CURRENT_DATE(), MONTH), DATE_TRUNC(DATE_TRUNC(CURRENT_DATE(), YEAR), MONTH), MONTH) + 
        CASE 
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('jan', 'january') THEN 0
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('feb', 'february') THEN 1
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('mar', 'march') THEN 2
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('apr', 'april') THEN 3
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('may') THEN 4
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('jun', 'june') THEN 5
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('jul', 'july') THEN 6
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('aug', 'august') THEN 7
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('sep', 'september') THEN 8
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('oct', 'october') THEN 9
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('nov', 'november') THEN 10
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('dec', 'december') THEN 11
            ELSE SAFE_CAST(COALESCE(a.month, b.month) AS INT64) - 1
        END
    )) as month_name,
         -- 🌟 Date column for time series line graph
    PARSE_DATE('%Y-%m', CONCAT(CAST(COALESCE(a.year, b.year) AS STRING), '-', LPAD(CAST(
        CASE 
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('jan', 'january') THEN 1
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('feb', 'february') THEN 2
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('mar', 'march') THEN 3
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('apr', 'april') THEN 4
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('may') THEN 5
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('jun', 'june') THEN 6
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('jul', 'july') THEN 7
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('aug', 'august') THEN 8
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('sep', 'september') THEN 9
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('oct', 'october') THEN 10
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('nov', 'november') THEN 11
            WHEN LOWER(COALESCE(a.month, b.month)) IN ('dec', 'december') THEN 12
            ELSE SAFE_CAST(COALESCE(a.month, b.month) AS INT64)
        END AS STRING), 2, '0')
    )) as date_month,
        COALESCE(a.county, b.county) as county,
        COALESCE(a.test_centre, b.test_centre) as test_centre,
        COALESCE(a.test_category, b.test_category) as test_category,

        a.applications_received,
        a.applicants_waiting,
        a.applicants_scheduled,
        a.applicants_paused,
        a.applicants_not_eligible,

        b.tests_delivered,
        b.pass_rate,
        b.no_shows,
        b.notconducted_or_abandoned

    from pivoted_stats a
    full outer join pivoted_ro30_stats b
        on a.year = b.year
        and a.month = b.month
        and a.county = b.county
        and a.test_centre = b.test_centre
        and a.test_category = b.test_category

)

select * from combined_stats
