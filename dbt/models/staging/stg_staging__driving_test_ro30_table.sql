with 

source as (

    select * from {{ source('staging', 'driving_test_ro30_table') }}

),

renamed as (

    select
        statistic,
        SAFE_CAST(SPLIT(month, ' ')[0] AS STRING) AS year,
        SAFE_CAST(SPLIT(month, ' ')[1] AS STRING) AS month,
        `driving_test_categories` AS test_category,
        `driving_test_centre` AS test_centre_raw,
        -- Extract county name
        case 
            when driving_test_centre = 'All driving test centres' then 'Ireland'
            else REGEXP_EXTRACT(driving_test_centre, r",\s*(Co\.\s*\w+)")
        end as county,
        SAFE_CAST(value AS INT64) AS unit

    from source

)

select * from renamed
