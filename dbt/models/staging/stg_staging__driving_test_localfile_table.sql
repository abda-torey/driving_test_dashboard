with 

source as (

    select * from {{ source('staging', 'driving_test_localfile_table') }}

),

renamed as (

    select
        statistic_label,
        SAFE_CAST(SPLIT(month, ' ')[0] AS STRING) AS year,
        SAFE_CAST(SPLIT(month, ' ')[1] AS STRING) AS month,
        county,
        driving_test_categories,
        unit,
        value

    from source

)

select * from renamed
