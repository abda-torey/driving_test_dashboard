with 

source as (

    select * from {{ source('staging', 'driving_test_localfile_table') }}

),

renamed as (

    select
        statistic_label,
        month,
        county,
        driving_test_categories,
        unit,
        value

    from source

)

select * from renamed
