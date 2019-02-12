with source as (

    select * from source_data.listing_history

),

renamed as (

    select

        listing_id,
        nullif(replace(split_part(price, '$', 2), ',', ''), '') as price,
        case
            when available = 't'
                then true
            else false
        end as available,
        date as available_date

    from source

),

with_id as (

    select

        {{dbt_utils.surrogate_key('listing_id', 'available_date')}} as id,
        *

    from renamed

)

select * from with_id
