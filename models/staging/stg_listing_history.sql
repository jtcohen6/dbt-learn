with source as (

    select * from source_data.listing_history

),

renamed as (

    select

        listing_id,
        price,
        available,
        date as available_date

    from source

)

select * from renamed
