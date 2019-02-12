with source as (

    select * from source_data.reviews

),

renamed as (

    select

        id as reviews_id,
        review as review_id,
        listing_id,
        reviewer_name,
        comments,
        date as review_date

    from source

)

select * from renamed
