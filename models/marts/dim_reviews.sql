with all_reviews as (

    select * from {{ref('stg_reviews')}}

),

agg as (

    select

        review_id,
        reviewer_name,
        count(distinct listing_id) as total_listings,
        count(reviews_id) as total_reviews
    from all_reviews

    group by 1,2

)

select * from agg
