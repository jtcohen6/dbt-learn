with all_reviews as (

    select * from {{ref('stg_listings')}}

),

agg as (

    select

        host_id,
        host_name,
        host_identity_verified,
        host_has_profile_pic,
        host_verifications,
        host_neighbourhood,
        is_superhost,
        acceptance_rate,
        response_rate,
        response_time,
        host_about,
        host_location,
        host_since,
        count(distinct listing_id) as total_listings,
        sum(number_of_reviews) as number_of_reviews,
        avg(review_scores_value) as avg_store_value,
        avg(review_scores_location) as avg_store_location,
        avg(review_scores_communication) as avg_store_communication,
        avg(review_scores_checkin) as avg_store_checkin,
        avg(review_scores_cleanliness) as avg_store_cleanliness,
        avg(review_scores_accuracy) as avg_store_accuracy,
        avg(review_scores_rating) as avg_store_rating,
        min(first_review) as first_review_date,
        max(last_review) as last_review_date

    from all_reviews
    {{dbt_utils.group_by(13)}}

)

select * from agg
