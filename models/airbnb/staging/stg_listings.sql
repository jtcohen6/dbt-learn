with source as (

    select * from source_data.listings

),

renamed as (

    select

        id as listing_id,
        host_id,
--listing info
        name as listing_name,
        maximum_nights,
        minimum_nights,
        accommodates,
        longitude,
        latitude,
        extra_people,
        guests_included,
        is_business_travel_ready,
        instant_bookable,
        square_feet,
        amenities,
        bed_type,
        beds,
        bedrooms,
        bathrooms,
        room_type,
        property_type,
        description,
        space,
        access,
        transit,
        summary,
        house_rules,
        experiences_offered,

--geography
        country,
        country_code,
        smart_location,
        market,
        zipcode,
        state,
        city,
        neighbourhood_cleansed,
        neighbourhood,
        street,
        neighborhood_overview,
        is_location_exact,
        notes,

--availability
        has_availability,
        availability_365,
        availability_90,
        availability_60,
        availability_30,
--requirements

        require_guest_phone_verification,
        require_guest_profile_picture,
        cancellation_policy,
        license,
        requires_license,
        interaction,

--pricing
        cleaning_fee,
        security_deposit,
        nullif(replace(split_part(monthly_price, '$', 2), ',', ''), '') as monthly_price,
        nullif(replace(split_part(weekly_price, '$', 2), ',', ''), '') as weekly_price,
        nullif(replace(split_part(price, '$', 2), ',', ''), '') as price,

--review scores
        review_scores_value,
        review_scores_location,
        review_scores_communication,
        review_scores_checkin,
        review_scores_cleanliness,
        review_scores_accuracy,
        review_scores_rating,
        number_of_reviews,
        last_review,
        first_review,

--host details
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
        host_since

    from source

)

select * from renamed
