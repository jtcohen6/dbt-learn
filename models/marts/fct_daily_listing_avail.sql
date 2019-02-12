with listing_history as (

    select * from {{ref('stg_listing_history')}}

),

info as (

    select * from {{ref('stg_listings')}}

),

calendar as (

    select * from {{ref('util_calendar')}}

),

historical_dates as (

    select

        *,
        lead(available_date) over (
            partition by listing_id
            order by available_date)
        as next_available_date

    from listing_history

),

daily_listings as (

    select * from calendar
    join historical_dates
        on date_day >= available_date
        and date_day < next_available_date

),

all_info as (

    select

        daily.date_day,
        daily.listing_id,
        daily.available,
        info.listing_name,
        info.property_type,
        info.room_type,
        info.bedrooms,
        info.beds,
        info.square_feet,
        info.state,
        info.city,
        info.neighbourhood,
        case
            when daily.price is null
                then coalesce(
                    lag(daily.price ignore nulls) over (
                        partition by listing_id
                        order by date_day),
                    info.price)
            else daily.price
        end as price

    from daily_listings daily
    left join info using (listing_id)

),

with_id as (

    select

        {{dbt_utils.surrogate_key('date_day', 'listing_id')}} as id,
        *

    from all_info

)

select * from with_id
