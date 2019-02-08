with listing_history as (

    select * from {{ref('stg_listing_history')}}

),

listing_info as (

    select * from {{ref('stg_listings')}}

),

calendar as (

    select * from {{ref('util_calendar')}}

),

historical_dates as (

    select

        *,
        lead(??, 1) over (
            partition by ??
            order by ??)
        as next_available_date

    from listing_history

),

all_listing_info as (

    select
        [columns]
    from historical_dates
    left join listing_info using ()

),

daily_listings as (

    select
        calendar.date_day,
        [ columns ]
    from calendar
    join all_listing_info
        on

),

all_prices as (

    select
        date_day,
        listing_id,
        ...
        case
            when price is null
                then lag(price ignore nulls) over (partition by ?? order by ??)
            else price
        end as price
    from daily_listings

)

select * from prices
