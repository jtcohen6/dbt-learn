with calendar as (

    select * from {{ref('util_calendar')}}

),

months as (

    select distinct
        date_trunc('month', date_day)::date as date_month
    from calendar

)

select * from months
