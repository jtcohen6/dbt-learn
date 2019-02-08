{{config(materialized = 'table')}}

with source as (

    select * from {{ref('metrics__unioned')}}

),

days as (

    select * from {{ref('util_calendar')}}

),

metric_names as (

    select distinct
        metric_name,
        metric_channel,
        metric_type
    from source

),

filled as (

    select * from days
    cross join metric_names

),

joined as (

    select

        {{dbt_utils.surrogate_key('filled.metric_name','filled.metric_type','filled.date_day')}} as id,
        filled.date_day,
        filled.metric_channel,
        filled.metric_name,
        filled.metric_type,
        coalesce(metric_value, 0) as metric_value

    from filled
    left join source
        on filled.date_day = date(source.date_day)
        and filled.metric_name = source.metric_name
        and filled.metric_type = source.metric_type

)

select * from joined
