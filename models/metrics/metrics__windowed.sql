{{config(materialized = 'table')}}

with metrics as (

    select * from {{ref('metrics__filled')}}

),

agg_windowed as (

    select

        metrics.*,

        --trailing 7 days
        sum(metric_value) over (
                partition by metric_name, metric_type
                order by date_day
                rows between 6 preceding and current row
        ) as seven_days_prior,

        sum(metric_value) over (
            partition by metric_name, metric_type
            order by date_day
            rows between 13 preceding and 7 preceding
        ) as fourteen_days_prior,
            
        sum(metric_value) over (
            partition by metric_name, metric_type
            order by date_day
              rows between 20 preceding and 14 preceding
        ) as twenty_one_days_prior,
            
        sum(metric_value) over (
            partition by metric_name, metric_type
            order by date_day
              rows between 27 preceding and 21 preceding
        ) as twenty_eight_days_prior

    from metrics

)

select * from unioned
