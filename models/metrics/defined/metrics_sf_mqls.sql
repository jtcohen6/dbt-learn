with sf_mql_base as (

    select * from {{ref('stg_sf_leads')}}
    where first_mql_date is not null

),

metric_source as (

    select
    
        first_mql_date::date as metric_date,
        'mql' as metric_name,
        'marketing' as metric_channel,
        'count' as metric_type,
        count(*) as metric_value
        
    from sf_mql_base
    where first_mql_date is not null
    group by 1,2,3,4

)

select * from metric_source
