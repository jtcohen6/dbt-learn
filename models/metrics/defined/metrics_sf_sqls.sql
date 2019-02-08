with source as (

    select * from {{ref('stg_sf_opportunity')}}
),

metric_source as (

    select
    
        created_at::date as metric_date,
        'sql' as metric_name,
        'marketing' as metric_channel,
        'count' as metric_type,
        count(*) as metric_value

    from source
    where created_at is not null
    group by 1,2,3,4

)

select * from metric_source
