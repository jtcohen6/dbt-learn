with sf_leads_base as (
    
    select * from {{ref('stg_sf_leads')}}
    where email is not null
    --anton does not consider it to be a lead if the email is null
),

metric_source as (

    select

        created_at::date as metric_date,
        'lead' as metric_name,
        'marketing' as metric_channel,
        'count' as metric_type,
        count(*) as metric_value


    from sf_leads_base
    where created_at is not null
    group by 1,2,3,4
    
)

select * from metric_source
