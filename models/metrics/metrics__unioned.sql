{{config(materialized = 'table')}}

{% set table_list = [
    ref('metrics_sf_leads'),
    ref('metrics_sf_mql'),
    ref('metrics_sf_sql'),

]%}

with metrics_unioned as (

    {% for table in table_list %}
    select * from {{table}}
    {% if not loop.last %} union all {% endif %}
    {% endfor %}

),

final as (

    select

        {{dbt_utils.surrogate_key('metric_name','metric_type','date_day')}} as id,
        date_day,
        metric_channel,
        metric_name,
        metric_type,
        metric_value

    from metrics_unioned

)

select * from final
