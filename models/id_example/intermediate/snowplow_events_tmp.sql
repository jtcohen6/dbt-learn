--this model helps stitch in order email when available
--cart to order mapping available from Feb 2017 onward
{{
    config(
        materialized = 'incremental',
        unique_key = 'event_id'
    )
}}

with sp_events as (

    select * from raw.snowplow.event
    {% if is_incremental() %}

    where collector_tstamp > (select max(collector_tstamp) from {{this}})

    {% endif %}
),

orders as (

    select * from {{ref('stg_orders')}}

),

final as (

    select
        sp_events.*,
        orders.order_cart_id,
        orders.email,
        coalesce(orders.email, sp_events.user_id) as user_id
    from sp_events s
    left join orders on sp_events.user_id = carts.order_cart_id

)

select * from final
