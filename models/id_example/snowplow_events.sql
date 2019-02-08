with snowplow_sessions as (

    select * from {{ ref('snowplow_events_tmp') }}

),

id_map as (

    select * from {{ ref('snowplow_id_map') }}

),

stitched as (

    select
        snowplow_sessions.*,
        coalesce(id.user_id, user_snowplow_domain_id) as inferred_user_id

    from snowplow_sessions as s
    left outer join id_map as id using (domain_userid)

)

select * from stitched
