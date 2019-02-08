with snowplow_sessions as (

    select * from {{ ref('snowplow_events_tmp') }}

),

id_map as (

    select * from {{ ref('snowplow_id_map') }}

),

stitched as (

    select
        s.*,
        coalesce(id.user_id, s.user_snowplow_domain_id) as inferred_user_id

    from snowplow_sessions s
    left outer join id_map as id using (domain_userid)

)

select * from stitched
