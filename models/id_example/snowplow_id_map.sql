with all_events as (

    select * from {{ ref('snowplow_events_tmp') }}

),

relevant_events as (

    select
        domain_userid,
        user_id,
        collector_tstamp

    from all_events
    where user_id is not null
      and domain_userid is not null
      and collector_tstamp is not null

),

id_mapped as (

    select distinct
        domain_userid,
        last_value(user_id)
            over (
                partition by domain_userid
                order by collector_tstamp nulls first
                rows between unbounded preceding and unbounded following)
        as user_id,
        max(collector_tstamp)
            over (partition by domain_userid) as max_tstamp
    from relevant_events

)

select * from id_mapped
