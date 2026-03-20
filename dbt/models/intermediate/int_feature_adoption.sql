{{
    config(
        materialized='table',
        cluster_by=["feature_name"]
    )
}}

/*
    int_feature_adoption
    ────────────────────
    Which features are used by which plan types, usage frequency,
    and breadth score per user.
*/

with feature_events as (

    select
        user_pseudo_id,
        user_id,
        feature_name,
        coalesce(param_plan_type, user_plan_type)               as plan_type,
        event_date,
        event_at
    from {{ ref('stg_ga4__events') }}
    where event_name = 'feature_used'
      and feature_name is not null

),

-- ── per user × feature aggregation ──
user_feature_usage as (

    select
        user_pseudo_id,
        feature_name,
        plan_type,
        count(*)                                                as usage_count,
        count(distinct event_date)                              as active_days,
        min(event_at)                                           as first_used_at,
        max(event_at)                                           as last_used_at
    from feature_events
    group by user_pseudo_id, feature_name, plan_type

),

-- ── per user breadth score ──
user_breadth as (

    select
        user_pseudo_id,
        count(distinct feature_name)                            as features_used_count,
        sum(usage_count)                                        as total_feature_events,
        -- breadth score = distinct features / 10 (total available)
        round(count(distinct feature_name) / 10.0, 2)          as feature_breadth_score
    from user_feature_usage
    group by user_pseudo_id

),

-- ── feature-level summary by plan ──
feature_plan_summary as (

    select
        feature_name,
        plan_type,
        count(distinct user_pseudo_id)                          as unique_users,
        sum(usage_count)                                        as total_events,
        avg(usage_count)                                        as avg_events_per_user,
        avg(active_days)                                        as avg_active_days
    from user_feature_usage
    group by feature_name, plan_type

)

select
    ufu.user_pseudo_id,
    ufu.feature_name,
    ufu.plan_type,
    ufu.usage_count,
    ufu.active_days,
    ufu.first_used_at,
    ufu.last_used_at,
    ub.features_used_count,
    ub.total_feature_events,
    ub.feature_breadth_score,
    fps.unique_users              as feature_plan_unique_users,
    fps.avg_events_per_user       as feature_plan_avg_events

from user_feature_usage ufu
left join user_breadth ub
    on ufu.user_pseudo_id = ub.user_pseudo_id
left join feature_plan_summary fps
    on ufu.feature_name = fps.feature_name
    and ufu.plan_type = fps.plan_type
