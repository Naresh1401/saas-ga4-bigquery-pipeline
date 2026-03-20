{{
    config(
        materialized='table',
        cluster_by=["user_plan_type", "is_identified"]
    )
}}

/*
    stg_ga4__users
    ──────────────
    User-level table: one row per user_pseudo_id.
    Aggregates lifetime metrics from sessions + events.
*/

with sessions as (

    select * from {{ ref('stg_ga4__sessions') }}

),

events as (

    select * from {{ ref('stg_ga4__events') }}

),

user_sessions as (

    select
        user_pseudo_id,

        -- pick the latest non-null user_id
        array_agg(user_id ignore nulls order by session_start_at desc limit 1)[safe_offset(0)]
                                                                as user_id,

        min(session_date)                                       as first_seen_date,
        max(session_date)                                       as last_seen_date,
        count(*)                                                as total_sessions,

        -- ── channel of first session ──
        array_agg(session_channel_group order by session_start_at limit 1)[safe_offset(0)]
                                                                as first_channel_group,

        -- ── device of first session ──
        array_agg(device_category order by session_start_at limit 1)[safe_offset(0)]
                                                                as primary_device,

        -- ── country of first session ──
        array_agg(geo_country order by session_start_at limit 1)[safe_offset(0)]
                                                                as geo_country,

        -- ── plan from latest session ──
        array_agg(user_plan_type order by session_start_at desc limit 1)[safe_offset(0)]
                                                                as user_plan_type,

        -- ── SaaS flags ──
        countif(is_trial_session)                               as trial_sessions,
        countif(is_conversion_session)                          as conversion_sessions,
        countif(has_feature_usage)                               as feature_usage_sessions,

        -- ── engagement ──
        sum(page_views)                                         as total_page_views,
        avg(session_duration_seconds)                           as avg_session_duration_sec,
        sum(total_engagement_msec)                              as total_engagement_msec

    from sessions
    group by user_pseudo_id

),

user_events as (

    select
        user_pseudo_id,
        countif(event_name = 'trial_started')                   as trial_started_count,
        countif(event_name = 'subscription_started')            as subscription_count,
        countif(event_name = 'invite_sent')                     as invites_sent,
        countif(event_name = 'api_key_created')                 as api_keys_created,
        countif(event_name = 'integration_connected')           as integrations_connected,
        countif(event_name = 'feature_used')                    as total_feature_events,
        count(distinct feature_name)                            as distinct_features_used

    from events
    group by user_pseudo_id

)

select
    us.user_pseudo_id,
    us.user_id,
    us.user_id is not null                                     as is_identified,
    us.first_seen_date,
    us.last_seen_date,
    date_diff(us.last_seen_date, us.first_seen_date, day)      as lifetime_days,
    us.total_sessions,
    us.first_channel_group,
    us.primary_device,
    us.geo_country,
    us.user_plan_type,
    us.trial_sessions,
    us.conversion_sessions,
    us.feature_usage_sessions,
    us.total_page_views,
    us.avg_session_duration_sec,
    us.total_engagement_msec,

    -- ── event-level SaaS metrics ──
    coalesce(ue.trial_started_count, 0)                        as trial_started_count,
    coalesce(ue.subscription_count, 0)                         as subscription_count,
    coalesce(ue.invites_sent, 0)                               as invites_sent,
    coalesce(ue.api_keys_created, 0)                           as api_keys_created,
    coalesce(ue.integrations_connected, 0)                     as integrations_connected,
    coalesce(ue.total_feature_events, 0)                       as total_feature_events,
    coalesce(ue.distinct_features_used, 0)                     as distinct_features_used

from user_sessions us
left join user_events ue
    on us.user_pseudo_id = ue.user_pseudo_id
