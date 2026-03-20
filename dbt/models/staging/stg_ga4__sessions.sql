{{
    config(
        materialized='table',
        partition_by={
            "field": "session_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["session_channel_group", "geo_country"]
    )
}}

/*
    stg_ga4__sessions
    ─────────────────
    Sessionise GA4 events using ga_session_id + user_pseudo_id.
    Adds channel grouping and SaaS-specific session flags.
*/

with events as (

    select * from {{ ref('stg_ga4__events') }}

),

sessionised as (

    select
        -- ── session key ──
        {{ dbt_utils.generate_surrogate_key(['user_pseudo_id', 'ga_session_id']) }}
                                                                as session_id,
        user_pseudo_id,
        user_id,
        ga_session_id,
        ga_session_number,

        -- ── session timing ──
        min(event_date)                                         as session_date,
        min(event_at)                                           as session_start_at,
        max(event_at)                                           as session_end_at,
        timestamp_diff(max(event_at), min(event_at), second)    as session_duration_seconds,

        -- ── engagement ──
        count(*)                                                as event_count,
        sum(engagement_time_msec)                               as total_engagement_msec,
        countif(event_name = 'page_view')                       as page_views,

        -- ── landing page ──
        array_agg(page_location order by event_timestamp limit 1)[safe_offset(0)]
                                                                as landing_page,

        -- ── exit page ──
        array_agg(page_location order by event_timestamp desc limit 1)[safe_offset(0)]
                                                                as exit_page,

        -- ── traffic source (first touch in session) ──
        array_agg(traffic_source order by event_timestamp limit 1)[safe_offset(0)]
                                                                as traffic_source,
        array_agg(traffic_medium order by event_timestamp limit 1)[safe_offset(0)]
                                                                as traffic_medium,
        array_agg(traffic_campaign order by event_timestamp limit 1)[safe_offset(0)]
                                                                as traffic_campaign,

        -- ── channel grouping ──
        case
            when array_agg(traffic_medium order by event_timestamp limit 1)[safe_offset(0)]
                 in ('cpc', 'ppc', 'paidsearch')                     then 'Paid Search'
            when array_agg(traffic_medium order by event_timestamp limit 1)[safe_offset(0)]
                 = 'organic'                                         then 'Organic'
            when array_agg(traffic_source order by event_timestamp limit 1)[safe_offset(0)]
                 = '(direct)'                                        then 'Direct'
            when array_agg(traffic_medium order by event_timestamp limit 1)[safe_offset(0)]
                 = 'social'                                          then 'Social'
            when array_agg(traffic_medium order by event_timestamp limit 1)[safe_offset(0)]
                 in ('referral', 'ref')                              then 'Referral'
            when array_agg(traffic_medium order by event_timestamp limit 1)[safe_offset(0)]
                 = 'email'                                           then 'Email'
            else 'Other'
        end                                                     as session_channel_group,

        -- ── device / geo ──
        array_agg(device_category order by event_timestamp limit 1)[safe_offset(0)]
                                                                as device_category,
        array_agg(device_os order by event_timestamp limit 1)[safe_offset(0)]
                                                                as device_os,
        array_agg(device_browser order by event_timestamp limit 1)[safe_offset(0)]
                                                                as device_browser,
        array_agg(geo_country order by event_timestamp limit 1)[safe_offset(0)]
                                                                as geo_country,
        array_agg(geo_region order by event_timestamp limit 1)[safe_offset(0)]
                                                                as geo_region,

        -- ── SaaS flags ──
        countif(event_name = 'trial_started') > 0               as is_trial_session,
        countif(event_name = 'subscription_started') > 0        as is_conversion_session,
        countif(event_name = 'upgrade_intent') > 0              as has_upgrade_intent,
        countif(event_name = 'feature_used') > 0                as has_feature_usage,
        countif(event_name = 'invite_sent') > 0                 as has_invite_sent,

        -- ── user plan (first value in session) ──
        array_agg(user_plan_type order by event_timestamp limit 1)[safe_offset(0)]
                                                                as user_plan_type

    from events
    group by user_pseudo_id, user_id, ga_session_id, ga_session_number

)

select * from sessionised
