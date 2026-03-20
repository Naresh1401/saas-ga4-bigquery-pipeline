{{
    config(
        materialized='table',
        partition_by={
            "field": "date_day",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["channel_group", "geo_country"]
    )
}}

/*
    fct_saas_web_kpis
    ─────────────────
    Daily traffic KPIs by channel / country / device +
    trial starts + paid conversions.
*/

with sessions as (

    select * from {{ ref('stg_ga4__sessions') }}

),

daily_kpis as (

    select
        session_date                                            as date_day,
        session_channel_group                                   as channel_group,
        geo_country,
        device_category,

        -- ── traffic ──
        count(*)                                                as sessions,
        count(distinct user_pseudo_id)                          as unique_users,
        sum(page_views)                                         as page_views,

        -- ── engagement ──
        avg(session_duration_seconds)                           as avg_session_duration_sec,
        avg(event_count)                                        as avg_events_per_session,
        safe_divide(countif(page_views = 1), count(*))          as bounce_rate,

        -- ── SaaS conversions ──
        countif(is_trial_session)                               as trial_starts,
        countif(is_conversion_session)                          as paid_conversions,
        countif(has_upgrade_intent)                              as upgrade_intents,

        -- ── conversion rates ──
        safe_divide(countif(is_trial_session), count(*))        as trial_start_rate,
        safe_divide(
            countif(is_conversion_session),
            nullif(countif(is_trial_session), 0)
        )                                                       as trial_to_paid_rate

    from sessions
    group by session_date, session_channel_group, geo_country, device_category

)

select * from daily_kpis
