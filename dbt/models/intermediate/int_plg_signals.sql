{{
    config(
        materialized='table',
        cluster_by=["is_pql"]
    )
}}

/*
    int_plg_signals
    ───────────────
    Product-led growth signals per user — feeds PQL scoring.
    Aggregates: api keys created, integrations connected, invites sent,
    sessions in last 7 days, distinct features used.
*/

with users as (

    select * from {{ ref('stg_ga4__users') }}

),

events as (

    select * from {{ ref('stg_ga4__events') }}

),

sessions as (

    select * from {{ ref('stg_ga4__sessions') }}

),

recent_sessions as (

    select
        user_pseudo_id,
        count(*)                                                as sessions_last_7d
    from sessions
    where session_date >= date_sub(current_date(), interval 7 day)
    group by user_pseudo_id

),

plg_signals as (

    select
        u.user_pseudo_id,
        u.user_id,
        u.user_plan_type,
        u.is_identified,
        u.first_seen_date,
        u.last_seen_date,
        u.total_sessions,

        -- ── core PLG signals ──
        u.api_keys_created,
        u.integrations_connected,
        u.invites_sent,
        u.distinct_features_used                                as features_used_count,
        coalesce(rs.sessions_last_7d, 0)                        as sessions_last_7d,

        -- ── PQL score ──
        round(
            (u.api_keys_created * 3.0)
            + (u.integrations_connected * 2.0)
            + (u.invites_sent * 2.0)
            + (u.distinct_features_used * 1.0)
            + (coalesce(rs.sessions_last_7d, 0) * 0.5),
            2
        )                                                       as pql_score,

        -- ── PQL flag (threshold = 8) ──
        round(
            (u.api_keys_created * 3.0)
            + (u.integrations_connected * 2.0)
            + (u.invites_sent * 2.0)
            + (u.distinct_features_used * 1.0)
            + (coalesce(rs.sessions_last_7d, 0) * 0.5),
            2
        ) > 8                                                   as is_pql,

        -- ── top signal ──
        case
            when u.api_keys_created >= 1 and u.integrations_connected >= 1
                then 'power_user'
            when u.invites_sent >= 2
                then 'viral_champion'
            when u.distinct_features_used >= 5
                then 'deep_adopter'
            when coalesce(rs.sessions_last_7d, 0) >= 5
                then 'highly_engaged'
            else 'early_stage'
        end                                                     as top_signal,

        -- ── recommended action ──
        case
            when round(
                    (u.api_keys_created * 3.0)
                    + (u.integrations_connected * 2.0)
                    + (u.invites_sent * 2.0)
                    + (u.distinct_features_used * 1.0)
                    + (coalesce(rs.sessions_last_7d, 0) * 0.5),
                    2
                 ) > 15
                then 'fast_track_to_sales'
            when round(
                    (u.api_keys_created * 3.0)
                    + (u.integrations_connected * 2.0)
                    + (u.invites_sent * 2.0)
                    + (u.distinct_features_used * 1.0)
                    + (coalesce(rs.sessions_last_7d, 0) * 0.5),
                    2
                 ) > 8
                then 'schedule_demo'
            when u.distinct_features_used >= 3
                then 'send_upgrade_email'
            else 'nurture'
        end                                                     as recommended_action

    from users u
    left join recent_sessions rs
        on u.user_pseudo_id = rs.user_pseudo_id

)

select * from plg_signals
