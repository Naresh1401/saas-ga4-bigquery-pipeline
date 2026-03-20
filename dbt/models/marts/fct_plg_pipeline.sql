{{
    config(
        materialized='table',
        cluster_by=["recommended_action", "is_pql"]
    )
}}

/*
    fct_plg_pipeline
    ────────────────
    Product-qualified leads (PQLs) — users showing buying signals
    based on product usage. Feeds CRM / sales outreach.
*/

with plg as (

    select * from {{ ref('int_plg_signals') }}

),

trial_funnel as (

    select
        user_pseudo_id,
        trial_started_at,
        funnel_stage,
        converted_plan_name,
        days_trial_to_paid
    from {{ ref('int_trial_funnel') }}

),

pipeline as (

    select
        p.user_pseudo_id,
        p.user_id,
        p.user_plan_type,
        p.is_identified,
        p.first_seen_date,
        p.last_seen_date,
        p.total_sessions,

        -- ── PLG signals ──
        p.api_keys_created,
        p.integrations_connected,
        p.invites_sent,
        p.features_used_count,
        p.sessions_last_7d,

        -- ── scoring ──
        p.pql_score,
        p.is_pql,
        p.top_signal,
        p.recommended_action,

        -- ── trial context ──
        tf.trial_started_at,
        tf.funnel_stage,
        tf.converted_plan_name,
        tf.days_trial_to_paid,

        -- ── score tier for bucketing ──
        case
            when p.pql_score > 15 then 'hot'
            when p.pql_score > 8  then 'warm'
            when p.pql_score > 4  then 'nurture'
            else 'cold'
        end                                                     as lead_tier

    from plg p
    left join trial_funnel tf
        on p.user_pseudo_id = tf.user_pseudo_id

)

select * from pipeline
order by pql_score desc
