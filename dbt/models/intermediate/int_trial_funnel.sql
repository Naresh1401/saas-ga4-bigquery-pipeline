{{
    config(
        materialized='table',
        cluster_by=["funnel_stage"]
    )
}}

/*
    int_trial_funnel
    ────────────────
    Per-user trial funnel: trial_started → feature_used → upgrade_intent →
    subscription_started, with timestamps for each stage.
*/

with trial_starts as (

    select
        user_pseudo_id,
        user_id,
        min(event_at)   as trial_started_at,
        min(event_date)  as trial_started_date
    from {{ ref('stg_ga4__events') }}
    where event_name = 'trial_started'
    group by user_pseudo_id, user_id

),

first_feature_use as (

    select
        user_pseudo_id,
        min(event_at)   as first_feature_used_at
    from {{ ref('stg_ga4__events') }}
    where event_name = 'feature_used'
    group by user_pseudo_id

),

first_upgrade_intent as (

    select
        user_pseudo_id,
        min(event_at)   as first_upgrade_intent_at
    from {{ ref('stg_ga4__events') }}
    where event_name = 'upgrade_intent'
    group by user_pseudo_id

),

first_subscription as (

    select
        user_pseudo_id,
        min(event_at)   as subscription_started_at,
        -- grab plan details from first conversion
        array_agg(subscription_plan_name ignore nulls order by event_at limit 1)[safe_offset(0)]
                         as converted_plan_name,
        array_agg(billing_cycle ignore nulls order by event_at limit 1)[safe_offset(0)]
                         as converted_billing_cycle,
        array_agg(mrr_value ignore nulls order by event_at limit 1)[safe_offset(0)]
                         as converted_mrr_value
    from {{ ref('stg_ga4__events') }}
    where event_name = 'subscription_started'
    group by user_pseudo_id

),

funnel as (

    select
        ts.user_pseudo_id,
        ts.user_id,
        ts.trial_started_at,
        ts.trial_started_date,
        ffu.first_feature_used_at,
        fui.first_upgrade_intent_at,
        fs.subscription_started_at,
        fs.converted_plan_name,
        fs.converted_billing_cycle,
        fs.converted_mrr_value,

        -- ── stage flags ──
        true                                                    as reached_trial,
        ffu.first_feature_used_at is not null                   as reached_feature_used,
        fui.first_upgrade_intent_at is not null                 as reached_upgrade_intent,
        fs.subscription_started_at is not null                  as reached_subscription,

        -- ── time between stages (days) ──
        timestamp_diff(ffu.first_feature_used_at, ts.trial_started_at, day)
                                                                as days_trial_to_feature,
        timestamp_diff(fui.first_upgrade_intent_at, ts.trial_started_at, day)
                                                                as days_trial_to_upgrade_intent,
        timestamp_diff(fs.subscription_started_at, ts.trial_started_at, day)
                                                                as days_trial_to_paid,

        -- ── funnel stage label ──
        case
            when fs.subscription_started_at is not null         then 'converted'
            when fui.first_upgrade_intent_at is not null        then 'upgrade_intent'
            when ffu.first_feature_used_at is not null          then 'feature_used'
            else 'trial_only'
        end                                                     as funnel_stage

    from trial_starts ts
    left join first_feature_use ffu on ts.user_pseudo_id = ffu.user_pseudo_id
    left join first_upgrade_intent fui on ts.user_pseudo_id = fui.user_pseudo_id
    left join first_subscription fs on ts.user_pseudo_id = fs.user_pseudo_id

)

select * from funnel
