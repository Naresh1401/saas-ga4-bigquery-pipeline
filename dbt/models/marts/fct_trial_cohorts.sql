{{
    config(
        materialized='table',
        cluster_by=["cohort_week"]
    )
}}

/*
    fct_trial_cohorts
    ─────────────────
    Weekly trial cohorts with 7/14/30-day conversion rates.
*/

with trial_funnel as (

    select * from {{ ref('int_trial_funnel') }}

),

cohorts as (

    select
        -- week bucket = Monday of trial start week
        date_trunc(trial_started_date, week(monday))            as cohort_week,
        user_pseudo_id,
        user_id,
        trial_started_at,
        subscription_started_at,
        days_trial_to_paid,
        funnel_stage,

        -- ── conversion flags by time window ──
        reached_subscription
            and days_trial_to_paid <= 7                         as converted_by_day_7,
        reached_subscription
            and days_trial_to_paid <= 14                        as converted_by_day_14,
        reached_subscription
            and days_trial_to_paid <= 30                        as converted_by_day_30,

        reached_feature_used                                    as used_feature,
        reached_upgrade_intent                                  as showed_upgrade_intent

    from trial_funnel

),

cohort_summary as (

    select
        cohort_week,

        count(*)                                                as cohort_size,
        countif(used_feature)                                   as activated_users,
        countif(showed_upgrade_intent)                          as upgrade_intent_users,

        -- ── conversion counts ──
        countif(converted_by_day_7)                             as converted_7d,
        countif(converted_by_day_14)                            as converted_14d,
        countif(converted_by_day_30)                            as converted_30d,

        -- ── conversion rates ──
        safe_divide(countif(converted_by_day_7), count(*))      as conversion_rate_7d,
        safe_divide(countif(converted_by_day_14), count(*))     as conversion_rate_14d,
        safe_divide(countif(converted_by_day_30), count(*))     as conversion_rate_30d,

        -- ── activation rate ──
        safe_divide(countif(used_feature), count(*))            as activation_rate,

        -- ── avg time-to-convert (among converters) ──
        avg(if(days_trial_to_paid is not null, days_trial_to_paid, null))
                                                                as avg_days_to_convert

    from cohorts
    group by cohort_week

)

select * from cohort_summary
order by cohort_week
