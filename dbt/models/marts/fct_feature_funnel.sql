{{
    config(
        materialized='table',
        cluster_by=["feature_name"]
    )
}}

/*
    fct_feature_funnel
    ──────────────────
    Feature adoption funnel — which features drive trial→paid conversion?
    Correlates feature usage with conversion outcomes.
*/

with feature_adoption as (

    select * from {{ ref('int_feature_adoption') }}

),

trial_funnel as (

    select * from {{ ref('int_trial_funnel') }}

),

-- distinct users per feature plus their conversion status
feature_conversion as (

    select
        fa.feature_name,
        fa.user_pseudo_id,
        fa.plan_type,
        fa.usage_count,
        fa.active_days,
        fa.feature_breadth_score,
        coalesce(tf.reached_subscription, false)                as converted,
        tf.days_trial_to_paid

    from feature_adoption fa
    left join trial_funnel tf
        on fa.user_pseudo_id = tf.user_pseudo_id

),

feature_summary as (

    select
        feature_name,

        -- ── usage volume ──
        count(distinct user_pseudo_id)                          as total_users,
        sum(usage_count)                                        as total_events,
        avg(usage_count)                                        as avg_usage_per_user,
        avg(active_days)                                        as avg_active_days,

        -- ── conversion impact ──
        countif(converted)                                      as converted_users,
        safe_divide(countif(converted), count(distinct user_pseudo_id))
                                                                as conversion_rate,

        -- ── avg feature breadth among converters vs non-converters ──
        avg(if(converted, feature_breadth_score, null))         as avg_breadth_converters,
        avg(if(not converted, feature_breadth_score, null))     as avg_breadth_non_converters,

        -- ── days to convert for users of this feature ──
        avg(if(converted, days_trial_to_paid, null))            as avg_days_to_convert

    from feature_conversion
    group by feature_name

),

ranked as (

    select
        *,
        rank() over (order by conversion_rate desc)             as conversion_rank,
        rank() over (order by total_users desc)                 as popularity_rank
    from feature_summary

)

select * from ranked
order by conversion_rate desc
