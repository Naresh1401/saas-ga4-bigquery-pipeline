/*
    Custom test: every conversion session must have a trial start upstream.
    A user with is_conversion_session = true must also appear in trial_funnel.
*/

with conversion_sessions as (

    select distinct user_pseudo_id
    from {{ ref('stg_ga4__sessions') }}
    where is_conversion_session = true

),

trial_users as (

    select distinct user_pseudo_id
    from {{ ref('int_trial_funnel') }}

)

select
    cs.user_pseudo_id
from conversion_sessions cs
left join trial_users tu
    on cs.user_pseudo_id = tu.user_pseudo_id
where tu.user_pseudo_id is null
