/*
    Custom test: trial_to_paid_days must be positive and < 90.
    Fails if any row has a negative or excessively large value.
*/

select
    user_pseudo_id,
    days_trial_to_paid
from {{ ref('int_trial_funnel') }}
where days_trial_to_paid is not null
  and (days_trial_to_paid < 0 or days_trial_to_paid >= 90)
