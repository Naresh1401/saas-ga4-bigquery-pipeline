/*
    Custom test: every session must belong to a known user.
    Relationship: stg_ga4__sessions.user_pseudo_id → stg_ga4__users.user_pseudo_id
*/

select
    s.session_id,
    s.user_pseudo_id
from {{ ref('stg_ga4__sessions') }} s
left join {{ ref('stg_ga4__users') }} u
    on s.user_pseudo_id = u.user_pseudo_id
where u.user_pseudo_id is null
