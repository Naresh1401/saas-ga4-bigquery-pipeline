{{
    config(
        materialized='view',
        partition_by={
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

/*
    stg_ga4__events
    ───────────────
    Flatten the nested GA4 event_params into named columns.
    One row per event.
*/

with source as (

    select * from {{ source('ga4', 'events') }}

),

flattened as (

    select
        -- ── identifiers ──
        parse_date('%Y%m%d', event_date)                       as event_date,
        event_timestamp,
        timestamp_micros(event_timestamp)                       as event_at,
        event_name,
        user_pseudo_id,
        user_id,

        -- ── session ids from event_params ──
        (select value.int_value
         from unnest(event_params) where key = 'ga_session_id'
        )                                                       as ga_session_id,

        (select value.int_value
         from unnest(event_params) where key = 'ga_session_number'
        )                                                       as ga_session_number,

        -- ── engagement ──
        (select value.int_value
         from unnest(event_params) where key = 'engagement_time_msec'
        )                                                       as engagement_time_msec,

        -- ── page params ──
        (select value.string_value
         from unnest(event_params) where key = 'page_location'
        )                                                       as page_location,

        (select value.string_value
         from unnest(event_params) where key = 'page_title'
        )                                                       as page_title,

        (select value.string_value
         from unnest(event_params) where key = 'page_referrer'
        )                                                       as page_referrer,

        -- ── SaaS-specific: feature_used ──
        (select value.string_value
         from unnest(event_params) where key = 'feature_name'
        )                                                       as feature_name,

        (select value.string_value
         from unnest(event_params) where key = 'plan_type'
        )                                                       as param_plan_type,

        (select value.string_value
         from unnest(event_params) where key = 'user_role'
        )                                                       as param_user_role,

        -- ── SaaS-specific: trial ──
        (select value.string_value
         from unnest(event_params) where key = 'trial_plan'
        )                                                       as trial_plan,

        -- ── SaaS-specific: upgrade / subscription ──
        (select value.string_value
         from unnest(event_params) where key = 'cta_location'
        )                                                       as cta_location,

        (select value.string_value
         from unnest(event_params) where key = 'target_plan'
        )                                                       as target_plan,

        (select value.string_value
         from unnest(event_params) where key = 'plan_name'
        )                                                       as subscription_plan_name,

        (select value.string_value
         from unnest(event_params) where key = 'billing_cycle'
        )                                                       as billing_cycle,

        (select value.int_value
         from unnest(event_params) where key = 'mrr_value'
        )                                                       as mrr_value,

        -- ── SaaS-specific: integration ──
        (select value.string_value
         from unnest(event_params) where key = 'integration_name'
        )                                                       as integration_name,

        -- ── SaaS-specific: api key ──
        (select value.string_value
         from unnest(event_params) where key = 'key_type'
        )                                                       as api_key_type,

        -- ── SaaS-specific: invite ──
        (select value.string_value
         from unnest(event_params) where key = 'invitee_email_domain'
        )                                                       as invitee_email_domain,

        -- ── SaaS-specific: survey ──
        (select value.string_value
         from unnest(event_params) where key = 'survey_type'
        )                                                       as survey_type,

        (select value.int_value
         from unnest(event_params) where key = 'score'
        )                                                       as survey_score,

        -- ── scroll / click / download ──
        (select value.int_value
         from unnest(event_params) where key = 'percent_scrolled'
        )                                                       as percent_scrolled,

        (select value.string_value
         from unnest(event_params) where key = 'link_url'
        )                                                       as link_url,

        (select value.string_value
         from unnest(event_params) where key = 'file_name'
        )                                                       as downloaded_file_name,

        -- ── user properties (from user_properties array) ──
        (select value.string_value
         from unnest(user_properties) where key = 'plan_type'
        )                                                       as user_plan_type,

        (select value.string_value
         from unnest(user_properties) where key = 'account_id'
        )                                                       as account_id,

        (select value.string_value
         from unnest(user_properties) where key = 'user_role'
        )                                                       as user_role,

        -- ── device / geo / traffic ──
        device.category                                         as device_category,
        device.operating_system                                 as device_os,
        device.browser                                          as device_browser,

        geo.country                                             as geo_country,
        geo.region                                              as geo_region,
        geo.city                                                as geo_city,

        traffic_source.source                                   as traffic_source,
        traffic_source.medium                                   as traffic_medium,
        traffic_source.name                                     as traffic_campaign,

        -- ── platform ──
        platform,
        stream_id

    from source

)

select * from flattened
