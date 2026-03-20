{{
    config(
        materialized='table',
        partition_by={
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["page_path"]
    )
}}

/*
    int_web_performance
    ───────────────────
    Page-level web metrics: bounce rate, avg engagement time,
    exit rate, and page views.
*/

with page_views as (

    select
        event_date,
        user_pseudo_id,
        ga_session_id,
        event_at,
        page_location,
        -- extract just the path from full URL
        regexp_extract(page_location, r'https?://[^/]+(/.*)$')  as page_path,
        page_title,
        engagement_time_msec
    from {{ ref('stg_ga4__events') }}
    where event_name = 'page_view'
      and page_location is not null

),

session_page_counts as (

    select
        user_pseudo_id,
        ga_session_id,
        count(*) as pages_in_session
    from page_views
    group by user_pseudo_id, ga_session_id

),

-- identify exit pages (last page_view per session)
exit_pages as (

    select
        user_pseudo_id,
        ga_session_id,
        page_path,
        row_number() over (
            partition by user_pseudo_id, ga_session_id
            order by event_at desc
        ) as rn
    from page_views

),

page_metrics as (

    select
        pv.event_date,
        pv.page_path,
        pv.page_title,

        -- ── volume ──
        count(*)                                                as page_views,
        count(distinct pv.user_pseudo_id)                       as unique_users,

        -- ── engagement ──
        avg(pv.engagement_time_msec / 1000.0)                   as avg_time_on_page_sec,
        sum(pv.engagement_time_msec / 1000.0)                   as total_time_on_page_sec,

        -- ── bounce rate (single-page sessions / total sessions hitting this page) ──
        safe_divide(
            countif(spc.pages_in_session = 1),
            count(distinct concat(pv.user_pseudo_id, '-', cast(pv.ga_session_id as string)))
        )                                                       as bounce_rate,

        -- ── exit rate ──
        safe_divide(
            countif(ep.rn = 1),
            count(*)
        )                                                       as exit_rate

    from page_views pv
    left join session_page_counts spc
        on pv.user_pseudo_id = spc.user_pseudo_id
        and pv.ga_session_id = spc.ga_session_id
    left join exit_pages ep
        on pv.user_pseudo_id = ep.user_pseudo_id
        and pv.ga_session_id = ep.ga_session_id
        and pv.page_path = ep.page_path
    group by pv.event_date, pv.page_path, pv.page_title

)

select * from page_metrics
