{{
    config(
        materialized='table',
        cluster_by=["content_type"]
    )
}}

/*
    dim_pages
    ─────────
    Page dimension — performance + content type classification.
*/

with web_perf as (

    select * from {{ ref('int_web_performance') }}

),

-- classify pages into content types
page_classification as (

    select
        page_path,
        case
            when page_path like '/blog%'         then 'blog'
            when page_path like '/docs%'         then 'documentation'
            when page_path = '/pricing'          then 'pricing'
            when page_path = '/signup'           then 'signup'
            when page_path = '/login'            then 'login'
            when page_path like '/integrations%' then 'integrations'
            when page_path = '/'                 then 'homepage'
            when page_path like '/dashboard%'    then 'product'
            when page_path like '/settings%'     then 'product'
            when page_path like '/team%'         then 'product'
            when page_path like '/billing%'      then 'product'
            when page_path like '/features%'     then 'features'
            else 'other'
        end                                                     as content_type
    from (select distinct page_path from web_perf)

),

page_summary as (

    select
        wp.page_path,
        pc.content_type,
        max(wp.page_title)                                      as page_title,

        -- ── aggregated metrics ──
        sum(wp.page_views)                                      as total_page_views,
        sum(wp.unique_users)                                    as total_unique_users,
        avg(wp.avg_time_on_page_sec)                            as avg_time_on_page_sec,
        avg(wp.bounce_rate)                                     as avg_bounce_rate,
        avg(wp.exit_rate)                                       as avg_exit_rate,

        -- ── date range ──
        min(wp.event_date)                                      as first_seen_date,
        max(wp.event_date)                                      as last_seen_date,
        count(distinct wp.event_date)                           as active_days

    from web_perf wp
    left join page_classification pc
        on wp.page_path = pc.page_path
    group by wp.page_path, pc.content_type

)

select
    *,
    rank() over (order by total_page_views desc)                as popularity_rank
from page_summary
order by total_page_views desc
