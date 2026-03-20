"""
SaaS GA4 Analytics Dashboard
=============================
Streamlit app with 5 tabs:
  1. Traffic + Trials       — sessions, trial starts, conversion by channel
  2. Trial Cohort Conversion — weekly cohort grid
  3. Feature Adoption        — which features predict conversion
  4. PLG Pipeline            — PQL list with scores
  5. Page Performance        — top pages by trial assists, bounce rate

Fallback mode: when BigQuery is unavailable, generates demo data locally.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# allow importing from src/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

# ──────────────────────────────────────────────────────────
# Page config
# ──────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SaaS GA4 Analytics",
    page_icon="📊",
    layout="wide",
)

# ──────────────────────────────────────────────────────────
# Data loaders — BigQuery with demo fallback
# ──────────────────────────────────────────────────────────
USE_BQ = os.environ.get("USE_BIGQUERY", "false").lower() == "true"


@st.cache_data(ttl=3600)
def load_from_bq(query: str) -> pd.DataFrame:
    from google.cloud import bigquery

    client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
    return client.query(query).to_dataframe()


def _demo_web_kpis() -> pd.DataFrame:
    """Generate 90 days of demo web KPI data."""
    np.random.seed(42)
    dates = pd.date_range(end=pd.Timestamp.today(), periods=90, freq="D")
    channels = ["Paid Search", "Organic", "Direct", "Social", "Referral", "Email"]
    rows = []
    for d in dates:
        for ch in channels:
            base = {"Paid Search": 120, "Organic": 200, "Direct": 80, "Social": 60, "Referral": 40, "Email": 30}[ch]
            sessions = int(base + np.random.normal(0, base * 0.2))
            trials = max(0, int(sessions * np.random.uniform(0.02, 0.08)))
            conversions = max(0, int(trials * np.random.uniform(0.05, 0.25)))
            rows.append({
                "date_day": d.date(),
                "channel_group": ch,
                "geo_country": "United States",
                "device_category": "desktop",
                "sessions": max(1, sessions),
                "unique_users": max(1, int(sessions * 0.75)),
                "page_views": int(sessions * 2.3),
                "trial_starts": trials,
                "paid_conversions": conversions,
                "bounce_rate": round(np.random.uniform(0.3, 0.6), 3),
                "trial_start_rate": round(trials / max(sessions, 1), 4),
            })
    return pd.DataFrame(rows)


def _demo_trial_cohorts() -> pd.DataFrame:
    np.random.seed(42)
    weeks = pd.date_range(end=pd.Timestamp.today(), periods=13, freq="W-MON")
    rows = []
    for w in weeks:
        size = int(np.random.uniform(80, 200))
        r7 = round(np.random.uniform(0.05, 0.15), 3)
        r14 = round(r7 + np.random.uniform(0.03, 0.10), 3)
        r30 = round(r14 + np.random.uniform(0.03, 0.12), 3)
        rows.append({
            "cohort_week": w.date(),
            "cohort_size": size,
            "activated_users": int(size * np.random.uniform(0.4, 0.7)),
            "converted_7d": int(size * r7),
            "converted_14d": int(size * r14),
            "converted_30d": int(size * r30),
            "conversion_rate_7d": r7,
            "conversion_rate_14d": r14,
            "conversion_rate_30d": r30,
            "activation_rate": round(np.random.uniform(0.4, 0.7), 3),
            "avg_days_to_convert": round(np.random.uniform(5, 18), 1),
        })
    return pd.DataFrame(rows)


def _demo_feature_funnel() -> pd.DataFrame:
    np.random.seed(42)
    features = [
        "dashboard", "reports", "automation", "integrations", "api",
        "team_management", "custom_fields", "workflows", "export", "notifications",
    ]
    rows = []
    for f in features:
        users = int(np.random.uniform(200, 2000))
        cr = round(np.random.uniform(0.05, 0.35), 3)
        rows.append({
            "feature_name": f,
            "total_users": users,
            "total_events": int(users * np.random.uniform(3, 15)),
            "avg_usage_per_user": round(np.random.uniform(3, 15), 1),
            "converted_users": int(users * cr),
            "conversion_rate": cr,
            "conversion_rank": 0,
            "popularity_rank": 0,
            "avg_days_to_convert": round(np.random.uniform(5, 20), 1),
        })
    df = pd.DataFrame(rows)
    df["conversion_rank"] = df["conversion_rate"].rank(ascending=False).astype(int)
    df["popularity_rank"] = df["total_users"].rank(ascending=False).astype(int)
    return df


def _demo_plg_pipeline() -> pd.DataFrame:
    np.random.seed(42)
    rows = []
    for i in range(150):
        api = np.random.randint(0, 4)
        integ = np.random.randint(0, 5)
        inv = np.random.randint(0, 6)
        feat = np.random.randint(0, 11)
        sess = np.random.randint(0, 16)
        score = round(api * 3 + integ * 2 + inv * 2 + feat * 1 + sess * 0.5, 2)
        tier = "hot" if score > 15 else "warm" if score > 8 else "nurture" if score > 4 else "cold"
        action = (
            "fast_track_to_sales" if score > 15
            else "schedule_demo" if score > 8
            else "send_upgrade_email" if feat >= 3
            else "nurture"
        )
        rows.append({
            "user_pseudo_id": f"user_{i:04d}",
            "user_id": f"uid_{i}" if np.random.random() < 0.5 else None,
            "pql_score": score,
            "is_pql": score > 8,
            "lead_tier": tier,
            "recommended_action": action,
            "api_keys_created": api,
            "integrations_connected": integ,
            "invites_sent": inv,
            "features_used_count": feat,
            "sessions_last_7d": sess,
            "top_signal": (
                "power_user" if api >= 1 and integ >= 1
                else "viral_champion" if inv >= 2
                else "deep_adopter" if feat >= 5
                else "highly_engaged" if sess >= 5
                else "early_stage"
            ),
        })
    return pd.DataFrame(rows)


def _demo_pages() -> pd.DataFrame:
    np.random.seed(42)
    pages = [
        ("/", "homepage"), ("/pricing", "pricing"), ("/features", "features"),
        ("/blog", "blog"), ("/blog/product-updates", "blog"),
        ("/docs", "documentation"), ("/docs/api", "documentation"),
        ("/integrations", "integrations"), ("/signup", "signup"),
        ("/dashboard", "product"), ("/settings", "product"),
    ]
    rows = []
    for path, ctype in pages:
        pvs = int(np.random.uniform(500, 15000))
        rows.append({
            "page_path": path,
            "content_type": ctype,
            "total_page_views": pvs,
            "total_unique_users": int(pvs * 0.65),
            "avg_time_on_page_sec": round(np.random.uniform(15, 120), 1),
            "avg_bounce_rate": round(np.random.uniform(0.2, 0.7), 3),
            "avg_exit_rate": round(np.random.uniform(0.1, 0.5), 3),
        })
    return pd.DataFrame(rows)


# ──────────────────────────────────────────────────────────
# Load data
# ──────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_web_kpis():
    if USE_BQ:
        return load_from_bq("select * from `fct_saas_web_kpis`")
    return _demo_web_kpis()


@st.cache_data(ttl=3600)
def load_trial_cohorts():
    if USE_BQ:
        return load_from_bq("select * from `fct_trial_cohorts`")
    return _demo_trial_cohorts()


@st.cache_data(ttl=3600)
def load_feature_funnel():
    if USE_BQ:
        return load_from_bq("select * from `fct_feature_funnel`")
    return _demo_feature_funnel()


@st.cache_data(ttl=3600)
def load_plg_pipeline():
    if USE_BQ:
        return load_from_bq("select * from `fct_plg_pipeline`")
    return _demo_plg_pipeline()


@st.cache_data(ttl=3600)
def load_pages():
    if USE_BQ:
        return load_from_bq("select * from `dim_pages`")
    return _demo_pages()


# ──────────────────────────────────────────────────────────
# Header
# ──────────────────────────────────────────────────────────
st.title("📊 SaaS GA4 Analytics Dashboard")
st.caption("GA4 → BigQuery → dbt → Dashboard  |  Product-Led Growth Analytics")

mode_label = "🟢 BigQuery" if USE_BQ else "🟡 Demo Data"
st.sidebar.markdown(f"**Data source:** {mode_label}")

# ──────────────────────────────────────────────────────────
# Tabs
# ──────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🚦 Traffic + Trials",
    "📈 Trial Cohorts",
    "🧩 Feature Adoption",
    "🎯 PLG Pipeline",
    "📄 Page Performance",
])

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 1 — Traffic + Trials
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab1:
    df_kpi = load_web_kpis()
    df_kpi["date_day"] = pd.to_datetime(df_kpi["date_day"])

    # KPI cards
    latest = df_kpi[df_kpi["date_day"] == df_kpi["date_day"].max()]
    total_sessions = latest["sessions"].sum()
    total_trials = latest["trial_starts"].sum()
    total_conv = latest["paid_conversions"].sum()
    conv_rate = total_conv / max(total_trials, 1)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Sessions (latest day)", f"{total_sessions:,}")
    c2.metric("Trial Starts", f"{total_trials:,}")
    c3.metric("Paid Conversions", f"{total_conv:,}")
    c4.metric("Trial→Paid Rate", f"{conv_rate:.1%}")

    # Daily sessions by channel
    daily = df_kpi.groupby(["date_day", "channel_group"], as_index=False).agg(
        sessions=("sessions", "sum"),
        trial_starts=("trial_starts", "sum"),
    )

    fig = px.area(
        daily, x="date_day", y="sessions", color="channel_group",
        title="Daily Sessions by Channel",
        labels={"date_day": "Date", "sessions": "Sessions"},
    )
    st.plotly_chart(fig, use_container_width=True)

    # Trial starts by channel
    fig2 = px.bar(
        daily.groupby("channel_group", as_index=False)["trial_starts"].sum()
            .sort_values("trial_starts", ascending=True),
        x="trial_starts", y="channel_group", orientation="h",
        title="Trial Starts by Channel (total)",
        color="channel_group",
    )
    st.plotly_chart(fig2, use_container_width=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 2 — Trial Cohort Conversion
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab2:
    df_cohort = load_trial_cohorts()
    df_cohort["cohort_week"] = pd.to_datetime(df_cohort["cohort_week"])

    st.subheader("Weekly Trial Cohort Conversion Rates")

    # Cohort heatmap
    heatmap_data = df_cohort[["cohort_week", "conversion_rate_7d", "conversion_rate_14d", "conversion_rate_30d"]].copy()
    heatmap_data["cohort_week"] = heatmap_data["cohort_week"].dt.strftime("%Y-%m-%d")
    heatmap_data = heatmap_data.set_index("cohort_week")
    heatmap_data.columns = ["7-day", "14-day", "30-day"]

    fig = go.Figure(data=go.Heatmap(
        z=heatmap_data.values * 100,
        x=heatmap_data.columns,
        y=heatmap_data.index,
        colorscale="YlOrRd",
        text=[[f"{v:.1f}%" for v in row] for row in heatmap_data.values * 100],
        texttemplate="%{text}",
        hovertemplate="Cohort: %{y}<br>Window: %{x}<br>Rate: %{z:.1f}%<extra></extra>",
    ))
    fig.update_layout(title="Conversion Rate by Cohort & Window", height=450)
    st.plotly_chart(fig, use_container_width=True)

    # Cohort size + activation
    col1, col2 = st.columns(2)
    with col1:
        fig3 = px.bar(
            df_cohort, x="cohort_week", y="cohort_size",
            title="Cohort Size Over Time",
        )
        st.plotly_chart(fig3, use_container_width=True)
    with col2:
        fig4 = px.line(
            df_cohort, x="cohort_week", y="activation_rate",
            title="Activation Rate (used a feature)",
            markers=True,
        )
        fig4.update_yaxes(tickformat=".0%")
        st.plotly_chart(fig4, use_container_width=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 3 — Feature Adoption
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab3:
    df_feat = load_feature_funnel()

    st.subheader("Features by Conversion Impact")

    fig = px.bar(
        df_feat.sort_values("conversion_rate", ascending=True),
        x="conversion_rate", y="feature_name", orientation="h",
        color="conversion_rate",
        color_continuous_scale="Greens",
        title="Feature Conversion Rate (trial→paid among feature users)",
        labels={"conversion_rate": "Conversion Rate", "feature_name": "Feature"},
    )
    fig.update_xaxes(tickformat=".0%")
    st.plotly_chart(fig, use_container_width=True)

    # Scatter: popularity vs conversion
    fig2 = px.scatter(
        df_feat, x="total_users", y="conversion_rate",
        size="total_events", text="feature_name",
        title="Feature Popularity vs. Conversion Impact",
        labels={"total_users": "Unique Users", "conversion_rate": "Conversion Rate"},
    )
    fig2.update_traces(textposition="top center")
    fig2.update_yaxes(tickformat=".0%")
    st.plotly_chart(fig2, use_container_width=True)

    st.dataframe(
        df_feat[["feature_name", "total_users", "conversion_rate", "avg_usage_per_user", "avg_days_to_convert"]]
        .sort_values("conversion_rate", ascending=False),
        use_container_width=True,
        hide_index=True,
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 4 — PLG Pipeline
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab4:
    df_plg = load_plg_pipeline()

    st.subheader("Product-Qualified Leads (PQL) Pipeline")

    pql_count = df_plg["is_pql"].sum()
    total_users = len(df_plg)

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Users Scored", f"{total_users:,}")
    c2.metric("PQLs (score > 8)", f"{pql_count:,}")
    c3.metric("PQL Rate", f"{pql_count / max(total_users, 1):.1%}")

    # Tier distribution
    tier_counts = df_plg["lead_tier"].value_counts().reset_index()
    tier_counts.columns = ["tier", "count"]
    tier_order = ["hot", "warm", "nurture", "cold"]
    tier_colors = {"hot": "#ef4444", "warm": "#f59e0b", "nurture": "#3b82f6", "cold": "#94a3b8"}

    fig = px.bar(
        tier_counts.set_index("tier").reindex(tier_order).reset_index(),
        x="tier", y="count", color="tier",
        color_discrete_map=tier_colors,
        title="Lead Tier Distribution",
    )
    st.plotly_chart(fig, use_container_width=True)

    # PQL table
    st.subheader("Top PQLs — Ready for Sales Outreach")
    pql_display = (
        df_plg[df_plg["is_pql"]]
        .sort_values("pql_score", ascending=False)
        .head(25)
        [[
            "user_pseudo_id", "user_id", "pql_score", "lead_tier",
            "recommended_action", "top_signal",
            "api_keys_created", "integrations_connected", "invites_sent",
            "features_used_count", "sessions_last_7d",
        ]]
    )
    st.dataframe(pql_display, use_container_width=True, hide_index=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 5 — Page Performance
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab5:
    df_pages = load_pages()

    st.subheader("Page Performance")

    fig = px.bar(
        df_pages.sort_values("total_page_views", ascending=True),
        x="total_page_views", y="page_path", orientation="h",
        color="content_type",
        title="Page Views by Page",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Bounce rate scatter
    fig2 = px.scatter(
        df_pages, x="total_page_views", y="avg_bounce_rate",
        size="total_unique_users", color="content_type",
        text="page_path",
        title="Page Views vs Bounce Rate",
        labels={"total_page_views": "Page Views", "avg_bounce_rate": "Bounce Rate"},
    )
    fig2.update_traces(textposition="top center")
    fig2.update_yaxes(tickformat=".0%")
    st.plotly_chart(fig2, use_container_width=True)

    st.dataframe(
        df_pages.sort_values("total_page_views", ascending=False),
        use_container_width=True,
        hide_index=True,
    )
