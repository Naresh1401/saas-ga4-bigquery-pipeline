"""
Product-Qualified Lead (PQL) Scoring
=====================================
Score = (api_key_created × 3) + (integrations_connected × 2) +
        (invites_sent × 2) + (features_used_count × 1) +
        (sessions_last_7d × 0.5)

PQL threshold: score > 8
Output: user_id, pql_score, top_signals, recommended_sales_action
Feeds CRM (HubSpot / Salesforce) for sales outreach.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import pandas as pd

# ──────────────────────────────────────────────────────────
# Weights & threshold
# ──────────────────────────────────────────────────────────
WEIGHTS = {
    "api_keys_created": 3.0,
    "integrations_connected": 2.0,
    "invites_sent": 2.0,
    "features_used_count": 1.0,
    "sessions_last_7d": 0.5,
}

PQL_THRESHOLD = float(os.environ.get("PQL_THRESHOLD", "8.0"))

SIGNAL_COLUMNS = list(WEIGHTS.keys())


# ──────────────────────────────────────────────────────────
# Scoring logic (pure Python — no BQ dependency)
# ──────────────────────────────────────────────────────────
@dataclass
class PQLResult:
    user_pseudo_id: str
    user_id: str | None
    pql_score: float
    is_pql: bool
    top_signals: list[str]
    recommended_action: str
    lead_tier: str


def compute_score(row: dict) -> float:
    """Compute PQL score from a dict/row of signal values."""
    return round(
        sum(row.get(col, 0) * weight for col, weight in WEIGHTS.items()),
        2,
    )


def classify_top_signals(row: dict) -> list[str]:
    """Return ordered list of the user's strongest signals."""
    scored = [
        (col, row.get(col, 0) * weight)
        for col, weight in WEIGHTS.items()
        if row.get(col, 0) > 0
    ]
    scored.sort(key=lambda x: x[1], reverse=True)
    return [s[0] for s in scored[:3]]


def recommend_action(score: float, signals: dict) -> str:
    """Map PQL score + signals to a recommended sales action."""
    if score > 15:
        return "fast_track_to_sales"
    if score > 8:
        return "schedule_demo"
    if signals.get("features_used_count", 0) >= 3:
        return "send_upgrade_email"
    return "nurture"


def lead_tier(score: float) -> str:
    if score > 15:
        return "hot"
    if score > 8:
        return "warm"
    if score > 4:
        return "nurture"
    return "cold"


def score_users(df: pd.DataFrame) -> pd.DataFrame:
    """Score a DataFrame of PLG signals and return enriched PQL DataFrame.

    Expected columns: user_pseudo_id, user_id, + SIGNAL_COLUMNS
    """
    results: list[dict] = []
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        score = compute_score(row_dict)
        top = classify_top_signals(row_dict)
        action = recommend_action(score, row_dict)
        tier = lead_tier(score)

        results.append(
            {
                "user_pseudo_id": row_dict["user_pseudo_id"],
                "user_id": row_dict.get("user_id"),
                "pql_score": score,
                "is_pql": score > PQL_THRESHOLD,
                "top_signals": top,
                "recommended_action": action,
                "lead_tier": tier,
                # preserve raw signals
                **{col: row_dict.get(col, 0) for col in SIGNAL_COLUMNS},
            }
        )

    return pd.DataFrame(results)


# ──────────────────────────────────────────────────────────
# BigQuery integration
# ──────────────────────────────────────────────────────────
def score_and_write_pqls(
    project_id: str | None = None,
    dataset: str = "ga4_analytics",
) -> pd.DataFrame:
    """Read PLG signals from BQ, score, and write PQLs back.

    Returns the scored DataFrame.
    """
    from google.cloud import bigquery

    project_id = project_id or os.environ.get("GCP_PROJECT_ID")
    client = bigquery.Client(project=project_id)

    query = f"""
        select
            user_pseudo_id,
            user_id,
            api_keys_created,
            integrations_connected,
            invites_sent,
            features_used_count,
            sessions_last_7d
        from `{project_id}.{dataset}.int_plg_signals`
    """
    df = client.query(query).to_dataframe()
    scored = score_users(df)

    # Write back — serialise top_signals list as JSON string for BQ
    write_df = scored.copy()
    write_df["top_signals"] = write_df["top_signals"].apply(
        lambda x: ",".join(x) if x else ""
    )

    table_ref = f"{project_id}.{dataset}.pql_scores"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(write_df, table_ref, job_config=job_config)
    job.result()
    print(f"✓ Wrote {len(write_df):,} PQL scores → {table_ref}")
    print(f"  PQLs (score > {PQL_THRESHOLD}): {scored['is_pql'].sum():,}")

    return scored


# ──────────────────────────────────────────────────────────
# Standalone / demo mode (no BQ)
# ──────────────────────────────────────────────────────────
def demo() -> pd.DataFrame:
    """Generate a demo DataFrame with fake data and score it."""
    import random

    random.seed(42)
    rows = []
    for i in range(200):
        rows.append(
            {
                "user_pseudo_id": f"user_{i:04d}",
                "user_id": f"uid_{i}" if random.random() < 0.5 else None,
                "api_keys_created": random.randint(0, 3),
                "integrations_connected": random.randint(0, 4),
                "invites_sent": random.randint(0, 5),
                "features_used_count": random.randint(0, 10),
                "sessions_last_7d": random.randint(0, 15),
            }
        )
    df = pd.DataFrame(rows)
    scored = score_users(df)
    return scored


if __name__ == "__main__":
    scored = demo()
    print(f"\n{'='*60}")
    print(f"PQL Scoring Demo — {len(scored)} users scored")
    print(f"{'='*60}")
    print(f"PQLs (score > {PQL_THRESHOLD}): {scored['is_pql'].sum()}")
    print(f"\nTier distribution:")
    print(scored["lead_tier"].value_counts().to_string())
    print(f"\nAction distribution:")
    print(scored["recommended_action"].value_counts().to_string())
    print(f"\nTop 10 PQLs:")
    top = scored.nlargest(10, "pql_score")[
        ["user_pseudo_id", "pql_score", "lead_tier", "top_signals", "recommended_action"]
    ]
    print(top.to_string(index=False))
