"""
GA4 BigQuery Export Schema Simulator — SaaS / B2B Events
=========================================================
Generates 90 days × ~30 000 events/day in the exact GA4 BigQuery export
schema, enriched with SaaS-specific custom events (trial_started,
feature_used, upgrade_intent, subscription_started, etc.).

Output: newline-delimited JSON files ready for `bq load --source_format=NEWLINE_DELIMITED_JSON`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from faker import Faker

# ──────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────
DAYS = 90
EVENTS_PER_DAY = 30_000

STANDARD_EVENTS = ["page_view", "session_start", "scroll", "click", "file_download"]
SAAS_EVENTS = [
    "trial_started",
    "feature_used",
    "invite_sent",
    "upgrade_intent",
    "subscription_started",
    "api_key_created",
    "integration_connected",
    "in_app_survey_completed",
]

# Weighted distribution — page_view is most common
EVENT_WEIGHTS: dict[str, float] = {
    "page_view": 0.35,
    "session_start": 0.15,
    "scroll": 0.12,
    "click": 0.10,
    "file_download": 0.03,
    "trial_started": 0.04,
    "feature_used": 0.08,
    "invite_sent": 0.02,
    "upgrade_intent": 0.04,
    "subscription_started": 0.02,
    "api_key_created": 0.01,
    "integration_connected": 0.02,
    "in_app_survey_completed": 0.02,
}

FEATURES = [
    "dashboard",
    "reports",
    "automation",
    "integrations",
    "api",
    "team_management",
    "custom_fields",
    "workflows",
    "export",
    "notifications",
]

PLAN_TYPES = ["free", "trial", "starter", "professional", "enterprise"]
USER_ROLES = ["admin", "member", "viewer", "owner"]

DEVICE_CATEGORIES = ["desktop", "mobile", "tablet"]
OPERATING_SYSTEMS = ["Windows", "Macintosh", "Linux", "Android", "iOS"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Samsung Internet"]

CHANNELS: list[tuple[str, str, str | None]] = [
    ("google", "cpc", "brand_q1"),
    ("google", "organic", None),
    ("(direct)", "(none)", None),
    ("linkedin", "social", "saas_launch"),
    ("twitter", "social", None),
    ("facebook", "cpc", "retarget_trial"),
    ("producthunt", "referral", None),
    ("bing", "cpc", "competitor_kw"),
    ("github", "referral", None),
    ("newsletter", "email", "weekly_digest"),
]

PAGES = [
    "/", "/pricing", "/features", "/blog", "/blog/product-updates",
    "/docs", "/docs/api", "/docs/quickstart", "/integrations",
    "/about", "/contact", "/signup", "/login", "/dashboard",
    "/settings", "/team", "/billing",
]

COUNTRIES = [
    ("United States", "California", "San Francisco"),
    ("United States", "New York", "New York"),
    ("United States", "Texas", "Austin"),
    ("United Kingdom", "England", "London"),
    ("Germany", "Bavaria", "Munich"),
    ("Canada", "Ontario", "Toronto"),
    ("India", "Karnataka", "Bangalore"),
    ("Australia", "New South Wales", "Sydney"),
    ("France", "Île-de-France", "Paris"),
    ("Brazil", "São Paulo", "São Paulo"),
]

NPS_SCORES = list(range(0, 11))
INTEGRATION_NAMES = ["slack", "hubspot", "salesforce", "jira", "zapier", "github", "stripe"]

fake = Faker()
Faker.seed(42)
random.seed(42)


# ──────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────
def _pseudo_id() -> str:
    """Generate a GA4-style user_pseudo_id (hash-like)."""
    return hashlib.sha256(fake.uuid4().encode()).hexdigest()[:32]


def _event_param(key: str, value: Any) -> dict:
    """Build a single GA4 event_params STRUCT entry."""
    param: dict[str, Any] = {"key": key, "value": {}}
    if isinstance(value, int):
        param["value"]["int_value"] = value
    elif isinstance(value, float):
        param["value"]["double_value"] = value
    elif isinstance(value, str):
        param["value"]["string_value"] = value
    else:
        param["value"]["string_value"] = str(value)
    return param


def _user_property(key: str, value: Any) -> dict:
    """Build a single GA4 user_properties STRUCT entry."""
    prop: dict[str, Any] = {"key": key, "value": {}}
    if isinstance(value, int):
        prop["value"]["int_value"] = value
        prop["value"]["set_timestamp_micros"] = None
    else:
        prop["value"]["string_value"] = str(value)
        prop["value"]["set_timestamp_micros"] = None
    return prop


# ──────────────────────────────────────────────────────────
# User pool (stable across the 90-day window)
# ──────────────────────────────────────────────────────────
NUM_USERS = 8_000


def build_user_pool() -> list[dict]:
    """Pre-generate a fixed pool of users for consistent cross-day references."""
    users = []
    for _ in range(NUM_USERS):
        pseudo_id = _pseudo_id()
        identified = random.random() < 0.45  # 45 % of users log in
        plan = random.choice(PLAN_TYPES)
        role = random.choice(USER_ROLES)
        account_id = f"acct_{random.randint(1000, 9999)}"
        geo = random.choice(COUNTRIES)
        device_cat = random.choices(DEVICE_CATEGORIES, weights=[0.65, 0.28, 0.07])[0]
        os_ = random.choice(OPERATING_SYSTEMS)
        browser = random.choice(BROWSERS)
        source, medium, campaign = random.choice(CHANNELS)

        users.append(
            {
                "user_pseudo_id": pseudo_id,
                "user_id": fake.user_name() if identified else None,
                "plan_type": plan,
                "user_role": role,
                "account_id": account_id,
                "geo": {"country": geo[0], "region": geo[1], "city": geo[2]},
                "device": {
                    "category": device_cat,
                    "operating_system": os_,
                    "browser": browser,
                    "language": "en-us",
                    "is_limited_ad_tracking": random.choice(["Yes", "No"]),
                    "mobile_brand_name": None,
                    "mobile_model_name": None,
                    "mobile_os_hardware_model": None,
                },
                "traffic_source": {
                    "source": source,
                    "medium": medium,
                    "name": campaign,
                },
            }
        )
    return users


# ──────────────────────────────────────────────────────────
# Event builders
# ──────────────────────────────────────────────────────────
def _base_params(event_name: str) -> list[dict]:
    """Common event params added to every event."""
    params = [
        _event_param("ga_session_id", random.randint(1_000_000, 9_999_999)),
        _event_param("ga_session_number", random.randint(1, 50)),
        _event_param("engagement_time_msec", random.randint(500, 120_000)),
    ]
    if event_name == "page_view":
        page = random.choice(PAGES)
        params.append(_event_param("page_location", f"https://app.example.com{page}"))
        params.append(_event_param("page_title", page.strip("/").replace("/", " – ").title() or "Home"))
        params.append(_event_param("page_referrer", random.choice(["https://google.com", "(not set)", "https://linkedin.com"])))
    return params


def _saas_params(event_name: str, user: dict) -> list[dict]:
    """SaaS-specific event params."""
    params: list[dict] = []
    if event_name == "feature_used":
        params.append(_event_param("feature_name", random.choice(FEATURES)))
        params.append(_event_param("plan_type", user["plan_type"]))
        params.append(_event_param("user_role", user["user_role"]))
    elif event_name == "trial_started":
        params.append(_event_param("trial_plan", random.choice(["starter", "professional"])))
    elif event_name == "invite_sent":
        params.append(_event_param("invitee_email_domain", fake.free_email_domain()))
    elif event_name == "upgrade_intent":
        params.append(_event_param("cta_location", random.choice(["header", "pricing_page", "settings", "in_app_banner"])))
        params.append(_event_param("target_plan", random.choice(["starter", "professional", "enterprise"])))
    elif event_name == "subscription_started":
        plan = random.choice(["starter", "professional", "enterprise"])
        params.append(_event_param("plan_name", plan))
        params.append(_event_param("billing_cycle", random.choice(["monthly", "annual"])))
        params.append(_event_param("mrr_value", random.choice([49, 99, 249, 499])))
    elif event_name == "api_key_created":
        params.append(_event_param("key_type", random.choice(["test", "production"])))
    elif event_name == "integration_connected":
        params.append(_event_param("integration_name", random.choice(INTEGRATION_NAMES)))
    elif event_name == "in_app_survey_completed":
        params.append(_event_param("survey_type", random.choice(["nps", "csat"])))
        params.append(_event_param("score", random.choice(NPS_SCORES)))
    elif event_name == "scroll":
        params.append(_event_param("percent_scrolled", random.choice([25, 50, 75, 90])))
    elif event_name == "click":
        params.append(_event_param("link_url", f"https://app.example.com{random.choice(PAGES)}"))
        params.append(_event_param("link_text", random.choice(["Sign Up", "Learn More", "Get Started", "Book Demo"])))
    elif event_name == "file_download":
        params.append(_event_param("file_name", random.choice(["whitepaper.pdf", "case_study.pdf", "api_docs.zip"])))
        params.append(_event_param("file_extension", random.choice(["pdf", "zip"])))
    return params


def build_event(event_date: str, user: dict, ts_base: int) -> dict:
    """Build a single GA4 BigQuery export row."""
    events = list(EVENT_WEIGHTS.keys())
    weights = list(EVENT_WEIGHTS.values())
    event_name = random.choices(events, weights=weights, k=1)[0]

    # random offset within the day (microseconds)
    offset_us = random.randint(0, 86_400_000_000 - 1)
    event_timestamp = ts_base + offset_us

    params = _base_params(event_name) + _saas_params(event_name, user)

    user_properties = [
        _user_property("plan_type", user["plan_type"]),
        _user_property("account_id", user["account_id"]),
        _user_property("user_role", user["user_role"]),
    ]

    row: dict[str, Any] = {
        "event_date": event_date,
        "event_timestamp": event_timestamp,
        "event_name": event_name,
        "event_params": params,
        "event_previous_timestamp": event_timestamp - random.randint(1_000_000, 60_000_000),
        "event_value_in_usd": None,
        "event_bundle_sequence_id": random.randint(1, 999_999),
        "event_server_timestamp_offset": random.randint(0, 500_000),
        "user_id": user["user_id"],
        "user_pseudo_id": user["user_pseudo_id"],
        "user_properties": user_properties,
        "user_first_touch_timestamp": ts_base - random.randint(0, 86_400_000_000 * 30),
        "device": user["device"],
        "geo": user["geo"],
        "traffic_source": user["traffic_source"],
        "stream_id": "2100000001",
        "platform": "WEB",
    }
    return row


# ──────────────────────────────────────────────────────────
# Main generator
# ──────────────────────────────────────────────────────────
def generate(
    output_dir: str = "data",
    days: int = DAYS,
    events_per_day: int = EVENTS_PER_DAY,
    single_file: bool = False,
) -> list[str]:
    """Write NDJSON files partitioned by date (mirrors GA4 daily shards).

    Returns list of created file paths.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    users = build_user_pool()
    end_date = datetime.now(tz=timezone.utc).date()
    start_date = end_date - timedelta(days=days - 1)

    created_files: list[str] = []
    all_file = None

    if single_file:
        all_path = out / "events_all.ndjson"
        all_file = open(all_path, "w", encoding="utf-8")  # noqa: SIM115
        created_files.append(str(all_path))

    try:
        for day_offset in range(days):
            current_date = start_date + timedelta(days=day_offset)
            event_date = current_date.strftime("%Y%m%d")
            ts_base = int(
                datetime.combine(current_date, datetime.min.time())
                .replace(tzinfo=timezone.utc)
                .timestamp()
                * 1_000_000
            )

            if not single_file:
                file_path = out / f"events_{event_date}.ndjson"
                fh = open(file_path, "w", encoding="utf-8")  # noqa: SIM115
                created_files.append(str(file_path))
            else:
                fh = all_file  # type: ignore[assignment]

            for _ in range(events_per_day):
                user = random.choice(users)
                event = build_event(event_date, user, ts_base)
                fh.write(json.dumps(event, default=str) + "\n")

            if not single_file:
                fh.close()

            pct = (day_offset + 1) / days * 100
            print(f"\r  [{pct:5.1f}%] Generated {event_date} — {events_per_day:,} events", end="", flush=True)
    finally:
        if all_file is not None:
            all_file.close()

    print()  # newline after progress
    total = days * events_per_day
    print(f"✓ Generated {total:,} events across {days} days → {output_dir}/")
    return created_files


# ──────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate GA4 BigQuery export data (SaaS events)")
    parser.add_argument("--output-dir", default="data", help="Output directory (default: data)")
    parser.add_argument("--days", type=int, default=DAYS, help=f"Number of days (default: {DAYS})")
    parser.add_argument(
        "--events-per-day",
        type=int,
        default=EVENTS_PER_DAY,
        help=f"Events per day (default: {EVENTS_PER_DAY:,})",
    )
    parser.add_argument("--single-file", action="store_true", help="Write all events to one file")
    args = parser.parse_args()

    generate(
        output_dir=args.output_dir,
        days=args.days,
        events_per_day=args.events_per_day,
        single_file=args.single_file,
    )


if __name__ == "__main__":
    main()
