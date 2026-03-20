"""
Tests for the GA4 simulator and PQL scoring module.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest

# ── make project importable ──
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "data"))
sys.path.insert(0, str(ROOT / "src"))


# ══════════════════════════════════════════════════════════
# GA4 Simulator tests
# ══════════════════════════════════════════════════════════
class TestGA4Simulator:
    """Tests for data/simulate_ga4.py"""

    def test_import(self):
        import simulate_ga4
        assert hasattr(simulate_ga4, "generate")
        assert hasattr(simulate_ga4, "build_user_pool")
        assert hasattr(simulate_ga4, "build_event")

    def test_build_user_pool_count(self):
        from simulate_ga4 import build_user_pool
        users = build_user_pool()
        assert len(users) == 8_000

    def test_user_pool_structure(self):
        from simulate_ga4 import build_user_pool
        users = build_user_pool()
        user = users[0]
        assert "user_pseudo_id" in user
        assert "user_id" in user
        assert "device" in user
        assert "geo" in user
        assert "traffic_source" in user
        assert "plan_type" in user
        assert len(user["user_pseudo_id"]) == 32

    def test_build_event_structure(self):
        from simulate_ga4 import build_event, build_user_pool
        users = build_user_pool()
        event = build_event("20250101", users[0], 1_704_067_200_000_000)

        # GA4 required fields
        assert event["event_date"] == "20250101"
        assert isinstance(event["event_timestamp"], int)
        assert isinstance(event["event_name"], str)
        assert isinstance(event["event_params"], list)
        assert isinstance(event["user_pseudo_id"], str)
        assert "device" in event
        assert "geo" in event
        assert "traffic_source" in event
        assert event["platform"] == "WEB"

    def test_event_params_format(self):
        from simulate_ga4 import build_event, build_user_pool
        users = build_user_pool()
        event = build_event("20250101", users[0], 1_704_067_200_000_000)

        for param in event["event_params"]:
            assert "key" in param
            assert "value" in param
            assert isinstance(param["key"], str)

    def test_event_names_valid(self):
        from simulate_ga4 import EVENT_WEIGHTS, build_event, build_user_pool
        import random

        random.seed(123)
        users = build_user_pool()
        valid_names = set(EVENT_WEIGHTS.keys())
        for _ in range(500):
            event = build_event("20250101", users[0], 1_704_067_200_000_000)
            assert event["event_name"] in valid_names

    def test_generate_writes_files(self):
        from simulate_ga4 import generate

        with tempfile.TemporaryDirectory() as tmpdir:
            files = generate(output_dir=tmpdir, days=2, events_per_day=50)
            assert len(files) == 2
            for f in files:
                assert os.path.exists(f)
                with open(f, "r") as fh:
                    lines = fh.readlines()
                assert len(lines) == 50

    def test_generate_valid_ndjson(self):
        from simulate_ga4 import generate

        with tempfile.TemporaryDirectory() as tmpdir:
            files = generate(output_dir=tmpdir, days=1, events_per_day=20)
            with open(files[0], "r") as fh:
                for line in fh:
                    row = json.loads(line)
                    assert "event_date" in row
                    assert "event_name" in row

    def test_generate_single_file_mode(self):
        from simulate_ga4 import generate

        with tempfile.TemporaryDirectory() as tmpdir:
            files = generate(output_dir=tmpdir, days=3, events_per_day=10, single_file=True)
            assert len(files) == 1
            with open(files[0], "r") as fh:
                lines = fh.readlines()
            assert len(lines) == 30  # 3 days × 10 events

    def test_device_structure(self):
        from simulate_ga4 import build_user_pool
        users = build_user_pool()
        device = users[0]["device"]
        assert "category" in device
        assert "operating_system" in device
        assert "browser" in device

    def test_geo_structure(self):
        from simulate_ga4 import build_user_pool
        users = build_user_pool()
        geo = users[0]["geo"]
        assert "country" in geo
        assert "region" in geo
        assert "city" in geo

    def test_user_properties_in_event(self):
        from simulate_ga4 import build_event, build_user_pool
        users = build_user_pool()
        event = build_event("20250101", users[0], 1_704_067_200_000_000)
        props = event["user_properties"]
        keys = {p["key"] for p in props}
        assert "plan_type" in keys
        assert "account_id" in keys
        assert "user_role" in keys


# ══════════════════════════════════════════════════════════
# PQL Scoring tests
# ══════════════════════════════════════════════════════════
class TestPQLScoring:
    """Tests for src/pql_scoring.py"""

    def test_import(self):
        import pql_scoring
        assert hasattr(pql_scoring, "compute_score")
        assert hasattr(pql_scoring, "score_users")

    def test_compute_score_formula(self):
        from pql_scoring import compute_score

        row = {
            "api_keys_created": 1,       # 1 × 3 = 3
            "integrations_connected": 2,  # 2 × 2 = 4
            "invites_sent": 1,            # 1 × 2 = 2
            "features_used_count": 5,     # 5 × 1 = 5
            "sessions_last_7d": 4,        # 4 × 0.5 = 2
        }
        expected = 3 + 4 + 2 + 5 + 2  # = 16.0
        assert compute_score(row) == expected

    def test_compute_score_zeros(self):
        from pql_scoring import compute_score
        assert compute_score({}) == 0.0

    def test_classify_top_signals(self):
        from pql_scoring import classify_top_signals

        row = {
            "api_keys_created": 2,
            "integrations_connected": 0,
            "invites_sent": 3,
            "features_used_count": 1,
            "sessions_last_7d": 10,
        }
        signals = classify_top_signals(row)
        assert isinstance(signals, list)
        assert len(signals) <= 3
        # invites_sent (3×2=6) and api_keys_created (2×3=6) should be top
        assert "invites_sent" in signals or "api_keys_created" in signals

    def test_recommend_action_hot(self):
        from pql_scoring import recommend_action
        assert recommend_action(20, {}) == "fast_track_to_sales"

    def test_recommend_action_warm(self):
        from pql_scoring import recommend_action
        assert recommend_action(10, {}) == "schedule_demo"

    def test_recommend_action_nurture(self):
        from pql_scoring import recommend_action
        assert recommend_action(3, {"features_used_count": 1}) == "nurture"

    def test_recommend_action_upgrade_email(self):
        from pql_scoring import recommend_action
        assert recommend_action(5, {"features_used_count": 4}) == "send_upgrade_email"

    def test_lead_tier(self):
        from pql_scoring import lead_tier
        assert lead_tier(20) == "hot"
        assert lead_tier(10) == "warm"
        assert lead_tier(6) == "nurture"
        assert lead_tier(2) == "cold"

    def test_score_users_dataframe(self):
        from pql_scoring import score_users

        df = pd.DataFrame([
            {
                "user_pseudo_id": "u1",
                "user_id": "uid1",
                "api_keys_created": 1,
                "integrations_connected": 1,
                "invites_sent": 1,
                "features_used_count": 3,
                "sessions_last_7d": 6,
            },
            {
                "user_pseudo_id": "u2",
                "user_id": None,
                "api_keys_created": 0,
                "integrations_connected": 0,
                "invites_sent": 0,
                "features_used_count": 1,
                "sessions_last_7d": 1,
            },
        ])
        scored = score_users(df)
        assert len(scored) == 2
        assert "pql_score" in scored.columns
        assert "is_pql" in scored.columns
        assert "recommended_action" in scored.columns
        assert "lead_tier" in scored.columns

        # u1: 3 + 2 + 2 + 3 + 3 = 13  → warm / PQL
        assert scored.iloc[0]["pql_score"] == 13.0
        assert bool(scored.iloc[0]["is_pql"]) is True

        # u2: 0 + 0 + 0 + 1 + 0.5 = 1.5  → cold
        assert scored.iloc[1]["pql_score"] == 1.5
        assert bool(scored.iloc[1]["is_pql"]) is False

    def test_demo_runs(self):
        from pql_scoring import demo
        result = demo()
        assert len(result) == 200
        assert "pql_score" in result.columns

    def test_pql_threshold_default(self):
        from pql_scoring import PQL_THRESHOLD
        assert PQL_THRESHOLD == 8.0

    def test_score_deterministic(self):
        from pql_scoring import compute_score
        row = {"api_keys_created": 2, "integrations_connected": 3, "invites_sent": 1,
               "features_used_count": 7, "sessions_last_7d": 10}
        s1 = compute_score(row)
        s2 = compute_score(row)
        assert s1 == s2
