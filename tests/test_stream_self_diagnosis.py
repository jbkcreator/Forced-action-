"""Tests for Sprint 4.2 Stream Self-Diagnosis Engine.

Real Postgres, rolled back after each test.
Tables created inline via StreamDiagnostics.__table__.create / PlatformDailyStats already exists.
"""
from __future__ import annotations

import pytest
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal
from sqlalchemy import text

from tests.conftest import fresh_db  # noqa: F401 — fixture


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

_pid_counter = 0


def _seed_property(db, county_id="hillsborough"):
    """Insert a minimal property row. Returns property_id."""
    global _pid_counter
    _pid_counter += 1
    row = db.execute(text("""
        INSERT INTO properties (parcel_id, address, county_id, created_at, updated_at)
        VALUES (:pid, '123 Test St', :cid, NOW(), NOW())
        RETURNING id
    """), {"pid": f"SD42-{_pid_counter}", "cid": county_id}).fetchone()
    return row[0]


def _seed_owner_with_contact(db, property_id, has_phone=True, has_email=False,
                               confidence="high"):
    db.execute(text("""
        INSERT INTO owners (property_id, owner_name, phone_1, email_1,
                            contact_info_confidence)
        VALUES (:pid, 'Test Owner',
                :phone, :email,
                :conf)
        ON CONFLICT (property_id) DO UPDATE
            SET phone_1=EXCLUDED.phone_1, email_1=EXCLUDED.email_1,
                contact_info_confidence=EXCLUDED.contact_info_confidence
    """), {
        "pid": property_id,
        "phone": "+18135550001" if has_phone else None,
        "email": "t@t.com" if has_email else None,
        "conf": confidence,
    })


def _seed_distress_score(db, property_id, score_date=None):
    if score_date is None:
        score_date = date.today()
    db.execute(text("""
        INSERT INTO distress_scores
            (property_id, final_cds_score, vertical_scores, score_date)
        VALUES (:pid, 75.0, '{}', :sd)
        ON CONFLICT DO NOTHING
    """), {"pid": property_id, "sd": score_date})


# ─────────────────────────────────────────────────────────────────────────────
# Tracer bullet: compute_enrichment_rate returns correct ratio
# ─────────────────────────────────────────────────────────────────────────────

_TEST_COUNTY = "test_sd42"  # unique slug — no real data in shared DB


def test_compute_enrichment_rate_correct_ratio(fresh_db):
    """2 properties scored in last 30d, 1 enriched → rate = 0.5."""
    from src.services.stream_metrics import compute_enrichment_rate

    pid1 = _seed_property(fresh_db, county_id=_TEST_COUNTY)
    pid2 = _seed_property(fresh_db, county_id=_TEST_COUNTY)

    _seed_owner_with_contact(fresh_db, pid1, has_phone=True, confidence="high")
    _seed_owner_with_contact(fresh_db, pid2, has_phone=False, confidence="low")

    _seed_distress_score(fresh_db, pid1)
    _seed_distress_score(fresh_db, pid2)

    fresh_db.flush()

    rate = compute_enrichment_rate(fresh_db, _TEST_COUNTY)
    assert rate is not None
    assert 0.0 <= rate <= 1.0
    assert abs(rate - 0.5) < 0.01


def test_compute_enrichment_rate_none_on_empty(fresh_db):
    """No scored properties → returns None (no denominator)."""
    from src.services.stream_metrics import compute_enrichment_rate

    rate = compute_enrichment_rate(fresh_db, "county_with_no_data_xyz")
    assert rate is None


# ─────────────────────────────────────────────────────────────────────────────
# compute_sms_delivery_rate
# ─────────────────────────────────────────────────────────────────────────────

def test_compute_sms_delivery_rate_correct_ratio(fresh_db):
    """3 SMS sent, 2 delivered → rate = 0.667."""
    from src.services.stream_metrics import compute_sms_delivery_rate

    sub_row = fresh_db.execute(text("""
        INSERT INTO subscribers (stripe_customer_id, tier, vertical, county_id,
                                 founding_member, status, has_saved_card,
                                 auto_mode_enabled, created_at, updated_at)
        VALUES ('cus_smstest42', 'free', 'roofing', :cid,
                false, 'active', false, false, NOW(), NOW())
        RETURNING id
    """), {"cid": _TEST_COUNTY}).fetchone()
    sub_id = sub_row[0]

    now = datetime.now(timezone.utc)
    for i in range(3):
        delivered = now if i < 2 else None
        fresh_db.execute(text("""
            INSERT INTO message_outcomes
                (subscriber_id, message_type, county_id, sent_at, delivered_at,
                 conversion_within_4h, conversion_within_24h, conversion_within_48h,
                 created_at)
            VALUES (:sid, 'sms', :cid, :sent, :delivered,
                    false, false, false, NOW())
        """), {"sid": sub_id, "cid": _TEST_COUNTY, "sent": now, "delivered": delivered})

    fresh_db.flush()

    rate = compute_sms_delivery_rate(fresh_db, _TEST_COUNTY)
    assert rate is not None
    assert abs(rate - 2 / 3) < 0.01


def test_compute_sms_delivery_rate_none_on_empty(fresh_db):
    from src.services.stream_metrics import compute_sms_delivery_rate
    assert compute_sms_delivery_rate(fresh_db, "county_xyz_no_data") is None


# ─────────────────────────────────────────────────────────────────────────────
# classify_breach rules
# ─────────────────────────────────────────────────────────────────────────────

def test_classify_breach_yellow_at_3_days():
    """3-day streak → severity yellow."""
    from src.services.stream_diagnosis import classify_breach

    result = classify_breach("enrichment_rate", [0.60, 0.62, 0.58], target=0.80, days_below=3)
    assert result["severity"] == "yellow"
    assert result["category"]
    assert isinstance(result["recommendations"], list)


def test_classify_breach_red_after_7_days():
    """7-day streak → severity red."""
    from src.services.stream_diagnosis import classify_breach

    result = classify_breach("enrichment_rate", [0.60] * 7, target=0.80, days_below=7)
    assert result["severity"] == "red"


def test_classify_breach_red_when_below_half_target():
    """Value < 50% of target → red regardless of days."""
    from src.services.stream_diagnosis import classify_breach

    result = classify_breach("enrichment_rate", [0.30, 0.28, 0.32], target=0.80, days_below=3)
    assert result["severity"] == "red"


# ─────────────────────────────────────────────────────────────────────────────
# polish_summary falls back to template on LLM error
# ─────────────────────────────────────────────────────────────────────────────

def test_polish_summary_never_raises_on_llm_failure():
    """LLM wrapper throws → template fallback, no exception."""
    from unittest.mock import patch
    from src.services.stream_diagnosis import polish_summary

    structured = {
        "metric_name": "enrichment_rate",
        "severity": "yellow",
        "category": "skip_trace_degraded",
        "observed_value": 0.62,
        "target_value": 0.80,
        "days_below": 3,
        "recommendations": ["Check Tracerfy quota"],
    }
    with patch("src.services.stream_diagnosis.call_claude_with_usage", side_effect=RuntimeError("LLM down")):
        summary = polish_summary(structured)

    assert isinstance(summary, str)
    assert len(summary) > 0


# ─────────────────────────────────────────────────────────────────────────────
# Streak lifecycle (open → update → resolve)
# ─────────────────────────────────────────────────────────────────────────────

def test_two_day_dip_no_diagnostic(fresh_db):
    """2 consecutive days below target → no stream_diagnostics row opened."""
    from src.services.stream_diagnosis import run_metric_lifecycle

    _ensure_stream_diagnostics_table(fresh_db)

    today = date.today()
    _upsert_stats(fresh_db, today - timedelta(days=1), "hillsborough", enrichment_rate=0.60)
    _upsert_stats(fresh_db, today, "hillsborough", enrichment_rate=0.62)
    fresh_db.flush()

    run_metric_lifecycle(fresh_db, "hillsborough", "enrichment_rate", today)

    count = fresh_db.execute(text(
        "SELECT count(*) FROM stream_diagnostics WHERE county_id='hillsborough' AND metric_name='enrichment_rate'"
    )).scalar()
    assert count == 0


def test_three_day_streak_opens_episode(fresh_db):
    """3-day streak → one open episode inserted."""
    from src.services.stream_diagnosis import run_metric_lifecycle

    _ensure_stream_diagnostics_table(fresh_db)

    today = date.today()
    for d in range(3):
        _upsert_stats(fresh_db, today - timedelta(days=2 - d), "hillsborough", enrichment_rate=0.60)
    fresh_db.flush()

    run_metric_lifecycle(fresh_db, "hillsborough", "enrichment_rate", today)

    row = fresh_db.execute(text("""
        SELECT severity, days_below, resolved_on
        FROM stream_diagnostics
        WHERE county_id='hillsborough' AND metric_name='enrichment_rate'
          AND resolved_on IS NULL
    """)).fetchone()
    assert row is not None
    assert row[0] == "yellow"
    assert row[1] == 3
    assert row[2] is None


def test_recovery_closes_episode(fresh_db):
    """Metric recovers → resolved_on set on existing open episode."""
    from src.services.stream_diagnosis import run_metric_lifecycle

    _ensure_stream_diagnostics_table(fresh_db)

    today = date.today()
    # Seed open episode from 3 days ago
    fresh_db.execute(text("""
        INSERT INTO stream_diagnostics
            (county_id, stream, metric_name, severity, observed_value, target_value,
             days_below, category, recommendations, detected_on)
        VALUES ('hillsborough','lead_pipeline','enrichment_rate','yellow',
                0.60, 0.80, 4, 'default', '[]', :det)
    """), {"det": today - timedelta(days=3)})
    # Today's stats: recovered
    _upsert_stats(fresh_db, today, "hillsborough", enrichment_rate=0.85)
    fresh_db.flush()

    run_metric_lifecycle(fresh_db, "hillsborough", "enrichment_rate", today)

    resolved = fresh_db.execute(text("""
        SELECT resolved_on FROM stream_diagnostics
        WHERE county_id='hillsborough' AND metric_name='enrichment_rate'
    """)).scalar()
    assert resolved == today


def test_idempotent_same_day(fresh_db):
    """Running lifecycle twice same day → still one open episode."""
    from src.services.stream_diagnosis import run_metric_lifecycle

    _ensure_stream_diagnostics_table(fresh_db)

    today = date.today()
    for d in range(3):
        _upsert_stats(fresh_db, today - timedelta(days=2 - d), "hillsborough", enrichment_rate=0.60)
    fresh_db.flush()

    run_metric_lifecycle(fresh_db, "hillsborough", "enrichment_rate", today)
    run_metric_lifecycle(fresh_db, "hillsborough", "enrichment_rate", today)

    count = fresh_db.execute(text("""
        SELECT count(*) FROM stream_diagnostics
        WHERE county_id='hillsborough' AND metric_name='enrichment_rate'
    """)).scalar()
    assert count == 1


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers for lifecycle tests
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_stream_diagnostics_table(db):
    from src.core.models import StreamDiagnostics, Base
    StreamDiagnostics.__table__.create(db.bind, checkfirst=True)


def _upsert_stats(db, run_date, county_id, **metric_cols):
    """Upsert a platform_daily_stats row with the given metric columns."""
    set_clause = ", ".join(f"{k}=:{k}" for k in metric_cols)
    col_clause = ", ".join(metric_cols.keys())
    val_clause = ", ".join(f":{k}" for k in metric_cols)
    db.execute(text(f"""
        INSERT INTO platform_daily_stats
            (run_date, county_id,
             signals_scraped, signals_matched, signals_skipped,
             properties_scored, properties_with_signals, score_runs_total,
             leads_new, leads_updated, leads_unchanged, leads_qualified, leads_upgraded,
             tier_ultra_platinum, tier_platinum, tier_gold, tier_silver, tier_bronze,
             {col_clause}, created_at, updated_at)
        VALUES (:run_date, :county_id,
                0,0,0, 0,0,0, 0,0,0,0,0, 0,0,0,0,0,
                {val_clause}, NOW(), NOW())
        ON CONFLICT (run_date, county_id) DO UPDATE SET {set_clause}
    """), {"run_date": run_date, "county_id": county_id, **metric_cols})
