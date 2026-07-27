"""
fa037 — Revenue Signal Score tests.

Covers the scoring service additions:
  - compute_score_detail (pure function, dict shape, bounds, bands)
  - band_for (threshold edges)
  - update_revenue_signal_score (UPSERT user_segments + audit row)
  - get_revenue_signal_score (safe default for missing rows)
  - reclassify_safe action-aware delegation

Plus regression tests for the 2 new event hooks:
  - _on_subscription_deleted churn path
  - record_opt_out STOP path

All DB I/O is stubbed via fake sessions — pattern mirrors
tests/test_lifecycle_self_healing.py and tests/test_lifecycle_autonomy_report.py.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


# ──────────────────────────────────────────────────────────────────────────
# Fake-session helpers
# ──────────────────────────────────────────────────────────────────────────

class _FakeResult:
    def __init__(self, *, first=None, scalar=None, fetchall=None, rowcount=0):
        self._first = first
        self._scalar = scalar
        self._fetchall = fetchall or []
        self.rowcount = rowcount

    def first(self):
        return self._first

    def scalar(self):
        return self._scalar

    def scalar_one_or_none(self):
        return self._first

    def fetchall(self):
        return self._fetchall


def _ns(**kwargs):
    return SimpleNamespace(**kwargs)


class _FakeSession:
    """Queue-based fake session — pops one canned result per execute()."""
    def __init__(self, results=None):
        if results is None:
            results = []
        if not isinstance(results, list):
            results = [results]
        self._results = list(results)
        self.calls = []
        self._get_responses = {}   # {(Model, pk): row}

    def execute(self, statement, params=None):
        self.calls.append({"sql": str(statement), "params": params})
        if not self._results:
            return _FakeResult()
        return self._results.pop(0)

    def get(self, model, pk):
        return self._get_responses.get((model.__name__, pk))

    def flush(self):
        pass

    def add(self, obj):
        self.calls.append({"add": obj})

    def queue_get(self, model, pk, row):
        """Accept either a class (Subscriber) or a string ("Subscriber")."""
        name = model if isinstance(model, str) else model.__name__
        self._get_responses[(name, pk)] = row


# ──────────────────────────────────────────────────────────────────────────
# Band thresholds (band_for + BANDS table)
# ──────────────────────────────────────────────────────────────────────────

class TestBandFor:

    @pytest.mark.parametrize("score,expected", [
        (0, "low"), (29, "low"),
        (30, "medium"), (59, "medium"),
        (60, "high"), (79, "high"),
        (80, "very_high"), (100, "very_high"),
    ])
    def test_thresholds(self, score, expected):
        """Test #1 — band_for honours the four documented bands at edges."""
        from src.services.revenue_signal import band_for
        assert band_for(score) == expected

    def test_clips_negative(self):
        """Test #2 — negative input clipped to 0 → low."""
        from src.services.revenue_signal import band_for
        assert band_for(-5) == "low"

    def test_clips_above_100(self):
        """Test #3 — overflow input clipped to 100 → very_high."""
        from src.services.revenue_signal import band_for
        assert band_for(250) == "very_high"


# ──────────────────────────────────────────────────────────────────────────
# compute_score_detail — pure function, dict shape, breakdown sums
# ──────────────────────────────────────────────────────────────────────────

class TestComputeScoreDetail:

    def test_safe_default_when_subscriber_missing(self):
        """Test #4 — unknown subscriber → score=0, band=low, zero breakdown."""
        from src.services.revenue_signal import compute_score_detail
        sess = _FakeSession()
        out = compute_score_detail(subscriber_id=9999, db=sess)
        assert out["score"] == 0
        assert out["band"] == "low"
        assert all(v == 0 for v in out["breakdown"].values())
        assert out["reasons"] == []

    def test_returns_dict_shape(self):
        """Test #5 — happy path returns all four top-level keys."""
        from src.services.revenue_signal import compute_score_detail
        sess = _FakeSession()
        sess.queue_get("Subscriber", 1, _ns(
            id=1, updated_at=datetime.now(timezone.utc),
            tier="lite", status="active",
        ))
        # WalletBalance lookup — return None to drive wallet_lock_status=0.
        # _spend_velocity + _lead_interaction_rate each consume one execute.
        sess._results = [
            _FakeResult(first=None),       # SELECT WalletBalance
            _FakeResult(scalar=0),         # _spend_velocity sum
            _FakeResult(scalar=0),         # _lead_interaction_rate count
        ]
        out = compute_score_detail(1, sess)
        assert set(out.keys()) == {"score", "band", "breakdown", "reasons"}
        assert isinstance(out["score"], int)
        assert isinstance(out["breakdown"], dict)
        assert isinstance(out["reasons"], list)

    def test_breakdown_keys_match_weights(self):
        """Test #6 — breakdown has one entry per WEIGHTS component."""
        from src.services.revenue_signal import compute_score_detail, WEIGHTS
        sess = _FakeSession()
        sess.queue_get("Subscriber", 1, _ns(
            id=1, updated_at=datetime.now(timezone.utc),
            tier="lite", status="active",
        ))
        sess._results = [
            _FakeResult(first=None),
            _FakeResult(scalar=0),
            _FakeResult(scalar=0),
        ]
        out = compute_score_detail(1, sess)
        assert set(out["breakdown"].keys()) == set(WEIGHTS.keys())

    def test_score_clipped_to_0_100_floor(self):
        """Test #7 — even with all-zero signals score never goes below 0."""
        from src.services.revenue_signal import compute_score_detail
        sess = _FakeSession()
        sess.queue_get("Subscriber", 1, _ns(
            id=1, updated_at=datetime.now(timezone.utc) - timedelta(days=120),
            tier="lite", status="active",
        ))
        sess._results = [
            _FakeResult(first=None),
            _FakeResult(scalar=0),
            _FakeResult(scalar=0),
        ]
        out = compute_score_detail(1, sess)
        assert 0 <= out["score"] <= 100

    def test_score_clipped_to_0_100_ceiling(self):
        """Test #8 — even with all signals saturating score never exceeds 100."""
        from src.services.revenue_signal import compute_score_detail
        # Simulate: very recent activity + power-tier wallet + heavy spend.
        sess = _FakeSession()
        sess.queue_get("Subscriber", 1, _ns(
            id=1, updated_at=datetime.now(timezone.utc),
            tier="autopilot_pro", status="active",
        ))
        sess._results = [
            _FakeResult(first=_ns(wallet_tier="power")),  # SELECT WalletBalance
            _FakeResult(scalar=10000),                    # _spend_velocity sum
            _FakeResult(scalar=10000),                    # _lead_interaction_rate
        ]
        with patch("src.services.revenue_signal._zip_competition", return_value=1.0):
            out = compute_score_detail(1, sess)
        assert out["score"] <= 100

    def test_breakdown_sum_approximates_score(self):
        """Test #9 — breakdown values sum to the int score (within ±2 rounding)."""
        from src.services.revenue_signal import compute_score_detail
        sess = _FakeSession()
        sess.queue_get("Subscriber", 1, _ns(
            id=1, updated_at=datetime.now(timezone.utc) - timedelta(days=3),
            tier="lite", status="active",
        ))
        sess._results = [
            _FakeResult(first=_ns(wallet_tier="growth")),
            _FakeResult(scalar=5),
            _FakeResult(scalar=3),
        ]
        out = compute_score_detail(1, sess)
        # Components are independent rounded ints, so the sum should be the
        # score exactly (no double-clipping in the happy case).
        assert abs(sum(out["breakdown"].values()) - out["score"]) <= 2

    def test_reasons_populated_when_signals_present(self):
        """Test #10 — reasons list has ≥1 plain-English entry when score > 0."""
        from src.services.revenue_signal import compute_score_detail
        sess = _FakeSession()
        sess.queue_get("Subscriber", 1, _ns(
            id=1, updated_at=datetime.now(timezone.utc),
            tier="lite", status="active",
        ))
        sess._results = [
            _FakeResult(first=_ns(wallet_tier="power")),
            _FakeResult(scalar=20),
            _FakeResult(scalar=10),
        ]
        out = compute_score_detail(1, sess)
        assert len(out["reasons"]) >= 1
        assert all(isinstance(r, str) for r in out["reasons"])
        assert len(out["reasons"]) <= 3


# ──────────────────────────────────────────────────────────────────────────
# compute_score back-compat
# ──────────────────────────────────────────────────────────────────────────

class TestComputeScoreBackCompat:

    def test_returns_int_matching_detail(self):
        """Test #11 — compute_score is an int shim around compute_score_detail."""
        from src.services import revenue_signal

        sess_for_detail = _FakeSession()
        sess_for_detail.queue_get("Subscriber", 1, _ns(
            id=1, updated_at=datetime.now(timezone.utc),
            tier="lite", status="active",
        ))
        sess_for_detail._results = [
            _FakeResult(first=None),
            _FakeResult(scalar=0),
            _FakeResult(scalar=0),
            # compute_score also tries to write — one more SELECT for UserSegment.
            _FakeResult(first=None),
        ]
        score = revenue_signal.compute_score(1, sess_for_detail)
        assert isinstance(score, int)
        assert 0 <= score <= 100


# ──────────────────────────────────────────────────────────────────────────
# get_revenue_signal_score — read-only, safe default
# ──────────────────────────────────────────────────────────────────────────

class TestGetRevenueSignalScore:

    def test_safe_default_when_no_row(self):
        """Test #12 — no UserSegment row → score=0, band=low, history=[]."""
        from src.services.revenue_signal import get_revenue_signal_score
        sess = _FakeSession(_FakeResult(first=None))
        out = get_revenue_signal_score(subscriber_id=42, db=sess)
        assert out["score"] == 0
        assert out["band"] == "low"
        assert out["updated_at"] is None
        assert out["last_action"] is None
        assert out["reasons"]   # at least the fallback "no signals yet"

    def test_returns_stored_values(self):
        """Test #13 — present UserSegment row populates the full dict."""
        from src.services.revenue_signal import get_revenue_signal_score
        now = datetime(2026, 5, 25, 12, 0, tzinfo=timezone.utc)
        stored = {
            "spend_velocity": 20, "engagement_recency": 18,
            "wallet_lock_status": 15, "lead_interaction_rate": 17,
            "zip_competition": 8,
        }
        sess = _FakeSession(_FakeResult(first=_ns(
            revenue_signal_score=78,
            revenue_signal_band="high",
            revenue_signal_breakdown=stored,
            revenue_signal_updated_at=now,
            last_significant_action_at=now,
            revenue_signal_last_action="wallet_txn",
        )))
        out = get_revenue_signal_score(42, sess)
        assert out["score"] == 78
        assert out["band"] == "high"
        assert out["breakdown"] == stored
        assert out["last_action"] == "wallet_txn"
        assert "T" in out["updated_at"]   # ISO format


# ──────────────────────────────────────────────────────────────────────────
# update_revenue_signal_score — canonical write + audit row
# ──────────────────────────────────────────────────────────────────────────

class TestUpdateRevenueSignalScore:

    def test_writes_user_segments_and_audit_row(self):
        """Test #14 — single call hits both INSERT statements (upsert + audit)."""
        from src.services.revenue_signal import (
            update_revenue_signal_score, ACTION_WALLET_TXN,
        )

        sess = _FakeSession()
        sess.queue_get("Subscriber", 1, _ns(
            id=1, updated_at=datetime.now(timezone.utc),
            tier="lite", status="active",
        ))
        sess._results = [
            _FakeResult(first=_ns(revenue_signal_score=50)),  # old score
            # compute_score_detail consumes: WalletBalance, spend, lead-rate
            _FakeResult(first=_ns(wallet_tier="growth")),
            _FakeResult(scalar=10),
            _FakeResult(scalar=5),
            _FakeResult(),   # user_segments UPSERT
            _FakeResult(),   # revenue_signal_score_events INSERT
        ]
        out = update_revenue_signal_score(
            1, action_type=ACTION_WALLET_TXN,
            metadata={"amount": 3, "txn_type": "debit"},
            db=sess,
        )

        # Two new INSERTs landed (UPSERT + audit row).
        sqls = [c["sql"] for c in sess.calls if "sql" in c]
        upserts = [s for s in sqls if "INSERT INTO user_segments" in s]
        audits  = [s for s in sqls if "INSERT INTO revenue_signal_score_events" in s]
        assert len(upserts) == 1
        assert len(audits) == 1
        # Returned dict carries the new shape.
        assert out["last_action"] == ACTION_WALLET_TXN
        assert out["updated_at"] is not None

    def test_audit_row_captures_action_and_metadata(self):
        """Test #15 — params on the audit INSERT include action_type + metadata."""
        from src.services.revenue_signal import (
            update_revenue_signal_score, ACTION_SUBSCRIPTION_DELETED,
        )

        sess = _FakeSession()
        sess.queue_get("Subscriber", 1, _ns(
            id=1, updated_at=datetime.now(timezone.utc),
            tier="lite", status="active",
        ))
        sess._results = [
            _FakeResult(first=_ns(revenue_signal_score=30)),
            _FakeResult(first=None),
            _FakeResult(scalar=0),
            _FakeResult(scalar=0),
            _FakeResult(),
            _FakeResult(),
        ]
        update_revenue_signal_score(
            1, action_type=ACTION_SUBSCRIPTION_DELETED,
            metadata={"churn_tag": "churned_founding"},
            db=sess,
        )
        audit_call = next(
            c for c in sess.calls
            if "sql" in c and "INSERT INTO revenue_signal_score_events" in c["sql"]
        )
        assert audit_call["params"]["action"] == "subscription_deleted"
        assert '"churn_tag"' in audit_call["params"]["meta"]

    def test_null_action_still_writes_audit(self):
        """Test #16 — action_type=None still emits an audit row (label NULL)."""
        from src.services.revenue_signal import update_revenue_signal_score

        sess = _FakeSession()
        sess.queue_get("Subscriber", 1, _ns(
            id=1, updated_at=datetime.now(timezone.utc),
            tier="lite", status="active",
        ))
        sess._results = [
            _FakeResult(first=None),
            _FakeResult(first=None),
            _FakeResult(scalar=0),
            _FakeResult(scalar=0),
            _FakeResult(),
            _FakeResult(),
        ]
        update_revenue_signal_score(1, action_type=None, db=sess)
        audit_call = next(
            c for c in sess.calls
            if "sql" in c and "INSERT INTO revenue_signal_score_events" in c["sql"]
        )
        assert audit_call["params"]["action"] is None

    def test_safe_default_when_subscriber_missing(self):
        """Test #17 — unknown subscriber → returns safe-default, no writes."""
        from src.services.revenue_signal import (
            update_revenue_signal_score, ACTION_WALLET_TXN,
        )
        sess = _FakeSession()
        # No queue_get → sess.get returns None.
        out = update_revenue_signal_score(
            9999, action_type=ACTION_WALLET_TXN, db=sess,
        )
        assert out["score"] == 0
        assert out["band"] == "low"
        # No INSERTs at all.
        for c in sess.calls:
            assert "sql" not in c or "INSERT" not in c["sql"]

    def test_raises_when_db_missing(self):
        """Test #18 — db is required; passing None raises ValueError cleanly."""
        from src.services.revenue_signal import update_revenue_signal_score
        with pytest.raises(ValueError):
            update_revenue_signal_score(1, action_type="x", db=None)


# ──────────────────────────────────────────────────────────────────────────
# reclassify_safe — action-aware delegation
# ──────────────────────────────────────────────────────────────────────────

class TestReclassifySafeActionAware:

    def test_action_kwarg_delegates_to_updater(self):
        """Test #19 — action_type triggers the fa037 write path."""
        from src.services import segmentation_engine

        with patch("src.services.revenue_signal.update_revenue_signal_score") as mock_update, \
             patch("src.services.revenue_signal.recompute") as mock_recompute, \
             patch("src.services.segmentation_engine.classify") as mock_classify:
            segmentation_engine.reclassify_safe(
                7, db=MagicMock(),
                action_type="wallet_txn", metadata={"a": 1},
            )
        mock_update.assert_called_once()
        mock_recompute.assert_not_called()
        mock_classify.assert_called_once()

    def test_no_action_kwarg_uses_pre_fa037_path(self):
        """Test #20 — bare call (no kwarg) still uses recompute (back-compat)."""
        from src.services import segmentation_engine

        with patch("src.services.revenue_signal.update_revenue_signal_score") as mock_update, \
             patch("src.services.revenue_signal.recompute") as mock_recompute, \
             patch("src.services.segmentation_engine.classify") as mock_classify:
            segmentation_engine.reclassify_safe(7, db=MagicMock())
        mock_update.assert_not_called()
        mock_recompute.assert_called_once()
        mock_classify.assert_called_once()

    def test_never_raises(self):
        """Test #21 — exception inside delegate is swallowed."""
        from src.services import segmentation_engine

        with patch("src.services.revenue_signal.update_revenue_signal_score",
                   side_effect=RuntimeError("boom")), \
             patch("src.services.segmentation_engine.classify"):
            # Must not propagate.
            segmentation_engine.reclassify_safe(
                7, db=MagicMock(), action_type="x",
            )


# ──────────────────────────────────────────────────────────────────────────
# New event hooks — churn + opt-out
# ──────────────────────────────────────────────────────────────────────────

class TestChurnHook:

    def test_subscription_deleted_fires_score_update(self):
        """Test #22 — _on_subscription_deleted calls reclassify_safe with
        action_type=ACTION_SUBSCRIPTION_DELETED."""
        from src.services import stripe_webhooks

        fake_sub = _ns(
            id=42, stripe_customer_id="cus_x", status="active", email=None,
            grace_expires_at=None, ghl_stage=None,
            payment_failed_at=None, recovery_day1_sent=False,
            recovery_day3_sent=False, founding_member=False,
        )
        sess = MagicMock()
        # First execute returns subscriber, second returns the territories list.
        sess.execute.return_value.scalar_one_or_none.return_value = fake_sub
        scalars_mock = MagicMock()
        scalars_mock.all.return_value = []
        sess.execute.return_value.scalars.return_value = scalars_mock

        with patch.object(stripe_webhooks, "push_subscriber_to_ghl"), \
             patch("src.services.segmentation_engine.reclassify_safe") as mock_reclass:
            stripe_webhooks._on_subscription_deleted(
                {"customer": "cus_x"}, sess,
            )
        mock_reclass.assert_called_once()
        kwargs = mock_reclass.call_args.kwargs
        assert kwargs["action_type"] == "subscription_deleted"
        assert kwargs["metadata"]["churn_tag"] == "churned_regular"


class TestOptOutHook:

    def test_record_opt_out_fires_score_update_when_subscriber_resolves(self):
        """Test #23 — STOP keyword → score update with ACTION_SMS_OPT_OUT."""
        # phonenumbers is a C extension not installed in the test env.
        # Stub it before the sms_compliance import chain triggers phone_utils.
        sys.modules.setdefault("phonenumbers", MagicMock())
        from src.services import sms_compliance

        sess = MagicMock()
        # SmsOptOut existing → None (so we proceed to insert + score hook).
        # Then SmsOptIn lookup → returns row with subscriber_id=99.
        ret_existing = MagicMock(); ret_existing.scalar_one_or_none.return_value = None
        ret_optin    = MagicMock(); ret_optin.scalar_one_or_none.return_value = _ns(subscriber_id=99)
        sess.execute.side_effect = [ret_existing, ret_optin]

        with patch.object(sms_compliance, "_normalize", side_effect=lambda p: p), \
             patch("src.services.segmentation_engine.reclassify_safe") as mock_reclass:
            sms_compliance.record_opt_out(
                "+15555550100", "STOP", "twilio_inbound", sess,
            )
        mock_reclass.assert_called_once()
        kwargs = mock_reclass.call_args.kwargs
        assert kwargs["action_type"] == "sms_opt_out"
        assert kwargs["metadata"]["keyword"] == "STOP"
        assert kwargs["metadata"]["source"] == "twilio_inbound"

    def test_opt_out_score_hook_exception_does_not_block_suppression(self):
        """Test #24 — if the score update raises, the suppression write
        still proceeds (compliance must always win)."""
        sys.modules.setdefault("phonenumbers", MagicMock())
        from src.services import sms_compliance

        sess = MagicMock()
        ret_existing = MagicMock(); ret_existing.scalar_one_or_none.return_value = None
        ret_optin    = MagicMock(); ret_optin.scalar_one_or_none.return_value = _ns(subscriber_id=99)
        sess.execute.side_effect = [ret_existing, ret_optin]

        with patch.object(sms_compliance, "_normalize", side_effect=lambda p: p), \
             patch("src.services.segmentation_engine.reclassify_safe",
                   side_effect=RuntimeError("simulated DB hiccup")):
            # Should NOT propagate.
            sms_compliance.record_opt_out(
                "+15555550100", "STOP", "twilio_inbound", sess,
            )
        # The SmsOptOut row was still added.
        sess.add.assert_called_once()


# ──────────────────────────────────────────────────────────────────────────
# Discipline guard — existing callsites all pass action_type
# ──────────────────────────────────────────────────────────────────────────

class TestActionLabelDiscipline:

    def test_all_existing_callsites_pass_action_type(self):
        """Test #25 — every call to reclassify_safe in the wired event paths
        passes action_type=... so the audit trail is meaningful.

        Strict file-scan: load each file as text and grep for
        `reclassify_safe(` calls. Every one must include `action_type=`.
        This keeps the discipline from quietly regressing — a fresh dev
        adding a new callsite without the kwarg will break this test.
        """
        import re
        from pathlib import Path

        repo_root = Path(__file__).resolve().parent.parent
        targets = [
            "src/services/stripe_webhooks.py",
            "src/services/wallet_engine.py",
            "src/services/wallet_to_lock.py",
            "src/services/sms_commands.py",
            "src/services/sms_compliance.py",
            "src/tasks/auto_mode_followup.py",
        ]

        # Pattern matches both single-line and multi-line `reclassify_safe(`
        # call sites — we then check the surrounding 240 chars for action_type=.
        pat = re.compile(r"reclassify_safe\s*\(", re.MULTILINE)
        offenders: list[str] = []
        for rel in targets:
            text = (repo_root / rel).read_text(encoding="utf-8")
            for m in pat.finditer(text):
                window = text[m.start(): m.start() + 280]
                if "action_type=" not in window:
                    offenders.append(f"{rel}:{text.count(chr(10), 0, m.start()) + 1}")

        assert not offenders, (
            "These reclassify_safe call sites are missing action_type=... — "
            "every wired event must label itself for the audit trail: "
            + ", ".join(offenders)
        )
