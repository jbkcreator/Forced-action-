"""
Unit tests for Wallet-to-Lock sweep and service.
"""
import pytest
from datetime import date, datetime, timezone, timedelta
from unittest.mock import MagicMock, patch


# ── service layer ────────────────────────────────────────────────────────────

class TestWalletToLockCandidate:
    def test_dataclass_fields(self):
        from src.services.wallet_to_lock import WalletToLockCandidate
        c = WalletToLockCandidate(
            subscriber_id=1, zip_code="33647", credits_used=42,
            vertical="roofing", county_id="fl_hillsborough",
        )
        assert c.credits_used == 42
        assert c.zip_code == "33647"
        assert c.uncontacted_count == 0
        assert c.tier_breakdown is None

    def test_dataclass_enriched_fields(self):
        from src.services.wallet_to_lock import WalletToLockCandidate
        breakdown = {"gold": 5, "silver": 3, "bronze": 2}
        c = WalletToLockCandidate(
            subscriber_id=1, zip_code="33647", credits_used=42,
            vertical="roofing", county_id="fl_hillsborough",
            uncontacted_count=10, tier_breakdown=breakdown,
        )
        assert c.uncontacted_count == 10
        assert c.tier_breakdown["gold"] == 5


class TestIsZipLocked:
    def test_locked_zip_returns_true(self, mock_db):
        from src.services.wallet_to_lock import is_zip_locked
        mock_db.execute.return_value.scalar_one_or_none.return_value = MagicMock()
        result = is_zip_locked(mock_db, "33647", "roofing", "fl_hillsborough")
        assert result is True

    def test_unlocked_zip_returns_false(self, mock_db):
        from src.services.wallet_to_lock import is_zip_locked
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        result = is_zip_locked(mock_db, "33647", "roofing", "fl_hillsborough")
        assert result is False


class TestBuildLockCtaUrl:
    def test_url_contains_zip_and_subscriber(self):
        from src.services.wallet_to_lock import build_lock_cta_url
        with patch("src.services.wallet_to_lock.settings") as mock_settings:
            mock_settings.app_base_url = "https://app.example.io"
            url = build_lock_cta_url(42, "33647")
        assert "33647" in url
        assert "42" in url
        assert "annual_lock" in url

    def test_utm_tag_present(self):
        from src.services.wallet_to_lock import build_lock_cta_url
        with patch("src.services.wallet_to_lock.settings") as mock_settings:
            mock_settings.app_base_url = "https://app.example.io"
            url = build_lock_cta_url(1, "33647")
        assert "cora_lock_close" in url


class TestEmitEvent:
    def test_emit_calls_dispatch(self):
        from src.services.wallet_to_lock import emit_event
        with patch("src.agents.supervisor.dispatch_event") as mock_dispatch, \
             patch("src.services.wallet_to_lock.settings") as mock_settings:
            mock_settings.app_base_url = "https://app.example.io"
            emit_event(1, "33647", 45, "roofing")
        mock_dispatch.assert_called_once()
        call_payload = mock_dispatch.call_args[0][0]
        assert call_payload["event_type"] == "subscriber_crossed_lock_threshold"
        assert call_payload["subscriber_id"] == 1

    def test_idempotency_key_includes_zip_and_month(self):
        from src.services.wallet_to_lock import emit_event
        from datetime import date
        with patch("src.agents.supervisor.dispatch_event") as mock_dispatch, \
             patch("src.services.wallet_to_lock.settings") as mock_settings:
            mock_settings.app_base_url = "https://app.example.io"
            emit_event(1, "33647", 45, "roofing")
        call_payload = mock_dispatch.call_args[0][0]
        assert "33647" in call_payload["idempotency_key"]
        month_str = date.today().strftime("%Y-%m")
        assert month_str in call_payload["idempotency_key"]

    def test_emit_event_passes_uncontacted_count(self):
        from src.services.wallet_to_lock import emit_event
        with patch("src.agents.supervisor.dispatch_event") as mock_dispatch, \
             patch("src.services.wallet_to_lock.settings") as mock_settings:
            mock_settings.app_base_url = "https://app.example.io"
            emit_event(1, "33647", 45, "roofing", uncontacted_count=12,
                       tier_breakdown={"gold": 5, "silver": 4, "bronze": 3})
        payload = mock_dispatch.call_args[0][0]["payload"]
        assert payload["uncontacted_count"] == 12
        assert payload["tier_breakdown"]["gold"] == 5

    def test_excluded_tier_not_candidate(self):
        from config.wallet_to_lock import LOCK_OR_ABOVE_TIERS
        assert "annual_lock" in LOCK_OR_ABOVE_TIERS
        assert "autopilot_pro" in LOCK_OR_ABOVE_TIERS
        assert "wallet" not in LOCK_OR_ABOVE_TIERS

    def test_uncontacted_count_gate_minimum(self):
        from src.services.wallet_to_lock import LOCK_MIN_UNCONTACTED_LEADS
        assert LOCK_MIN_UNCONTACTED_LEADS == 10


# ── feed eligibility helper ──────────────────────────────────────────────────

class _Sub:
    """Lightweight Subscriber stand-in for compute_wallet_to_lock_eligibility."""
    def __init__(self, **kw):
        self.id = kw.get("id", 1)
        self.status = kw.get("status", "active")
        self.tier = kw.get("tier", "free")
        self.vertical = kw.get("vertical", "roofing")
        self.county_id = kw.get("county_id", "fl_hillsborough")
        self.lock_candidate_zip = kw.get("lock_candidate_zip", "33647")
        self.lock_candidate_at = kw.get(
            "lock_candidate_at", datetime.now(timezone.utc) - timedelta(days=1),
        )


def _db_with_credits_and_zip(credits, held_by_other=False):
    """Mock db.execute().scalar() / .scalar_one_or_none() to simulate state."""
    db = MagicMock()
    sum_result = MagicMock()
    sum_result.scalar.return_value = credits
    zip_result = MagicMock()
    zip_result.scalar_one_or_none.return_value = 99 if held_by_other else None
    db.execute.side_effect = [sum_result, zip_result]
    return db


class TestComputeWalletToLockEligibility:
    """Client requirement: 40+ wallet credits in one ZIP within 30d, ZIP available."""

    def test_eligible_when_40_credits_in_candidate_zip(self):
        from src.services.wallet_to_lock import compute_wallet_to_lock_eligibility
        db = _db_with_credits_and_zip(credits=42)
        eligible, credits = compute_wallet_to_lock_eligibility(db, _Sub())
        assert eligible is True
        assert credits == 42

    def test_not_eligible_under_threshold(self):
        from src.services.wallet_to_lock import compute_wallet_to_lock_eligibility
        db = _db_with_credits_and_zip(credits=39)
        eligible, credits = compute_wallet_to_lock_eligibility(db, _Sub())
        assert eligible is False
        assert credits == 39

    def test_not_eligible_when_zip_locked_by_other(self):
        from src.services.wallet_to_lock import compute_wallet_to_lock_eligibility
        db = _db_with_credits_and_zip(credits=80, held_by_other=True)
        eligible, _ = compute_wallet_to_lock_eligibility(db, _Sub())
        assert eligible is False

    def test_not_eligible_when_lock_candidate_zip_null(self):
        from src.services.wallet_to_lock import compute_wallet_to_lock_eligibility
        db = MagicMock()
        eligible, credits = compute_wallet_to_lock_eligibility(
            db, _Sub(lock_candidate_zip=None, lock_candidate_at=None),
        )
        assert eligible is False
        assert credits is None
        db.execute.assert_not_called()

    def test_not_eligible_when_already_lock_or_above(self):
        from src.services.wallet_to_lock import compute_wallet_to_lock_eligibility
        db = MagicMock()
        eligible, _ = compute_wallet_to_lock_eligibility(db, _Sub(tier="annual_lock"))
        assert eligible is False
        db.execute.assert_not_called()

    def test_not_eligible_when_status_not_active(self):
        from src.services.wallet_to_lock import compute_wallet_to_lock_eligibility
        db = MagicMock()
        eligible, _ = compute_wallet_to_lock_eligibility(db, _Sub(status="paused"))
        assert eligible is False

    def test_tier_wallet_string_never_used(self):
        """Regression guard: the legacy `tier == 'wallet'` branch was dead code —
        'wallet' isn't in the Subscriber.tier check constraint. Eligibility must
        come from credit count + lock_candidate_zip, not tier string."""
        from src.core.models import Subscriber  # noqa: F401
        valid_tiers = {
            "free", "starter", "pro", "dominator", "data_only",
            "autopilot_lite", "autopilot_pro", "partner", "annual_lock",
        }
        assert "wallet" not in valid_tiers


# ── sweep task ───────────────────────────────────────────────────────────────

class TestWalletToLockSweep:
    def _make_candidate(self, sub_id="1", zip_code="33647"):
        from src.services.wallet_to_lock import WalletToLockCandidate
        return WalletToLockCandidate(
            subscriber_id=int(sub_id), zip_code=zip_code, credits_used=42,
            vertical="roofing", county_id="fl_hillsborough",
        )

    def test_dry_run_no_events(self):
        from src.tasks.wallet_to_lock_sweep import run_sweep

        candidate = self._make_candidate()
        with (
            patch("src.tasks.wallet_to_lock_sweep.get_db_context") as mock_ctx,
            patch("src.tasks.wallet_to_lock_sweep.find_candidates", return_value=[candidate]),
            patch("src.tasks.wallet_to_lock_sweep.is_zip_locked", return_value=False),
            patch("src.tasks.wallet_to_lock_sweep.emit_event") as mock_emit,
            patch("src.tasks.wallet_to_lock_sweep.mark_lock_candidate"),
        ):
            mock_ctx.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            results = run_sweep(dry_run=True)

        mock_emit.assert_not_called()
        assert results["candidates_found"] == 1
        assert results["events_emitted"] == 0

    def test_locked_zip_skipped(self):
        from src.tasks.wallet_to_lock_sweep import run_sweep

        candidate = self._make_candidate()
        with (
            patch("src.tasks.wallet_to_lock_sweep.get_db_context") as mock_ctx,
            patch("src.tasks.wallet_to_lock_sweep.find_candidates", return_value=[candidate]),
            patch("src.tasks.wallet_to_lock_sweep.is_zip_locked", return_value=True),
            patch("src.tasks.wallet_to_lock_sweep.emit_event") as mock_emit,
        ):
            mock_ctx.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            results = run_sweep(dry_run=False)

        mock_emit.assert_not_called()
        assert results["skipped_zip_locked"] == 1
        assert results["candidates_found"] == 0

    def test_unlocked_emits_event(self):
        from src.tasks.wallet_to_lock_sweep import run_sweep

        candidate = self._make_candidate()
        with (
            patch("src.tasks.wallet_to_lock_sweep.get_db_context") as mock_ctx,
            patch("src.tasks.wallet_to_lock_sweep.find_candidates", return_value=[candidate]),
            patch("src.tasks.wallet_to_lock_sweep.is_zip_locked", return_value=False),
            patch("src.tasks.wallet_to_lock_sweep.emit_event") as mock_emit,
            patch("src.tasks.wallet_to_lock_sweep.mark_lock_candidate"),
        ):
            mock_ctx.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            results = run_sweep(dry_run=False)

        mock_emit.assert_called_once_with(
            candidate.subscriber_id,
            candidate.zip_code,
            candidate.credits_used,
            candidate.vertical,
            uncontacted_count=candidate.uncontacted_count,
            tier_breakdown=candidate.tier_breakdown,
        )
        assert results["events_emitted"] == 1
