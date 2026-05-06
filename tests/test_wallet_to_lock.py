"""
Unit tests for Wallet-to-Lock sweep and service.
"""
import pytest
from datetime import date
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

    def test_excluded_tier_not_candidate(self):
        from config.wallet_to_lock import LOCK_OR_ABOVE_TIERS
        assert "annual_lock" in LOCK_OR_ABOVE_TIERS
        assert "autopilot_pro" in LOCK_OR_ABOVE_TIERS
        assert "wallet" not in LOCK_OR_ABOVE_TIERS


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

        mock_emit.assert_called_once_with(candidate.subscriber_id, candidate.zip_code, candidate.credits_used, candidate.vertical)
        assert results["events_emitted"] == 1
