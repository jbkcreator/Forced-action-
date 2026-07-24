"""Founder unlimited-reveal waiver (ADR 0037).

Founders reveal leads at $0 across the wallet-unlock and hot-lead-unlock
surfaces, are never wallet-enrollment candidates, and every comp reveal is still
recorded (but never hits revenue).
"""

from unittest.mock import MagicMock, patch

from src.core.models import WalletBalance


class TestRevealIsFree:
    def _row(self, tier, status="active"):
        row = MagicMock()
        row.tier = tier
        row.status = status
        return row

    def test_founder_tier_is_free(self):
        from src.services.entitlement_service import reveal_is_free
        db = MagicMock()
        db.execute.return_value.fetchone.return_value = self._row("founder", "active")
        assert reveal_is_free(db, 1) is True

    def test_pro_tier_is_not_free(self):
        from src.services.entitlement_service import reveal_is_free
        db = MagicMock()
        db.execute.return_value.fetchone.return_value = self._row("pro", "active")
        assert reveal_is_free(db, 1) is False

    def test_no_account_is_not_free(self):
        from src.services.entitlement_service import reveal_is_free
        db = MagicMock()
        db.execute.return_value.fetchone.return_value = None
        assert reveal_is_free(db, 1) is False

    def test_founder_grace_is_not_free(self):
        # PR #163 review comment 2: only a fully active founder gets the waiver.
        from src.services.entitlement_service import reveal_is_free
        db = MagicMock()
        db.execute.return_value.fetchone.return_value = self._row("founder", "grace")
        assert reveal_is_free(db, 1) is False

    def test_founder_past_due_is_not_free(self):
        from src.services.entitlement_service import reveal_is_free
        db = MagicMock()
        db.execute.return_value.fetchone.return_value = self._row("founder", "past_due")
        assert reveal_is_free(db, 1) is False

    def test_founder_churned_is_not_free(self):
        from src.services.entitlement_service import reveal_is_free
        db = MagicMock()
        db.execute.return_value.fetchone.return_value = self._row("founder", "churned")
        assert reveal_is_free(db, 1) is False


class TestWalletCompDebit:
    def test_founder_unlock_is_comped_no_deduction(self, mock_db):
        from src.services.wallet_engine import debit
        wallet = WalletBalance(id=1, subscriber_id=1, wallet_tier="starter_wallet",
                               credits_remaining=10, credits_used_total=0)
        mock_db.execute.return_value.scalar_one_or_none.return_value = wallet
        with patch("src.services.entitlement_service.reveal_is_free", return_value=True):
            result = debit(1, "lead_unlock", mock_db)
        assert result is True
        # No credits spent.
        assert wallet.credits_remaining == 10
        assert wallet.credits_used_total == 0
        # A $0 comp txn was recorded, tagged so it stays out of revenue/enrollment.
        added = [c.args[0] for c in mock_db.add.call_args_list]
        txn = added[-1]
        assert txn.amount == 0
        assert txn.txn_type == "debit"
        assert txn.description.startswith("founder_comp:")

    def test_non_reveal_action_not_comped_for_founder(self, mock_db):
        # Only lead_unlock is waived; skip_trace still costs credits.
        from src.services.wallet_engine import debit
        wallet = WalletBalance(id=1, subscriber_id=1, wallet_tier="starter_wallet",
                               credits_remaining=10, credits_used_total=0)
        mock_db.execute.return_value.scalar_one_or_none.return_value = wallet
        with patch("src.services.entitlement_service.reveal_is_free", return_value=True):
            result = debit(1, "skip_trace", mock_db)  # costs 2, not in _FOUNDER_COMP_ACTIONS
        assert result is True
        assert wallet.credits_remaining == 8


class TestFounderEnrollmentShortCircuit:
    def test_founder_never_enrolls(self, mock_db):
        from src.services.wallet_engine import check_enrollment_triggers
        sub = MagicMock()
        sub.has_saved_card = True      # would normally trigger starter_wallet
        sub.wallet_opt_out = False
        mock_db.get.return_value = sub
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        with patch("src.services.entitlement_service.reveal_is_free", return_value=True):
            assert check_enrollment_triggers(1, mock_db) is None


class TestHotLeadCompFulfillment:
    def test_bad_property_id_returns_false(self):
        from src.services.stripe_webhooks import fulfill_founder_comp_reveal
        db = MagicMock()
        subscriber = MagicMock(id=1)
        assert fulfill_founder_comp_reveal(subscriber, "not-an-int", db) is False
