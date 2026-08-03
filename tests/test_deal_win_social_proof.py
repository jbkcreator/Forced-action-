from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from src.services.deal_win_social_proof import maybe_send_deal_win_social_proof_prompt


def _subscriber():
    return SimpleNamespace(
        id=7,
        email="winner@example.com",
        name="Winner",
    )


def _outcome(**overrides):
    payload = {
        "id": 42,
        "deal_amount": 15000,
        "deal_size_bucket": "10_25k",
        "created_at": datetime(2026, 7, 28, tzinfo=timezone.utc),
    }
    payload.update(overrides)
    return SimpleNamespace(**payload)


def test_social_proof_skips_small_win():
    with patch("src.services.deal_win_social_proof._within_cooldown", return_value=False):
        sent = maybe_send_deal_win_social_proof_prompt(_subscriber(), _outcome(deal_amount=8000, deal_size_bucket="5_10k"), object())
    assert sent is False


def test_social_proof_skips_pre_go_live():
    settings = SimpleNamespace(app_base_url="https://app.test", deal_win_testimonial_go_live_at=date(2026, 7, 28))
    with patch("src.services.deal_win_social_proof.get_settings", return_value=settings), \
         patch("src.services.deal_win_social_proof._within_cooldown", return_value=False):
        sent = maybe_send_deal_win_social_proof_prompt(
            _subscriber(),
            _outcome(created_at=datetime(2026, 7, 27, tzinfo=timezone.utc)),
            object(),
        )
    assert sent is False


def test_social_proof_sends_email_only_and_tracks():
    db = object()
    settings = SimpleNamespace(app_base_url="https://app.test", deal_win_testimonial_go_live_at=date(2026, 7, 28))
    with patch("src.services.deal_win_social_proof.get_settings", return_value=settings), \
         patch("src.services.deal_win_social_proof._within_cooldown", return_value=False), \
         patch("src.services.referral_engine.ensure_referral_code", return_value="abc123"), \
         patch("src.services.deal_win_social_proof._reserve_funnel_row", return_value=55), \
         patch("src.services.signed_links.encode_prompt_attribution_token", return_value="tok"), \
         patch("src.services.email.send_email", return_value=True) as send_email, \
         patch("src.services.transactional_email_tracking.log_transactional_email_send") as log_send, \
         patch("src.services.deal_win_social_proof._mark_send_status") as mark_status:
        sent = maybe_send_deal_win_social_proof_prompt(_subscriber(), _outcome(), db)
    assert sent is True
    assert send_email.call_count == 1
    assert send_email.call_args.kwargs["to"] == "winner@example.com"
    mark_status.assert_called_once_with(55, False, True, db)
    assert log_send.call_count == 1
