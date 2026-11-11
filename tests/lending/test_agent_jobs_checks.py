"""P1.1 job checks: lane, consent, caps, content and hours, and the cold (Instantly) / warm (GHL) email rail."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from src.lending.agent_jobs.checks import check_job, rail_for
from src.lending.agent_jobs.job import AgentJob
from src.lending.ghl_crm import GhlContact

NOW = datetime(2026, 11, 12, 15, 0, tzinfo=timezone.utc)  # 10:00 AM Eastern
PHONE = "+18135550142"
EMAIL = "mark@example.com"


def _job(**overrides) -> AgentJob:
    data = dict(job_id="7f1c2e9a-3b4d-4c5e-8f60-718293a4b5c6", lane="NDL", created_by="LendingFlow follow up",
                created_at=NOW - timedelta(hours=1), type="send_sms", ghl_contact_id="ghl-123",
                contact_label="Mark T., Tampa", template_version="LF-D2-SMS-v1", body="Hi Mark, any questions?",
                reason="Day 2 touch", requires_approval=True, not_before=None, expires_at=NOW + timedelta(days=1))
    data.update(overrides)
    return AgentJob.model_validate(data)


def _contact(**overrides) -> GhlContact:
    data = dict(contact_id="ghl-123", phone=PHONE, email=EMAIL, dnd_channels=frozenset(), tags=frozenset())
    data.update(overrides)
    return GhlContact(**data)


def _open(_phone, **_kwargs) -> bool:
    return True


def _closed(_phone, **_kwargs) -> bool:
    return False


@pytest.fixture
def db(lending_db):
    from packages.agent_core.schema import apply_to

    apply_to(lending_db.connection(), "lending")  # pending_actions, for the cold-email cap
    return lending_db


def _consent(db) -> None:
    db.execute(text("INSERT INTO lending.text_consents (phone, source) VALUES (:p, 'web_form')"), {"p": PHONE})


def _sent_cold_email(db, at: datetime) -> None:
    db.execute(text(
        "INSERT INTO lending.pending_actions (tool_name, channel, payload, status, recipient_email, executed_at, "
        "created_at, updated_at) VALUES ('agent_job:send_email', 'instantly_email', :payload, 'sent', :e, :at, :at, :at)"),
        {"payload": json.dumps({}), "e": EMAIL, "at": at})


def _check(db, job, contact=None, **kwargs):
    return check_job(db, job, _contact() if contact is None else contact, NOW, text_window=kwargs.pop("text_window", _open),
                     **kwargs)


def test_unknown_contact_is_error(db) -> None:
    verdict = check_job(db, _job(), None, NOW, text_window=_open)
    assert (verdict.outcome, verdict.status) == ("block", "error")
    assert "contact not found" in verdict.reason


def test_wrong_lane_is_blocked_lane(db) -> None:
    verdict = _check(db, _job(lane="BACKFLIP"))
    assert (verdict.outcome, verdict.status) == ("block", "blocked_lane")


def test_backflip_in_body_is_blocked_lane(db) -> None:
    assert _check(db, _job(body="Backflip can match this")).status == "blocked_lane"


def test_backflip_contact_email_is_blocked_lane(db) -> None:
    assert _check(db, _job(type="add_note"), _contact(email="x@backflip.com")).status == "blocked_lane"


def test_expired_job(db) -> None:
    job = _job(not_before=NOW - timedelta(days=2), expires_at=NOW - timedelta(minutes=1))
    assert _check(db, job).status == "expired"


def test_suppressed_email_blocked_consent(db) -> None:
    db.execute(text("INSERT INTO lending.suppression_list (email, reason, source_channel) "
                    "VALUES (:e, 'OPT_OUT', 'email')"), {"e": EMAIL})
    verdict = _check(db, _job(type="send_email", subject="Hi"))
    assert verdict.status == "blocked_consent"


def test_do_not_contact_phone_blocked(db) -> None:
    _consent(db)
    db.execute(text("INSERT INTO lending.contacts (phone, do_not_contact) VALUES (:p, true)"), {"p": PHONE})
    assert _check(db, _job()).status == "blocked_consent"


def test_sms_without_consent_blocked(db) -> None:
    assert _check(db, _job()).status == "blocked_consent"


def test_sms_with_consent_is_go(db) -> None:
    _consent(db)
    verdict = _check(db, _job())
    assert (verdict.outcome, verdict.rail) == ("go", "ghl")


def test_ghl_dnd_sms_blocked(db) -> None:
    _consent(db)
    assert _check(db, _job(), _contact(dnd_channels=frozenset({"SMS"}))).status == "blocked_consent"


def test_sms_without_phone_is_error(db) -> None:
    assert _check(db, _job(), _contact(phone=None)).status == "error"


def test_warm_email_with_ghl_email_dnd_blocked(db) -> None:
    verdict = _check(db, _job(type="send_email", subject="Hi"), _contact(dnd_channels=frozenset({"Email"})))
    assert verdict.status == "blocked_consent"


def test_cold_email_routes_to_instantly_despite_ghl_email_dnd(db) -> None:
    cold = _contact(tags=frozenset({"ndl-cold"}), dnd_channels=frozenset({"Email", "SMS"}))
    verdict = _check(db, _job(type="send_email", subject="Hi"), cold)
    assert (verdict.outcome, verdict.rail) == ("go", "instantly")


def test_cold_email_without_instantly_config_is_error(db) -> None:
    cold = _contact(tags=frozenset({"ndl-cold"}))
    verdict = _check(db, _job(type="send_email", subject="Hi"), cold, instantly_ready=False)
    assert verdict.status == "error"
    assert "cold email rail not configured" in verdict.reason


def test_body_quoting_rate_is_error(db) -> None:
    _consent(db)
    verdict = _check(db, _job(body="Rates from 7.5% this week"))
    assert verdict.status == "error"


def test_cold_email_cap_3_days(db) -> None:
    _sent_cold_email(db, NOW - timedelta(days=2))
    cold = _contact(tags=frozenset({"ndl-cold"}))
    assert _check(db, _job(type="send_email", subject="Hi"), cold).status == "blocked_cap"


def test_cold_email_cap_4_in_14_days(db) -> None:
    for days in (4, 7, 10, 13):
        _sent_cold_email(db, NOW - timedelta(days=days))
    cold = _contact(tags=frozenset({"ndl-cold"}))
    assert _check(db, _job(type="send_email", subject="Hi"), cold).status == "blocked_cap"


def test_cold_email_after_gap_is_go(db) -> None:
    for days in (4, 20):
        _sent_cold_email(db, NOW - timedelta(days=days))
    cold = _contact(tags=frozenset({"ndl-cold"}))
    assert _check(db, _job(type="send_email", subject="Hi"), cold).outcome == "go"


def test_warm_email_not_capped(db) -> None:
    _sent_cold_email(db, NOW - timedelta(days=1))
    assert _check(db, _job(type="send_email", subject="Hi")).outcome == "go"


def test_not_before_future_is_held(db) -> None:
    _consent(db)
    verdict = _check(db, _job(not_before=NOW + timedelta(hours=2)))
    assert (verdict.outcome, verdict.held_reason, verdict.release_at) == ("hold", "not_before", NOW + timedelta(hours=2))


def test_message_outside_window_is_held(db) -> None:
    _consent(db)
    verdict = _check(db, _job(), text_window=_closed)
    assert (verdict.outcome, verdict.held_reason) == ("hold", "hours")
    assert verdict.release_at == NOW + timedelta(minutes=15)


def test_email_without_phone_uses_eastern_window(db) -> None:
    night = datetime(2026, 11, 13, 2, 0, tzinfo=timezone.utc)  # 9:00 PM Eastern
    job = _job(type="send_email", subject="Hi", created_at=night - timedelta(hours=1), expires_at=night + timedelta(days=1))
    verdict = check_job(db, job, _contact(phone=None), night, text_window=_open)
    assert (verdict.outcome, verdict.held_reason) == ("hold", "hours")


def test_crm_job_ignores_window(db) -> None:
    verdict = _check(db, _job(type="move_stage", body="Application"), text_window=_closed)
    assert (verdict.outcome, verdict.rail) == ("go", "ghl")


def test_rail_for() -> None:
    assert rail_for(_job(type="send_email", subject="Hi"), _contact(tags=frozenset({"ndl-cold"}))) == "instantly"
    assert rail_for(_job(type="send_email", subject="Hi"), _contact()) == "ghl"
    assert rail_for(_job(), _contact(tags=frozenset({"ndl-cold"}))) == "ghl"


def test_cold_email_awaiting_approval_counts_toward_cap(db) -> None:
    db.execute(text(
        "INSERT INTO lending.pending_actions (tool_name, channel, payload, status, recipient_email, created_at, "
        "updated_at) VALUES ('agent_job:send_email', 'instantly_email', '{}', 'pending', :e, :at, :at)"),
        {"e": EMAIL, "at": NOW - timedelta(hours=1)})
    cold = _contact(tags=frozenset({"ndl-cold"}))
    assert _check(db, _job(type="send_email", subject="Hi"), cold).status == "blocked_cap"
