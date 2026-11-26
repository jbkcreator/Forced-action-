"""R1 0.11 SMS line-type gate: a landline is never texted; mobile and VoIP are; Telnyx fills a missing line type; a
still-unknown number is texted only with text consent. No DB, no network."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from config.settings import get_settings
from src.lending import compliance
from src.lending.compliance import SmsBlock, sms_block_reason, sms_eligible
from src.lending.ghl_sms import GhlAccount, GhlSendRefused, GhlSmsError, GhlSmsSender

PHONE = "+18135558601"
NOW = datetime(2026, 11, 25, 15, 0, tzinfo=timezone.utc)
ACCOUNT = GhlAccount(api_key="k", location_id="loc")


class Store:
    """Stands in for the three DB helpers the gate uses."""

    def __init__(self, monkeypatch, line_type=None, checked_at=None, lookups_today=0, consented=False):
        self.line_type, self.checked_at, self.lookups_today, self.saved = line_type, checked_at, lookups_today, []
        monkeypatch.setattr("src.lending.consent.has_text_consent", lambda db, phone: consented)
        monkeypatch.setattr(compliance, "_stored_line_type", lambda db, phone: (self.line_type, self.checked_at))
        monkeypatch.setattr(compliance, "_lookups_today", lambda db, now: self.lookups_today)
        monkeypatch.setattr(compliance, "_save_lookup", lambda db, phone, t, now: self.saved.append((phone, t)))


class Lookup:
    def __init__(self, answer="mobile", error=None):
        self.answer, self.error, self.calls = answer, error, 0

    def __call__(self, phone):
        self.calls += 1
        if self.error:
            raise self.error
        return {"type": self.answer}


@pytest.fixture(autouse=True)
def _fallback_on(monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "line_type_telnyx_fallback_enabled", True, raising=False)
    monkeypatch.setattr(settings, "line_type_max_lookups_per_day", 200, raising=False)


@pytest.mark.parametrize("stored, expected", [
    ("mobile", None), ("landline", SmsBlock.LANDLINE), ("voip", None),
])
def test_stored_line_type_decides_without_a_lookup(monkeypatch, stored, expected):
    Store(monkeypatch, line_type=stored)
    lookup = Lookup()
    assert sms_block_reason(None, PHONE, now=NOW, lookup=lookup) == expected
    assert lookup.calls == 0


def test_synthetic_landline_lead_fails_the_sms_eligibility_gate(monkeypatch):
    Store(monkeypatch, line_type="landline")
    assert sms_eligible(None, PHONE, now=NOW, lookup=Lookup()) is False


def test_missing_line_type_is_looked_up_once_and_saved(monkeypatch):
    store = Store(monkeypatch)
    lookup = Lookup("landline")
    assert sms_block_reason(None, PHONE, now=NOW, lookup=lookup) == SmsBlock.LANDLINE
    assert lookup.calls == 1 and store.saved == [(PHONE, "landline")]


def test_missing_line_type_looked_up_as_mobile_may_be_texted(monkeypatch):
    Store(monkeypatch)
    assert sms_eligible(None, PHONE, now=NOW, lookup=Lookup("mobile")) is True


def test_lookup_failure_fails_closed_and_saves_nothing(monkeypatch):
    store = Store(monkeypatch)
    assert sms_block_reason(None, PHONE, now=NOW, lookup=Lookup(error=RuntimeError("down"))) == SmsBlock.UNKNOWN
    assert store.saved == []


def test_daily_cap_reached_fails_closed_without_a_lookup(monkeypatch):
    Store(monkeypatch, lookups_today=200)
    lookup = Lookup()
    assert sms_block_reason(None, PHONE, now=NOW, lookup=lookup) == SmsBlock.UNKNOWN
    assert lookup.calls == 0


def test_fallback_off_fails_closed_without_a_lookup(monkeypatch):
    monkeypatch.setattr(get_settings(), "line_type_telnyx_fallback_enabled", False, raising=False)
    Store(monkeypatch)
    lookup = Lookup()
    assert sms_block_reason(None, PHONE, now=NOW, lookup=lookup) == SmsBlock.UNKNOWN
    assert lookup.calls == 0


def test_recent_unknown_answer_is_not_paid_for_again(monkeypatch):
    Store(monkeypatch, line_type="unknown", checked_at=NOW - timedelta(days=5))
    lookup = Lookup()
    assert sms_block_reason(None, PHONE, now=NOW, lookup=lookup) == SmsBlock.UNKNOWN
    assert lookup.calls == 0


def test_old_unknown_answer_is_looked_up_again(monkeypatch):
    Store(monkeypatch, line_type="unknown", checked_at=NOW - timedelta(days=31))
    assert sms_block_reason(None, PHONE, now=NOW, lookup=Lookup("mobile")) is None


def test_invalid_phone_is_blocked(monkeypatch):
    Store(monkeypatch)
    assert sms_block_reason(None, "12", now=NOW, lookup=Lookup()) == SmsBlock.UNKNOWN


class NoHttp:
    def __init__(self):
        self.calls = 0

    def __call__(self, *args, **kwargs):
        self.calls += 1
        raise AssertionError("no GHL call expected")


def test_sender_refuses_a_landline_before_any_ghl_call():
    request = NoHttp()
    with pytest.raises(GhlSendRefused) as refused:
        GhlSmsSender(ACCOUNT, "+18135550100", request=request, sms_gate=lambda p: SmsBlock.LANDLINE)(PHONE, "hi")
    assert refused.value.reason == "landline" and request.calls == 0


def test_gate_failure_is_a_retryable_error_not_a_refusal():
    def broken(phone):
        raise RuntimeError("db down")

    request = NoHttp()
    with pytest.raises(GhlSmsError) as failed:
        GhlSmsSender(ACCOUNT, "+18135550100", request=request, sms_gate=broken)(PHONE, "hi")
    assert not isinstance(failed.value, GhlSendRefused) and not failed.value.ambiguous and request.calls == 0


def test_internal_alerts_skip_the_line_type_gate():
    class Ok:
        def __init__(self, status, body):
            self.status_code, self._body = status, body

        def json(self):
            return self._body

    responses = [Ok(200, {"contact": {"id": "c1"}}), Ok(201, {"messageId": "m1"})]
    sender = GhlSmsSender(ACCOUNT, "+18135550100", request=lambda *a, **k: responses.pop(0), internal_alert=True,
                          sms_gate=lambda p: SmsBlock.LANDLINE)
    assert sender(PHONE, "alarm") == "m1"


def test_unknown_with_text_consent_is_texted(monkeypatch):
    """An inbound lead (LendingFlow, website with SMS consent) is still texted when Telnyx cannot answer."""
    Store(monkeypatch, consented=True)
    assert sms_block_reason(None, PHONE, now=NOW, lookup=Lookup(error=RuntimeError("down"))) is None


def test_landline_with_text_consent_is_still_blocked(monkeypatch):
    Store(monkeypatch, line_type="landline", consented=True)
    assert sms_block_reason(None, PHONE, now=NOW, lookup=Lookup()) == SmsBlock.LANDLINE


def test_telnyx_voip_answer_is_texted(monkeypatch):
    Store(monkeypatch)
    assert sms_eligible(None, PHONE, now=NOW, lookup=Lookup("voip")) is True
