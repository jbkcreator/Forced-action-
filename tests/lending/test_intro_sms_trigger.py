"""T-09: the intro SMS is a GHL workflow that starts on the tag T-11's push adds for a lead we may text.

The decision is made here, in code, at push time: a lead gets ``lendingflow-intro-ready`` in a second add-tags call
only when it has a consent certificate AND ``deliver_pending`` has just re-checked the current suppression list and
do-not-contact flag (``sms_clear``). Re-checking matters because a lead can sit in retry for up to 72 hours and an
opt-out after intake does not change its stored ``suppressed`` flag. Renaming a tag, or adding the ready tag for a
lead that must not be texted, would break or bypass the workflow (docs/lending/intro-sms.md), so these assertions fail
loudly instead.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import text

from config.lending_lendingflow import GHL_TAG_CONSENT_MISSING, GHL_TAG_INTRO_READY, GHL_TAG_LENDINGFLOW
from config.settings import get_settings
from src.lending import lendingflow
from src.lending.ghl_account import GhlAccount
from src.lending.lendingflow import deliver_pending, parse_lendingflow, release_held_intro_sms, save_lead
from src.lending.lendingflow_ghl import LendingFlowGhlSink, _intro_name, _intro_ready_tags, _tags
from src.lending.web_leads import DeliveryError
from src.services import ghl_webhook

FIXTURE = Path(__file__).parent / "fixtures" / "lendingflow_fake.json"
OPT_OUT_PHONE, OPT_OUT_EMAIL = "+17275550151", "t09.optout@example.com"


def _lead(consent, *, sms_clear=True):
    return {"id": 1, "lead_uuid": "u-1", "vendor_lead_id": "LF-1", "lead_source_campaign": "60 Second Loan Match",
            "phone": "+17275550100", "email": None, "first_name": None, "last_name": None,
            "consent_status": consent, "sms_clear": sms_clear,
            "received_at": datetime(2026, 10, 9, tzinfo=timezone.utc)}


class _Calls(list):
    status: dict


@pytest.fixture
def ghl_calls(monkeypatch):
    """Record every GHL call the sink makes; no pipeline stage is configured, whatever the local .env says."""
    settings = get_settings()
    monkeypatch.setattr(settings, "lending_ghl_pipeline_id", None)
    monkeypatch.setattr(settings, "lending_ghl_stage_lendingflow_new", None)
    calls = _Calls()
    status = {"ready_tag": 200}

    class Resp:
        def __init__(self, code):
            self.status_code = code

        def json(self):
            return {"contact": {"id": "c1"}}

    def fake(method, url, **kw):
        step = url.rsplit("/", 1)[-1]
        body = kw.get("json")
        calls.append((method, step, body))
        is_ready_tag = step == "tags" and body and body.get("tags") == [GHL_TAG_INTRO_READY]
        return Resp(status["ready_tag"] if is_ready_tag else 200)

    monkeypatch.setattr(ghl_webhook, "_ghl_request", fake)
    calls.status = status
    return calls


def _tag_calls(calls):
    return [c[2]["tags"] for c in calls if c[1] == "tags"]


# ------------------------------------------------------------------ the tag contract (no database)

def test_workflow_trigger_and_guard_tag_names_are_stable():
    assert GHL_TAG_LENDINGFLOW == "lendingflow"
    assert GHL_TAG_CONSENT_MISSING == "lendingflow-consent-missing"
    assert GHL_TAG_INTRO_READY == "lendingflow-intro-ready"


def test_tags_list_is_unchanged_by_the_ready_tag():
    assert _tags({"consent_status": "present"}) == ["lendingflow"]
    assert _tags({"consent_status": "missing"}) == ["lendingflow", "lendingflow-consent-missing"]


@pytest.mark.parametrize("lead, expected", [
    ({"consent_status": "present", "sms_clear": True}, ["lendingflow-intro-ready"]),
    ({"consent_status": "present", "sms_clear": False}, []),  # suppressed since intake
    ({"consent_status": "present"}, []),  # no re-check happened: fail closed
    ({"consent_status": "present", "sms_clear": "yes"}, []),  # only an explicit True counts
    ({"consent_status": "missing", "sms_clear": True}, []),
    ({"consent_status": "", "sms_clear": True}, []),
    ({"consent_status": "anything-else", "sms_clear": True}, []),
])
def test_ready_tag_needs_a_consent_certificate_and_an_explicit_clear(lead, expected):
    assert _intro_ready_tags(lead) == expected


def test_lead_with_consent_gets_the_ready_tag_in_a_second_call_after_the_first(ghl_calls):
    LendingFlowGhlSink(GhlAccount("k", "loc")).push(_lead("present"))
    assert _tag_calls(ghl_calls) == [["lendingflow"], ["lendingflow-intro-ready"]]
    steps = [c[1] for c in ghl_calls]
    assert steps.index("upsert") < steps.index("tags") < len(steps) - 1 - steps[::-1].index("tags")
    assert steps[-1] == "notes"  # the rest of the push is unchanged and still runs after the ready tag


def test_lead_without_consent_never_gets_the_ready_tag(ghl_calls):
    LendingFlowGhlSink(GhlAccount("k", "loc")).push(_lead("missing"))
    assert _tag_calls(ghl_calls) == [["lendingflow", "lendingflow-consent-missing"]]
    assert all(GHL_TAG_INTRO_READY not in (c[2] or {}).get("tags", []) for c in ghl_calls)


def test_a_lead_that_is_no_longer_clear_is_pushed_without_the_ready_tag(ghl_calls):
    LendingFlowGhlSink(GhlAccount("k", "loc")).push(_lead("present", sms_clear=False))
    assert _tag_calls(ghl_calls) == [["lendingflow"]]
    assert [c[1] for c in ghl_calls][-1] == "notes"  # CRM delivery is separate from SMS eligibility


def test_a_failed_ready_tag_call_fails_the_push_so_the_sweep_retries_it(ghl_calls):
    ghl_calls.status["ready_tag"] = 500
    with pytest.raises(DeliveryError):
        LendingFlowGhlSink(GhlAccount("k", "loc")).push(_lead("present"))
    assert [c[1] for c in ghl_calls][-1] == "tags"  # stopped there: no card, no note until the retry succeeds


# ------------------------------------------------------------------ a retry after an opt-out (database)

class _DownSink:
    """GHL is down: the first delivery attempt fails and the lead goes to retry."""

    def push(self, lead):
        raise DeliveryError("GHL down")


@pytest.fixture
def lf_db(lending_db):
    from migrations.apply_lending_lendingflow import apply_to

    apply_to(lending_db.get_bind())
    return lending_db


OPEN_NOW = datetime(2026, 10, 14, 16, 0, tzinfo=timezone.utc)    # 12:00 ET / 11:00 CT: inside the window for a Florida lead
CLOSED_NOW = datetime(2026, 10, 14, 10, 0, tzinfo=timezone.utc)  # 06:00 ET: outside it
RECEIVED = datetime(2026, 10, 14, 9, 0, tzinfo=timezone.utc)


@pytest.fixture
def intro_on(monkeypatch):
    """The T-09 switch on and the intro_name field configured, as in production once Josh confirms consent and DNC."""
    settings = get_settings()
    monkeypatch.setattr(settings, "lending_intro_sms_enabled", True)
    monkeypatch.setattr(settings, "lending_ghl_cf_intro_name", "cf-intro-name")
    return settings


def _received_at(db, lead_id, at):
    db.execute(text("UPDATE lending.lendingflow_leads SET received_at = :t WHERE id = :i"), {"t": at, "i": lead_id})


def _tagged_at(db, lead_id):
    return db.execute(text("SELECT intro_sms_tagged_at FROM lending.lendingflow_leads WHERE id = :i"),
                      {"i": lead_id}).scalar_one()


def _save_eligible_lead(db):
    """A lead with a consent certificate that is not suppressed at intake. Unique identifiers: the shared database
    already holds committed LendingFlow rows, and a repeated vendor id or phone+email would be a duplicate."""
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    payload.update({"lead_id": "T09-OPTOUT-1", "phone": "(727) 555-0151", "email": OPT_OUT_EMAIL})
    result = save_lead(db, parse_lendingflow(payload), payload)
    db.commit()
    return result.lead_id


def _ghl_status(db, lead_id):
    return db.execute(text("SELECT ghl_status FROM lending.lendingflow_leads WHERE id = :i"), {"i": lead_id}).scalar_one()


def _opt_out_by_phone(db):
    db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) "
                    "VALUES (:p, 'OPT_OUT', 'sms')"), {"p": OPT_OUT_PHONE})


def _opt_out_by_email_only(db):
    db.execute(text("INSERT INTO lending.suppression_list (email, reason, source_channel) "
                    "VALUES (:e, 'OPT_OUT', 'email')"), {"e": OPT_OUT_EMAIL})


def _opt_out_by_contact_flag(db):
    db.execute(text("UPDATE lending.contacts SET do_not_contact = true WHERE phone = :p"), {"p": OPT_OUT_PHONE})


OPT_OUTS = [pytest.param(_opt_out_by_phone, id="phone-suppression"),
            pytest.param(_opt_out_by_email_only, id="email-only-suppression"),
            pytest.param(_opt_out_by_contact_flag, id="contact-do-not-contact")]


@pytest.mark.parametrize("opt_out", OPT_OUTS)
def test_retry_after_an_opt_out_delivers_the_contact_but_never_adds_the_ready_tag(lf_db, ghl_calls, intro_on, opt_out):
    lead_id = _save_eligible_lead(lf_db)
    deliver_pending(lf_db, _DownSink(), lead_id=lead_id)  # first delivery fails: the lead waits in retry
    assert _ghl_status(lf_db, lead_id) == "failed"
    opt_out(lf_db)  # the borrower opts out while delivery is pending; no GHL DND sync happens in this test at all
    followups = deliver_pending(lf_db, LendingFlowGhlSink(GhlAccount("k", "loc")), lead_id=lead_id, now=OPEN_NOW)
    assert len(followups) == 1  # the CRM delivery still happened
    assert _ghl_status(lf_db, lead_id) in ("synced", "contact_only")
    assert _tag_calls(ghl_calls) == [["lendingflow"]]  # but nothing can start the intro-SMS workflow


def test_retry_without_an_opt_out_still_adds_the_ready_tag(lf_db, ghl_calls, intro_on):
    lead_id = _save_eligible_lead(lf_db)
    deliver_pending(lf_db, _DownSink(), lead_id=lead_id)
    followups = deliver_pending(lf_db, LendingFlowGhlSink(GhlAccount("k", "loc")), lead_id=lead_id, now=OPEN_NOW)
    assert len(followups) == 1
    assert _tag_calls(ghl_calls) == [["lendingflow"], ["lendingflow-intro-ready"]]


def test_a_failed_suppression_recheck_withholds_the_tag_but_still_delivers(lf_db, ghl_calls, intro_on, monkeypatch, caplog):
    lead_id = _save_eligible_lead(lf_db)

    def boom(*args, **kwargs):
        raise RuntimeError(f"db down {OPT_OUT_PHONE} {OPT_OUT_EMAIL}")

    monkeypatch.setattr(lendingflow, "_suppression_reason", boom)
    with caplog.at_level(logging.DEBUG):
        followups = deliver_pending(lf_db, LendingFlowGhlSink(GhlAccount("k", "loc")), lead_id=lead_id, now=OPEN_NOW)
    assert len(followups) == 1
    assert _tag_calls(ghl_calls) == [["lendingflow"]]  # fail closed
    assert "7275550151" not in caplog.text and "example.com" not in caplog.text  # class name only, never the message
    assert any("suppression re-check failed" in r.getMessage() for r in caplog.records)


# ------------------------------------------------------------------ the greeting: the intro_name custom field (no database)

def _upsert_body(calls):
    return next(c[2] for c in calls if c[1] == "upsert")


@pytest.mark.parametrize("first_name, expected", [("Maria", "Maria"), ("  Maria ", "Maria"), (None, "there"),
                                                  ("", "there"), ("   ", "there")])
def test_intro_name_is_the_first_name_or_there(first_name, expected):
    assert _intro_name({"first_name": first_name}) == expected


def test_upsert_sets_the_intro_name_field_so_the_workflow_never_reads_an_empty_name(ghl_calls, intro_on):
    LendingFlowGhlSink(GhlAccount("k", "loc")).push({**_lead("present"), "first_name": "Maria"})
    assert _upsert_body(ghl_calls)["customFields"] == [{"id": "cf-intro-name", "value": "Maria"}]
    ghl_calls.clear()
    LendingFlowGhlSink(GhlAccount("k", "loc")).push(_lead("present"))  # no first name
    body = _upsert_body(ghl_calls)
    assert body["customFields"] == [{"id": "cf-intro-name", "value": "there"}]
    assert "firstName" not in body  # a missing name never overwrites the real name already on the GHL contact


def test_upsert_sends_no_custom_field_when_it_is_not_configured(ghl_calls, monkeypatch):
    monkeypatch.setattr(get_settings(), "lending_ghl_cf_intro_name", None)
    LendingFlowGhlSink(GhlAccount("k", "loc")).push(_lead("present"))
    assert "customFields" not in _upsert_body(ghl_calls)


def test_push_reports_whether_it_added_the_ready_tag(ghl_calls):
    sink = LendingFlowGhlSink(GhlAccount("k", "loc"))
    assert sink.push(_lead("present")).intro_ready_tagged is True
    assert sink.push(_lead("present", sms_clear=False)).intro_ready_tagged is False
    assert sink.push(_lead("missing")).intro_ready_tagged is False


# ------------------------------------------------------------------ off switch, send window, hold and release (database)

def _deliver(db, lead_id, now):
    return deliver_pending(db, LendingFlowGhlSink(GhlAccount("k", "loc")), lead_id=lead_id, now=now)


def _held_lead(db, *, received=RECEIVED, delivered_at=CLOSED_NOW):
    """A consenting lead delivered to GHL outside the send window: contact synced, ready tag withheld."""
    lead_id = _save_eligible_lead(db)
    _received_at(db, lead_id, received)
    _deliver(db, lead_id, delivered_at)
    assert _tagged_at(db, lead_id) is None
    return lead_id


def test_switch_off_means_nobody_is_ever_tagged(lf_db, ghl_calls, monkeypatch):
    monkeypatch.setattr(get_settings(), "lending_intro_sms_enabled", False)
    monkeypatch.setattr(get_settings(), "lending_ghl_cf_intro_name", "cf-intro-name")
    lead_id = _save_eligible_lead(lf_db)
    followups = _deliver(lf_db, lead_id, OPEN_NOW)
    assert len(followups) == 1  # the CRM delivery is unaffected
    assert _tag_calls(ghl_calls) == [["lendingflow"]]
    assert _tagged_at(lf_db, lead_id) is None
    assert release_held_intro_sms(lf_db, LendingFlowGhlSink(GhlAccount("k", "loc")), now=OPEN_NOW) == 0


def test_switch_on_without_the_intro_name_field_texts_nobody(lf_db, ghl_calls, intro_on, monkeypatch, caplog):
    monkeypatch.setattr(get_settings(), "lending_ghl_cf_intro_name", None)
    lead_id = _save_eligible_lead(lf_db)
    with caplog.at_level(logging.ERROR):
        _deliver(lf_db, lead_id, OPEN_NOW)
    assert _tag_calls(ghl_calls) == [["lendingflow"]]
    assert any("LENDING_GHL_CF_INTRO_NAME" in r.getMessage() for r in caplog.records)


def test_inside_the_window_the_lead_is_tagged_at_push_and_stamped(lf_db, ghl_calls, intro_on):
    lead_id = _save_eligible_lead(lf_db)
    _received_at(lf_db, lead_id, RECEIVED)
    _deliver(lf_db, lead_id, OPEN_NOW)
    assert _tag_calls(ghl_calls) == [["lendingflow"], ["lendingflow-intro-ready"]]
    assert _tagged_at(lf_db, lead_id) == OPEN_NOW


def test_outside_the_window_the_lead_is_delivered_but_held(lf_db, ghl_calls, intro_on):
    lead_id = _held_lead(lf_db)
    assert _tag_calls(ghl_calls) == [["lendingflow"]]
    assert _ghl_status(lf_db, lead_id) in ("synced", "contact_only")


def test_a_held_lead_is_tagged_once_when_the_window_opens(lf_db, ghl_calls, intro_on):
    lead_id = _held_lead(lf_db)
    sink = LendingFlowGhlSink(GhlAccount("k", "loc"))
    assert release_held_intro_sms(lf_db, sink, now=CLOSED_NOW) == 0  # still closed
    assert release_held_intro_sms(lf_db, sink, now=OPEN_NOW) == 1
    assert _tag_calls(ghl_calls) == [["lendingflow"], ["lendingflow-intro-ready"]]
    assert _tagged_at(lf_db, lead_id) == OPEN_NOW
    assert release_held_intro_sms(lf_db, sink, now=OPEN_NOW) == 0  # idempotent: no second tag call
    assert _tag_calls(ghl_calls) == [["lendingflow"], ["lendingflow-intro-ready"]]


def test_a_lead_is_never_released_a_day_late(lf_db, ghl_calls, intro_on):
    lead_id = _held_lead(lf_db, received=OPEN_NOW - timedelta(hours=25))
    assert release_held_intro_sms(lf_db, LendingFlowGhlSink(GhlAccount("k", "loc")), now=OPEN_NOW) == 0
    assert _tagged_at(lf_db, lead_id) is None


def test_a_borrower_who_opts_out_while_held_is_never_tagged(lf_db, ghl_calls, intro_on):
    lead_id = _held_lead(lf_db)
    _opt_out_by_phone(lf_db)
    assert release_held_intro_sms(lf_db, LendingFlowGhlSink(GhlAccount("k", "loc")), now=OPEN_NOW) == 0
    assert _tag_calls(ghl_calls) == [["lendingflow"]]
    assert _tagged_at(lf_db, lead_id) is None


def test_switching_off_stops_the_release(lf_db, ghl_calls, intro_on, monkeypatch):
    _held_lead(lf_db)
    monkeypatch.setattr(get_settings(), "lending_intro_sms_enabled", False)
    assert release_held_intro_sms(lf_db, LendingFlowGhlSink(GhlAccount("k", "loc")), now=OPEN_NOW) == 0
    assert _tag_calls(ghl_calls) == [["lendingflow"]]


def test_a_failed_release_is_left_for_the_next_sweep(lf_db, ghl_calls, intro_on):
    class TagFails:
        def add_intro_ready_tag(self, contact_id):
            raise DeliveryError("add intro-ready tag: HTTP 500")

    lead_id = _held_lead(lf_db)
    assert release_held_intro_sms(lf_db, TagFails(), now=OPEN_NOW) == 0
    assert _tagged_at(lf_db, lead_id) is None
    assert release_held_intro_sms(lf_db, LendingFlowGhlSink(GhlAccount("k", "loc")), now=OPEN_NOW) == 1


# ------------------------------------------------------------------ PR #360 review findings (database)

PACIFIC_PHONE, TAMPA_ZIP = "+16502530000", "33602"


def _make_pacific_with_tampa_property(db, lead_id):
    db.execute(text("UPDATE lending.lendingflow_leads SET phone = :p, property_zip = :z, property_state = 'FL' "
                    "WHERE id = :i"), {"p": PACIFIC_PHONE, "z": TAMPA_ZIP, "i": lead_id})


def test_a_pacific_borrower_with_a_tampa_property_is_held_until_8am_pacific(lf_db, ghl_calls, intro_on):
    """9:00 ET is 6:00 PT. The Tampa ZIP must not stand in for the phone's zone, at push or at release."""
    lead_id = _save_eligible_lead(lf_db)
    _make_pacific_with_tampa_property(lf_db, lead_id)
    nine_et = datetime(2026, 10, 14, 13, 0, tzinfo=timezone.utc)
    eleven_et = datetime(2026, 10, 14, 15, 0, tzinfo=timezone.utc)  # 8:00 PT
    _received_at(lf_db, lead_id, nine_et - timedelta(hours=2))
    _deliver(lf_db, lead_id, nine_et)  # initial delivery
    assert _tag_calls(ghl_calls) == [["lendingflow"]] and _tagged_at(lf_db, lead_id) is None
    sink = LendingFlowGhlSink(GhlAccount("k", "loc"))
    assert release_held_intro_sms(lf_db, sink, now=nine_et) == 0  # deferred release, still 6:00 PT
    assert release_held_intro_sms(lf_db, sink, now=eleven_et) == 1
    assert _tagged_at(lf_db, lead_id) == eleven_et


def test_an_overnight_lead_is_still_released_when_its_window_opens(lf_db, ghl_calls, intro_on):
    """Pacific phone + Florida property: open 11:00-20:00 ET. A lead arriving at 20:05 ET waits 14 h 55 min."""
    lead_id = _save_eligible_lead(lf_db)
    _make_pacific_with_tampa_property(lf_db, lead_id)
    arrived = datetime(2026, 10, 14, 0, 5, tzinfo=timezone.utc)  # 20:05 ET the evening before
    opens = datetime(2026, 10, 14, 15, 0, tzinfo=timezone.utc)   # 11:00 ET = 8:00 PT
    _received_at(lf_db, lead_id, arrived)
    _deliver(lf_db, lead_id, arrived + timedelta(minutes=1))
    assert _tagged_at(lf_db, lead_id) is None
    assert release_held_intro_sms(lf_db, LendingFlowGhlSink(GhlAccount("k", "loc")), now=opens) == 1
    assert _tagged_at(lf_db, lead_id) == opens
