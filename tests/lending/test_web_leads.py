"""WP-GL-11: website lead form service — validation, consent evidence, delivery state."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from config.lending_web import GHL_MAX_ATTEMPTS, SMS_CONSENT_TEXT
from src.lending.consent import has_text_consent
from src.lending.web_leads import (
    DeliveryError,
    InvalidWebLead,
    PushResult,
    build_input,
    deliver_pending,
    save_web_lead,
)

PHONE_RAW = "(813) 555-0142"
PHONE = "+18135550142"


def _form(**overrides):
    form = {"name": "Dana Builder", "phone": PHONE_RAW, "email": "Dana@Example.com", "property_city": "Tampa",
            "deal_type": "Fix and flip", "completed_projects_3y": "1 to 2"}
    form.update(overrides)
    return form


def _data(**overrides):
    return build_input(_form(**overrides), ip_address="203.0.113.9", user_agent="pytest")


class RecordingSink:
    def __init__(self, result=None, error=None):
        self.result = result or PushResult(contact_id="ghl_1", pipeline_card=True)
        self.error = error
        self.pushed = []

    def push(self, lead):
        self.pushed.append(dict(lead))
        if self.error:
            raise self.error
        return self.result


def _row(db, lead_id):
    return db.execute(text("SELECT * FROM lending.web_leads WHERE id = :i"), {"i": lead_id}).mappings().one()


def test_build_input_normalizes_phone_and_lowercases_email():
    data = _data()
    assert data.phone == PHONE and data.email == "dana@example.com"
    assert data.sms_consent is False and data.deal_drop_optin is False  # unticked by default


@pytest.mark.parametrize("overrides,message", [
    ({"name": "  "}, "name"),
    ({"phone": "12345"}, "mobile"),
    ({"phone": ""}, "mobile"),
    ({"email": "not-an-email"}, "email"),
])
def test_build_input_rejects_unusable_submissions(overrides, message):
    with pytest.raises(InvalidWebLead, match=message):
        _data(**overrides)


def test_build_input_strips_control_characters_and_caps_length():
    data = _data(name="Dana\x00\nBuilder", property_city="x" * 500)
    assert "\x00" not in data.name and "\n" not in data.name
    assert len(data.property_city) == 80


@pytest.mark.parametrize("value,expected", [("yes", True), ("YES", True), ("on", True), ("no", False), ("", False), (None, False)])
def test_consent_flag_parsing(value, expected):
    assert _data(sms_consent=value).sms_consent is expected


def test_unticked_lead_is_saved_without_any_text_consent(web_leads_db):
    lead_id, created = save_web_lead(web_leads_db, _data())
    row = _row(web_leads_db, lead_id)
    assert created and row["sms_consent"] is False and row["ghl_status"] == "pending"
    assert has_text_consent(web_leads_db, PHONE) is False
    assert web_leads_db.execute(text("SELECT count(*) FROM lending.text_consents WHERE phone = :p"), {"p": PHONE}).scalar() == 0


def test_ticked_lead_records_consent_evidence(web_leads_db):
    data = build_input(_form(sms_consent="yes", consent_text=SMS_CONSENT_TEXT, page_url="https://nextdeallending.com/"),
                       ip_address="203.0.113.9", user_agent="pytest")
    lead_id, _ = save_web_lead(web_leads_db, data)
    row = _row(web_leads_db, lead_id)
    assert row["sms_consent"] is True and row["consent_text"] == SMS_CONSENT_TEXT
    assert row["consent_text_matches"] is True
    assert row["ip_address"] == "203.0.113.9" and row["page_url"] == "https://nextdeallending.com/"
    assert row["received_at"] is not None
    assert has_text_consent(web_leads_db, PHONE) is True
    source = web_leads_db.execute(text("SELECT source FROM lending.text_consents WHERE phone = :p"), {"p": PHONE}).scalar()
    assert source == "web_form"


def test_changed_label_text_is_stored_verbatim_and_flagged(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data(sms_consent="yes", consent_text="I agree to texts."))
    row = _row(web_leads_db, lead_id)
    assert row["consent_text"] == "I agree to texts." and row["consent_text_matches"] is False


def test_suppressed_number_is_saved_but_never_gains_consent(web_leads_db):
    web_leads_db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES (:p, 'OPT_OUT', 'sms')"), {"p": PHONE})
    lead_id, created = save_web_lead(web_leads_db, _data(sms_consent="yes"))
    row = _row(web_leads_db, lead_id)
    assert created and row["suppressed"] is True and row["sms_consent"] is True  # evidence of what was ticked
    assert has_text_consent(web_leads_db, PHONE) is False


def test_suppressed_by_email_alone(web_leads_db):
    web_leads_db.execute(text("INSERT INTO lending.suppression_list (email, reason, source_channel) VALUES ('dana@example.com', 'OPT_OUT', 'email')"))
    lead_id, _ = save_web_lead(web_leads_db, _data(phone="(813) 555-0177"))
    assert _row(web_leads_db, lead_id)["suppressed"] is True


def test_the_reason_a_lead_was_suppressed_is_stored(web_leads_db):
    web_leads_db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES (:p, 'OPT_OUT', 'sms')"), {"p": PHONE})
    on_list, _ = save_web_lead(web_leads_db, _data())
    web_leads_db.execute(text("INSERT INTO lending.contacts (phone, do_not_contact) VALUES ('+18135550166', true)"))
    flagged, _ = save_web_lead(web_leads_db, _data(phone="(813) 555-0166"))
    clear, _ = save_web_lead(web_leads_db, _data(phone="(813) 555-0155"))
    assert [_row(web_leads_db, i)["suppression_reason"] for i in (on_list, flagged, clear)] == [
        "suppression_list", "do_not_contact", None]
    assert [_row(web_leads_db, i)["suppressed"] for i in (on_list, flagged, clear)] == [True, True, False]


def test_the_opt_out_list_is_the_reason_when_both_gates_fire(web_leads_db):
    web_leads_db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES (:p, 'OPT_OUT', 'sms')"), {"p": PHONE})
    web_leads_db.execute(text("INSERT INTO lending.contacts (phone, do_not_contact) VALUES (:p, true)"), {"p": PHONE})
    lead_id, _ = save_web_lead(web_leads_db, _data())
    assert _row(web_leads_db, lead_id)["suppression_reason"] == "suppression_list"


def test_unticked_later_submission_does_not_erase_earlier_consent(web_leads_db):
    save_web_lead(web_leads_db, _data(sms_consent="yes"))
    web_leads_db.execute(text("UPDATE lending.web_leads SET received_at = now() - interval '1 day'"))
    save_web_lead(web_leads_db, _data(sms_consent="no"))
    assert has_text_consent(web_leads_db, PHONE) is True
    assert web_leads_db.execute(text("SELECT count(*) FROM lending.web_leads")).scalar() == 2


def test_double_submit_inside_the_window_is_one_lead(web_leads_db):
    first, created_first = save_web_lead(web_leads_db, _data())
    second, created_second = save_web_lead(web_leads_db, _data())
    assert (created_first, created_second) == (True, False) and first == second


def test_pending_lead_is_delivered_and_marked_synced(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data())
    sink = RecordingSink()
    assert deliver_pending(web_leads_db, sink, lead_id=lead_id) == 1
    row = _row(web_leads_db, lead_id)
    assert row["ghl_status"] == "synced" and row["ghl_contact_id"] == "ghl_1" and row["ghl_attempts"] == 1
    assert row["ghl_synced_at"] is not None
    assert deliver_pending(web_leads_db, sink, lead_id=lead_id) == 0  # synced leads are never re-sent
    assert len(sink.pushed) == 1


def test_lead_without_a_pipeline_stage_is_contact_only(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data())
    deliver_pending(web_leads_db, RecordingSink(PushResult("ghl_9", pipeline_card=False)), lead_id=lead_id)
    assert _row(web_leads_db, lead_id)["ghl_status"] == "contact_only"


def test_delivery_failure_keeps_the_lead_and_counts_the_attempt(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data())
    assert deliver_pending(web_leads_db, RecordingSink(error=DeliveryError("contact upsert: HTTP 500")), lead_id=lead_id) == 0
    row = _row(web_leads_db, lead_id)
    assert row["ghl_status"] == "failed" and row["ghl_attempts"] == 1 and row["ghl_last_error"] == "contact upsert: HTTP 500"


def test_unexpected_sink_error_is_recorded_without_leaking_its_message(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data())
    deliver_pending(web_leads_db, RecordingSink(error=RuntimeError("secret phone +18135550142")), lead_id=lead_id)
    assert _row(web_leads_db, lead_id)["ghl_last_error"] == "unexpected RuntimeError"


def test_sweep_retries_failed_leads_only_after_the_wait(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data())
    now = datetime.now(timezone.utc)
    deliver_pending(web_leads_db, RecordingSink(error=DeliveryError("x")), lead_id=lead_id, now=now)
    sink = RecordingSink()
    assert deliver_pending(web_leads_db, sink, now=now + timedelta(seconds=30)) == 0  # too soon
    assert deliver_pending(web_leads_db, sink, now=now + timedelta(minutes=5)) == 1
    assert _row(web_leads_db, lead_id)["ghl_status"] == "synced"


def test_leads_past_the_attempt_cap_are_left_failed(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data())
    web_leads_db.execute(text("UPDATE lending.web_leads SET ghl_status = 'failed', ghl_attempts = :n WHERE id = :i"),
                         {"n": GHL_MAX_ATTEMPTS, "i": lead_id})
    sink = RecordingSink()
    assert deliver_pending(web_leads_db, sink, now=datetime.now(timezone.utc) + timedelta(hours=1)) == 0
    assert sink.pushed == []


def test_without_ghl_configured_leads_wait_and_no_attempt_is_burned(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data())
    assert deliver_pending(web_leads_db, None, lead_id=lead_id) == 0
    row = _row(web_leads_db, lead_id)
    assert row["ghl_status"] == "pending" and row["ghl_attempts"] == 0
