"""WP-GL-11: website lead form service — validation, consent evidence, delivery state."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from config.lending_web import CALL_CONSENT_TEXT, SMS_CONSENT_TEXT
from src.lending.consent import has_text_consent
from src.lending.web_leads import (
    DeliveryError,
    InvalidWebLead,
    PushResult,
    build_input,
    deliver_pending,
    returning_alert_text,
    merge_into_existing,
    save_web_lead,
    will_be_contacted,
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


def test_the_three_boxes_are_read_independently():
    data = _data(call_consent="yes")
    assert (data.sms_consent, data.call_consent, data.deal_drop_optin) == (False, True, False)
    data = _data(sms_consent="yes", deal_drop_optin="yes")
    assert (data.sms_consent, data.call_consent, data.deal_drop_optin) == (True, False, True)


def test_the_new_form_fields_are_read_and_the_state_is_normalised():
    data = _data(loan_amount="$250,000", property_address="12 Oak Ave", property_state="florida", credit_band="680-719")
    assert (data.loan_amount, data.property_address, data.property_state, data.credit_band) == (
        250_000, "12 Oak Ave", "FL", "680-719")
    assert _data(property_state="fl").property_state == "FL"
    assert _data(property_state="Ontario").property_state == "Ontario"  # unrecognised: kept as sent, never dropped
    empty = _data()
    assert (empty.loan_amount, empty.property_address, empty.property_state, empty.credit_band) == (None, None, None, None)


def test_the_new_text_fields_are_capped_and_stripped_of_control_characters():
    data = _data(property_address="1 Main\x00 St" + "x" * 500, credit_band="y" * 200)
    assert "\x00" not in data.property_address and len(data.property_address) == 200
    assert len(data.credit_band) == 20  # the column's width


@pytest.mark.parametrize("typed,dollars", [("$250,000", 250_000), ("250000", 250_000), ("250k", 250_000),
                                           ("$1.5M", 1_500_000), (" 75K ", 75_000), ("1,500,000.50", 1_500_000)])
def test_a_typed_loan_amount_becomes_whole_dollars(typed, dollars):
    assert _data(loan_amount=typed).loan_amount == dollars


@pytest.mark.parametrize("typed", ["abc", "0", "-5", "1,2,3", "250 thousand", "999999999999", "$"])
def test_a_loan_amount_that_is_not_an_amount_is_refused_not_stored_as_nothing(typed):
    with pytest.raises(InvalidWebLead, match="loan amount"):
        _data(loan_amount=typed)


def test_a_blank_loan_amount_is_simply_no_answer():
    assert _data(loan_amount="").loan_amount is None and _data(loan_amount=None).loan_amount is None


def test_the_weekly_deal_drop_is_by_email_so_it_needs_an_email_address():
    with pytest.raises(InvalidWebLead, match="email"):
        _data(deal_drop_optin="yes", email="")
    with pytest.raises(InvalidWebLead, match="email"):
        _data(deal_drop_optin="yes", email=None)
    assert _data(deal_drop_optin="yes").deal_drop_optin is True  # the default form has an email
    assert _data(email=None).deal_drop_optin is False  # unticked: no email needed


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


def test_a_call_only_lead_stores_the_call_box_and_never_gains_text_consent(web_leads_db):
    data = _data(call_consent="yes", call_consent_text=CALL_CONSENT_TEXT)
    lead_id, _ = save_web_lead(web_leads_db, data)
    row = _row(web_leads_db, lead_id)
    assert row["call_consent"] is True and row["sms_consent"] is False
    assert row["call_consent_text"] == CALL_CONSENT_TEXT and row["call_consent_text_matches"] is True
    assert row["consent_text"] is None and row["consent_text_matches"] is None
    assert has_text_consent(web_leads_db, PHONE) is False  # a call box is not consent to text
    assert web_leads_db.execute(text("SELECT count(*) FROM lending.text_consents WHERE phone = :p"), {"p": PHONE}).scalar() == 0


def test_a_text_only_lead_does_not_record_call_consent(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data(sms_consent="yes", consent_text=SMS_CONSENT_TEXT))
    row = _row(web_leads_db, lead_id)
    assert row["sms_consent"] is True and row["call_consent"] is False and row["call_consent_text_matches"] is None
    assert has_text_consent(web_leads_db, PHONE) is True


def test_a_changed_call_label_is_stored_verbatim_and_flagged(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data(call_consent="yes", call_consent_text="Call me."))
    row = _row(web_leads_db, lead_id)
    assert row["call_consent_text"] == "Call me." and row["call_consent_text_matches"] is False


def test_the_new_fields_are_stored(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data(loan_amount="250k", property_address="12 Oak Ave",
                                                   property_state="Florida", credit_band="680-719"))
    row = _row(web_leads_db, lead_id)
    assert (row["loan_amount"], row["property_address"], row["property_state"], row["credit_band"]) == (
        250_000, "12 Oak Ave", "FL", "680-719")


def test_will_be_contacted_needs_the_text_box_or_the_call_box_and_a_clear_number(web_leads_db):
    none, _ = save_web_lead(web_leads_db, _data(phone="(813) 555-0101"))
    text_only, _ = save_web_lead(web_leads_db, _data(phone="(813) 555-0102", sms_consent="yes"))
    call_only, _ = save_web_lead(web_leads_db, _data(phone="(813) 555-0103", call_consent="yes"))
    web_leads_db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES ('+18135550104', 'OPT_OUT', 'sms')"))
    blocked, _ = save_web_lead(web_leads_db, _data(phone="(813) 555-0104", call_consent="yes"))
    assert [will_be_contacted(web_leads_db, i) for i in (none, text_only, call_only, blocked)] == [False, True, True, False]


def test_a_text_box_and_a_call_box_submission_are_not_the_same_duplicate(web_leads_db):
    first, created_first = save_web_lead(web_leads_db, _data(sms_consent="yes"))
    second, created_second = save_web_lead(web_leads_db, _data(call_consent="yes"))
    assert created_first and created_second and first != second


def test_a_repeat_fills_in_missing_new_fields_and_never_overwrites_or_touches_consent(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data(sms_consent="yes", loan_amount="$100K"))
    again = _data(sms_consent="yes", loan_amount="$900K", property_address="12 Oak Ave", credit_band="720+",
                  property_state="TX")
    assert merge_into_existing(web_leads_db, lead_id, again) is True
    row = _row(web_leads_db, lead_id)
    assert (row["loan_amount"], row["property_address"], row["credit_band"], row["property_state"]) == (
        100_000, "12 Oak Ave", "720+", "TX")  # the stored amount is kept, the gaps are filled
    assert row["sms_consent"] is True and row["call_consent"] is False
    assert merge_into_existing(web_leads_db, lead_id, again) is False  # nothing new the second time


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


def _fail_once(db, lead_id, now, **error_kwargs):
    deliver_pending(db, RecordingSink(error=DeliveryError("contact upsert: HTTP 500", **error_kwargs)), lead_id=lead_id, now=now)


@pytest.mark.parametrize("failures,wait_minutes", [(1, 2), (2, 5), (3, 15), (4, 60), (5, 240), (9, 240)])
def test_the_wait_before_a_retry_grows_with_each_failure_and_then_holds(web_leads_db, failures, wait_minutes):
    lead_id, _ = save_web_lead(web_leads_db, _data())
    now = datetime.now(timezone.utc)
    web_leads_db.execute(text("UPDATE lending.web_leads SET ghl_status = 'failed', ghl_attempts = :n, ghl_last_attempt_at = :t WHERE id = :i"),
                         {"n": failures, "t": now, "i": lead_id})
    assert deliver_pending(web_leads_db, RecordingSink(), now=now + timedelta(minutes=wait_minutes, seconds=-30)) == 0
    assert deliver_pending(web_leads_db, RecordingSink(), now=now + timedelta(minutes=wait_minutes, seconds=30)) == 1


def test_a_long_outage_does_not_strand_the_lead_it_is_delivered_once_ghl_recovers(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data())
    now = datetime.now(timezone.utc)
    _fail_once(web_leads_db, lead_id, now)
    for hours in range(2, 17, 2):  # sweeps every two hours for 16h, all failing: far past the old 20-minute budget
        deliver_pending(web_leads_db, RecordingSink(error=DeliveryError("contact upsert: HTTP 503")), now=now + timedelta(hours=hours))
    row = _row(web_leads_db, lead_id)
    assert row["ghl_status"] == "failed" and row["ghl_attempts"] >= 6
    recovered = RecordingSink()
    assert deliver_pending(web_leads_db, recovered, now=now + timedelta(hours=30)) == 1
    assert _row(web_leads_db, lead_id)["ghl_status"] == "synced" and len(recovered.pushed) == 1


def test_a_rejected_key_does_not_use_up_attempts_and_is_retried_every_sweep(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data())
    now = datetime.now(timezone.utc)
    for step in range(8):
        deliver_pending(web_leads_db, RecordingSink(error=DeliveryError("contact upsert: HTTP 401", config_error=True)),
                        now=now + timedelta(minutes=5 * step), **({"lead_id": lead_id} if step == 0 else {}))
    row = _row(web_leads_db, lead_id)
    assert row["ghl_attempts"] == 0 and row["ghl_status"] == "failed" and row["ghl_last_error"] == "contact upsert: HTTP 401"
    fixed = RecordingSink()
    assert deliver_pending(web_leads_db, fixed, now=now + timedelta(minutes=45)) == 1  # key fixed: next sweep delivers


def test_a_lead_older_than_the_give_up_window_is_left_alone(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data())
    web_leads_db.execute(text("UPDATE lending.web_leads SET ghl_status = 'failed', received_at = now() - interval '73 hours' WHERE id = :i"),
                         {"i": lead_id})
    sink = RecordingSink()
    assert deliver_pending(web_leads_db, sink) == 0 and sink.pushed == []


def test_without_ghl_configured_leads_wait_and_no_attempt_is_burned(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data())
    assert deliver_pending(web_leads_db, None, lead_id=lead_id) == 0
    row = _row(web_leads_db, lead_id)
    assert row["ghl_status"] == "pending" and row["ghl_attempts"] == 0


# Chunk 1 Addendum 1.2.2 / 1.1.2

@pytest.mark.parametrize("name,moved,expected", [
    ("Dana Builder", True, ":recycle: Returning Borrower Resubmitted Form: Dana B. - Moved to New Lead"),
    ("Dana", False, ":recycle: Returning Borrower Resubmitted Form: Dana - Left in its current stage"),
])
def test_returning_alert_names_the_borrower_by_first_name_and_initial(name, moved, expected):
    text_ = returning_alert_text(name, lead_id=7, moved=moved, contact_url=None)
    assert text_.startswith(expected) and "Lead `7`" in text_ and "Builder" not in text_


def test_returning_alert_links_the_ghl_contact():
    assert "<https://ghl/c|Open in GHL>" in returning_alert_text("Dana Builder", 7, True, "https://ghl/c")


@pytest.mark.parametrize("answer,expected", [("yes", "owner_occupied"), ("no", "investment"), (None, "unknown")])
def test_build_input_reads_the_occupancy_answer(answer, expected):
    assert _data(occupancy=answer).occupancy == expected


def test_a_returning_lead_is_alerted_once(web_leads_db, monkeypatch):
    posted = []
    monkeypatch.setattr("src.lending.web_leads.post_ops_alert", lambda message: posted.append(message) or True)
    lead_id, _ = save_web_lead(web_leads_db, _data())
    sink = RecordingSink(PushResult("ghl_1", pipeline_card=True, returning=True, moved_to_new_lead=True))
    deliver_pending(web_leads_db, sink, lead_id=lead_id)
    web_leads_db.execute(text("UPDATE lending.web_leads SET ghl_status = 'pending' WHERE id = :i"), {"i": lead_id})
    deliver_pending(web_leads_db, sink, lead_id=lead_id)  # a re-delivery (merged details) never re-alerts
    assert len(posted) == 1 and "Moved to New Lead" in posted[0]
    assert _row(web_leads_db, lead_id)["returning_alerted_at"] is not None


def test_a_first_time_lead_is_not_alerted(web_leads_db, monkeypatch):
    posted = []
    monkeypatch.setattr("src.lending.web_leads.post_ops_alert", lambda message: posted.append(message) or True)
    lead_id, _ = save_web_lead(web_leads_db, _data())
    deliver_pending(web_leads_db, RecordingSink(), lead_id=lead_id)
    assert posted == []


def test_an_owner_occupied_lead_is_saved_but_never_delivered(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data(occupancy="yes", sms_consent="yes"))
    assert _row(web_leads_db, lead_id)["ghl_status"] == "excluded"
    sink = RecordingSink()
    assert deliver_pending(web_leads_db, sink, lead_id=lead_id) == 0 and sink.pushed == []


def test_a_returning_alert_slack_refuses_is_queued_in_coras_inbox(web_leads_db, monkeypatch):
    queued = []
    monkeypatch.setattr("src.lending.web_leads.post_ops_alert", lambda message: False)
    monkeypatch.setattr("src.lending.web_leads.add_to_inbox_in_own_session", lambda **event: queued.append(event))
    lead_id, _ = save_web_lead(web_leads_db, _data())
    deliver_pending(web_leads_db, RecordingSink(PushResult("ghl_1", pipeline_card=True, returning=True)), lead_id=lead_id)
    assert [e["event_type"] for e in queued] == ["web_lead_returning_alert_failed"]


def test_the_latest_occupancy_answer_wins_inside_the_dedup_window(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data(occupancy="no"))
    assert merge_into_existing(web_leads_db, lead_id, _data(occupancy="yes")) is False
    assert _row(web_leads_db, lead_id)["ghl_status"] == "excluded"
    assert merge_into_existing(web_leads_db, lead_id, _data(occupancy="no")) is True  # released: delivered again
    assert _row(web_leads_db, lead_id)["ghl_status"] == "pending"


def test_a_yes_answer_is_applied_even_when_the_same_repeat_adds_a_detail(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data(email=None, occupancy="no"))
    again = _data(email="dana@example.com", property_address="12 Oak Ave", occupancy="yes")
    assert merge_into_existing(web_leads_db, lead_id, again) is True  # the detail was added
    row = _row(web_leads_db, lead_id)
    assert (row["ghl_status"], row["occupancy"], row["email"]) == ("excluded", "owner_occupied", "dana@example.com")
    sink = RecordingSink()
    assert deliver_pending(web_leads_db, sink, lead_id=lead_id) == 0 and sink.pushed == []


def test_a_no_answer_releases_an_excluded_lead_even_when_the_same_repeat_adds_a_detail(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data(email=None, occupancy="yes"))
    again = _data(email="dana@example.com", occupancy="no")
    assert merge_into_existing(web_leads_db, lead_id, again) is True
    row = _row(web_leads_db, lead_id)
    assert (row["ghl_status"], row["occupancy"], row["email"]) == ("pending", "investment", "dana@example.com")
    sink = RecordingSink()
    assert deliver_pending(web_leads_db, sink, lead_id=lead_id) == 1 and len(sink.pushed) == 1


def test_a_yes_answer_with_a_new_detail_does_not_exclude_a_lead_already_in_ghl(web_leads_db):
    lead_id, _ = save_web_lead(web_leads_db, _data(email=None, occupancy="no"))
    deliver_pending(web_leads_db, RecordingSink(), lead_id=lead_id)
    assert merge_into_existing(web_leads_db, lead_id, _data(email="dana@example.com", occupancy="yes")) is True
    row = _row(web_leads_db, lead_id)
    assert row["occupancy"] == "owner_occupied" and row["ghl_status"] == "pending"  # re-queued, never excluded after delivery


@pytest.mark.parametrize("name", ["<!channel>", "<!here> Smith", "<@U123ABC> Builder", "<https://attacker.example|Dana>", "A&B <b>"])
def test_returning_alert_escapes_slack_markup_in_the_name(name):
    text_ = returning_alert_text(name, lead_id=7, moved=True, contact_url=None)
    body = text_.split(": ", 1)[1].split(" - ", 1)[0]
    assert "<" not in body and ">" not in body
    assert "<!channel>" not in text_ and "<!here>" not in text_ and "<@U123ABC>" not in text_ and "attacker.example|" not in text_


def test_returning_alert_keeps_its_own_ghl_link_live():
    text_ = returning_alert_text("<!channel>", 7, True, "https://ghl/c")
    assert "&lt;!channel&gt;" in text_ and "<https://ghl/c|Open in GHL>" in text_
