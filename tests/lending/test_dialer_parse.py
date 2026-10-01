"""parse_event / normalize_code on the CDR sample from BatchDialer's API documentation
(GET /api/v2/cdrs/last, 'Response Example'). Documentation sample, not a live capture."""
from __future__ import annotations

from src.lending import dispositions as d

DOC_CDR = {
    "id": 123456, "direction": "out", "callStartTime": "2026-02-20T14:30:00Z", "callEndTime": "2026-02-20T14:32:30Z",
    "did": "5551234567", "customerNumber": "5559876543", "disposition": "ANSWER", "mood": "", "duration": 150,
    "status": "active", "agent": {"id": 42, "firstname": "John", "lastname": "Doe"},
    "contact": {"id": 789, "firstname": "Jane", "lastname": "Smith", "status": "active"},
    "campaign": {"id": 10, "name": "Spring Campaign"}, "client": {"id": 5, "name": "Acme Corp"},
    "callid": "abc-def-123", "voicemailid": None, "recordingenabled": 1,
    "callRecordUrl": "/api/callrecording/123456", "comments": ["Called back"],
}


def test_documented_cdr_maps_every_field_we_use():
    ev = d.parse_event(DOC_CDR)
    assert (ev.call_id, ev.contact_id, ev.seat_id, ev.seat_name) == ("123456", "789", "42", "John Doe")
    assert (ev.campaign_id, ev.caller_id_number, ev.phone, ev.duration) == ("10", "5551234567", "5559876543", 150)
    assert ev.started_at.isoformat() == "2026-02-20T14:30:00+00:00" and ev.ended_at.isoformat() == "2026-02-20T14:32:30+00:00"
    assert ev.recording_ref == "https://app.batchdialer.com/api/callrecording/123456"


def test_direction_out_is_stored_as_outbound_so_the_attempt_cap_counts_it():
    assert d.parse_event(DOC_CDR).direction == "outbound"
    assert d.parse_event({**DOC_CDR, "direction": "in"}).direction == "inbound"
    assert d.parse_event({**DOC_CDR, "direction": "outbound"}).direction == "outbound"


def test_answer_status_is_not_an_unknown_code():
    assert d.normalize_code("ANSWER") == (None, True)
    assert d.normalize_code("Answered") == (None, True)


def test_telephony_no_answer_maps_to_our_code():
    assert d.normalize_code("NO ANSWER") == ("NO_ANSWER", True)
    assert d.normalize_code("Busy") == ("NO_ANSWER", True)
    assert d.normalize_code("FAILED") == ("CALL_FAILED", True)


def test_a_cdr_with_no_id_is_rejected():
    bad = dict(DOC_CDR)
    del bad["id"]
    assert d.parse_event(bad) is None


def test_unknown_direction_is_stored_as_null_not_as_is():
    assert d.parse_event({**DOC_CDR, "direction": "outgoing"}).direction is None
    assert d.parse_event({**DOC_CDR, "direction": "2"}).direction is None


def test_direction_is_case_insensitive_and_blank_is_none():
    assert d.parse_event({**DOC_CDR, "direction": "OUT"}).direction == "outbound"
    assert d.parse_event({**DOC_CDR, "direction": None}).direction is None
    assert d.parse_event({**DOC_CDR, "direction": ""}).direction is None


def test_seat_name_with_only_a_first_name():
    cdr = {**DOC_CDR, "agent": {"id": 42, "firstname": "John"}}
    assert d.parse_event(cdr).seat_name == "John"


def test_absolute_recording_url_is_unchanged():
    assert d.parse_event({**DOC_CDR, "callRecordUrl": "https://x/rec"}).recording_ref == "https://x/rec"


# The 13 results created in BatchDialer (group "Lending"): 3 built-ins keep their own names.
BATCHDIALER_RESULT_NAMES = {
    "No Answer": "NO_ANSWER",
    "LEFT_VOICEMAIL": "LEFT_VOICEMAIL",
    "BAD_NUMBER": "BAD_NUMBER",
    "CALL_FAILED": "CALL_FAILED",
    "WRONG_PERSON": "WRONG_PERSON",
    "NOT_DECISION_MAKER": "NOT_DECISION_MAKER",
    "REFERRED": "REFERRED",
    "Do Not Call": "DNC_REQUEST",
    "CONNECTED_NOT_INTERESTED": "CONNECTED_NOT_INTERESTED",
    "Call Back": "CALLBACK_REQUESTED",
    "DATA_NURTURE_ONLY": "DATA_NURTURE_ONLY",
    "GATE_FAILED_NURTURE": "GATE_FAILED_NURTURE",
    "BOOKED": "BOOKED",
}


def test_every_result_created_in_batchdialer_maps_to_its_code():
    from config.lending_dispositions import DISPOSITIONS

    assert set(BATCHDIALER_RESULT_NAMES.values()) == set(DISPOSITIONS)
    for name, code in BATCHDIALER_RESULT_NAMES.items():
        assert d.normalize_code(name) == (code, True), name
