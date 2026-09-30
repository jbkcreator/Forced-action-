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
