"""Recording state, the BatchDialer contact link and the dialer-result mapping, from the call record alone (pure)."""
from __future__ import annotations

from src.lending.dispositions import contact_view_url, parse_event, recording_status_for, resolve_dialer_code


def test_view_url_uses_the_contact_id():
    assert contact_view_url("559662160") == "https://app.batchdialer.com/contacts/view/559662160"


def test_no_contact_id_gives_no_view_url():
    assert contact_view_url(None) is None and contact_view_url("") is None


def test_talk_time_means_recorded():
    assert recording_status_for(48, True) == "recorded" and recording_status_for(48, None) == "recorded"


def test_no_talk_time_means_none():
    assert recording_status_for(0, True) == "none" and recording_status_for(None, True) == "none"


def test_recording_switched_off_means_none():
    assert recording_status_for(48, False) == "none"


def test_recording_flag_is_parsed_from_the_cdr():
    ev = parse_event({"id": 1223346777, "duration": 48, "recordingenabled": 1, "contact": {"id": 559662160}})
    assert ev.recording_enabled is True and ev.contact_id == "559662160"
    assert parse_event({"id": 1, "recordingenabled": 0}).recording_enabled is False
    assert parse_event({"id": 2}).recording_enabled is None


# ── "Unknown" and machine hang-ups (resolve_dialer_code) ─────────────────────

def test_unknown_with_no_talk_time_is_no_answer():
    assert resolve_dialer_code("Unknown", 0) == ("NO_ANSWER", True)
    assert resolve_dialer_code("Unknown", None) == ("NO_ANSWER", True)


def test_unknown_with_talk_time_has_no_code_and_is_not_an_unknown_code():
    assert resolve_dialer_code("Unknown", 288) == (None, True)


def test_answering_machine_is_no_answer():
    assert resolve_dialer_code("Answering Machine", 0) == ("NO_ANSWER", True)


def test_caller_picked_left_voicemail_is_kept():
    assert resolve_dialer_code("LEFT_VOICEMAIL", 48) == ("LEFT_VOICEMAIL", True)
