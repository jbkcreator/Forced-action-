"""tests/agents/reply_concierge/test_wp_t2_6_end_to_end.py

One path, start to finish, with a mocked DB session but real (unmocked)
function wiring across backflip_email_parser -> backflip_stage_ingest ->
fa_max_file_state -> stage_monitor. Proves the modules actually compose,
not just that each one passes its own isolated unit tests.

fetchone() call order traced against the real code (not guessed):
  1. backflip_stage_ingest.resolve_opportunity_by_backflip_ref()
  2. fa_max_file_state.ensure_file_state() -> get_file_state() -- an
     existing row is returned, so ensure_file_state() short-circuits
     before it ever reaches _best_effort_contact_email() or issues an
     INSERT (no extra fetchone() call to account for there).
  3. backflip_stage_ingest.apply_parsed_event()'s own second
     get_file_state() call (line ~73), made to pass contact_email into
     stage_monitor.send_first_chase_touch().

record_document_request()'s INSERT ... ON CONFLICT DO NOTHING has no
RETURNING clause, so it calls session.execute() but never .fetchone() --
it does not consume a side_effect slot. Because the mocked file_state's
contact_email is None, send_first_chase_touch() logs and returns False
immediately (no governance/consent queries), so only 3 fetchone() calls
happen in total.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from src.agents.reply_concierge.backflip_email_parser import parse_backflip_notification
from src.agents.reply_concierge.backflip_stage_ingest import apply_parsed_event


def test_document_request_email_flows_to_file_state_row():
    db = MagicMock()
    resolved_row = MagicMock()
    resolved_row._mapping = {"opportunity_id": "opp-1", "person_id": "p-1"}

    existing_file_state_row = MagicMock()
    existing_file_state_row._mapping = {
        "opportunity_id": "opp-1", "person_id": "p-1", "backflip_stage": "submitted",
        "contact_email": None, "last_stage_change_at": None,
        "last_borrower_touch_at": None, "expected_next_stage": None,
        "stall_flagged_at": None,
    }

    # 1: resolve_opportunity_by_backflip_ref. 2: ensure_file_state's
    # get_file_state lookup (existing row -> no INSERT branch hit). 3:
    # apply_parsed_event's own get_file_state() call used to feed
    # send_first_chase_touch (contact_email=None -> it skips, no further
    # DB calls, so no fourth entry is needed).
    db.execute.return_value.fetchone.side_effect = [
        resolved_row,
        existing_file_state_row,
        existing_file_state_row,
    ]

    event = parse_backflip_notification(
        "Action needed on BF-77812: Documents requested",
        "We need the following document: Bank Statement (last 2 months).",
    )
    assert event is not None
    assert event.event_type == "document_request"

    applied = apply_parsed_event(db, event, source="email_parsed", actor="backflip_email_poller")
    assert applied is True

    insert_calls = [
        call for call in db.execute.call_args_list
        if "INSERT INTO fa_max_document_requests" in str(call.args[0])
    ]
    assert len(insert_calls) == 1
    assert insert_calls[0].args[1]["document_name"] == "Bank Statement (last 2 months)"
