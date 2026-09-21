"""tests/agents/reply_concierge/test_backflip_email_parser.py

No real Backflip notification email samples exist yet (Q7/Q8/Q13 still
open per the client clarifications doc) -- these fixtures are an assumed,
reasonable format. Recalibrate the moment real samples arrive; the parser
is isolated into this one module specifically so that recalibration never
touches stage_monitor.py, fa_max_file_state.py, or the poller wiring.
"""
from __future__ import annotations

from src.agents.reply_concierge.backflip_email_parser import parse_backflip_notification


def test_stage_change_under_review():
    subject = "Application BF-10293 — Now Under Review"
    body = "Your application BF-10293 has moved to Under Review."
    result = parse_backflip_notification(subject, body)
    assert result is not None
    assert result.event_type == "stage_change"
    assert result.stage == "under_review"
    assert result.backflip_ref == "BF-10293"


def test_stage_change_conditional_approval():
    subject = "BF-55521: Conditional Approval Issued"
    body = "Conditional approval has been issued for BF-55521."
    result = parse_backflip_notification(subject, body)
    assert result.stage == "conditional_approval"
    assert result.backflip_ref == "BF-55521"


def test_document_request():
    subject = "Action needed on BF-77812: Documents requested"
    body = "We need the following document: Bank Statement (last 2 months)."
    result = parse_backflip_notification(subject, body)
    assert result.event_type == "document_request"
    assert result.document_name == "Bank Statement (last 2 months)"
    assert result.backflip_ref == "BF-77812"


def test_cleared_to_close():
    subject = "BF-33012 is Cleared to Close"
    body = "Congratulations — BF-33012 has been cleared to close."
    result = parse_backflip_notification(subject, body)
    assert result.stage == "cleared_to_close"


def test_terms_email():
    subject = "Term Sheet Ready — BF-90011"
    body = (
        "Term sheet for BF-90011: Loan amount $250,000.00, term 12 months."
    )
    result = parse_backflip_notification(subject, body)
    assert result.event_type == "terms"
    assert result.loan_amount_cents == 25_000_000
    assert result.maturity_months == 12
    assert result.backflip_ref == "BF-90011"


def test_unrecognized_email_returns_none():
    result = parse_backflip_notification("Weekly newsletter", "Nothing relevant here.")
    assert result is None


def test_missing_backflip_ref_returns_none():
    result = parse_backflip_notification("Now Under Review", "Your file has moved to Under Review.")
    assert result is None
