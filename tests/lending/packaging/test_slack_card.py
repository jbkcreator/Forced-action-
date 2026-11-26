"""Seam 4 — Slack card (pure block render, no DB, no Slack).

Prior art: tests/lending/test_milo_slack_card.py.
"""
from __future__ import annotations

import json

import pytest

from src.lending.packaging.slack_card import build_card


def _verdict(outcomes: list[str]) -> str:
    return json.dumps({
        "outcome": outcomes[0] if outcomes else "pass",
        "per_doc": [
            {"upload_id": i + 1, "upload_table": "doc_chase_uploads",
             "item_code": "bank_statements", "outcome": o,
             "reasons": [f"reason {o}"]}
            for i, o in enumerate(outcomes)
        ],
    })


def test_stamp_present_when_box_unconfirmed():
    card = build_card(
        package_id=1,
        lender_key="corevest",
        box_confirmed=False,
        checklist=[],
        verdict_json=_verdict(["pass"]),
    )
    text = json.dumps(card.blocks)
    assert "not confirmed" in text.lower() or "do not submit" in text.lower()
    assert not card.can_submit


def test_no_stamp_when_box_confirmed_and_all_pass():
    card = build_card(
        package_id=2,
        lender_key="rcn_capital",
        box_confirmed=True,
        checklist=[{"item_code": "bank_statements", "label": "Bank statements", "received": True}],
        verdict_json=_verdict(["pass"]),
    )
    assert card.can_submit


def test_mark_submitted_disabled_when_doc_blocked():
    card = build_card(
        package_id=3,
        lender_key="abl",
        box_confirmed=True,
        checklist=[],
        verdict_json=_verdict(["block"]),
    )
    assert not card.can_submit
    text = json.dumps(card.blocks)
    assert "BLOCKED" in text or "block" in text.lower()


def test_mark_submitted_disabled_when_doc_quarantined():
    card = build_card(
        package_id=4,
        lender_key="temple_view",
        box_confirmed=True,
        checklist=[],
        verdict_json=_verdict(["quarantine"]),
    )
    assert not card.can_submit
    text = json.dumps(card.blocks)
    assert "QUARANTINE" in text or "quarantine" in text.lower()


def test_missing_item_shown_in_checklist():
    checklist = [
        {"item_code": "bank_statements", "label": "Bank statements", "received": False},
        {"item_code": "entity_docs", "label": "LLC documents", "received": True},
    ]
    card = build_card(
        package_id=5,
        lender_key="corevest",
        box_confirmed=True,
        checklist=checklist,
        verdict_json=_verdict(["pass", "pass"]),
    )
    text = json.dumps(card.blocks)
    assert "Bank statements" in text
    assert "LLC documents" in text


def test_dismiss_button_always_present():
    card = build_card(
        package_id=6,
        lender_key="rcn_capital",
        box_confirmed=False,
        checklist=[],
        verdict_json=_verdict([]),
    )
    text = json.dumps(card.blocks)
    assert "dismiss" in text.lower() or "Dismiss" in text
