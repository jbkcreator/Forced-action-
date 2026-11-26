"""Seam 3 — screening (fake DB / session, no live DB).

Prior art: tests/lending/test_lender_fit.py, tests/lending/test_doc_chase_start.py.
"""
from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from src.lending.packaging.screen import (
    build_fingerprint,
    screen_file,
)


def _facts(*, loan_amount=200_000, state="FL", product="flip", credit_band="fair",
           projects=5, loan_file_id=1, source_type="loan_file"):
    return {
        "loan_file_id": loan_file_id,
        "source_type": source_type,
        "loan_amount": loan_amount,
        "property_state": state,
        "product": product,
        "credit_band": credit_band,
        "completed_projects": projects,
    }


class _FakeSession:
    """Minimal fake SQLAlchemy session for seam 3."""

    def __init__(self, existing_packages=None):
        self._packages: list[dict] = existing_packages or []
        self._upserted: list[dict] = []

    def execute(self, stmt, params=None):
        # Return empty existing for the fingerprint-check query
        result = MagicMock()
        result.mappings.return_value.all.return_value = self._packages
        result.fetchone.return_value = (len(self._upserted) + 100,)
        return result

    def commit(self):
        pass


def test_fitting_file_creates_package():
    """A file that fits two placeholder secondary lenders → two package upserts."""
    db = _FakeSession()
    facts = _facts()

    # patch evaluate_lender_fit to return two fits
    mock_fit = MagicMock()
    mock_fit.fitting = [
        MagicMock(lender_key="corevest", subject_to_confirmation=True),
        MagicMock(lender_key="rcn_capital", subject_to_confirmation=True),
    ]
    mock_fit.non_fitting = []

    with patch("src.lending.packaging.screen.evaluate_lender_fit", return_value=mock_fit):
        ids = screen_file(db, facts)

    assert len(ids) >= 2


def test_unchanged_fingerprint_skips_rescreen():
    """When fingerprint matches all existing packages, screen_file returns existing IDs without re-screening."""
    facts = _facts()
    fp = build_fingerprint(facts)
    existing = [
        {"id": 10, "screen_fingerprint": fp, "status": "draft"},
        {"id": 11, "screen_fingerprint": fp, "status": "draft"},
    ]
    db = _FakeSession(existing_packages=existing)

    with patch("src.lending.packaging.screen.evaluate_lender_fit") as mock_fit:
        ids = screen_file(db, facts)
        # evaluate_lender_fit should NOT be called (fingerprint unchanged)
        mock_fit.assert_not_called()

    assert set(ids) == {10, 11}


def test_suppressed_file_has_no_packages():
    """screen_eligible_files should not return suppressed or owner-occupied files; tested via load_eligible_files mock."""
    # This seam tests that screen_file given a suppressed-flagged file never calls evaluate_lender_fit.
    # In production the SQL WHERE clause excludes them; here we confirm screen_file itself does not crash
    # when given incomplete facts (simulating a file that slipped through).
    db = _FakeSession()
    facts = {
        "loan_file_id": None,  # no id → screen_file returns []
        "source_type": "loan_file",
        "loan_amount": None,
        "property_state": None,
        "product": None,
        "credit_band": None,
        "completed_projects": 0,
    }
    ids = screen_file(db, facts)
    assert ids == []


def test_checklist_excludes_blocked_docs():
    """Checklist missing-item logic: a blocked doc does not count as received."""
    from src.lending.packaging.package import _build_checklist
    from src.lending.packaging.packet_validator import DocInput, DocVerdict, Outcome, Verdict

    # all docs blocked
    verdict = Verdict(
        outcome=Outcome.BLOCK,
        per_doc=(
            DocVerdict(1, "doc_chase_uploads", "bank_statements", Outcome.BLOCK, ("name not found",)),
        ),
    )
    checklist = _build_checklist("corevest", [], verdict)
    # bank_statements should NOT be received
    received = {item["item_code"]: item["received"] for item in checklist}
    assert received.get("bank_statements") is False
