"""LEARN-v2.2 T-LEARN-03 — win/loss reason-code unit tests.

Focuses on the closed-set taxonomy and the validation-before-db contract of
record_loss (the ValueError path must raise before any db access, so it is
testable without a real session).
"""
import pytest

from src.services.opportunity_outcome import LOSS_REASON_CODES, record_loss


def test_loss_reason_codes_is_the_spec_eight():
    assert set(LOSS_REASON_CODES) == {
        "timing", "price", "trust", "fit",
        "no_urgency", "wrong_contact", "competitor", "no_response",
    }
    assert len(LOSS_REASON_CODES) == 8


def test_record_loss_rejects_unknown_code_before_touching_db():
    # db=None proves validation happens before any db.execute call.
    with pytest.raises(ValueError):
        record_loss(None, "OPP-2026-00001", reason_code="ghosted", coded_by="admin")


@pytest.mark.parametrize("code", LOSS_REASON_CODES)
def test_record_loss_accepts_each_valid_code_past_validation(code):
    # A valid code must pass validation and only then attempt db access.
    # With db=None the very next step (db.execute) raises AttributeError, which
    # confirms we got past the ValueError gate for every valid code.
    with pytest.raises(AttributeError):
        record_loss(None, "OPP-2026-00001", reason_code=code, coded_by="admin")


def test_won_outcome_needs_no_reason_code():
    # 'won' is not part of the loss taxonomy — it is not in LOSS_REASON_CODES.
    assert "won" not in LOSS_REASON_CODES
    assert "lost" not in LOSS_REASON_CODES
