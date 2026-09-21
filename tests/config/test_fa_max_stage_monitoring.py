"""tests/config/test_fa_max_stage_monitoring.py"""
from __future__ import annotations

import pytest

from config.fa_max_stage_monitoring import (
    BACKFLIP_STAGE_KEYS,
    DOC_CHASE_ESCALATE_BUSINESS_DAYS,
    DOC_CHASE_FOLLOWUP_BUSINESS_DAYS,
    STALL_THRESHOLD_BUSINESS_DAYS,
    STATUS_TOUCH_INTERVAL_BUSINESS_DAYS,
    TERMINAL_BACKFLIP_STAGES,
    validate_stage_monitoring_config,
)


def test_stage_keys_include_all_spec_stages():
    expected = {
        "submitted", "under_review", "conditional_approval",
        "docs_requested", "cleared_to_close", "funded", "declined",
    }
    assert expected == BACKFLIP_STAGE_KEYS


def test_terminal_stages_are_subset_of_stage_keys():
    assert TERMINAL_BACKFLIP_STAGES <= BACKFLIP_STAGE_KEYS
    assert TERMINAL_BACKFLIP_STAGES == {"funded", "declined"}


def test_doc_chase_escalate_after_followup():
    assert DOC_CHASE_ESCALATE_BUSINESS_DAYS > DOC_CHASE_FOLLOWUP_BUSINESS_DAYS


def test_validate_passes_with_shipped_defaults():
    validate_stage_monitoring_config()  # must not raise


def test_thresholds_are_positive():
    assert STALL_THRESHOLD_BUSINESS_DAYS > 0
    assert STATUS_TOUCH_INTERVAL_BUSINESS_DAYS > 0
