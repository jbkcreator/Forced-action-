"""Spec §3.1/§3.2 values for the lending compliance floor (Wave 0)."""
from datetime import time

import pytest

from config import lending_compliance as cfg


def test_spec_values():
    assert cfg.CALL_WINDOW_START == time(8, 0)      # recipient-local FTSA floor
    assert cfg.CALL_WINDOW_END == time(20, 0)
    assert cfg.ET_WINDOW_START == time(9, 0)        # client hard stop, Eastern
    assert cfg.ET_WINDOW_END == time(19, 15)
    assert cfg.SHIFT_GROUPS == {"A": (time(9, 0), time(15, 0)), "B": (time(13, 0), time(19, 15))}
    assert cfg.MAX_ATTEMPTS_PER_PERIOD == 3
    assert cfg.ATTEMPT_PERIOD_HOURS == 24
    assert cfg.DNC_SCRUB_MAX_AGE_DAYS == 7
    assert cfg.STOP_PROPAGATION_SLA_SECONDS == 60
    assert cfg.GEORGIA_ALLOWED_ENTITY_TYPES == frozenset({"LLC", "LP", "CORPORATION"})


def test_reason_codes_are_stable_strings():
    assert cfg.ReasonCode.NO_FRESH_SCRUB.value == "NO_FRESH_SCRUB"
    assert {c.value for c in cfg.ReasonCode} == {
        "INVALID_PHONE", "NO_FRESH_SCRUB", "SCRUB_FAILED", "NATIONAL_DNC", "STATE_DNC", "LITIGATOR",
        "SUPPRESSED", "GA_NATURAL_PERSON", "OUTSIDE_CALL_WINDOW", "ATTEMPT_CAP_REACHED",
        "BACKFLIP_CONFLICT", "BACKFLIP_FEED_STALE", "BACKFLIP_FEED_UNAVAILABLE",
    }


def test_validate_accepts_shipped_config():
    cfg.validate_lending_compliance_config()


@pytest.mark.parametrize("attr,value", [
    ("CALL_WINDOW_END", time(7, 0)),
    ("MAX_ATTEMPTS_PER_PERIOD", 0),
    ("DNC_SCRUB_MAX_AGE_DAYS", 0),
    ("ET_WINDOW_END", time(8, 0)),
])
def test_validate_rejects_bad_values(monkeypatch, attr, value):
    monkeypatch.setattr(cfg, attr, value)
    with pytest.raises(ValueError):
        cfg.validate_lending_compliance_config()
