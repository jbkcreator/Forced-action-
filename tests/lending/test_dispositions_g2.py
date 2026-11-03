"""G2: 13 call results cut to the 10 shared by BatchDialer and GHL; old codes stay readable (pure, no DB)."""
from __future__ import annotations

import pytest

from config import lending_dispositions as cfg
from src.lending.dispositions import normalize_code

TEN = ("NO_ANSWER", "LEFT_VOICEMAIL", "CALLBACK_REQUESTED", "BOOKED", "CONNECTED_NOT_INTERESTED",
       "BAD_NUMBER", "WRONG_PERSON", "REFERRED", "DNC_REQUEST", "NURTURE")
LEGACY = {"CALL_FAILED": "NO_ANSWER", "NOT_DECISION_MAKER": "WRONG_PERSON",
          "DATA_NURTURE_ONLY": "NURTURE", "GATE_FAILED_NURTURE": "NURTURE"}


def test_list_is_the_agreed_ten():
    assert set(cfg.DISPOSITIONS) == set(TEN) and len(cfg.DISPOSITIONS) == 10
    assert cfg.DISPOSITION_LIST_VERSION != "2026-10-01"


def test_legacy_aliases():
    assert cfg.LEGACY_DISPOSITION_ALIASES == LEGACY
    assert not set(cfg.LEGACY_DISPOSITION_ALIASES) & set(cfg.DISPOSITIONS)


@pytest.mark.parametrize("raw,code", [("FAILED", "NO_ANSWER"), ("Congestion", "NO_ANSWER"), ("Nurture", "NURTURE"),
                                      # old-list codes from a dialer not yet switched: kept as sent (lossless)
                                      ("call failed", "CALL_FAILED"), ("Not Decision Maker", "NOT_DECISION_MAKER"),
                                      ("GATE_FAILED_NURTURE", "GATE_FAILED_NURTURE")])
def test_normalize_maps_system_codes_and_keeps_legacy(raw, code):
    assert normalize_code(raw) == (code, True)


def test_unknown_still_unknown():
    assert normalize_code("Hot Lead") == (None, False)


def test_unanswered_is_no_answer_and_voicemail():
    assert cfg.UNANSWERED_CODES == {"NO_ANSWER", "LEFT_VOICEMAIL"}


def test_answered_excludes_legacy_and_unanswered():
    assert "NURTURE" in cfg.ANSWERED_CODES and "CALL_FAILED" not in cfg.ANSWERED_CODES
    assert not cfg.ANSWERED_CODES & (cfg.UNANSWERED_CODES | {"BAD_NUMBER"})


def test_scoreboard_sets_keep_counting_history():
    assert {"NURTURE", "DATA_NURTURE_ONLY", "GATE_FAILED_NURTURE"} <= cfg.LIVE_CONVERSATION_CODES
    assert "GATE_FAILED_NURTURE" in cfg.GATED_CODES and "GATE_FAILED_NURTURE" in cfg.NURTURE_SENT_CODES


def test_default_cause_covers_nurture_and_legacy():
    assert cfg.DEFAULT_CAUSE["NURTURE"] == "fit"
    assert cfg.DEFAULT_CAUSE["NOT_DECISION_MAKER"] == "contactability"


def test_validate_allows_legacy_but_refuses_overlap(monkeypatch):
    cfg.validate_dispositions_config()
    monkeypatch.setattr(cfg, "LEGACY_DISPOSITION_ALIASES", {"BOOKED": "NO_ANSWER"})
    with pytest.raises(ValueError):
        cfg.validate_dispositions_config()


def test_legacy_alias_must_point_into_the_ten(monkeypatch):
    monkeypatch.setattr(cfg, "LEGACY_DISPOSITION_ALIASES", {"OLD": "NOPE"})
    with pytest.raises(ValueError):
        cfg.validate_dispositions_config()


def test_ghl_failed_status_is_no_answer():
    from config.lending_followup import GHL_CALL_OUTCOME_CODES
    assert GHL_CALL_OUTCOME_CODES["failed"] == "NO_ANSWER"


def test_alarm_not_connected_codes():
    from config.lending_alarms import NOT_CONNECTED_CODES
    assert set(NOT_CONNECTED_CODES) == {"NO_ANSWER", "LEFT_VOICEMAIL", "BAD_NUMBER", "CALL_FAILED"}


def test_attempt_log_reads_legacy_row_as_new_code():
    from src.lending.attempt_log import CallRow as AttemptRow  # noqa: N814
    import dataclasses
    fields = {f.name: None for f in dataclasses.fields(AttemptRow)}
    row = AttemptRow(**{**fields, "disposition": "NOT_DECISION_MAKER"})
    assert row.code == "WRONG_PERSON"


def test_gate_failed_column_on_model():
    from src.lending.models import LendingCallDisposition as CallDisposition
    col = CallDisposition.__table__.c.gate_failed
    assert col.nullable and str(col.type) == "BOOLEAN"


def test_gate_failed_migration_skips_when_column_exists():
    from unittest.mock import MagicMock
    from migrations.apply_lending_call_gate_failed import apply_to
    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = 1
    apply_to(conn)
    assert conn.execute.call_count == 1  # only the existence check: no lock taken


def test_gate_failed_migration_adds_column():
    from unittest.mock import MagicMock
    from migrations.apply_lending_call_gate_failed import apply_to
    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = None
    apply_to(conn)
    assert "ADD COLUMN gate_failed boolean" in str(conn.execute.call_args_list[-1].args[0])
