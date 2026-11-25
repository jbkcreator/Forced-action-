"""R1 0.5 T3 against Postgres (closing_db: throwaway schema, rolled back): the ghl_stage journal handler writes one
stage event and one current stage however often a body is delivered, and records GHL's stage when it differs.
The loan bridge is replaced by a recorder (its own DB tests: test_loan_state_machine_db.py)."""
from __future__ import annotations

import pytest
from sqlalchemy import text

from src.lending.inbound import ghl_stage

BODY = {"opportunity_id": "opp-9", "stage_name": "Funded", "pipeline_id": "pipe-1", "phone": "(813) 555-0142"}


@pytest.fixture
def bridge(monkeypatch):
    applied: list[str] = []
    monkeypatch.setattr(ghl_stage, "apply_stage",
                        lambda db, *, opportunity_id, stage_name, phone=None: applied.append(stage_name.lower()))
    return applied


def _ghl_says(stage):
    return lambda opportunity_id: ghl_stage.GhlStage(stage_name=stage, pipeline_id="pipe-1")


def _events(db):
    return db.execute(text("SELECT stage_key FROM lending.ghl_stage_events ORDER BY id")).scalars().all()


def _current(db):
    return db.execute(text("SELECT stage_key FROM lending.ghl_opportunity_stage WHERE ghl_opportunity_id = 'opp-9'")
                      ).scalars().all()


def test_ten_deliveries_are_one_stage_event_and_one_current_stage(closing_db, bridge):
    for _ in range(10):
        ghl_stage.handle_stage(closing_db, BODY, reader=_ghl_says("Funded"))
    assert _events(closing_db) == ["funded"] and _current(closing_db) == ["funded"]


def test_a_late_submitted_after_funded_keeps_funded_and_is_still_stored(closing_db, bridge):
    ghl_stage.handle_stage(closing_db, BODY, reader=_ghl_says("Funded"))
    ghl_stage.handle_stage(closing_db, {**BODY, "stage_name": "Submitted to lender"}, reader=_ghl_says("Funded"))
    assert _events(closing_db) == ["funded", "submitted to lender"]
    assert _current(closing_db) == ["funded"] and set(bridge) == {"funded"}


def test_a_reconcile_payload_records_a_dropped_stage(closing_db, bridge):
    ghl_stage.handle_stage(closing_db, {"opportunity_id": "opp-9", "stage_name": "Funded", "pipeline_id": "pipe-1",
                                        "source": "reconcile"}, reader=None)
    assert _events(closing_db) == ["funded"] and _current(closing_db) == ["funded"] and bridge == ["funded"]
