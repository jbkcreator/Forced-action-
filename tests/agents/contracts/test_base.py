import pytest
from pydantic import BaseModel

from src.agents.contracts.base import (
    HandoffRejected,
    check_against_model,
    record_rejection,
)


def test_handoff_rejected_rejects_unknown_boundary():
    with pytest.raises(ValueError):
        HandoffRejected("not_a_real_boundary", ["x"], "ref-1")


def test_handoff_rejected_carries_fields():
    exc = HandoffRejected("cora_to_relay", ["thread_id: missing"], "idem-123")
    assert exc.boundary == "cora_to_relay"
    assert exc.missing_fields == ["thread_id: missing"]
    assert exc.reference_id == "idem-123"
    assert "idem-123" in str(exc)


def test_record_rejection_writes_a_row(fresh_db):
    from sqlalchemy import text

    row_id = record_rejection(
        fresh_db,
        boundary="hunter_to_cora",
        missing_fields=["confidence_score: 40 < 70"],
        reference_id="OPP-2026-00042",
        payload_snapshot={"opportunity_thread_id": "OPP-2026-00042"},
    )
    fresh_db.commit()

    row = fresh_db.execute(
        text("SELECT boundary, reference_id, missing_fields FROM handoff_rejections WHERE id = :id"),
        {"id": row_id},
    ).mappings().first()
    assert row["boundary"] == "hunter_to_cora"
    assert row["reference_id"] == "OPP-2026-00042"
    assert row["missing_fields"] == ["confidence_score: 40 < 70"]


class _Toy(BaseModel):
    name: str
    age: int


def test_check_against_model_ok_case():
    result = check_against_model(_Toy, {"name": "a", "age": 5})
    assert result.ok is True
    assert result.missing_fields == []
    assert result.model is not None
    assert result.model.name == "a"


def test_check_against_model_missing_field_case():
    result = check_against_model(_Toy, {"name": "a"})
    assert result.ok is False
    assert any("age" in f for f in result.missing_fields)
    assert result.model is None
