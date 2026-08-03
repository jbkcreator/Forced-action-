"""
Dev->Vera handoff contract (QUALITY-v2.2 Q3).

Reasoned per decision D1 -- mirrors FORGE's PR shape (spec §1.10, PR labeled
`agent-draft`) and Vera's own standing jobs that expect a closure to be
reconcilable against an open finding. Same Flag F5 scope boundary as
Task 5's Vera->Dev contract: validator only, no GitHub PR writer.
"""
from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, field_validator

from src.agents.contracts.base import HandoffCheckResult, check_against_model

_REQUIRED_TEXT_FIELDS = (
    "commit_hash", "plain_english_diff", "touches", "risk_class",
    "test_evidence", "rollback_plan", "closes_finding_id",
)


class DevToVeraClosure(BaseModel):
    commit_hash: str
    plain_english_diff: str
    touches: str
    risk_class: str
    test_evidence: str
    rollback_plan: str
    claimed_done_at: datetime
    closes_finding_id: str
    label: Literal["agent-draft"] = "agent-draft"

    @field_validator(*_REQUIRED_TEXT_FIELDS)
    @classmethod
    def _nonempty(cls, v: str, info) -> str:
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} must be non-empty")
        return v


def check_closure(data: dict) -> HandoffCheckResult:
    """Checks a plain dict against the Dev->Vera contract. Returns
    pass/fail + missing fields, same convention as vera_to_dev.check_finding."""
    return check_against_model(DevToVeraClosure, data)
