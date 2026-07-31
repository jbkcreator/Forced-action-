"""
Vera->Dev handoff contract (QUALITY-v2.2 Q3).

Spec §1.1.10 names the exact 7 fields a Vera finding must carry, delivered
as a GitHub issue labeled `vera-finding` (spec :191). Flag F5: no GitHub
integration exists anywhere in this codebase (confirmed -- no PyGithub, no
`gh` CLI, nothing in requirements.txt), so this module builds the
required-fields validator ONLY. The GitHub issue writer is Vera's own,
later, separate task; this contract is what that writer validates a finding
against before calling the GitHub API. Until that writer exists, Slack is
the interim escalation channel for an incomplete finding, per decision D2.
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, field_validator

from src.agents.contracts.base import HandoffCheckResult, check_against_model

_REQUIRED_TEXT_FIELDS = ("issue", "evidence", "repro", "suspected_cause", "proposed_fix", "effort", "risk")


class VeraToDevFinding(BaseModel):
    issue: str
    evidence: str
    repro: str
    suspected_cause: str
    proposed_fix: str
    effort: str
    risk: str
    label: Literal["vera-finding"] = "vera-finding"

    @field_validator(*_REQUIRED_TEXT_FIELDS)
    @classmethod
    def _nonempty(cls, v: str, info) -> str:
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} must be non-empty")
        return v


def check_finding(data: dict) -> HandoffCheckResult:
    """Checks a plain dict (whatever Vera's own reporting code assembles)
    against the 7-field Vera->Dev contract. Returns pass/fail + the missing
    fields -- never raises, since this is meant to be called from Vera's
    own reporting/escalation code, which decides what to do with a failing
    result (post to Slack per D2), not have control flow forced on it by an
    exception."""
    return check_against_model(VeraToDevFinding, data)
