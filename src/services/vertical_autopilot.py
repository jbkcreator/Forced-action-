"""
Vertical Autopilot — detection and scoring pipeline (REVINT-v2.2 I4).

Implements the 6-dimension vertical fit rubric. Probe loop (send → collect →
verdict → package) is deferred to the next session (I5).

Public surface:
    score_vertical(vertical_name, evidence, db)   → VerticalCandidatePacket
    check_legal_status(vertical_name)              → (legal_status, eligible_for_probe)
    evaluate_dim5(evidence)                        → int
    evaluate_dim6(vertical_name)                   → int
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from config.vertical_fit_rubric import (
    LEGAL_RISK_ALLOWLIST,
    LEGAL_RISK_BLOCKLIST_CATEGORIES,
    MIN_MONTHLY_RECORDS,
    MONEY_EVIDENCE_SIGNALS,
    URGENCY_EVIDENCE_SIGNALS,
    VERTICAL_FIT_THRESHOLD,
)
from src.core.models import VerticalCandidatePacket

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Legal gate (Dim 6)
# ---------------------------------------------------------------------------

def check_legal_status(vertical_name: str) -> tuple[str, bool]:
    """Return (legal_status, eligible_for_probe) for a vertical.

    Rules:
      - allowlist match        → ("approved", True)
      - blocklist category key present in vertical_name
                               → ("blocked", False)
      - unknown                → ("pending_review", False)
    """
    if vertical_name in LEGAL_RISK_ALLOWLIST:
        return "approved", True

    for category in LEGAL_RISK_BLOCKLIST_CATEGORIES:
        if category in vertical_name:
            logger.warning(
                "vertical_autopilot: %r matched blocklist category %r — blocked",
                vertical_name,
                category,
            )
            return "blocked", False

    # Unknown vertical — flag for founder review; never auto-probe
    logger.info(
        "vertical_autopilot: %r not in allowlist or blocklist — pending_review",
        vertical_name,
    )
    return "pending_review", False


def evaluate_dim6(vertical_name: str) -> int:
    """Return Dim 6 binary score (1 = approved, 0 = blocked or pending)."""
    legal_status, _ = check_legal_status(vertical_name)
    return 1 if legal_status == "approved" else 0


# ---------------------------------------------------------------------------
# Dim 5 — Buyer evidence (requires BOTH money AND urgency)
# ---------------------------------------------------------------------------

def evaluate_dim5(evidence: dict[str, Any]) -> int:
    """Return 1 only when at least one money signal AND one urgency signal are present."""
    has_money = any(signal in evidence for signal in MONEY_EVIDENCE_SIGNALS)
    has_urgency = any(signal in evidence for signal in URGENCY_EVIDENCE_SIGNALS)
    return 1 if (has_money and has_urgency) else 0


# ---------------------------------------------------------------------------
# Full 6-dimension scorer
# ---------------------------------------------------------------------------

def _evaluate_dim1(evidence: dict[str, Any]) -> int:
    """Dim 1: monthly record volume >= MIN_MONTHLY_RECORDS."""
    return 1 if evidence.get("monthly_records", 0) >= MIN_MONTHLY_RECORDS else 0


def _evaluate_dim2(evidence: dict[str, Any]) -> int:
    """Dim 2: identifiable decision-maker (owner/contact reachable)."""
    return 1 if evidence.get("identifiable_decision_maker", False) else 0


def _evaluate_dim3(evidence: dict[str, Any]) -> int:
    """Dim 3: clear pain point / distress signal present."""
    return 1 if evidence.get("clear_pain_point", False) else 0


def _evaluate_dim4(evidence: dict[str, Any]) -> int:
    """Dim 4: FA has a solution that maps to this vertical."""
    return 1 if evidence.get("fa_solution_exists", False) else 0


def score_vertical(
    vertical_name: str,
    evidence: dict[str, Any],
    db: Session,
) -> VerticalCandidatePacket:
    """Evaluate all 6 dimensions and persist a VerticalCandidatePacket.

    Args:
        vertical_name: Canonical vertical identifier (e.g. "tax_lien").
        evidence: Dict of signal keys → values. See rubric config for signal names.
        db: SQLAlchemy session. Caller is responsible for commit.

    Returns:
        Persisted (but not yet committed) VerticalCandidatePacket.
    """
    dim1 = _evaluate_dim1(evidence)
    dim2 = _evaluate_dim2(evidence)
    dim3 = _evaluate_dim3(evidence)
    dim4 = _evaluate_dim4(evidence)
    dim5 = evaluate_dim5(evidence)
    dim6 = evaluate_dim6(vertical_name)

    total = dim1 + dim2 + dim3 + dim4 + dim5 + dim6
    legal_status, eligible_for_probe = check_legal_status(vertical_name)

    # Hard gate: pending_review must never reach probe stage
    if legal_status == "pending_review":
        eligible_for_probe = False

    status = "candidate" if total >= VERTICAL_FIT_THRESHOLD else "pending_legal" if legal_status == "pending_review" else "candidate"
    if legal_status == "blocked":
        status = "killed"

    evidence_record = {
        "dim1": {"monthly_records": evidence.get("monthly_records"), "passed": bool(dim1)},
        "dim2": {"identifiable_decision_maker": evidence.get("identifiable_decision_maker"), "passed": bool(dim2)},
        "dim3": {"clear_pain_point": evidence.get("clear_pain_point"), "passed": bool(dim3)},
        "dim4": {"fa_solution_exists": evidence.get("fa_solution_exists"), "passed": bool(dim4)},
        "dim5": {
            "money_signals": [s for s in MONEY_EVIDENCE_SIGNALS if s in evidence],
            "urgency_signals": [s for s in URGENCY_EVIDENCE_SIGNALS if s in evidence],
            "passed": bool(dim5),
        },
        "dim6": {"legal_status": legal_status, "passed": bool(dim6)},
    }

    packet = VerticalCandidatePacket(
        vertical_name=vertical_name,
        dim1_score=dim1,
        dim2_score=dim2,
        dim3_score=dim3,
        dim4_score=dim4,
        dim5_score=dim5,
        dim6_score=dim6,
        total_score=total,
        legal_status=legal_status,
        eligible_for_probe=eligible_for_probe,
        evidence=evidence_record,
        status=status,
        created_at=datetime.now(timezone.utc),
    )
    db.add(packet)
    db.flush()

    logger.info(
        "vertical_autopilot: scored %r — %d/6 dims, legal=%s, eligible_for_probe=%s, status=%s",
        vertical_name,
        total,
        legal_status,
        eligible_for_probe,
        status,
    )
    return packet
