"""Persist the soft approval per lead (the interim deal record until T-11)."""
from __future__ import annotations

import json
from typing import Optional

from sqlalchemy import text

from src.lending.soft_approval.facts import SoftApprovalFacts


def save_soft_approval(
    db,
    *,
    phone: str,
    dialer_call_id: str,
    status: str,
    reason: Optional[str],
    facts: SoftApprovalFacts,
    figures: Optional[dict],
    lender_key: Optional[str],
    pdf: Optional[bytes],
    template_version: Optional[str],
    submitted_by: Optional[str],
) -> int:
    """Insert, or quietly update the lead's row when anything changed. Returns the row id. Does not commit.

    A resubmission with identical facts, figures and status changes nothing. A changed one overwrites
    the row and appends the replaced values to ``history``.
    """
    facts_json = facts.snapshot()
    existing = db.execute(
        text("SELECT id, status, facts, figures FROM lending.soft_approvals WHERE phone = :phone FOR UPDATE"),
        {"phone": phone},
    ).first()
    if existing and existing.status == status and existing.facts == facts_json and existing.figures == figures:
        return existing.id
    return db.execute(
        text(
            """
            INSERT INTO lending.soft_approvals
                (phone, dialer_call_id, status, reason, lender_key, facts, figures, pdf,
                 template_version, submitted_by)
            VALUES (:phone, :call_id, :status, :reason, :lender_key, CAST(:facts AS jsonb),
                    CAST(:figures AS jsonb), :pdf, :template_version, :submitted_by)
            ON CONFLICT (phone) DO UPDATE SET
                history = lending.soft_approvals.history || jsonb_build_array(jsonb_build_object(
                    'replaced_at', to_jsonb(now()),
                    'dialer_call_id', lending.soft_approvals.dialer_call_id,
                    'status', lending.soft_approvals.status,
                    'facts', lending.soft_approvals.facts,
                    'figures', lending.soft_approvals.figures,
                    'submitted_by', lending.soft_approvals.submitted_by)),
                dialer_call_id = EXCLUDED.dialer_call_id,
                status = EXCLUDED.status,
                reason = EXCLUDED.reason,
                lender_key = EXCLUDED.lender_key,
                facts = EXCLUDED.facts,
                figures = EXCLUDED.figures,
                pdf = EXCLUDED.pdf,
                template_version = EXCLUDED.template_version,
                submitted_by = EXCLUDED.submitted_by,
                updated_at = now()
            RETURNING id
            """
        ),
        {
            "phone": phone, "call_id": dialer_call_id, "status": status, "reason": reason,
            "lender_key": lender_key, "facts": json.dumps(facts_json),
            "figures": json.dumps(figures) if figures is not None else None,
            "pdf": pdf, "template_version": template_version, "submitted_by": submitted_by,
        },
    ).scalar_one()
