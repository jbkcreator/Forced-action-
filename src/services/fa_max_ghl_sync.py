"""FA Max ↔ GoHighLevel relationship-state sync boundary (WP-2).

IMPORTANT — BLOCKED ON OPEN CLARIFICATION:
    SOT.md clarification #16 (GHL/FA field-ownership conflict rule) is
    unanswered as of 2026-09-15. This module is a FAIL-CLOSED interface:
    it defines the boundary but does NOT auto-resolve conflicts. Both
    directions (push to GHL, pull from GHL) are documented here as stubs
    that log their intent and fail closed until #16 is answered and a real
    Fake/Live implementation is built behind this boundary.

    Do NOT call any GHL API directly from business logic. Wire calls through
    this module only so the boundary stays checkable by a structural test.

Ownership rule (interim, until #16 is resolved):
    - FA Max is authoritative for: lifecycle_state, opportunity stage,
      and any field WP-1 manages (person_id, opportunity_id).
    - GHL is authoritative for: contact first/last name, raw phone, raw
      email as received from the lead source.
    - CONFLICT: any field both systems may write is currently BLOCKED — this
      module logs the conflict and returns SyncResult.conflict without
      writing to either system.

This boundary is NOT the single-sender relay. It never produces outbound
contact to a borrower; it is a CRM sync operation only.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class SyncDirection(str, Enum):
    fa_to_ghl = "fa_to_ghl"
    ghl_to_fa = "ghl_to_fa"


class SyncOutcome(str, Enum):
    ok = "ok"
    conflict = "conflict"
    blocked = "blocked"
    error = "error"


@dataclass(frozen=True)
class SyncResult:
    outcome: SyncOutcome
    direction: SyncDirection
    person_id: Optional[str]
    ghl_contact_id: Optional[str]
    fields_synced: list
    conflict_fields: list
    message: str


def push_lifecycle_state_to_ghl(
    *,
    person_id: str,
    ghl_contact_id: str,
    new_lifecycle_state: str,
    actor: str,
) -> SyncResult:
    """Push an FA Max lifecycle state change to GHL custom field.

    STUB — blocked on SOT.md clarification #16.
    Returns SyncResult(outcome=blocked) without touching GHL until the
    field-ownership conflict rule is resolved and a Live adapter is built.
    """
    logger.warning(
        "fa_max_ghl_sync.push_lifecycle_state_to_ghl: STUB (blocked on SOT#16) "
        "person=%s ghl_contact=%s state=%s actor=%s",
        person_id, ghl_contact_id, new_lifecycle_state, actor,
    )
    return SyncResult(
        outcome=SyncOutcome.blocked,
        direction=SyncDirection.fa_to_ghl,
        person_id=person_id,
        ghl_contact_id=ghl_contact_id,
        fields_synced=[],
        conflict_fields=["lifecycle_state"],
        message=(
            "GHL sync blocked: SOT.md clarification #16 (field-ownership "
            "conflict rule) is unanswered. No data written to GHL."
        ),
    )


def pull_contact_fields_from_ghl(
    *,
    ghl_contact_id: str,
    fields: list,
    actor: str,
) -> SyncResult:
    """Pull specified GHL contact fields into FA Max.

    STUB — blocked on SOT.md clarification #16.
    Returns SyncResult(outcome=blocked) without reading from GHL until
    the ownership rule is confirmed.
    """
    logger.warning(
        "fa_max_ghl_sync.pull_contact_fields_from_ghl: STUB (blocked on SOT#16) "
        "ghl_contact=%s fields=%s actor=%s",
        ghl_contact_id, fields, actor,
    )
    return SyncResult(
        outcome=SyncOutcome.blocked,
        direction=SyncDirection.ghl_to_fa,
        person_id=None,
        ghl_contact_id=ghl_contact_id,
        fields_synced=[],
        conflict_fields=fields,
        message=(
            "GHL sync blocked: SOT.md clarification #16 (field-ownership "
            "conflict rule) is unanswered. No data read from GHL."
        ),
    )


def detect_field_conflict(
    fa_value: Any,
    ghl_value: Any,
    field_name: str,
) -> bool:
    """Return True if FA Max and GHL hold different values for the same field.

    Caller is responsible for deciding what to do with a conflict —
    this module never auto-resolves (fail-closed per SOT#16 pending answer).
    """
    if fa_value != ghl_value:
        logger.info(
            "fa_max_ghl_sync: conflict detected on field '%s' "
            "(fa=%r, ghl=%r) — not auto-resolved",
            field_name, fa_value, ghl_value,
        )
        return True
    return False
