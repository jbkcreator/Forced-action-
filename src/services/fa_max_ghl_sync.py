"""FA Max ↔ GoHighLevel relationship-state sync boundary (WP-2).

SOT.md clarification #16 is answered: Forced Action owns borrower, prospect,
and relationship data. Backflip GHL owns the status of a submitted loan in
its own system. This module remains a fail-closed integration boundary until
the GHL field mapping and live adapter are implemented.

    Do NOT call any GHL API directly from business logic. Wire calls through
    this module only so the boundary stays checkable by a structural test.

Ownership rule:
    - Forced Action is authoritative for all borrower, prospect, contact,
      relationship, lifecycle, and opportunity fields it stores.
    - Backflip GHL is authoritative only for Backflip loan status. That
      status may be recorded as external loan status, never as a replacement
      for the Forced Action relationship record.

This boundary is NOT the single-sender relay. It never produces outbound
contact to a borrower; it is a CRM sync operation only.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Explicit allowlist: a new GHL field must not silently gain write access to
# the Forced Action relationship record.
GHL_OWNED_FIELDS = frozenset({"backflip_loan_status"})


def ghl_may_update_field(field_name: str) -> bool:
    """Whether a GHL-origin update may enter the external-loan-status path."""
    return field_name in GHL_OWNED_FIELDS


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

    STUB — no live field mapping or adapter exists yet. No GHL write occurs.
    """
    logger.warning(
        "fa_max_ghl_sync.push_lifecycle_state_to_ghl: no live adapter "
        "person=%s ghl_contact=%s state=%s actor=%s",
        person_id, ghl_contact_id, new_lifecycle_state, actor,
    )
    return SyncResult(
        outcome=SyncOutcome.blocked,
        direction=SyncDirection.fa_to_ghl,
        person_id=person_id,
        ghl_contact_id=ghl_contact_id,
        fields_synced=[],
        conflict_fields=[],
        message="GHL sync blocked: live field mapping and adapter are not configured. No data written to GHL.",
    )


def pull_contact_fields_from_ghl(
    *,
    ghl_contact_id: str,
    fields: list,
    actor: str,
) -> SyncResult:
    """Pull specified GHL contact fields into FA Max.

    Rejects any attempt to import GHL contact or relationship fields. Even
    allowed loan status remains blocked until an external-status adapter is
    implemented; this function never writes the Forced Action record.
    """
    logger.warning(
        "fa_max_ghl_sync.pull_contact_fields_from_ghl: no live adapter "
        "ghl_contact=%s fields=%s actor=%s",
        ghl_contact_id, fields, actor,
    )
    forbidden = [field for field in fields if not ghl_may_update_field(field)]
    return SyncResult(
        outcome=SyncOutcome.conflict if forbidden else SyncOutcome.blocked,
        direction=SyncDirection.ghl_to_fa,
        person_id=None,
        ghl_contact_id=ghl_contact_id,
        fields_synced=[],
        conflict_fields=forbidden,
        message=("GHL cannot overwrite Forced Action owned fields. No data imported."
                 if forbidden else "GHL loan status adapter is not configured. No data imported."),
    )


def detect_field_conflict(
    fa_value: Any,
    ghl_value: Any,
    field_name: str,
) -> bool:
    """Return True if FA Max and GHL hold different values for the same field.

    Caller is responsible for applying the ownership rule; this function
    is diagnostic only and never authorizes a write.
    """
    if fa_value != ghl_value:
        logger.info(
            "fa_max_ghl_sync: conflict detected on field '%s' "
            "(fa=%r, ghl=%r) — not auto-resolved",
            field_name, fa_value, ghl_value,
        )
        return True
    return False
