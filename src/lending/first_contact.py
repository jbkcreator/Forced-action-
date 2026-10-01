"""Write-once snapshot of a lead at its first contact.

Captured the first time a caller reaches a phone: the scoring inputs with
their provenance, the rank, the source tag, the caller, the script version
and the date. Later calls never overwrite it, so point-in-time cohorts can be
built when a trained model arrives. Warm and cold leads are kept apart by the
``warm`` flag.

Called by the call-event intake once a call to the phone is recorded. Does not
commit: the caller owns the transaction.
"""
from __future__ import annotations

import json
from dataclasses import fields
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text as sa_text

from src.lending.lead_facts import load_lead_facts
from src.lending.lead_scoring import LeadScore, LeadSignals, score_lead
from src.services.phone_utils import normalize as normalize_phone

# Scoring dates (the 90-day maturity window) follow the callers' Eastern calendar day.
LENDING_TIMEZONE = ZoneInfo("America/New_York")


def _jsonable(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def signals_payload(signals: LeadSignals) -> dict[str, dict[str, Any]]:
    """Each input's value and provenance, JSON-ready."""
    return {
        field.name: {
            "value": _jsonable(getattr(signals, field.name).value),
            "provenance": getattr(signals, field.name).provenance.value,
        }
        for field in fields(signals)
    }


def record_first_contact(
    session,
    *,
    phone: str,
    contacted_at: datetime,
    caller_seat: Optional[str],
    script_version: Optional[str],
    source_tag: Optional[str],
    queue: Optional[str],
    warm: bool,
    signals: LeadSignals,
    score: LeadScore,
) -> bool:
    """Store the snapshot if this phone has none yet. True when a row was written."""
    normalized = normalize_phone(phone)
    if not normalized:
        raise ValueError("cannot snapshot an invalid phone")
    if contacted_at.tzinfo is None:
        raise ValueError("contacted_at must be timezone-aware")
    result = session.execute(
        sa_text(
            """
            INSERT INTO lending.first_contact_snapshots
                (phone, first_contact_at, caller_seat, script_version, source_tag,
                 queue, warm, rank, signals, labels)
            VALUES
                (:phone, :first_contact_at, :caller_seat, :script_version, :source_tag,
                 :queue, :warm, :rank, CAST(:signals AS jsonb), CAST(:labels AS jsonb))
            ON CONFLICT (phone) DO NOTHING
            """
        ),
        {
            "phone": normalized,
            "first_contact_at": contacted_at,
            "caller_seat": caller_seat,
            "script_version": script_version,
            "source_tag": source_tag,
            "queue": queue,
            "warm": warm,
            "rank": score.rank,
            "signals": json.dumps(signals_payload(signals)),
            "labels": json.dumps(score.labels),
        },
    )
    return result.rowcount == 1


def has_first_contact(session, phone: str) -> bool:
    """Whether a snapshot already exists for this phone."""
    normalized = normalize_phone(phone)
    if not normalized:
        return False
    return session.execute(
        sa_text("SELECT 1 FROM lending.first_contact_snapshots WHERE phone = :phone"),
        {"phone": normalized},
    ).first() is not None


def snapshot_first_contact(
    session,
    *,
    phone: str,
    property_id: Optional[int],
    contacted_at: datetime,
    caller_seat: Optional[str],
    script_version: Optional[str],
    source_tag: Optional[str],
    queue: Optional[str],
    warm: bool,
) -> bool:
    """Score the lead as it stands now and snapshot it, once per phone.

    The single call the call-record intake makes for every call: later calls to
    the same phone return False after one indexed lookup, without loading facts.
    A call with no resolvable property is still snapshotted with every input
    missing, so the first contact is never lost. Does not commit.
    """
    if has_first_contact(session, phone):
        return False
    if contacted_at.tzinfo is None:
        raise ValueError("contacted_at must be timezone-aware")
    today = contacted_at.astimezone(LENDING_TIMEZONE).date()
    facts = load_lead_facts(session, [property_id], today=today).get(property_id) if property_id else None
    signals = facts.signals if facts else LeadSignals()
    return record_first_contact(
        session,
        phone=phone,
        contacted_at=contacted_at,
        caller_seat=caller_seat,
        script_version=script_version,
        source_tag=source_tag,
        queue=queue,
        warm=warm,
        signals=signals,
        score=score_lead(signals, today=today),
    )
