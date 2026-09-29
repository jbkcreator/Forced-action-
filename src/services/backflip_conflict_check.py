"""Per-borrower Backflip conflict check for dialer loads.

Every pool record is matched against the active Backflip identifier snapshot
(fa_max_backflip_campaign_contacts) before it can be loaded into the dialer.
A record is blocked when it matches Backflip on any criterion: phone hash,
primary email, email domain (see config/backflip_conflict.py), entity name
or parcel ID. Every matched criterion is reported, not only the first, so
each decision is fully auditable.

The check fails closed. A missing or stale snapshot blocks every record,
because no record can be proven clear without a current view of Backflip's
book; a record with no usable identifier is blocked for the same reason.

The snapshot is loaded once per run and matched in memory, so a pool of any
size costs two queries, and every decision is written in one batch insert.
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Iterable, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.backflip_conflict import MATCH_EMAIL_DOMAIN
from config.settings import get_settings
from src.core.database import get_db_context
from src.services.fa_max_backflip_feed import (
    normalize_email,
    normalize_entity_name,
    normalize_parcel_id,
)
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)

REASON_CONFLICT = "backflip_conflict"
REASON_FEED_UNAVAILABLE = "backflip_feed_unavailable"
REASON_FEED_STALE = "backflip_feed_stale"
REASON_NO_IDENTIFIERS = "no_matchable_identifiers"

CRITERION_PHONE_HASH = "phone_hash"
CRITERION_PRIMARY_EMAIL = "primary_email"
CRITERION_EMAIL_DOMAIN = "email_domain"
CRITERION_ENTITY_NAME = "entity_name"
CRITERION_PARCEL_ID = "parcel_id"

AUDIT_GATE = "dialer"


@dataclass(frozen=True)
class BorrowerRecord:
    """One pool record as the conflict check sees it."""

    record_ref: str
    phone: str | None = None
    email: str | None = None
    entity_name: str | None = None
    parcel_id: str | None = None


@dataclass(frozen=True)
class BackflipIdentifierIndex:
    """In-memory view of the active Backflip snapshot for one run.

    block_reason is set when the snapshot cannot be trusted; the identifier
    sets are then empty and every record is blocked.
    """

    block_reason: str | None
    phone_hashes: frozenset[str] = frozenset()
    emails: frozenset[str] = frozenset()
    email_domains: frozenset[str] = frozenset()
    entity_names: frozenset[str] = frozenset()
    parcel_ids: frozenset[str] = frozenset()


@dataclass(frozen=True)
class ConflictDecision:
    record_ref: str
    blocked: bool
    reason: str | None
    matched_criteria: tuple[str, ...]
    recipient_masked: str
    recipient_sha256: str | None


def hash_phone(normalized_phone: str) -> str:
    """SHA-256 of an already-normalized phone, the same digest the audit table stores."""
    return hashlib.sha256(normalized_phone.encode()).hexdigest()


def _email_domain(email: str) -> str:
    return email.split("@", 1)[1]


def load_backflip_identifier_index(session: Session) -> BackflipIdentifierIndex:
    """Read feed freshness and the active snapshot once, and index it by criterion."""
    max_age_hours = get_settings().fa_max_backflip_feed_max_age_hours
    fresh = session.execute(
        text(
            "SELECT last_success_at >= now() - make_interval(hours => :max_age) "
            "FROM fa_max_backflip_campaign_feed WHERE id = 1"
        ),
        {"max_age": max_age_hours},
    ).scalar_one_or_none()
    if fresh is None:
        return BackflipIdentifierIndex(block_reason=REASON_FEED_UNAVAILABLE)
    if not fresh:
        return BackflipIdentifierIndex(block_reason=REASON_FEED_STALE)

    rows = session.execute(
        text(
            "SELECT identifier_kind, identifier_value "
            "FROM fa_max_backflip_campaign_contacts WHERE active"
        )
    ).fetchall()

    values_by_kind: dict[str, set[str]] = {
        "phone": set(), "email": set(), "entity_name": set(), "parcel_id": set(),
    }
    for kind, value in rows:
        bucket = values_by_kind.get(kind)
        if bucket is not None:
            bucket.add(value)

    emails = frozenset(values_by_kind["email"])
    return BackflipIdentifierIndex(
        block_reason=None,
        phone_hashes=frozenset(hash_phone(phone) for phone in values_by_kind["phone"]),
        emails=emails,
        email_domains=frozenset(_email_domain(email) for email in emails),
        entity_names=frozenset(values_by_kind["entity_name"]),
        parcel_ids=frozenset(values_by_kind["parcel_id"]),
    )


def _mask(value: str) -> str:
    return f"...{value[-4:]}" if len(value) > 4 else "***"


def _decide(
    record: BorrowerRecord, index: BackflipIdentifierIndex, *, match_email_domain: bool,
) -> ConflictDecision:
    phone = normalize_phone(record.phone) if record.phone else None
    email = normalize_email(record.email)
    entity_name = normalize_entity_name(record.entity_name)
    parcel_id = normalize_parcel_id(record.parcel_id)
    phone_digest = hash_phone(phone) if phone else None

    audit_identifier = phone or email
    masked = _mask(audit_identifier) if audit_identifier else "***"
    digest = phone_digest or (hashlib.sha256(email.encode()).hexdigest() if email else None)

    def decision(blocked: bool, reason: str | None, criteria: tuple[str, ...] = ()) -> ConflictDecision:
        return ConflictDecision(record.record_ref, blocked, reason, criteria, masked, digest)

    if index.block_reason:
        return decision(True, index.block_reason)
    if not (phone or email or entity_name or parcel_id):
        return decision(True, REASON_NO_IDENTIFIERS)

    matched: list[str] = []
    if phone_digest and phone_digest in index.phone_hashes:
        matched.append(CRITERION_PHONE_HASH)
    if email and email in index.emails:
        matched.append(CRITERION_PRIMARY_EMAIL)
    if match_email_domain and email and _email_domain(email) in index.email_domains:
        matched.append(CRITERION_EMAIL_DOMAIN)
    if entity_name and entity_name in index.entity_names:
        matched.append(CRITERION_ENTITY_NAME)
    if parcel_id and parcel_id in index.parcel_ids:
        matched.append(CRITERION_PARCEL_ID)

    if matched:
        return decision(True, REASON_CONFLICT, tuple(matched))
    return decision(False, None)


def find_borrower_conflicts(
    records: Iterable[BorrowerRecord],
    index: BackflipIdentifierIndex,
    *,
    match_email_domain: bool = MATCH_EMAIL_DOMAIN,
) -> list[ConflictDecision]:
    """One decision per record, in input order. Pure: no database access."""
    decisions = [_decide(record, index, match_email_domain=match_email_domain) for record in records]
    blocked = sum(1 for item in decisions if item.blocked)
    logger.info(
        "[BackflipConflict] checked=%d blocked=%d clear=%d feed_block=%s",
        len(decisions), blocked, len(decisions) - blocked, index.block_reason,
    )
    return decisions


def record_dialer_conflict_decisions(decisions: Sequence[ConflictDecision]) -> None:
    """Write every decision to fa_max_backflip_suppression_decisions in one batch.

    Commits in its own transaction so the audit trail survives a rollback in
    the caller's load. A failure raises: a load must not proceed without a
    durable record of what was blocked and why.
    """
    if not decisions:
        return
    params = [
        {
            "gate": AUDIT_GATE,
            "masked": item.recipient_masked,
            "digest": item.recipient_sha256,
            "suppressed": item.blocked,
            "reason": item.reason,
            "subject_ref": item.record_ref,
            "criteria": list(item.matched_criteria) or None,
        }
        for item in decisions
    ]
    with get_db_context() as session:
        session.execute(
            text(
                "INSERT INTO fa_max_backflip_suppression_decisions "
                "(gate, recipient_masked, recipient_sha256, suppressed, reason, "
                "subject_ref, matched_criteria) "
                "VALUES (:gate, :masked, :digest, :suppressed, :reason, "
                ":subject_ref, :criteria)"
            ),
            params,
        )
