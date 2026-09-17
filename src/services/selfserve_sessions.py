"""Self-serve session persistence — WP-7 WI-3.

Two identity resolutions on submit, not one (plan §1.5): `buyer_entity_id`
(WP-3/WP-4 deed-side identity) and `person_id` (WP-1 governance-side
identity, required by relay_approval_queue's live CHECK constraint for any
outbound send this session later triggers). Nothing in this codebase bridges
the two yet, so both are resolved independently here — flagged to the team
lead as shared infrastructure, not solved inside WP-7.

Known limitation, documented rather than papered over: fa_max_persons has no
contact (email/phone) column or table yet, so there is no way to match an
existing person by contact info. Every selfserve submission therefore
creates a NEW fa_max_persons row. Closing this requires WP-1 to add a contact
anchor to fa_max_persons — tracked as plan §7 Q10.
"""
from __future__ import annotations

import json
import logging
import uuid
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.core.models import SelfserveSession
from src.services.phone_utils import normalize as normalize_phone

logger = logging.getLogger(__name__)

_VALID_CONSENT_CHANNELS = ("email", "sms", "voice")


def create_session(
    db: Session,
    prefill_snapshot: dict,
    tracked_link_id: Optional[int] = None,
    property_id: Optional[int] = None,
) -> SelfserveSession:
    session_row = SelfserveSession(
        token=str(uuid.uuid4()),
        tracked_link_id=tracked_link_id,
        property_id=property_id,
        prefill_snapshot=prefill_snapshot,
        status="prefilled" if prefill_snapshot.get("fields") else "started",
    )
    db.add(session_row)
    db.flush()
    return session_row


def get_session_by_token(db: Session, token: str) -> Optional[SelfserveSession]:
    row = db.execute(
        text(
            "SELECT id, token, tracked_link_id, property_id, buyer_entity_id, "
            "person_id, prefill_snapshot, corrections, confirmations, contact, "
            "status, handoff_ref, handed_off_at, started_at, last_activity_at "
            "FROM selfserve_sessions WHERE token = :token"
        ),
        {"token": token},
    ).mappings().first()
    if row is None:
        return None
    return SelfserveSession(**dict(row))


def resolve_buyer_entity_id(db: Session, property_id: Optional[int]) -> Optional[int]:
    """If the property's current owner is already resolved into a
    BuyerEntity, attach to it. Otherwise NULL for the nightly resolver
    (unchanged from the original plan — reuses WP-3/WP-4's existing
    buyer_entity_links traceability table, no new resolution logic)."""
    if property_id is None:
        return None
    row = db.execute(
        text(
            """
            SELECT bel.buyer_entity_id
            FROM owners o
            JOIN buyer_entity_links bel
              ON bel.source_table = 'owners' AND bel.source_id = o.id
            WHERE o.property_id = :property_id
            ORDER BY bel.match_confidence DESC
            LIMIT 1
            """
        ),
        {"property_id": property_id},
    ).first()
    return row.buyer_entity_id if row else None


def resolve_or_create_person(db: Session, source_reference: str) -> str:
    """Create a fa_max_persons row for this session (see module docstring —
    contact-based matching against an existing person is not possible yet)."""
    row = db.execute(
        text(
            "INSERT INTO fa_max_persons (source, source_reference) "
            "VALUES ('selfserve_flow', :ref) RETURNING person_id"
        ),
        {"ref": source_reference},
    ).first()
    if row is None:
        raise RuntimeError("fa_max_persons insert did not return a person_id")
    return str(row.person_id)


def find_possible_person_match(
    db: Session, email: Optional[str], phone: Optional[str], exclude_token: str
) -> Optional[str]:
    """Look for a DIFFERENT selfserve session whose recorded contact shares
    this email or normalized phone. `fa_max_persons` has no contact column
    (module docstring), so this reads the one place contact info actually
    lives today — other sessions' `contact` JSONB — as a stopgap.

    Never merges. Per spec (failure-behavior section, L567): "Identity
    resolution is uncertain. Records stay separate and a possible-match flag
    routes to EXCEPTIONS. Never auto-merged below a confidence threshold."
    """
    phone_norm = normalize_phone(phone) if phone else None
    if not email and not phone_norm:
        return None
    row = db.execute(
        text(
            """
            SELECT person_id FROM selfserve_sessions
            WHERE token <> :exclude_token AND person_id IS NOT NULL
              AND (
                (:email IS NOT NULL AND lower(contact->>'email') = lower(:email))
                OR (:phone IS NOT NULL AND contact->>'phone' = :phone)
              )
            ORDER BY started_at DESC
            LIMIT 1
            """
        ),
        {"exclude_token": exclude_token, "email": email, "phone": phone_norm},
    ).first()
    return str(row.person_id) if row else None


def flag_possible_identity_match(db: Session, new_person_id: str, existing_person_id: str, session_token: str) -> None:
    """Surface a possible-duplicate person to the EXCEPTIONS lane for human
    review (spec L567) — never auto-merged. Writes into the existing WP-2
    relay_approval_queue/Slack-delivery pipeline (src/services/relay/), so
    WP-7 does not need its own Slack-posting code: whatever sweep already
    turns pending EXCEPTIONS-lane rows into Slack cards picks this up too.
    Idempotent on session_token — a re-submit does not duplicate the flag."""
    db.execute(
        text(
            """
            INSERT INTO relay_approval_queue
                (idempotency_key, venture_key, lane, channel, recipient, payload,
                 status, agent_name, autonomy_tier_at_send, person_id)
            VALUES
                (:idempotency_key, 'fa_max_lending', 'EXCEPTIONS', 'noop', 'n/a',
                 CAST(:payload AS JSONB), 'pending', 'selfserve_identity_check', 'A', :person_id)
            ON CONFLICT (idempotency_key) DO NOTHING
            """
        ),
        {
            "idempotency_key": f"selfserve-possible-match-{session_token}",
            "payload": json.dumps({
                "reason": "possible_duplicate_person",
                "new_person_id": new_person_id,
                "existing_person_id": existing_person_id,
                "session_token": session_token,
                "source": "wp7_selfserve",
            }),
            "person_id": new_person_id,
        },
    )


def record_consent(db: Session, person_id: str, channels: list[str], source: str = "selfserve_flow") -> None:
    """Write one fa_max_person_consent row per channel the borrower checked.
    Idempotent — (person_id, channel) is unique, a re-submit updates in place."""
    for channel in channels:
        if channel not in _VALID_CONSENT_CHANNELS:
            logger.warning("record_consent: skipping unknown channel=%r", channel)
            continue
        db.execute(
            text(
                """
                INSERT INTO fa_max_person_consent (person_id, channel, consented, source, consented_at)
                VALUES (:person_id, :channel, true, :source, now())
                ON CONFLICT (person_id, channel)
                DO UPDATE SET consented = true, source = :source, consented_at = now()
                """
            ),
            {"person_id": person_id, "channel": channel, "source": source},
        )


def has_consent(db: Session, person_id: str, channel: str) -> bool:
    row = db.execute(
        text(
            "SELECT consented FROM fa_max_person_consent "
            "WHERE person_id = :person_id AND channel = :channel"
        ),
        {"person_id": person_id, "channel": channel},
    ).first()
    return bool(row and row.consented)


def is_backflip_suppressed(db: Session, email: Optional[str] = None, phone: Optional[str] = None) -> bool:
    """Check the active-Backflip-touch suppression store (WI-4). Client's
    answered attribution rule: if a prospect already has an active Backflip
    campaign touch in motion, Forced Action holds off. The table is live but
    empty until Backflip's feed ships (plan §7 Q7) — returns False for
    everyone until then, which is correct, not a bug to work around."""
    phone_norm = normalize_phone(phone) if phone else None
    if not email and not phone_norm:
        return False
    row = db.execute(
        text(
            """
            SELECT 1 FROM fa_max_backflip_campaign_contacts
            WHERE active = true AND (
                (identifier_kind = 'email' AND identifier_value = :email)
                OR (identifier_kind = 'phone' AND identifier_value = :phone)
            )
            LIMIT 1
            """
        ),
        {"email": (email or "").lower(), "phone": phone_norm or ""},
    ).first()
    return row is not None


def submit_session(
    db: Session,
    token: str,
    corrections: Optional[dict],
    confirmations: dict,
    contact: dict,
    consent_channels: list[str],
) -> SelfserveSession:
    """WI-3's submit path: resolve both identities, persist what the borrower
    entered, record consent. Does not handoff — that is WI-6's job."""
    session_row = get_session_by_token(db, token)
    if session_row is None:
        raise ValueError(f"selfserve session not found for token={token!r}")

    person_id = resolve_or_create_person(db, source_reference=token)
    buyer_entity_id = resolve_buyer_entity_id(db, session_row.property_id)

    possible_match = find_possible_person_match(
        db, email=contact.get("email"), phone=contact.get("phone"), exclude_token=token
    )
    if possible_match:
        flag_possible_identity_match(db, new_person_id=person_id, existing_person_id=possible_match, session_token=token)

    if consent_channels:
        record_consent(db, person_id, consent_channels)

    db.execute(
        text(
            """
            UPDATE selfserve_sessions
            SET corrections = CAST(:corrections AS JSONB),
                confirmations = CAST(:confirmations AS JSONB),
                contact = CAST(:contact AS JSONB),
                person_id = :person_id,
                buyer_entity_id = :buyer_entity_id,
                status = 'confirmed',
                last_activity_at = now()
            WHERE token = :token
            """
        ),
        {
            "corrections": json.dumps(corrections) if corrections is not None else None,
            "confirmations": json.dumps(confirmations),
            "contact": json.dumps(contact),
            "person_id": person_id,
            "buyer_entity_id": buyer_entity_id,
            "token": token,
        },
    )
    db.flush()
    updated = get_session_by_token(db, token)
    if updated is None:
        raise RuntimeError(f"selfserve session vanished mid-submit for token={token!r}")
    return updated


def mark_abandoned(db: Session, token: str) -> None:
    """Keep whatever was captured (plan §7 Q8) — status changes, data stays.
    No outbound contact fires against an abandoned session without a
    consent row; that check lives in the outbound path (WI-4/WI-6), not here."""
    db.execute(
        text(
            "UPDATE selfserve_sessions SET status = 'abandoned', last_activity_at = now() "
            "WHERE token = :token AND status NOT IN ('handed_off', 'confirmed')"
        ),
        {"token": token},
    )


def list_stale_session_tokens(db: Session, older_than_hours: int) -> list[str]:
    """Sessions still in-flight (never confirmed or handed off) whose last
    activity is older than the cutoff — candidates for mark_abandoned. Used
    by src/tasks/selfserve_abandonment_sweep.py."""
    rows = db.execute(
        text(
            "SELECT token FROM selfserve_sessions "
            "WHERE status IN ('started', 'prefilled') "
            "AND last_activity_at < now() - make_interval(hours => :hours)"
        ),
        {"hours": older_than_hours},
    ).fetchall()
    return [str(r.token) for r in rows]


def list_consented_abandoned_contacts(db: Session, channel: str) -> list[dict]:
    """Abandoned sessions whose contact has recorded consent for `channel` —
    the pre-filtered interface a future rescue-outreach agent (spec item 21)
    would read from. Built now, cheap, and exercises has_consent's contract;
    the outreach agent itself sending anything is out of WP-7 scope."""
    rows = db.execute(
        text(
            "SELECT token, person_id, contact FROM selfserve_sessions "
            "WHERE status = 'abandoned' AND person_id IS NOT NULL"
        )
    ).mappings().all()
    return [
        {"token": str(row["token"]), "person_id": str(row["person_id"]), "contact": row["contact"]}
        for row in rows
        if has_consent(db, str(row["person_id"]), channel)
    ]
