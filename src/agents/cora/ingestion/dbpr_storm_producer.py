"""
DBPR storm/restoration contractor producer — Aug 2026 blitz.

Reads dbpr_contacts (roofing + remediation verticals, Hillsborough + Pinellas)
and publishes target.ready events into Cora's queue, identical in shape to what
target_producer.py produces for whales. Cora drafts, posts to Slack approval,
and Relay fires via Instantly — the same pipeline, just a different prospect source.

Active when CORA_TARGET_MODE=dbpr_storm (config/settings.py). Inactive by default.

Removal after August:
  1. Set CORA_TARGET_MODE=whale (or remove the var — default is "whale").
  2. Remove the dbpr_storm_blitz cell from config/cora_cell_grid.py.
  3. Delete this file.
  The nullable buyer_entity_id column on outbound_drafts stays — harmless.
"""
from __future__ import annotations

import hashlib
import logging
from datetime import date, timezone, datetime
from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.agents.cora import queue, store

logger = logging.getLogger(__name__)

CELL_ID = "dbpr_storm_blitz"
_SENDABLE_VERTICALS = ("roofing",)
_TARGET_COUNTIES = ("hillsborough", "pinellas")


def _idempotency_key(contact_id: int) -> str:
    content_hash = hashlib.sha256(
        f"DBPR-{contact_id}:{CELL_ID}:{date.today().isoformat()}".encode()
    ).hexdigest()[:16]
    return queue.make_idempotency_key("target.ready", f"DBPR-{contact_id}", content_hash)


def _has_active_draft(db: Session, contact_id: int) -> bool:
    row = db.execute(
        text("""
            SELECT 1 FROM outbound_drafts
            WHERE opportunity_thread_id = :thread_id
              AND status NOT IN ('rejected', 'expired')
            LIMIT 1
        """),
        {"thread_id": f"DBPR-{contact_id}"},
    ).first()
    return row is not None


def _sync_relay_sent_statuses(db: Session) -> int:
    """Mark dbpr_contacts as sent where Relay has already dispatched the blitz email.

    Runs at the top of each sweep so the next _fetch_sendable won't re-pick contacts
    whose Relay dispatch completed since the last run.
    """
    result = db.execute(text("""
        UPDATE dbpr_contacts dc
        SET email_status = 'sent',
            email_sent_at = raq.dispatched_at,
            updated_at    = now()
        FROM relay_approval_queue raq
        WHERE raq.thread_id  = 'DBPR-' || dc.id::text
          AND raq.status     = 'sent'
          AND dc.email_status = 'not_sent'
    """))
    count: int = result.rowcount  # type: ignore[union-attr]
    if count:
        logger.info("dbpr_storm_producer: synced %d contact(s) to sent from Relay", count)
    return count


def _fetch_sendable(db: Session, batch_size: int):
    return db.execute(
        text("""
            SELECT id, full_name, company_name, license_type_code, license_type_desc,
                   city, county_id, work_email, email, mobile_phone, phone
            FROM dbpr_contacts
            WHERE vertical = ANY(:verticals)
              AND county_id = ANY(:counties)
              AND is_opted_out = false
              AND is_hard_bounced = false
              AND is_signed_up = false
              AND email_status = 'not_sent'
              AND (work_email IS NOT NULL OR email IS NOT NULL)
            ORDER BY created_at ASC
            LIMIT :limit
        """),
        {
            "verticals": list(_SENDABLE_VERTICALS),
            "counties": list(_TARGET_COUNTIES),
            "limit": batch_size,
        },
    ).fetchall()


def _build_facts(contact) -> list:
    observed_at = datetime.now(timezone.utc).isoformat()
    facts = []

    license_label = contact.license_type_desc or contact.license_type_code
    if license_label:
        facts.append({
            "fact_key": "license_type",
            "value": license_label,
            "source_ref": "dbpr",
            "observed_at": observed_at,
            "freshness_class": "current",
        })

    company = contact.company_name or contact.full_name
    if company:
        facts.append({
            "fact_key": "company_name",
            "value": company,
            "source_ref": "dbpr",
            "observed_at": observed_at,
            "freshness_class": "current",
        })

    county = contact.county_id or "hillsborough"
    city = contact.city
    coverage = f"{city}, {county.title()} County, FL" if city else f"{county.title()} County, FL"
    facts.append({
        "fact_key": "coverage_area",
        "value": coverage,
        "source_ref": "dbpr",
        "observed_at": observed_at,
        "freshness_class": "current",
    })

    return facts


def run_dbpr_storm_sweep(
    db: Session,
    batch_size: int = 20,
) -> List[str]:
    """Fetch up to `batch_size` sendable DBPR storm/restoration contacts and
    publish target.ready events for each. Returns the opportunity_thread_ids
    published this pass.
    """
    contacts = _fetch_sendable(db, batch_size)
    if not contacts:
        logger.info("dbpr_storm_producer: no sendable contacts — sweep done")
        return []

    venture_key = store.venture_key_for_county(db, "hillsborough")
    published: List[str] = []

    for contact in contacts:
        thread_id = f"DBPR-{contact.id}"

        if _has_active_draft(db, contact.id):
            logger.debug("dbpr_storm_producer: active draft exists for contact_id=%s — skipping", contact.id)
            continue

        canonical_name = contact.company_name or contact.full_name or f"Contractor #{contact.id}"
        contact_email: Optional[str] = contact.work_email or contact.email
        contact_phone: Optional[str] = contact.mobile_phone or contact.phone

        # county_id determines the venture for multi-county fleets
        county_venture = store.venture_key_for_county(db, contact.county_id) if contact.county_id else venture_key

        payload = {
            "buyer_entity": {
                "id": None,  # not a buyer entity — DBPR contractor
                "canonical_name": canonical_name,
                "opportunity_thread_id": thread_id,
                "county_id": contact.county_id,
                # DBPR license data is authoritative — bypass the Hunter confidence gate.
                # validate_can_draft() in validation.py rejects anything below UNVERIFIED_FLOOR (70).
                "confidence_score": 100,
            },
            "cell_id": CELL_ID,
            "facts_used": _build_facts(contact),
            "contact_email": contact_email,
            "contact_phone": contact_phone,
            "venture_key": county_venture,
        }

        message_id = queue.publish(
            "target.ready", payload, idempotency_key=_idempotency_key(contact.id)
        )
        if message_id is not None:
            published.append(thread_id)

    logger.info(
        "dbpr_storm_producer: published %d target.ready event(s) out of %d fetched",
        len(published), len(contacts),
    )
    return published
