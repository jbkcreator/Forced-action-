"""
Direct-mail fallback resolver.

Priority chain for resolving a usable mailing address when skip-trace returns no phone:
  1. enriched_contacts (source='tax_collector') — billing address from tax upload
  2. voters.mailing_address                     — any voter at the property with a
                                                   separate mailing address on file
  3. owners.mailing_address                     — appraiser-provided mailing address

Called from the skip-trace waterfall on final MISS to set owners.direct_mail_eligible.
"""

import logging
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


@dataclass
class MailingResolution:
    address: Optional[str]
    source: Optional[str]  # 'tax_collector' | 'voter' | 'owner' | None


def resolve_best_mailing_address(property_id: int, session: Session) -> MailingResolution:
    """
    Return the highest-confidence mailing address available for a property.

    Priority: tax_collector enriched contact → voter mailing → owner mailing.
    Returns MailingResolution(address=None, source=None) when nothing is found.
    """
    # 1. Tax-collector billing address
    row = session.execute(
        text("""
            SELECT mailing_address FROM enriched_contacts
            WHERE property_id = :pid AND source = 'tax_collector'
              AND mailing_address IS NOT NULL
            ORDER BY updated_at DESC NULLS LAST
            LIMIT 1
        """),
        {"pid": property_id},
    ).mappings().first()
    if row and row["mailing_address"]:
        return MailingResolution(address=row["mailing_address"], source="tax_collector")

    # 2. Voter mailing address (any voter at this property with a separate mailing)
    row = session.execute(
        text("""
            SELECT mailing_address FROM voters
            WHERE property_id = :pid AND mailing_address IS NOT NULL
            ORDER BY updated_at DESC NULLS LAST
            LIMIT 1
        """),
        {"pid": property_id},
    ).mappings().first()
    if row and row["mailing_address"]:
        return MailingResolution(address=row["mailing_address"], source="voter")

    # 3. Owner mailing address (appraiser-provided)
    row = session.execute(
        text("""
            SELECT mailing_address FROM owners
            WHERE property_id = :pid AND mailing_address IS NOT NULL
            LIMIT 1
        """),
        {"pid": property_id},
    ).mappings().first()
    if row and row["mailing_address"]:
        return MailingResolution(address=row["mailing_address"], source="owner")

    return MailingResolution(address=None, source=None)


def flag_direct_mail_eligible(property_id: int, session: Session) -> bool:
    """
    Resolve the best mailing address for a property; if found, set
    owners.direct_mail_eligible = true.

    Returns True if the flag was set, False if no address was found.
    """
    resolution = resolve_best_mailing_address(property_id, session)
    if not resolution.address:
        return False

    session.execute(
        text("""
            UPDATE owners
            SET direct_mail_eligible = true
            WHERE property_id = :pid
              AND direct_mail_eligible = false
        """),
        {"pid": property_id},
    )
    logger.info(
        "direct_mail_eligible=true for property_id=%d (source=%s)",
        property_id, resolution.source,
    )
    return True
