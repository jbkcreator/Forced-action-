"""Migration: ofr_mortgage_brokers — WP-W0-1 Pool 3.

Stores Florida OFR Chapter 494 mortgage-broker BUSINESS licenses (MBR / MBRB),
loaded from the OFR "Ch 494 Businesses - NMLS (MBR-MBRB)" bulk CSV download.

This is the authoritative FL mortgage-broker registry (spec §4.1 "professional
licensing registries").  Individual loan originators (the LO file) are NOT
loaded here — they are employees, carry no employer link and no phone, and are
not brokers.  See docs / team decision.

Pool 3's extractor (_extract_pool3_mortgage_broker) auto-activates once this
table exists and holds rows.

Idempotent — safe to re-run.

Usage:
    PYTHONPATH=. python migrations/apply_ofr_mortgage_brokers.py
"""

from __future__ import annotations

import logging
import sys

from sqlalchemy import text

from src.core.database import get_db_context

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


DDL = """
CREATE TABLE IF NOT EXISTS ofr_mortgage_brokers (
    id                      BIGSERIAL PRIMARY KEY,

    -- OFR identity
    license_number          TEXT        NOT NULL UNIQUE,   -- e.g. MBR8164 (idempotency key)
    license_type            TEXT,                          -- MBR | MBRB
    nmls_id                 TEXT,
    firm_name               TEXT,

    -- Primary (business) address
    prim_address_1          TEXT,
    prim_address_2          TEXT,
    prim_city               TEXT,
    county                  TEXT,                           -- OFR COUNTY (may be blank)
    prim_state              TEXT,
    prim_zip                TEXT,

    -- Contact
    phone_raw               TEXT,
    normalized_phone        TEXT,                           -- E.164 via phone_utils, or NULL

    -- License lifecycle
    status                  TEXT,                           -- Approved | Expired | Terminated | ...
    status_effective_date   DATE,
    initial_approval        DATE,

    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Pool 3 extraction filter: active brokers by county
CREATE INDEX IF NOT EXISTS idx_ofr_brokers_status_county
    ON ofr_mortgage_brokers (status, county);

CREATE INDEX IF NOT EXISTS idx_ofr_brokers_nmls
    ON ofr_mortgage_brokers (nmls_id);
"""


def apply(session) -> None:
    logger.info("Applying ofr_mortgage_brokers DDL…")
    session.execute(text(DDL))
    session.commit()
    logger.info("Done — ofr_mortgage_brokers ready.")


def main() -> None:
    with get_db_context() as session:
        apply(session)


if __name__ == "__main__":
    main()
    sys.exit(0)
