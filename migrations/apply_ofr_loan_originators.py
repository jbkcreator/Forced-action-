"""Migration: ofr_loan_originators — List 4 "brokers and LOs" gap (WP-W0-1).

Stores Florida OFR individual Loan Originator (LO) licenses, loaded from the
OFR "LO" bulk download (3 monthly zips split by surname range: AI, JR, SZ)
at real.flofr.com/Public/LO/.

NATIONWIDE NMLS registry, confirmed from real sample data: most records are
out-of-state individuals (Michigan, Oregon addresses seen in the AI file)
holding a remote FL LO license, NOT a Florida-residents file. Phone is blank
on virtually every sampled row — every row needs skip-trace before calling.

NOT auto-wired into Pool 3's extraction. Josh's own list taxonomy names
List 4 "brokers and LOs", but our Pool 3 only loads broker businesses
(MBR/MBRB) today. This table lets us measure real LO volume/coverage before
the client decides whether LOs are in scope for launch — loading this table
does not by itself add LOs to the dialer pool.

Idempotent — safe to re-run.

Usage:
    PYTHONPATH=. python migrations/apply_ofr_loan_originators.py
"""

from __future__ import annotations

import logging
import sys

from sqlalchemy import text

from src.core.database import get_db_context

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


DDL = """
CREATE TABLE IF NOT EXISTS ofr_loan_originators (
    id                      BIGSERIAL PRIMARY KEY,

    -- OFR identity
    license_number          TEXT        NOT NULL UNIQUE,   -- e.g. LO121274 (idempotency key)
    nmls_id                 TEXT,
    last_name               TEXT,
    first_name              TEXT,
    middle_name             TEXT,

    -- Primary (home/business) address — frequently out-of-state; see module docstring
    prim_address_1          TEXT,
    prim_address_2          TEXT,
    prim_city               TEXT,
    county                  TEXT,                           -- OFR COUNTY (may be out-of-state)
    prim_state              TEXT,
    prim_zip                TEXT,

    -- Contact
    phone_raw               TEXT,                           -- blank on nearly every real row
    normalized_phone        TEXT,                           -- E.164 via phone_utils, or NULL

    -- License lifecycle
    status                  TEXT,                           -- Approved | Expired | ...
    status_effective_date   DATE,
    initial_approval        DATE,

    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Volume/coverage check: how many Approved LOs are actually FL-based, by state
CREATE INDEX IF NOT EXISTS idx_ofr_los_status_state
    ON ofr_loan_originators (status, prim_state);

CREATE INDEX IF NOT EXISTS idx_ofr_los_nmls
    ON ofr_loan_originators (nmls_id);
"""


def apply(session) -> None:
    logger.info("Applying ofr_loan_originators DDL…")
    session.execute(text(DDL))
    session.commit()
    logger.info("Done — ofr_loan_originators ready.")


def main() -> None:
    with get_db_context() as session:
        apply(session)


if __name__ == "__main__":
    main()
    sys.exit(0)
