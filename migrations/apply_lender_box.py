"""
FA MAX — Lender Box Engine schema.

Two tables:

- lender_box_programs   One row per Backflip lending program.  Stores every
                        numeric constraint as a column so the row is directly
                        readable and editable without JSON unpacking.  Programs
                        change; editing a row is a data operation, not a deploy.

- lender_box_geographies  Child table.  One row per (program, state) pair with
                          an optional county column.  NULL county means the
                          program is open across the whole state.  is_excluded
                          allows a state-wide program to carve out specific
                          counties.

Seed data is sourced from Backflip's public program pages plus synthetic
LTC/LTV/size numbers from the FA MAX Amendment (loans $150K–$2M, construction
at 85% LTC).  Replace with confirmed Backflip program rules once received.

Run once:

    PYTHONPATH=. python migrations/apply_lender_box.py

Idempotent — CREATE TABLE IF NOT EXISTS, INSERT ... ON CONFLICT DO NOTHING.
"""
import sys

sys.path.insert(0, ".")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from sqlalchemy import text
from src.core.database import get_db_context


CREATE_PROGRAMS = """
CREATE TABLE IF NOT EXISTS lender_box_programs (
    program_key             VARCHAR(60)     PRIMARY KEY,
    name                    VARCHAR(120)    NOT NULL,
    is_active               BOOLEAN         NOT NULL DEFAULT true,

    -- Loan size limits (USD)
    min_loan_amount         NUMERIC(14, 2)  NOT NULL,
    max_loan_amount         NUMERIC(14, 2)  NOT NULL,

    -- Leverage limits (stored as decimals: 0.85 = 85%)
    -- NULL means the limit does not apply for this program.
    max_ltc                 NUMERIC(5, 4),
    max_ltv                 NUMERIC(5, 4),

    -- Loan term
    min_loan_term_months    SMALLINT        NOT NULL,
    max_loan_term_months    SMALLINT        NOT NULL,

    -- Property types
    allowed_property_types  TEXT[]          NOT NULL,
    excluded_property_types TEXT[]          NOT NULL DEFAULT '{}',

    -- Borrower experience — 0 means no minimum.
    min_borrower_prior_loans SMALLINT       NOT NULL DEFAULT 0,

    -- Free-text notes visible in the EXCEPTIONS card.
    notes                   TEXT,

    -- Rule lifecycle
    effective_date          DATE            NOT NULL DEFAULT CURRENT_DATE,
    expiry_date             DATE,

    created_at              TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT ck_lender_box_programs_loan_range
        CHECK (min_loan_amount > 0 AND max_loan_amount >= min_loan_amount),
    CONSTRAINT ck_lender_box_programs_term_range
        CHECK (min_loan_term_months > 0 AND max_loan_term_months >= min_loan_term_months),
    CONSTRAINT ck_lender_box_programs_ltc
        CHECK (max_ltc IS NULL OR (max_ltc > 0 AND max_ltc <= 1)),
    CONSTRAINT ck_lender_box_programs_ltv
        CHECK (max_ltv IS NULL OR (max_ltv > 0 AND max_ltv <= 1))
)
"""

CREATE_GEOGRAPHIES = """
CREATE TABLE IF NOT EXISTS lender_box_geographies (
    id          BIGSERIAL   PRIMARY KEY,
    program_key VARCHAR(60) NOT NULL REFERENCES lender_box_programs (program_key) ON DELETE CASCADE,
    state       CHAR(2)     NOT NULL,
    county      VARCHAR(80),
    is_excluded BOOLEAN     NOT NULL DEFAULT false,

    CONSTRAINT uq_lender_box_geographies_program_state_county
        UNIQUE NULLS NOT DISTINCT (program_key, state, county)
)
"""

INDEX_STATEMENTS = [
    "CREATE INDEX IF NOT EXISTS ix_lender_box_programs_active ON lender_box_programs (is_active) WHERE is_active = true",
    "CREATE INDEX IF NOT EXISTS ix_lender_box_geographies_program ON lender_box_geographies (program_key)",
    "CREATE INDEX IF NOT EXISTS ix_lender_box_geographies_state ON lender_box_geographies (state)",
]

# ---------------------------------------------------------------------------
# Seed data — sourced from backflip.com/loans plus Amendment doc numbers.
# Marked synthetic where confirmed values are not yet available.
# Replace with exact Backflip program rules once received from the client.
# ---------------------------------------------------------------------------

SEED_PROGRAMS = """
INSERT INTO lender_box_programs (
    program_key, name,
    min_loan_amount, max_loan_amount,
    max_ltc, max_ltv,
    min_loan_term_months, max_loan_term_months,
    allowed_property_types, excluded_property_types,
    min_borrower_prior_loans,
    notes
) VALUES
(
    'fix_and_flip',
    'Fix & Flip',
    150000, 2000000,
    0.8500, 0.7500,   -- 85% LTC from Amendment; 75% LTV synthetic
    6, 18,
    ARRAY['single_family','condo','duplex','triplex','fourplex'],
    ARRAY['commercial','mobile_home','multifamily_5plus'],
    0,
    'Core Backflip bridge product. LTC/LTV synthetic pending confirmed rules.'
),
(
    'new_construction',
    'New Construction',
    200000, 3000000,
    0.8500, 0.7000,   -- 85% LTC confirmed in Amendment; 70% LTV synthetic
    12, 24,
    ARRAY['single_family','duplex','triplex','fourplex'],
    ARRAY['commercial','mobile_home','multifamily_5plus'],
    0,
    'Ground-up construction. Scored at 85% LTC with capital interest per Amendment. LTV synthetic.'
),
(
    'dscr_rental',
    'DSCR Rental',
    150000, 2000000,
    NULL, 0.8000,     -- LTC not applicable; 80% LTV synthetic
    120, 360,
    ARRAY['single_family','condo','duplex','triplex','fourplex'],
    ARRAY['commercial','mobile_home','multifamily_5plus'],
    0,
    'Qualifies on cash flow, not personal income. LTV synthetic pending confirmed rules.'
),
(
    'home_equity',
    'Home Equity',
    50000, 500000,
    NULL, 0.8500,     -- LTC not applicable; 85% LTV synthetic
    12, 60,
    ARRAY['single_family','condo'],
    ARRAY['commercial','mobile_home','multifamily_5plus','duplex','triplex','fourplex'],
    0,
    'HELOC and HEI products. Included for completeness; unlikely to be primary Josh use case.'
)
ON CONFLICT (program_key) DO NOTHING
"""

# Josh operates in FL now, GA next per the Amendment.
SEED_GEOGRAPHIES = """
INSERT INTO lender_box_geographies (program_key, state, county, is_excluded)
VALUES
    ('fix_and_flip',     'FL', NULL, false),
    ('fix_and_flip',     'GA', NULL, false),
    ('new_construction', 'FL', NULL, false),
    ('new_construction', 'GA', NULL, false),
    ('dscr_rental',      'FL', NULL, false),
    ('dscr_rental',      'GA', NULL, false),
    ('home_equity',      'FL', NULL, false)
ON CONFLICT (program_key, state, county) DO NOTHING
"""


def main() -> int:
    with get_db_context() as db:
        db.execute(text(CREATE_PROGRAMS))
        db.execute(text(CREATE_GEOGRAPHIES))
        for stmt in INDEX_STATEMENTS:
            db.execute(text(stmt))
        db.execute(text(SEED_PROGRAMS))
        db.execute(text(SEED_GEOGRAPHIES))
        db.commit()

        programs = db.execute(text(
            "SELECT program_key, name, is_active, min_loan_amount, max_loan_amount "
            "FROM lender_box_programs ORDER BY program_key"
        )).fetchall()
        geos = db.execute(text(
            "SELECT program_key, state, county, is_excluded "
            "FROM lender_box_geographies ORDER BY program_key, state"
        )).fetchall()

    print("programs:")
    for p in programs:
        print(f"  {p.program_key}: {p.name}  active={p.is_active}  "
              f"loan=${p.min_loan_amount:,.0f}–${p.max_loan_amount:,.0f}")
    print("geographies:")
    for g in geos:
        county = g.county or "*"
        print(f"  {g.program_key}  {g.state}/{county}  excluded={g.is_excluded}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
