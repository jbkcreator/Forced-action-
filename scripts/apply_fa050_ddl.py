"""Apply fa050 expansion_icp_channels DDL directly via SQLAlchemy.

Alembic CLI is unusable (multi-head tree). Creates the table idempotently
and inserts the REI Investor seed row.

Usage: python scripts/apply_fa050_ddl.py
"""

from sqlalchemy import text
from src.core.database import Database

DDL = [
    """
    CREATE TABLE IF NOT EXISTS expansion_icp_channels (
        id             SERIAL PRIMARY KEY,
        key            VARCHAR(40)    NOT NULL UNIQUE,
        display_name   VARCHAR(80)    NOT NULL,
        price_monthly  NUMERIC(10,2)  NOT NULL,
        persona        TEXT,
        data_source    VARCHAR(120),
        feed_scope     VARCHAR(20)    NOT NULL DEFAULT 'single_county'
                           CHECK (feed_scope IN ('single_county','multi_county')),
        landing_slug   VARCHAR(60)    NOT NULL UNIQUE,
        status         VARCHAR(16)    NOT NULL DEFAULT 'gated'
                           CHECK (status IN ('configured','gated','approved','live','retired')),
        created_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        updated_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
    )
    """,
]

SEED = """
    INSERT INTO expansion_icp_channels
        (key, display_name, price_monthly, persona, data_source,
         feed_scope, landing_slug, status)
    VALUES
        (
            'rei_investor',
            'REI Investor',
            197.00,
            'Real-estate investors (buy-hold / flip), often out-of-market, '
            'seeking distressed + bankruptcy deal flow.',
            'distressed_investment+bankruptcy',
            'single_county',
            'rei-investor',
            'gated'
        )
    ON CONFLICT (key) DO NOTHING
"""


def main() -> None:
    db = Database()
    with db.session_scope() as s:
        for stmt in DDL:
            s.execute(text(stmt))
        s.execute(text(SEED))
    print("fa050 DDL applied: expansion_icp_channels created, REI Investor seed inserted.")


if __name__ == "__main__":
    main()
