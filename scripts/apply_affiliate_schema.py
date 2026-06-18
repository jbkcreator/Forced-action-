"""One-off DDL apply for the Affiliate Program (Stream D) schema.

Alembic CLI is unusable on this repo (multi-head tree), so the migration file
fa081_affiliate_program.py documents the change and this script applies it.
Creates the 4 affiliate tables if absent (checkfirst=True) — never drops.
"""
import sys

from sqlalchemy import create_engine, inspect

from config.settings import get_settings
from src.core.models import (
    Base,
    Affiliate,
    AffiliateReferral,
    SubscriptionInvoice,
    AffiliatePayoutLedger,
)

TABLES = [
    Affiliate.__table__,
    AffiliateReferral.__table__,
    SubscriptionInvoice.__table__,
    AffiliatePayoutLedger.__table__,
]


def main() -> int:
    url = str(get_settings().database_url)
    engine = create_engine(url)
    before = set(inspect(engine).get_table_names())
    Base.metadata.create_all(engine, tables=TABLES, checkfirst=True)
    after = set(inspect(engine).get_table_names())
    created = sorted(after - before)
    existing = sorted({t.name for t in TABLES} & before)
    print("created:", created or "(none — all already present)")
    if existing:
        print("already existed (left untouched):", existing)
    engine.dispose()
    return 0


if __name__ == "__main__":
    sys.exit(main())
