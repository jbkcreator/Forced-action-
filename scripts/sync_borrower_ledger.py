"""Nightly incremental borrower-ledger ingestion."""
from __future__ import annotations

import argparse
import logging
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config.settings import get_settings
from src.services.borrower_ledger_ingest import sync_borrower_ledger


def main(*, as_of: date, dry_run: bool = False) -> None:
    engine = create_engine(get_settings().database_url)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        stats = sync_borrower_ledger(session, as_of=as_of)
        if dry_run:
            session.rollback()
        else:
            session.commit()
    print(
        f"borrower ledger {'preview' if dry_run else 'sync'}: "
        f"attempted={stats.attempted} inserted={stats.inserted} skipped={stats.skipped}"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--date", type=date.fromisoformat, default=date.today())
    args = parser.parse_args()
    main(as_of=args.date, dry_run=args.dry_run)
