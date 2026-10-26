"""Tag the identifiable test rows ``is_test`` and log every one (Q35, Q42).

Rules (config/lending_test_data.py): a test email domain on the row, the test partner by name, and rows that belong to
a tagged lead, loan file or partner (a loan file of a test partner, the routing row of a test lead or file). Rows are
tagged, never deleted, and nothing here touches suppression or consent. Every tagged row is written to
``lending.test_data_cleanup_log`` (table, row id, rule: no names, numbers or emails).

A dry run does the work inside a savepoint and rolls it back, so it reports exactly what ``--apply`` would tag.

    python -m src.lending.test_data            # dry run
    python -m src.lending.test_data --apply    # tag and log
"""
from __future__ import annotations

import argparse
import logging
import uuid
from dataclasses import dataclass, field
from typing import Optional

from sqlalchemy import text

from config.lending_test_data import TEST_EMAIL_DOMAINS, TEST_PARTNER_NAME_PREFIX

logger = logging.getLogger(__name__)

_DOMAIN = "split_part(lower({column}), '@', 2) = ANY(:domains)"
# (table, rule, condition). Order matters: a row is tagged by its own data first, then what hangs off a tagged row.
_STEPS: tuple[tuple[str, str, str], ...] = (
    ("public.subscribers", "email_domain", _DOMAIN.format(column="email")),
    ("public.checkout_provisioning_failures", "email_domain", _DOMAIN.format(column="email")),
    ("lending.contacts", "email_domain", _DOMAIN.format(column="email")),
    ("lending.lendingflow_leads", "email_domain", _DOMAIN.format(column="email")),
    ("lending.web_leads", "email_domain", _DOMAIN.format(column="email")),
    ("lending.partners", "email_domain", _DOMAIN.format(column="email")),
    ("lending.partners", "partner_name", "lower(name) LIKE :partner_prefix"),
    ("lending.loan_files", "email_domain", _DOMAIN.format(column="borrower_email")),
    ("lending.loan_files", "test_partner",
     "partner_id IN (SELECT id FROM lending.partners WHERE is_test)"),
    ("lending.file_routing", "test_lead",
     "lead_id IN (SELECT id FROM lending.lendingflow_leads WHERE is_test)"),
    ("lending.file_routing", "test_loan_file",
     "loan_file_id IN (SELECT id FROM lending.loan_files WHERE is_test)"),
)


def is_test_email(email: Optional[str]) -> bool:
    """True for an address on a test domain: the same rule the cleanup uses, applied to rows as they are created."""
    domain = (email or "").strip().lower().rpartition("@")[2]
    return bool(domain) and domain in TEST_EMAIL_DOMAINS


@dataclass
class TagReport:
    run_id: str
    applied: bool
    tagged: dict[str, list[int]] = field(default_factory=dict)  # "table (rule)" -> row ids
    skipped_tables: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return sum(len(ids) for ids in self.tagged.values())


def _table_exists(db, table: str) -> bool:
    return db.execute(text("SELECT to_regclass(:name) IS NOT NULL"), {"name": table}).scalar() is True


def tag_test_rows(db, *, apply: bool) -> TagReport:
    """Tag the identifiable test rows. ``apply=False`` reports them and leaves nothing changed; ``apply=True`` leaves
    the changes in the caller's transaction (the caller commits)."""
    report = TagReport(run_id=f"test-tag-{uuid.uuid4().hex[:12]}", applied=apply)
    params = {"domains": list(TEST_EMAIL_DOMAINS), "partner_prefix": f"{TEST_PARTNER_NAME_PREFIX}%"}
    savepoint = db.begin_nested()
    try:
        for table, rule, condition in _STEPS:
            if not _table_exists(db, table):
                if table not in report.skipped_tables:
                    report.skipped_tables.append(table)
                continue
            rows = db.execute(text(
                f"WITH tagged AS (UPDATE {table} SET is_test = true WHERE NOT is_test AND ({condition}) RETURNING id) "
                "INSERT INTO lending.test_data_cleanup_log (run_id, table_name, row_id, rule) "
                "SELECT :run_id, :table_name, id, :rule FROM tagged RETURNING row_id"),
                {**params, "run_id": report.run_id, "table_name": table, "rule": rule}).all()
            if rows:
                report.tagged[f"{table} ({rule})"] = sorted(row.row_id for row in rows)
    except Exception:
        savepoint.rollback()
        raise
    if apply:
        savepoint.commit()
    else:
        savepoint.rollback()
    return report


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Tag the identifiable test rows is_test and log them (Q42)")
    parser.add_argument("--apply", action="store_true", help="tag and log (default: dry run, nothing changes)")
    args = parser.parse_args(argv)
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    from src.lending.db import lending_session

    with lending_session() as db:
        report = tag_test_rows(db, apply=args.apply)
    for label, ids in report.tagged.items():
        logger.info("[test-data] %s: %d row(s): %s", label, len(ids), ids[:50])
    for table in report.skipped_tables:
        logger.info("[test-data] %s does not exist: skipped", table)
    logger.info("[test-data] run %s: %d row(s) %s", report.run_id, report.total,
                "tagged and logged" if args.apply else "would be tagged (dry run: nothing changed)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
