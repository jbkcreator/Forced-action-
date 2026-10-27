"""Tag the identifiable test rows ``is_test`` and log every one (Q35, Q42).

Rules (config/lending_test_data.py): a test email domain on the row, the test partner by name, and rows that belong to
a tagged lead, loan file or partner (a loan file of a test partner, the routing row of a test lead or file). Rows are
tagged, never deleted, and nothing here touches suppression or consent. Every tagged row is written to
``lending.test_data_cleanup_log`` (table, row id, rule: no names, numbers or emails).

A dry run does the work inside a savepoint and rolls it back, so it reports exactly what ``--apply`` would tag.

    python -m src.lending.test_data                       # dry run
    python -m src.lending.test_data --apply               # tag and log
    python -m src.lending.test_data --apply --lead-ids 268,1192   # also tag test leads named by a person
    python -m src.lending.test_data --apply --delete      # also delete the tagged rows that can be deleted

Deleting (``--apply --delete`` only; the cron job never deletes) removes ONLY rows already tagged ``is_test``, one row
at a time in its own savepoint, after copying the row into ``lending.test_data_deleted_rows``. The database decides what
can go: a row something still points at, or an append-only record (routing events, consent certificates), refuses the
delete and the row is kept and counted. Never deleted: subscribers (tag only), email opt-outs and the suppression list
(tag only: they are what blocks a send, see ``NEVER_DELETED``), and a test contact that is on the do-not-contact list,
the suppression list or has an opt-out, because deleting it could let a blocked number be contacted again.
"""
from __future__ import annotations

import argparse
import logging
import uuid
from dataclasses import dataclass, field
from typing import Optional

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from config.lending_test_data import TEST_EMAIL_DOMAINS, TEST_PARTNER_NAME_PREFIX

logger = logging.getLogger(__name__)

_DOMAIN = "split_part(lower({column}), '@', 2) = ANY(:domains)"
# (table, rule, condition). Order matters: a row is tagged by its own data first, then what hangs off a tagged row.
_STEPS: tuple[tuple[str, str, str], ...] = (
    ("public.subscribers", "email_domain", _DOMAIN.format(column="email")),
    ("public.consent_acceptances", "email_domain", _DOMAIN.format(column="email")),
    ("public.non_buyer_nurture_sequences", "email_domain", _DOMAIN.format(column="email")),
    ("public.checkout_recovery", "email_domain", _DOMAIN.format(column="email")),
    ("public.email_opt_outs", "email_domain", _DOMAIN.format(column="email")),
    ("lending.suppression_list", "email_domain", _DOMAIN.format(column="email")),
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


MAX_NAMED_LEADS = 50


def _steps_with_named_leads(lead_ids: list[int]) -> list[tuple[str, str, str]]:
    """``_STEPS`` with the leads a person named inserted before the rows that follow a tagged lead."""
    steps = list(_STEPS)
    if lead_ids:
        first_routing = next(i for i, (table, _, _) in enumerate(steps) if table == "lending.file_routing")
        steps.insert(first_routing, ("lending.lendingflow_leads", "named_by_operator", "id = ANY(:lead_ids)"))
    return steps


def tag_test_rows(db, *, apply: bool, lead_ids: Optional[list[int]] = None) -> TagReport:
    """Tag the identifiable test rows. ``apply=False`` reports them and leaves nothing changed; ``apply=True`` leaves
    the changes in the caller's transaction (the caller commits). ``lead_ids`` names LendingFlow leads that are test
    leads but cannot be recognised by a test email domain (rule ``named_by_operator``, logged like the rest)."""
    lead_ids = sorted(set(lead_ids or []))
    if len(lead_ids) > MAX_NAMED_LEADS:
        raise ValueError(f"name at most {MAX_NAMED_LEADS} leads at a time")
    report = TagReport(run_id=f"test-tag-{uuid.uuid4().hex[:12]}", applied=apply)
    params = {"domains": list(TEST_EMAIL_DOMAINS), "partner_prefix": f"{TEST_PARTNER_NAME_PREFIX}%", "lead_ids": lead_ids}
    savepoint = db.begin_nested()
    try:
        for table, rule, condition in _steps_with_named_leads(lead_ids):
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


# (table, [(child table, its foreign key column)]): a row is deleted together with the rows that point at it, in
# one savepoint, so a refusal (an append-only record or another dependent) rolls the whole row back. Children are
# copied to the archive too. ``public.subscribers`` is deliberately absent: tag only.
DELETE_ORDER: tuple[tuple[str, tuple[tuple[str, str], ...]], ...] = (
    ("lending.lendingflow_leads", (("lending.file_routing", "lead_id"),
                                    ("lending.lendingflow_enrichment", "lead_id"),
                                    ("lending.lendingflow_replacements", "lead_id"))),
    ("lending.web_leads", ()),
    ("lending.loan_files", (("lending.file_routing", "loan_file_id"),
                             ("lending.partner_submissions", "file_id"),
                             ("lending.partner_file_uploads", "file_id"))),
    ("lending.partners", ()),
    ("lending.contacts", ()),
    ("public.checkout_provisioning_failures", ()),
    ("public.consent_acceptances", ()),
    ("public.non_buyer_nurture_sequences", ()),
    ("public.checkout_recovery", ()),
)
# Tagged (so reports leave them out) but never deleted: these rows are the live send guards. is_email_suppressed()
# reads email_opt_outs and the lending compliance checks read suppression_list, and the address may still belong to a
# retained subscriber, so deleting the row would let it be contacted again. An archive copy does not block anything.
NEVER_DELETED: tuple[str, ...] = ("public.email_opt_outs", "lending.suppression_list")
# A test contact is only a candidate when none of these says the number must stay blocked.
_CONTACT_PROTECTED = (
    "do_not_contact"
    " OR EXISTS (SELECT 1 FROM lending.suppression_list s WHERE s.phone = {t}.phone)"
    " OR EXISTS (SELECT 1 FROM lending.opt_out_events o WHERE o.phone_hash = {t}.phone_hash)"
)
_ARCHIVE_DELETE = (
    "WITH gone AS (DELETE FROM {table} AS t WHERE {where} RETURNING to_jsonb(t) AS data) "
    "INSERT INTO lending.test_data_deleted_rows (run_id, table_name, row_id, row_data) "
    "SELECT :run_id, '{table}', (data ->> 'id')::bigint, data FROM gone RETURNING row_id"
)


@dataclass
class DeleteReport:
    run_id: str
    applied: bool
    deleted: dict[str, int] = field(default_factory=dict)
    kept: dict[str, dict[str, int]] = field(default_factory=dict)  # table -> reason -> rows

    @property
    def total_deleted(self) -> int:
        return sum(self.deleted.values())


def _candidate_ids(db, table: str) -> tuple[list[int], int]:
    """(ids that may be deleted, number kept because they are protected) for the tagged rows of ``table``."""
    ids = [row.id for row in db.execute(text(f"SELECT id FROM {table} WHERE is_test ORDER BY id")).all()]
    if table != "lending.contacts" or not ids:
        return ids, 0
    protected = {row.id for row in db.execute(text(
        f"SELECT id FROM lending.contacts WHERE is_test AND ({_CONTACT_PROTECTED.format(t='lending.contacts')})")).all()}
    return [i for i in ids if i not in protected], len(protected)


def delete_test_rows(db, *, apply: bool) -> DeleteReport:
    """Delete the tagged test rows that can be deleted, copying each (and what points at it) into
    ``lending.test_data_deleted_rows`` first. ``apply=False`` reports what would happen and leaves nothing changed.
    Does not commit."""
    report = DeleteReport(run_id=f"test-del-{uuid.uuid4().hex[:12]}", applied=apply)
    outer = db.begin_nested()
    try:
        for table, children in DELETE_ORDER:
            if not _table_exists(db, table):
                continue
            ids, protected = _candidate_ids(db, table)
            if protected:
                report.kept.setdefault(table, {})["protected_contact"] = protected
            for row_id in ids:
                row = db.begin_nested()
                try:
                    params = {"id": row_id, "run_id": report.run_id}
                    for child, column in children:
                        if _table_exists(db, child):
                            db.execute(text(_ARCHIVE_DELETE.format(table=child, where=f"t.{column} = :id")), params).all()
                    stored = db.execute(text(_ARCHIVE_DELETE.format(table=table, where="t.id = :id AND t.is_test")),
                                        params).first()
                except DBAPIError:  # something still points at it, or it is an append-only record: keep it whole
                    row.rollback()
                    kept = report.kept.setdefault(table, {})
                    kept["has_dependents"] = kept.get("has_dependents", 0) + 1
                    continue
                row.commit()
                if stored is not None:
                    report.deleted[table] = report.deleted.get(table, 0) + 1
    except Exception:
        outer.rollback()
        raise
    if apply:
        outer.commit()
    else:
        outer.rollback()
    return report


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Tag the identifiable test rows is_test and log them (Q42)")
    parser.add_argument("--apply", action="store_true", help="tag and log (default: dry run, nothing changes)")
    parser.add_argument("--lead-ids", default="",
                        help="comma-separated LendingFlow lead ids that are test leads but have no test email domain")
    parser.add_argument("--delete", action="store_true",
                        help="also delete the tagged test rows that can be deleted (a copy is kept); with --apply to act")
    args = parser.parse_args(argv)
    try:
        lead_ids = [int(part) for part in args.lead_ids.split(",") if part.strip()]
    except ValueError:
        parser.error("--lead-ids must be comma-separated numbers")
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    from src.lending.db import lending_session

    with lending_session() as db:
        report = tag_test_rows(db, apply=args.apply, lead_ids=lead_ids)
        deletion = delete_test_rows(db, apply=args.apply) if args.delete else None
    for label, ids in report.tagged.items():
        logger.info("[test-data] %s: %d row(s): %s", label, len(ids), ids[:50])
    for table in report.skipped_tables:
        logger.info("[test-data] %s does not exist: skipped", table)
    logger.info("[test-data] run %s: %d row(s) %s", report.run_id, report.total,
                "tagged and logged" if args.apply else "would be tagged (dry run: nothing changed)")
    if deletion is not None:
        for table, count in deletion.deleted.items():
            logger.info("[test-data] %s: %d row(s) %s", table, count,
                        "deleted (copy kept)" if args.apply else "would be deleted (dry run: nothing changed)")
        for table, reasons in deletion.kept.items():
            logger.info("[test-data] %s: kept %s", table, reasons)
        logger.info("[test-data] delete run %s: %d row(s) %s", deletion.run_id, deletion.total_deleted,
                    "deleted" if args.apply else "would be deleted (dry run)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
