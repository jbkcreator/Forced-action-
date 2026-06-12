"""
Phase 0b/0c (ADR 0015): normalize all stored phones to strict E.164 and
quarantine junk.

`phone_utils.normalize` does both jobs: valid numbers come back canonical
(+1XXXXXXXXXX), junk/unparseable numbers (all-1s, invalid NANP exchanges
like `(220) 000-0338`) come back None and are nulled.

Targets:
  enriched_contacts.mobile_phone / landline
  owners.phone_1..3 (shift-compacted so phone_1 holds the best remaining),
    phone_metadata keys realigned, skip_trace_success recomputed

Usage:
  python scripts/backfill_phone_normalization.py --dry-run
  python scripts/backfill_phone_normalization.py --apply
  python scripts/backfill_phone_normalization.py --apply --table owners

A CSV audit of every changed row is written next to this script
(phone_backfill_audit_<table>.csv). Idempotent: a second run changes nothing.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text  # noqa: E402

from src.core.database import get_db_context  # noqa: E402
from src.services.phone_utils import normalize  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("phone_backfill")

BATCH = 10_000
AUDIT_DIR = Path(__file__).resolve().parent


def _audit_writer(table: str):
    path = AUDIT_DIR / f"phone_backfill_audit_{table}.csv"
    fh = path.open("w", newline="", encoding="utf-8")
    writer = csv.writer(fh)
    writer.writerow(["row_id", "column", "old_value", "new_value", "action"])
    fh.flush()
    return fh, writer, path


def _classify(old: str | None) -> tuple[str | None, str]:
    """Return (new_value, action) for one stored phone string."""
    if old is None or old.strip() == "":
        return None, "noop"
    norm = normalize(old)
    if norm == old:
        return old, "noop"
    if norm is None:
        return None, "nulled_junk"
    return norm, "normalized"


def backfill_enriched_contacts(session, apply: bool, writer) -> dict:
    stats = {"scanned": 0, "normalized": 0, "nulled_junk": 0}
    last_id = 0
    while True:
        rows = session.execute(
            text(
                "SELECT id, mobile_phone, landline FROM enriched_contacts "
                "WHERE id > :last AND (mobile_phone IS NOT NULL OR landline IS NOT NULL) "
                "ORDER BY id LIMIT :lim"
            ),
            {"last": last_id, "lim": BATCH},
        ).fetchall()
        if not rows:
            break
        last_id = rows[-1].id
        stats["scanned"] += len(rows)
        log.info(f"Scanned {stats['scanned']} enriched_contacts...")

        updates = []
        for r in rows:
            new_mobile, act_m = _classify(r.mobile_phone)
            new_land, act_l = _classify(r.landline)
            if act_m == "noop" and act_l == "noop":
                continue
            for col, old, new, act in (
                ("mobile_phone", r.mobile_phone, new_mobile, act_m),
                ("landline", r.landline, new_land, act_l),
            ):
                if act != "noop":
                    stats[act] += 1
                    writer.writerow([r.id, col, old, new, act])
            updates.append({"id": r.id, "mobile": new_mobile, "land": new_land})

        if updates and apply:
            session.execute(
                text(
                    "UPDATE enriched_contacts AS ec SET "
                    "  mobile_phone = v.mobile, landline = v.land "
                    "FROM (VALUES (:id, :mobile, :land)) AS v(id, mobile, land) "
                    "WHERE ec.id = v.id"
                ),
                updates,
            )
            session.commit()
            log.info(f"Applied batch of {len(updates)} enriched_contacts updates")
    return stats


def backfill_owners(session, apply: bool, writer) -> dict:
    stats = {"scanned": 0, "normalized": 0, "nulled_junk": 0, "success_flag_cleared": 0}
    last_id = 0
    while True:
        rows = session.execute(
            text(
                "SELECT id, phone_1, phone_2, phone_3, email_1, "
                "       skip_trace_success, phone_metadata "
                "FROM owners "
                "WHERE id > :last AND "
                "      (phone_1 IS NOT NULL OR phone_2 IS NOT NULL OR phone_3 IS NOT NULL) "
                "ORDER BY id LIMIT :lim"
            ),
            {"last": last_id, "lim": BATCH},
        ).fetchall()
        if not rows:
            break
        last_id = rows[-1].id
        stats["scanned"] += len(rows)
        log.info(f"Scanned {stats['scanned']} owners...")

        updates = []
        for r in rows:
            old_phones = [r.phone_1, r.phone_2, r.phone_3]
            slot_results = [_classify(p) for p in old_phones]
            if all(act == "noop" for _, act in slot_results):
                continue

            # Shift-compact surviving numbers into phone_1..3, dedup preserving order
            survivors: list[str] = []
            for new, _ in slot_results:
                if new and new not in survivors:
                    survivors.append(new)
            new_phones = (survivors + [None, None, None])[:3]

            # Realign phone_metadata: keep entries whose old slot survived, keyed by new slot
            meta = r.phone_metadata or {}
            new_meta: dict = {}
            for idx, (new, _) in enumerate(slot_results):
                if new and new in new_phones:
                    old_slot_meta = meta.get(f"phone_{idx + 1}")
                    if old_slot_meta is not None:
                        new_meta[f"phone_{new_phones.index(new) + 1}"] = old_slot_meta

            for idx in range(3):
                old, (new_v, act) = old_phones[idx], slot_results[idx]
                shifted = new_phones[idx]
                if act != "noop":
                    stats[act] += 1
                if old != shifted:
                    writer.writerow([r.id, f"phone_{idx + 1}", old, shifted,
                                     act if act != "noop" else "shifted"])

            new_success = bool(new_phones[0] or r.email_1)
            if r.skip_trace_success and not new_success:
                stats["success_flag_cleared"] += 1
                writer.writerow([r.id, "skip_trace_success", True, False, "cleared"])

            updates.append({
                "id": r.id,
                "p1": new_phones[0], "p2": new_phones[1], "p3": new_phones[2],
                "meta": json.dumps(new_meta) if new_meta else None,
                "success": new_success,
            })

        if updates and apply:
            session.execute(
                text(
                    "UPDATE owners AS o SET "
                    "  phone_1 = v.p1, phone_2 = v.p2, phone_3 = v.p3, "
                    "  phone_metadata = CAST(v.meta AS jsonb), "
                    "  skip_trace_success = v.success "
                    "FROM (VALUES (:id, :p1, :p2, :p3, :meta, :success)) "
                    "  AS v(id, p1, p2, p3, meta, success) "
                    "WHERE o.id = v.id"
                ),
                updates,
            )
            session.commit()
            log.info(f"Applied batch of {len(updates)} updates")
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="report only, write nothing")
    mode.add_argument("--apply", action="store_true", help="write changes")
    parser.add_argument("--table", choices=["enriched_contacts", "owners", "all"],
                        default="all")
    args = parser.parse_args()
    apply = bool(args.apply)

    with get_db_context() as session:
        if args.table in ("enriched_contacts", "all"):
            fh, writer, path = _audit_writer("enriched_contacts")
            try:
                stats = backfill_enriched_contacts(session, apply, writer)
            finally:
                fh.close()
            log.info("[enriched_contacts] %s | audit: %s", stats, path)

        if args.table in ("owners", "all"):
            fh, writer, path = _audit_writer("owners")
            try:
                stats = backfill_owners(session, apply, writer)
            finally:
                fh.close()
            log.info("[owners] %s | audit: %s", stats, path)

    log.info("Done (%s mode).", "APPLY" if apply else "DRY-RUN")


if __name__ == "__main__":
    main()
