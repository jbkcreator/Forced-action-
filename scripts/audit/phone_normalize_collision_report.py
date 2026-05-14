"""
Dry-run audit: find sms_opt_outs / sms_opt_ins rows whose phone value is not in
canonical E.164 format (+1XXXXXXXXXX) and report what the backfill would do.

The backfill only touches phones that are NOT structurally E.164 already.
A phone that is already +1 followed by exactly 10 digits is treated as OK even
if phonenumbers.is_valid_number() would reject it (e.g. synthetic 555 numbers).

Actions reported per row:
  UPDATE  — normalize() returns a different valid E.164; no collision in the table
  COLLIDE — normalized form already exists; row would be deleted (de-dup wins)
  DELETE  — normalize() returns None and phone is not structurally E.164; true junk
  OK      — already +1XXXXXXXXXX; no change

Run:
    python scripts/audit/phone_normalize_collision_report.py

Exit 0 regardless of findings.
"""

from __future__ import annotations

import re
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sqlalchemy import text
from src.core.database import Database
from src.services.phone_utils import normalize

_E164_RE = re.compile(r"^\+1\d{10}$")


def _is_e164(phone: str) -> bool:
    return bool(_E164_RE.match(phone or ""))


def _audit_table(db, table: str) -> list[dict]:
    rows = db.execute(text(f"SELECT id, phone FROM {table} ORDER BY id")).fetchall()
    existing_phones = {r.phone for r in rows}
    results = []
    for row in rows:
        stored = row.phone
        if _is_e164(stored):
            results.append({"id": row.id, "phone": stored, "action": "OK", "note": ""})
            continue
        normed = normalize(stored)
        if normed is None:
            action = "DELETE"
            note = "not E.164 and not normalizable - junk row"
        elif normed in existing_phones:
            action = "COLLIDE"
            note = f"normalized form {normed!r} already exists — row would be deleted"
        else:
            action = "UPDATE"
            note = f"{stored!r} -> {normed!r}"
        results.append({"id": row.id, "phone": stored, "action": action, "note": note})
    return results


def main() -> None:
    db_factory = Database()
    with db_factory.session_scope() as db:
        for table in ("sms_opt_outs", "sms_opt_ins"):
            results = _audit_table(db, table)
            counts = {"OK": 0, "UPDATE": 0, "COLLIDE": 0, "DELETE": 0}
            for r in results:
                counts[r["action"]] += 1

            print(f"\n{'='*60}")
            print(f"  {table}  ({len(results)} rows)")
            print(f"  OK={counts['OK']}  UPDATE={counts['UPDATE']}  COLLIDE={counts['COLLIDE']}  DELETE={counts['DELETE']}")
            print(f"{'='*60}")

            non_ok = [r for r in results if r["action"] != "OK"]
            if not non_ok:
                print("  All rows already canonical. No backfill needed.")
            else:
                col_w = [4, 24, 8, 0]
                print(f"  {'ID':<{col_w[0]}}  {'phone':<{col_w[1]}}  {'action':<{col_w[2]}}  note")
                print("  " + "-" * 70)
                for r in non_ok:
                    print(f"  {r['id']:<{col_w[0]}}  {r['phone']:<{col_w[1]}}  {r['action']:<{col_w[2]}}  {r['note']}")

    print("\nDone. Run fa018_phone_normalize_backfill migration to apply changes.")


if __name__ == "__main__":
    main()
