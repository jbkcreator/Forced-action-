"""
One-time Pinellas parcel_id repair.

Pinellas PCPAO master data has both:
  - STRAP: internal appraiser key, e.g. 152826008870010070
  - PARCEL_NUMBER: public parcel number, e.g. 26-28-15-00887-001-0070

The Pinellas master load previously saved STRAP into properties.parcel_id.
This script replaces that value with the real parcel number in compact form:
26-28-15-00887-001-0070 -> 262815008870010070.

Dry-run by default:
  .\\.venv\\Scripts\\python.exe scripts/backfill_pinellas_parcel_ids.py --csv data/reference/pinellas/RP_PROPERTY_INFO.csv

Apply:
  .\\.venv\\Scripts\\python.exe scripts/backfill_pinellas_parcel_ids.py --csv data/reference/pinellas/RP_PROPERTY_INFO.csv --apply
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Iterable

import pandas as pd
from sqlalchemy import bindparam, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.database import get_db_context

COUNTY_ID = "pinellas"
DEFAULT_CSV = Path("data/reference/pinellas/RP_PROPERTY_INFO.csv")
REPORT_DIR = Path("reports/audit")


def _compact(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    cleaned = re.sub(r"[^0-9A-Za-z]", "", str(value).strip()).upper()
    return cleaned or None


def _strap_candidates(raw_strap: object) -> set[str]:
    """
    Return plausible old values currently stored in properties.parcel_id.

    The known bad value is raw STRAP compact digits. Include a couple of
    defensive variants so the script can repair rows loaded through earlier
    normalizer experiments too.
    """
    raw = "" if raw_strap is None or pd.isna(raw_strap) else str(raw_strap).strip()
    compact = _compact(raw)
    candidates = {raw} if raw else set()
    if compact:
        candidates.add(compact)
        if len(compact) == 18:
            candidates.add(
                f"{compact[0:2]}-{compact[2:4]}-{compact[4:6]}-"
                f"{compact[6:11]}-{compact[11:14]}-{compact[14:18]}"
            )
    return {c for c in candidates if c}


def _chunks(csv_path: Path, chunksize: int) -> Iterable[pd.DataFrame]:
    for chunk in pd.read_csv(csv_path, dtype=str, chunksize=chunksize):
        chunk.columns = chunk.columns.str.upper()
        yield chunk


def _fetch_properties(session, parcel_ids: list[str]) -> dict[str, int]:
    if not parcel_ids:
        return {}
    stmt = (
        text(
            """
            SELECT id, parcel_id
            FROM properties
            WHERE county_id = :county_id
              AND parcel_id IN :parcel_ids
            """
        )
        .bindparams(bindparam("parcel_ids", expanding=True))
    )
    rows = session.execute(stmt, {"county_id": COUNTY_ID, "parcel_ids": parcel_ids}).mappings()
    return {row["parcel_id"]: row["id"] for row in rows}


def _update_property(session, property_id: int, new_parcel_id: str) -> None:
    session.execute(
        text(
            """
            UPDATE properties
            SET parcel_id = :new_parcel_id,
                updated_at = NOW()
            WHERE id = :property_id
            """
        ),
        {"property_id": property_id, "new_parcel_id": new_parcel_id},
    )


def _bulk_update_properties(session, updates: list[tuple[int, str]]) -> None:
    """Update a chunk of properties in one statement instead of one UPDATE per row."""
    if not updates:
        return

    values_sql = []
    params = {}
    for idx, (property_id, new_parcel_id) in enumerate(updates):
        id_key = f"id_{idx}"
        parcel_key = f"parcel_{idx}"
        values_sql.append(f"(:{id_key}, :{parcel_key})")
        params[id_key] = property_id
        params[parcel_key] = new_parcel_id

    session.execute(
        text(
            f"""
            UPDATE properties AS p
            SET parcel_id = v.new_parcel_id,
                updated_at = NOW()
            FROM (VALUES {", ".join(values_sql)}) AS v(property_id, new_parcel_id)
            WHERE p.id = v.property_id
            """
        ),
        params,
    )


def run(csv_path: Path, *, apply: bool = False, chunksize: int = 25_000, report_path: Path | None = None) -> dict:
    stats = defaultdict(int)
    report_rows: list[dict] = []
    seen_new_ids: dict[str, str] = {}

    with get_db_context() as session:
        for chunk_num, chunk in enumerate(_chunks(csv_path, chunksize), start=1):
            if "STRAP" not in chunk.columns or "PARCEL_NUMBER" not in chunk.columns:
                raise RuntimeError("CSV must include STRAP and PARCEL_NUMBER columns")

            mappings: list[tuple[set[str], str, str]] = []
            old_lookup_values: set[str] = set()
            new_values: set[str] = set()

            for row in chunk.itertuples(index=False):
                row_dict = row._asdict()
                strap = row_dict.get("STRAP")
                parcel_number = row_dict.get("PARCEL_NUMBER")
                new_id = _compact(parcel_number)
                old_candidates = _strap_candidates(strap)

                stats["csv_rows"] += 1
                if not old_candidates or not new_id:
                    stats["invalid_rows"] += 1
                    continue
                if new_id in seen_new_ids and seen_new_ids[new_id] != str(strap):
                    stats["duplicate_new_id_in_csv"] += 1
                    report_rows.append({
                        "status": "duplicate_new_id_in_csv",
                        "strap": strap,
                        "new_parcel_id": new_id,
                        "property_id": "",
                        "old_parcel_id": "",
                    })
                    continue

                seen_new_ids[new_id] = str(strap)
                mappings.append((old_candidates, new_id, str(strap)))
                old_lookup_values.update(old_candidates)
                new_values.add(new_id)

            existing_old = _fetch_properties(session, sorted(old_lookup_values))
            existing_new = _fetch_properties(session, sorted(new_values))
            updates: list[tuple[int, str]] = []

            for old_candidates, new_id, strap in mappings:
                old_matches = [(old, existing_old[old]) for old in old_candidates if old in existing_old]
                already_property_id = existing_new.get(new_id)

                if not old_matches:
                    if already_property_id:
                        stats["already_updated"] += 1
                        report_rows.append({
                            "status": "already_updated",
                            "strap": strap,
                            "new_parcel_id": new_id,
                            "property_id": already_property_id,
                            "old_parcel_id": new_id,
                        })
                    else:
                        stats["not_found"] += 1
                    continue

                # There should be only one because properties.parcel_id is unique.
                old_parcel_id, property_id = old_matches[0]
                if already_property_id and already_property_id != property_id:
                    stats["conflicts"] += 1
                    report_rows.append({
                        "status": "conflict_new_id_exists",
                        "strap": strap,
                        "new_parcel_id": new_id,
                        "property_id": property_id,
                        "old_parcel_id": old_parcel_id,
                        "conflicting_property_id": already_property_id,
                    })
                    continue

                stats["matched"] += 1
                report_rows.append({
                    "status": "would_update" if not apply else "updated",
                    "strap": strap,
                    "new_parcel_id": new_id,
                    "property_id": property_id,
                    "old_parcel_id": old_parcel_id,
                })
                if apply:
                    updates.append((property_id, new_id))
                    stats["updated"] += 1

            if apply:
                print(
                    f"chunk={chunk_num} bulk_updating={len(updates)} "
                    f"rows_seen={stats['csv_rows']}",
                    flush=True,
                )
                _bulk_update_properties(session, updates)
                session.commit()

            print(
                f"chunk={chunk_num} rows={stats['csv_rows']} "
                f"matched={stats['matched']} updated={stats['updated']} "
                f"already={stats['already_updated']} conflicts={stats['conflicts']} "
                f"not_found={stats['not_found']}",
                flush=True,
            )

        if not apply:
            session.rollback()

    if report_path is None:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        suffix = "apply" if apply else "dry_run"
        report_path = REPORT_DIR / f"pinellas_parcel_id_backfill_{suffix}.csv"
    if report_rows:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        keys = sorted({key for row in report_rows for key in row})
        with report_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=keys)
            writer.writeheader()
            writer.writerows(report_rows)
        print(f"report={report_path}")

    result = dict(stats)
    result["mode"] = "apply" if apply else "dry_run"
    result["report_path"] = str(report_path) if report_rows else None
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Pinellas properties.parcel_id from STRAP to PARCEL_NUMBER.")
    parser.add_argument("--csv", dest="csv_path", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--chunksize", type=int, default=25_000)
    parser.add_argument("--apply", action="store_true", help="Actually update the database. Omit for dry-run.")
    parser.add_argument("--report", dest="report_path", type=Path, default=None)
    args = parser.parse_args()

    result = run(args.csv_path, apply=args.apply, chunksize=args.chunksize, report_path=args.report_path)
    print(result)


if __name__ == "__main__":
    main()
