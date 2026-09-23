"""Import operator-verified FA Max person identifiers from CSV.

Columns: person_id, email, phone. Each row needs a person_id and at least
one identifier. Existing identifiers cannot be silently assigned to another
person; PostgreSQL's primary key rejects that conflict for operator review.
"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path
from uuid import UUID

from sqlalchemy import text

from src.core.database import get_db_context
from src.services.phone_utils import normalize


def parse_identifiers(path: Path) -> set[tuple[str, str, str]]:
    result: set[tuple[str, str, str]] = set()
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        headers = {name.strip().lower() for name in reader.fieldnames or []}
        if "person_id" not in headers or not headers.intersection({"email", "phone"}):
            raise ValueError("CSV needs person_id and email or phone columns")
        for line_no, raw in enumerate(reader, start=2):
            row = {(key or "").strip().lower(): (value or "").strip()
                   for key, value in raw.items()}
            try:
                person_id = str(UUID(row.get("person_id", "")))
            except ValueError as exc:
                raise ValueError(f"invalid person_id on line {line_no}") from exc
            email = row.get("email", "").lower()
            phone = row.get("phone", "")
            if not email and not phone:
                raise ValueError(f"missing identifier on line {line_no}")
            if email:
                if email.count("@") != 1 or "." not in email.split("@", 1)[1]:
                    raise ValueError(f"invalid email on line {line_no}")
                result.add((person_id, "email", email))
            if phone:
                canonical = normalize(phone)
                if canonical is None:
                    raise ValueError(f"invalid phone on line {line_no}")
                result.add((person_id, "phone", canonical))
    if not result:
        raise ValueError("empty contact-identifier import")
    return result


def run(path: Path, *, dry_run: bool = False) -> int:
    identifiers = parse_identifiers(path)
    if dry_run:
        return len(identifiers)
    with get_db_context() as session:
        for person_id, kind, value in identifiers:
            session.execute(text(
                "INSERT INTO fa_max_person_contact_identifiers "
                "(person_id, identifier_kind, identifier_value, source) "
                "VALUES (CAST(:person_id AS uuid), :kind, :value, 'operator_csv') "
                "ON CONFLICT (identifier_kind, identifier_value) DO UPDATE SET "
                "verified_at = now() "
                "WHERE fa_max_person_contact_identifiers.person_id = EXCLUDED.person_id"
            ), {"person_id": person_id, "kind": kind, "value": value})
            owner = session.execute(text(
                "SELECT person_id::text FROM fa_max_person_contact_identifiers "
                "WHERE identifier_kind = :kind AND identifier_value = :value"
            ), {"kind": kind, "value": value}).scalar_one()
            if owner != person_id:
                raise ValueError("identifier belongs to a different person; resolve identity first")
    return len(identifiers)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    print(f"verified identifiers: {run(args.file, dry_run=args.dry_run)}")
