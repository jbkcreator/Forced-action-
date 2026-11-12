"""Storage for the PropertyRadar calling lists: ``lending.pr_list_pulls`` and ``lending.pr_list_records``."""
from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from datetime import date
from typing import Any, Mapping, Optional

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

logger = logging.getLogger(__name__)

_RECORD_COLUMNS: tuple[str, ...] = (
    "list_key", "pull_id", "radar_id", "state", "county", "apn", "property_address", "city", "zip",
    "property_type", "owner_name", "ownership_type", "owner_mailing_address", "owner_mailing_city",
    "owner_mailing_state", "owner_mailing_zip", "principal_name", "loan_type", "lender_name",
    "loan_recorded_date", "loan_amount", "loan_term_years", "est_maturity_date", "last_transfer_date",
    "last_transfer_value", "last_transfer_seller", "last_transfer_type", "down_payment_pct",
    "prior_transfer_months", "qualifies", "match_strength", "exclusion_reason", "contact_key", "raw",
)
_UPDATED_ON_CONFLICT = [c for c in _RECORD_COLUMNS if c not in ("list_key", "radar_id")]
# What a row keeps when its parsed columns cannot be stored: enough to find, judge and re-parse it.
_KEPT_ON_FALLBACK = ("list_key", "pull_id", "radar_id", "qualifies", "match_strength", "exclusion_reason",
                     "contact_key", "raw")

_UPSERT_RECORDS = text(
    f"INSERT INTO lending.pr_list_records ({', '.join(_RECORD_COLUMNS)}) "
    f"VALUES ({', '.join(':' + c if c != 'raw' else 'CAST(:raw AS jsonb)' for c in _RECORD_COLUMNS)}) "
    "ON CONFLICT (list_key, radar_id) DO UPDATE SET "
    + ", ".join(f"{c} = EXCLUDED.{c}" for c in _UPDATED_ON_CONFLICT)
    + ", last_seen_at = now()"
)


def start_pull(db, list_key: str, window_start: date, window_end: date,
               criteria: Sequence[Mapping[str, Any]], matching_count: int) -> int:
    return int(db.execute(text(
        "INSERT INTO lending.pr_list_pulls (list_key, window_start, window_end, criteria, matching_count) "
        "VALUES (:k, :s, :e, CAST(:c AS jsonb), :m) RETURNING id"
    ), {"k": list_key, "s": window_start, "e": window_end, "c": json.dumps(list(criteria)), "m": matching_count}).scalar())


def store_records(db, pull_id: int, rows: Sequence[Mapping[str, Any]]) -> int:
    """Upsert one batch; every row is paid for, so none may be dropped.

    One statement for the batch. If the database rejects it (a value it cannot take), each row is
    retried in its own savepoint, and a row still rejected is stored with only its identity,
    qualification and raw record (the parsed columns left empty), so the paid data is kept.
    Returns the rows written.
    """
    if not rows:
        return 0
    params = [_params(row, pull_id) for row in rows]
    try:
        with db.begin_nested():
            db.execute(_UPSERT_RECORDS, params)
        return len(params)
    except DBAPIError:
        logger.warning("[calling-list] batch of %d rejected; storing row by row", len(params))
    for row in params:
        try:
            with db.begin_nested():
                db.execute(_UPSERT_RECORDS, row)
        except DBAPIError as exc:
            logger.warning("[calling-list] radar_id %s stored raw only: %s", row["radar_id"], type(exc).__name__)
            with db.begin_nested():
                db.execute(_UPSERT_RECORDS, {**dict.fromkeys(_RECORD_COLUMNS), **{k: row[k] for k in _KEPT_ON_FALLBACK}})
    return len(params)


def _params(row: Mapping[str, Any], pull_id: int) -> dict[str, Any]:
    return {**row, "pull_id": pull_id, "raw": json.dumps(row["raw"], default=str)}


def raw_held_by_other_lists(db, list_key: str, radar_ids: Sequence[str]) -> dict[str, dict]:
    """RadarID -> the raw record another calling list already bought (same export fields), so it is copied
    instead of bought again."""
    if not radar_ids:
        return {}
    rows = db.execute(text(
        "SELECT DISTINCT ON (radar_id) radar_id, raw FROM lending.pr_list_records "
        "WHERE radar_id = ANY(:ids) AND list_key <> :k ORDER BY radar_id, last_seen_at DESC"
    ), {"ids": list(radar_ids), "k": list_key}).all()
    return {r.radar_id: dict(r.raw) for r in rows}


def stored_radar_ids(db, list_key: str) -> set[str]:
    return {r[0] for r in db.execute(text(
        "SELECT radar_id FROM lending.pr_list_records WHERE list_key = :k"), {"k": list_key})}


def update_pull(db, pull_id: int, *, exports_consumed: int, records_stored: int,
                status: Optional[str] = None, error: Optional[str] = None) -> None:
    db.execute(text(
        "UPDATE lending.pr_list_pulls SET exports_consumed = :x, records_stored = :r, "
        "status = COALESCE(:st, status), error = COALESCE(:err, error), "
        "finished_at = CASE WHEN :st IN ('done', 'failed') THEN now() ELSE finished_at END WHERE id = :id"
    ), {"x": exports_consumed, "r": records_stored, "st": status, "err": error, "id": pull_id})


def list_report(db, list_key: str) -> dict[str, Any]:
    """What a list holds now: records, qualifying records, the people to reach, and why records failed."""
    totals = db.execute(text(
        "SELECT count(*) AS records, count(*) FILTER (WHERE qualifies) AS qualifying, "
        "count(DISTINCT contact_key) FILTER (WHERE qualifies) AS unique_contacts, "
        "count(*) FILTER (WHERE qualifies AND principal_name IS NOT NULL) AS qualifying_with_principal "
        "FROM lending.pr_list_records WHERE list_key = :k"
    ), {"k": list_key}).mappings().one()

    def grouped(column: str, where: str) -> dict[str, int]:
        rows = db.execute(text(
            f"SELECT coalesce({column}, 'unknown') AS v, count(*) AS n FROM lending.pr_list_records "
            f"WHERE list_key = :k AND {where} GROUP BY 1 ORDER BY 2 DESC"
        ), {"k": list_key}).all()
        return {row.v: row.n for row in rows}

    return {
        **dict(totals),
        "by_match_strength": grouped("match_strength", "qualifies"),
        "unique_contacts_by_match_strength": {
            row.v: row.n for row in db.execute(text(
                "SELECT match_strength AS v, count(DISTINCT contact_key) AS n FROM lending.pr_list_records "
                "WHERE list_key = :k AND qualifies GROUP BY 1"
            ), {"k": list_key}).all()
        },
        "qualifying_by_ownership_type": grouped("ownership_type", "qualifies"),
        "excluded_by_reason": grouped("exclusion_reason", "NOT qualifies"),
    }
