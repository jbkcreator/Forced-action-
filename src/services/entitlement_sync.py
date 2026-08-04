"""Re-propagate `plans.entitlements` onto per-account `lead_entitlement` snapshots.

`customer_accounts.lead_entitlement` is a snapshot of `plans.entitlements` taken
once, by `revenue_engine.record_subscription_active()`, at subscription-activation
time. Nothing re-writes it when the plan catalog changes later, so a catalog edit
(retiring a plan, re-pricing, changing allowances) silently leaves every existing
account on a stale snapshot. `lead_delivery.bucket_for()` reads the snapshot, not
the catalog, so a stale `{}` excludes the account from delivery candidacy for every
grade regardless of territory lock or headroom.

That is the failure mode `migrations/apply_pro_founder_plan_entitlements.py` had to
repair by hand for 33 of 34 pro accounts. This module is the re-trigger that makes
the repair unnecessary: call it after any write to the `plans` catalog.

Only accounts whose snapshot actually differs from their plan are written, so the
sync is idempotent and a clean run issues no UPDATE at all.

Every account snapshot is treated as catalog-managed, because
`record_subscription_active()` is the only writer of `lead_entitlement` and it
copies the catalog verbatim — no admin endpoint, override column, or other path
can currently produce a deliberately-divergent per-account value. If per-account
custom entitlements are ever introduced, this sync needs an explicit opt-out
(a provenance flag or catalog-version marker) BEFORE that feature ships:
otherwise the first resync after a catalog edit will overwrite them. A stale
non-empty snapshot and a hand-tuned one are indistinguishable without it.

    python -m src.services.entitlement_sync --dry-run
    python -m src.services.entitlement_sync --plan pro --plan founder_monthly
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Optional, Sequence

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _as_dict(value) -> dict:
    """Normalize a JSONB/JSON column value to a plain dict.

    Postgres returns a mapping; SQLite under the test type-adapter stores the column
    as TEXT, so a round-tripped value can arrive as a JSON string instead.
    """
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return {}
    return value if isinstance(value, dict) else {}


@dataclass
class DriftRow:
    """One account whose snapshot disagrees with its plan."""

    account_id: str
    plan_id: str
    snapshot: dict
    catalog: dict


@dataclass
class ResyncResult:
    """Outcome of a resync pass."""

    drifted: list[DriftRow] = field(default_factory=list)
    updated: int = 0
    dry_run: bool = False

    @property
    def plan_ids(self) -> set[str]:
        return {row.plan_id for row in self.drifted}


def find_entitlement_drift(
    db: Session, *, plan_ids: Optional[Sequence[str]] = None
) -> list[DriftRow]:
    """Accounts whose `lead_entitlement` no longer matches their plan's catalog entry.

    Accounts on a plan with empty catalog entitlements are excluded: an empty entry
    means the plan is unconfigured, and overwriting a populated snapshot with {} would
    revoke lead delivery for a paying account. Accounts whose `plan_tier` matches no
    catalog row are excluded by the join.
    """
    sql = (
        "SELECT ca.account_id, ca.plan_tier, ca.lead_entitlement, p.entitlements "
        "FROM customer_accounts ca "
        "JOIN plans p ON p.plan_id = ca.plan_tier "
        "WHERE p.entitlements IS NOT NULL"
    )
    params: dict = {}
    if plan_ids:
        sql += " AND ca.plan_tier IN :plan_ids"
        params["plan_ids"] = list(plan_ids)

    stmt = text(sql + " ORDER BY ca.plan_tier, ca.account_id")
    if plan_ids:
        stmt = stmt.bindparams(bindparam("plan_ids", expanding=True))

    rows = db.execute(stmt, params).fetchall()

    drift: list[DriftRow] = []
    for account_id, plan_id, snapshot, catalog in rows:
        catalog_dict = _as_dict(catalog)
        snapshot_dict = _as_dict(snapshot)
        if not catalog_dict or snapshot_dict == catalog_dict:
            continue
        drift.append(
            DriftRow(
                account_id=str(account_id),
                plan_id=plan_id,
                snapshot=snapshot_dict,
                catalog=catalog_dict,
            )
        )
    return drift


def resync_lead_entitlements(
    db: Session,
    *,
    plan_ids: Optional[Sequence[str]] = None,
    dry_run: bool = False,
) -> ResyncResult:
    """Copy `plans.entitlements` onto the snapshot of every account that has drifted.

    Call this after any mutation of the `plans` catalog. Scoping to `plan_ids` limits
    the blast radius to the plans that were actually edited; omitting it repairs every
    drifted account, which is the right choice for a scheduled consistency sweep.

    Returns the drift that was found (and, unless `dry_run`, repaired). The caller owns
    the transaction — this flushes but never commits. Writes are batched one statement
    per affected plan, not per account.
    """
    drifted = find_entitlement_drift(db, plan_ids=plan_ids)
    if not drifted:
        logger.info("entitlement_sync: no drift found")
        return ResyncResult(drifted=[], updated=0, dry_run=dry_run)

    for row in drifted:
        logger.info(
            "entitlement_sync: account=%s plan=%s snapshot=%s -> catalog=%s%s",
            row.account_id,
            row.plan_id,
            row.snapshot,
            row.catalog,
            " (dry-run)" if dry_run else "",
        )

    if dry_run:
        return ResyncResult(drifted=drifted, updated=0, dry_run=True)

    by_plan: dict[str, list[DriftRow]] = {}
    for row in drifted:
        by_plan.setdefault(row.plan_id, []).append(row)

    updated = 0
    for plan_id, rows in by_plan.items():
        # `plan_tier = :plan_id` re-checks the premise the SELECT established.
        # record_subscription_active() writes plan_tier and lead_entitlement
        # together, so an account whose subscription changed between the read
        # and this write no longer matches and keeps the correct snapshot its
        # new plan already gave it — instead of being stamped with the old
        # plan's entitlements. Skipped rows are simply absent from rowcount.
        stmt = text(
            "UPDATE customer_accounts SET lead_entitlement = :entitlements "
            "WHERE account_id IN :account_ids AND plan_tier = :plan_id"
        ).bindparams(bindparam("account_ids", expanding=True))
        result = db.execute(
            stmt,
            {
                "entitlements": json.dumps(rows[0].catalog),
                "account_ids": [row.account_id for row in rows],
                "plan_id": plan_id,
            },
        )
        # Trust rowcount — a short count now means a concurrent plan change was
        # correctly skipped, so falling back to len(rows) would over-report.
        if result.rowcount is not None and result.rowcount >= 0:
            updated += result.rowcount
            if result.rowcount < len(rows):
                logger.info(
                    "entitlement_sync: plan=%s expected %d row(s), updated %d — "
                    "%d account(s) changed plan concurrently and were skipped",
                    plan_id, len(rows), result.rowcount, len(rows) - result.rowcount,
                )
        else:
            updated += len(rows)

    db.flush()
    logger.info(
        "entitlement_sync: repaired %d account(s) across plan(s) %s",
        updated,
        ", ".join(sorted(by_plan)),
    )
    return ResyncResult(drifted=drifted, updated=updated, dry_run=False)


def main() -> int:
    import argparse

    from src.core.database import get_db_context

    parser = argparse.ArgumentParser(description="Re-sync per-account lead entitlement snapshots.")
    parser.add_argument(
        "--plan",
        action="append",
        dest="plans",
        help="Limit to a plan_id (repeatable). Omit to sweep every plan.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report drift without writing.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

    with get_db_context() as db:
        result = resync_lead_entitlements(db, plan_ids=args.plans, dry_run=args.dry_run)
        if not args.dry_run:
            db.commit()

    print(
        f"drifted={len(result.drifted)} updated={result.updated}"
        + (" (dry-run)" if result.dry_run else "")
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
