"""
WP-8B Real E2E Test — Manual ARV Override with Audit Trail
=============================================================
Run: PYTHONPATH=. python scripts/e2e_wp8b_override_test.py

Tests override_arv_result() against a real computed fa_max_arv_results row.
"""
from __future__ import annotations

import sys
from decimal import Decimal

from sqlalchemy import text

from src.core.database import get_db_context
from src.services.quote_ready.arv_persistence import get_published_arv, override_arv_result

BOLD  = "\033[1m"
GREEN = "\033[92m"
RED   = "\033[91m"
CYAN  = "\033[96m"
RESET = "\033[0m"

_results: list[tuple[str, bool, str]] = []
_PROPERTY_ID = 260391


def _pass(label: str, evidence: str) -> None:
    _results.append((label, True, evidence))
    print(f"  {GREEN}PASS{RESET} {label}")
    print(f"       {evidence}")


def _fail(label: str, reason: str) -> None:
    _results.append((label, False, reason))
    print(f"  {RED}FAIL{RESET} {label}")
    print(f"       {reason}")


def _section(title: str) -> None:
    print(f"\n{BOLD}{CYAN}{'='*68}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'='*68}{RESET}")


def main() -> None:
    _section("Manual ARV Override — real row, real audit trail")

    with get_db_context() as db:
        row = db.execute(
            text("SELECT arv_result_id::text, low, point, high FROM fa_max_arv_results "
                 "WHERE property_id = :p AND status = 'computed'"),
            {"p": _PROPERTY_ID},
        ).mappings().first()
    if row is None:
        _fail("WP-8B.8 setup", f"no 'computed' ARV row exists for property_id={_PROPERTY_ID} — "
                                 "run scripts/e2e_wp8b_test.py first to create one")
        print_summary()
        return

    arv_result_id = row["arv_result_id"]
    print(f"  targeting real arv_result_id={arv_result_id} (computed low={row['low']} point={row['point']} high={row['high']})")

    # Validation: blank reason rejected.
    with get_db_context() as db:
        try:
            override_arv_result(
                db, arv_result_id=arv_result_id, override_low=Decimal("300000"),
                override_point=Decimal("320000"), override_high=Decimal("340000"),
                reason="   ", overridden_by="wp8b_e2e_script",
            )
            _fail("WP-8B.9 rejects blank reason", "did NOT raise")
        except ValueError as e:
            _pass("WP-8B.9 rejects blank reason", str(e))

    # Validation: inverted range rejected.
    with get_db_context() as db:
        try:
            override_arv_result(
                db, arv_result_id=arv_result_id, override_low=Decimal("400000"),
                override_point=Decimal("320000"), override_high=Decimal("340000"),
                reason="test inverted range", overridden_by="wp8b_e2e_script",
            )
            _fail("WP-8B.10 rejects inverted range", "did NOT raise")
        except ValueError as e:
            _pass("WP-8B.10 rejects inverted range", str(e))

    # Real override, with a real reason a reviewer would actually write.
    with get_db_context() as db:
        applied = override_arv_result(
            db, arv_result_id=arv_result_id,
            override_low=Decimal("375000"), override_point=Decimal("395000"), override_high=Decimal("415000"),
            reason="Comps were all county-tier and one was a clear outlier at $680/sqft vs ~$250/sqft "
                   "median — hand-adjusted range down to reflect the tighter, more comparable set.",
            overridden_by="wp8b_e2e_script",
        )
        db.commit()
    if not applied:
        _fail("WP-8B.11 override applied", "override_arv_result() returned False for a fresh 'computed' row")
    else:
        _pass("WP-8B.11 override applied", f"arv_result_id={arv_result_id} status -> 'overridden'")

    # Re-running the SAME override on the SAME (now non-'computed') row must no-op, not double-apply.
    with get_db_context() as db:
        applied_again = override_arv_result(
            db, arv_result_id=arv_result_id,
            override_low=Decimal("1"), override_point=Decimal("1"), override_high=Decimal("1"),
            reason="should not apply — row is no longer in computed status",
            overridden_by="wp8b_e2e_script",
        )
        db.commit()
    if applied_again:
        _fail("WP-8B.12 no double-override", "a second override call on an already-overridden row was applied")
    else:
        _pass("WP-8B.12 no double-override", "second call correctly no-op'd (row no longer in 'computed' status)")

    # Raw row check: original computed values must be untouched.
    with get_db_context() as db:
        raw = db.execute(
            text("SELECT low, point, high, override_low, override_point, override_high, "
                 "override_reason, overridden_by, overridden_at, status "
                 "FROM fa_max_arv_results WHERE arv_result_id = :id ::uuid"),
            {"id": arv_result_id},
        ).mappings().first()
    print(f"\n  Raw row after override:")
    for k, v in raw.items():
        print(f"    {k}: {v}")
    if raw["low"] != row["low"] or raw["point"] != row["point"] or raw["high"] != row["high"]:
        _fail("WP-8B.13 original values preserved", "original computed low/point/high were modified by the override")
    else:
        _pass("WP-8B.13 original values preserved", "original computed figures untouched — override is additive, not destructive")

    # get_published_arv() must now reflect the OVERRIDE values, plus the audit fields.
    with get_db_context() as db:
        published = get_published_arv(db, _PROPERTY_ID)
    print(f"\n  get_published_arv() after override: low={published.low} point={published.point} "
          f"high={published.high} overridden={published.overridden} overridden_by={published.overridden_by}")
    if not (published.overridden and published.low == Decimal("375000.00")):
        _fail("WP-8B.14 published projection reflects override", f"{published}")
    else:
        _pass("WP-8B.14 published projection reflects override",
              f"low={published.low} (was {row['low']} before override) — downstream consumers now see the reviewed figure")

    print_summary()


def print_summary() -> None:
    _section("SUMMARY")
    total = len(_results)
    passed = sum(1 for _, ok, _ in _results if ok)
    failed = total - passed
    for label, ok, _ in _results:
        icon = f"{GREEN}✓{RESET}" if ok else f"{RED}✗{RESET}"
        print(f"  {icon}  {label}")
    print(f"\n{BOLD}Result: {GREEN}{passed} passed{RESET} / {RED}{failed} failed{RESET} / {total} total{RESET}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
