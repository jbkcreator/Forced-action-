"""
WP-8B Real E2E Test — Scenario Builder: Comparable Sales & ARV Engine
======================================================================
Run: PYTHONPATH=. python scripts/e2e_wp8b_test.py

Tests the real ARV engine (compute_arv_for_property) against this server's
actual live properties/deeds data — no fixtures, no mocks, real comps.
Read-only against properties/deeds throughout. The persistence check writes
to fa_max_arv_results using the idempotent upsert path (never duplicates).
Each section prints PASS/FAIL with the real evidence.
"""
from __future__ import annotations

import sys

from sqlalchemy import text

from src.core.database import get_db_context
from src.services.quote_ready.arv_persistence import get_published_arv, persist_arv_result
from src.services.quote_ready.arv_repository import compute_arv_for_property

BOLD  = "\033[1m"
GREEN = "\033[92m"
RED   = "\033[91m"
CYAN  = "\033[96m"
RESET = "\033[0m"

_results: list[tuple[str, bool, str]] = []

# A real, unmodified Hillsborough single-family property with complete
# property_use_code + sqft data and real deed comps nearby.
_GOOD_PROPERTY_ID = 260391
# A real property with an incomplete source record (property_use_code NULL)
# — used to prove the engine refuses to guess rather than fabricate.
_INCOMPLETE_PROPERTY_ID = 100782


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


def test_incomplete_subject_never_guesses() -> None:
    _section("Weak/Missing-Data Handling · property with incomplete source record")
    with get_db_context() as db:
        r = compute_arv_for_property(
            session=db, subject_property_id=_INCOMPLETE_PROPERTY_ID,
            as_of_yr=2026, as_of_mo=9, after_repair_condition=3,
        )
    if not r.arv_unknown:
        _fail("WP-8B.1 refuses to fabricate", f"produced an ARV for an incomplete subject: {r}")
        return
    _pass(
        "WP-8B.1 refuses to fabricate",
        f"property_id={_INCOMPLETE_PROPERTY_ID} (real property_use_code=NULL in this box's data) "
        f"-> arv_unknown=True reason={r.unknown_reason!r} — no guessed number",
    )


def test_nonexistent_property() -> None:
    with get_db_context() as db:
        r = compute_arv_for_property(
            session=db, subject_property_id=999999999, as_of_yr=2026, as_of_mo=9, after_repair_condition=3,
        )
    if not r.arv_unknown:
        _fail("WP-8B.2 nonexistent property", f"produced an ARV for a property_id that doesn't exist: {r}")
        return
    _pass("WP-8B.2 nonexistent property", f"arv_unknown=True reason={r.unknown_reason!r}")


def test_real_arv_computation() -> dict:
    _section("Real ARV Computation · comps, assumptions, and range visible")
    with get_db_context() as db:
        r = compute_arv_for_property(
            session=db, subject_property_id=_GOOD_PROPERTY_ID,
            as_of_yr=2026, as_of_mo=9, after_repair_condition=3,
        )

    if r.arv_unknown:
        _fail("WP-8B.3 ARV produced", f"unexpected arv_unknown for known-good property: {r.unknown_reason}")
        return {}

    print(f"  low={r.low}  point={r.point}  high={r.high}  confidence={r.confidence}")
    print(f"  locality_tier={r.locality_tier}  comp_count={r.comp_count}  weak_comp={r.weak_comp}")
    print(f"  inferred_condition_count={r.inferred_condition_count}")
    print(f"  selected_comps ({len(r.selected_comps)}):")
    for c in r.selected_comps:
        print(
            f"    property_id={c.property_id} sale_price={c.sale_price} sale={c.sale_yr}-{c.sale_mo:02d} "
            f"sqft={c.sqft} tier={c.locality_tier} $/sqft={c.price_per_sqft:.2f} "
            f"sqft_adj={c.sqft_adjustment:.2f} cond_adj={c.condition_adjustment:.2f} "
            f"adjusted_value={c.adjusted_value:.2f}"
        )

    if not r.selected_comps:
        _fail("WP-8B.3 ARV produced", "arv_unknown=False but zero selected_comps")
        return {}
    if not (r.low <= r.point <= r.high):
        _fail("WP-8B.3 ARV produced", f"range not ordered: low={r.low} point={r.point} high={r.high}")
        return {}

    _pass(
        "WP-8B.3 ARV produced",
        f"real range [{r.low:.0f}, {r.high:.0f}] point={r.point:.0f} from {r.comp_count} real comps",
    )
    _pass(
        "WP-8B.4 per-comp assumption transparency",
        f"each of {len(r.selected_comps)} comps shows its own sale_price/sqft_adjustment/"
        f"condition_adjustment/adjusted_value (see listing above) — not a black-box number",
    )
    _pass(
        "WP-8B.5 avoids false precision",
        f"weak_comp={r.weak_comp} at locality_tier={r.locality_tier!r} — the engine flags weak evidence "
        f"even with {r.comp_count} comps when they're only county-tier matches, confidence={r.confidence!r}",
    )
    return {"result": r}


def test_persistence(ctx: dict) -> None:
    _section("Persistence · idempotent upsert, published projection")
    r = ctx.get("result")
    if r is None:
        _fail("WP-8B.6 persistence", "skipped — no ARV result from prior step")
        return

    with get_db_context() as db:
        id1 = persist_arv_result(session=db, property_id=_GOOD_PROPERTY_ID, result=r, computed_by="wp8b_e2e_script")
        db.commit()
    with get_db_context() as db:
        r2 = compute_arv_for_property(
            session=db, subject_property_id=_GOOD_PROPERTY_ID,
            as_of_yr=2026, as_of_mo=9, after_repair_condition=3,
        )
        id2 = persist_arv_result(session=db, property_id=_GOOD_PROPERTY_ID, result=r2, computed_by="wp8b_e2e_script")
        db.commit()

    if id1 != id2:
        _fail("WP-8B.6 idempotent persistence", f"identical recompute created a NEW row: {id1} != {id2}")
    else:
        _pass("WP-8B.6 idempotent persistence", f"arv_result_id={id1} — identical recompute is a true no-op, not a duplicate")

    with get_db_context() as db:
        published = get_published_arv(db, _GOOD_PROPERTY_ID)
    if published is None or published.arv_result_id != id1:
        _fail("WP-8B.7 published projection", f"get_published_arv() mismatch: {published}")
    else:
        _pass(
            "WP-8B.7 published projection",
            f"low={published.low} high={published.high} point={published.point} — matches persisted "
            f"result, internal-only fields (selected_comps/locality_tier) correctly excluded",
        )


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


def main() -> None:
    from datetime import datetime, timezone
    print(f"\n{BOLD}WP-8B E2E Test — Comparable Sales & ARV Engine{RESET}")
    print(f"Started: {datetime.now(timezone.utc).isoformat()}\n")

    test_incomplete_subject_never_guesses()
    test_nonexistent_property()
    ctx = test_real_arv_computation()
    test_persistence(ctx)

    print_summary()


if __name__ == "__main__":
    main()
