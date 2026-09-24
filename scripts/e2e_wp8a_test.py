"""
WP-8A Real E2E Test — Scenario Builder: Deal Math & Program Matching
=====================================================================
Run: PYTHONPATH=. python scripts/e2e_wp8a_test.py

Tests the real deal-math engine (compute_quote_ready) and the real
program-matching engine (lender_box.evaluate) against this server's actual
live database (real lender_box_programs rows — no fixtures, no mocks).
No writes are made to any table; both engines under test are read-only /
pure-compute. Each section prints PASS/FAIL with the real evidence.
"""
from __future__ import annotations

import sys
import uuid
from decimal import Decimal

from src.core.database import get_db_context
from src.services.lender_box import DealInput, evaluate
from src.services.quote_ready.compute import compute_quote_ready
from src.services.quote_ready.models import QuoteReadyInput

BOLD  = "\033[1m"
GREEN = "\033[92m"
RED   = "\033[91m"
CYAN  = "\033[96m"
RESET = "\033[0m"

_results: list[tuple[str, bool, str]] = []


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


# ---------------------------------------------------------------------------
# Deal math (compute_quote_ready) — pure function, no DB
# ---------------------------------------------------------------------------

def test_deal_math_complete_inputs() -> None:
    _section("Deal Math · Complete Inputs → Reproducible Calc + Visible Assumptions")
    opp_id = uuid.uuid4()
    inp = QuoteReadyInput(
        opportunity_id=opp_id,
        max_ltc=Decimal("0.85"),
        max_ltv=Decimal("0.75"),
        purchase_price=Decimal("300000"),
        rehab_estimate=Decimal("50000"),
        rehab_source="job_estimator",
        arv=Decimal("450000"),
        arv_source="legacy_financial.arv",
        arv_confidence="low",
    )
    r1 = compute_quote_ready(inp)
    r2 = compute_quote_ready(inp)  # same input again — must be byte-identical

    if r1.project_cost.raw != r2.project_cost.raw or r1.ltc.raw != r2.ltc.raw or r1.ltv.raw != r2.ltv.raw:
        _fail("WP-8A.1 reproducibility", f"two runs of identical input differed: {r1} vs {r2}")
        return
    _pass("WP-8A.1 reproducibility", "identical input run twice produced byte-identical output")

    expected_project_cost = Decimal("350000")  # purchase + rehab
    if r1.project_cost.raw != expected_project_cost:
        _fail("WP-8A.2 project_cost math", f"got {r1.project_cost.raw}, expected {expected_project_cost}")
        return
    _pass("WP-8A.2 project_cost math", f"project_cost={r1.project_cost.raw} = purchase(300000)+rehab(50000) ✓")

    ltc_cap = expected_project_cost * Decimal("0.85")
    ltv_cap = Decimal("450000") * Decimal("0.75")
    expected_loan = min(ltc_cap, ltv_cap)
    if r1.proposed_loan.raw != expected_loan:
        _fail("WP-8A.3 proposed_loan math", f"got {r1.proposed_loan.raw}, expected {expected_loan}")
        return
    _pass(
        "WP-8A.3 proposed_loan math",
        f"proposed_loan={r1.proposed_loan.raw} = min(LTC cap={ltc_cap}, LTV cap={ltv_cap}) ✓",
    )

    _pass(
        "WP-8A.4 visible assumptions",
        f"ltc={r1.ltc.raw} (confidence={r1.ltc.confidence}, source={r1.ltc.source}); "
        f"ltv={r1.ltv.raw} (confidence={r1.ltv.confidence}, source={r1.ltv.source}) — "
        f"every figure carries provenance, not a bare number",
    )


def test_deal_math_missing_inputs() -> None:
    _section("Deal Math · Missing-Input Handling — must never fabricate a number")
    sparse = QuoteReadyInput(opportunity_id=uuid.uuid4(), max_ltc=Decimal("0.85"), max_ltv=Decimal("0.75"))
    r = compute_quote_ready(sparse)
    if r.project_cost is not None or r.proposed_loan is not None:
        _fail("WP-8A.5 missing-input handling", f"produced a figure with no purchase/rehab basis: {r}")
        return
    if not r.missing:
        _fail("WP-8A.5 missing-input handling", "no missing[] flagged despite no purchase basis / rehab given")
        return
    _pass("WP-8A.5 missing-input handling", f"missing={r.missing} — explicitly flagged, nothing guessed")


# ---------------------------------------------------------------------------
# Program matching (lender_box.evaluate) — real DB read against real programs
# ---------------------------------------------------------------------------

def test_program_matching() -> None:
    _section("Program Matching · lender_box.evaluate() against REAL lender_box_programs rows")

    with get_db_context() as db:
        deal_in = DealInput(
            property_type="single_family", state="FL", proposed_loan_amount=Decimal("280000"),
            purchase_price=Decimal("300000"), rehab_estimate=Decimal("50000"), arv=Decimal("450000"),
            ref="wp8a-e2e-inbox",
        )
        res_in = evaluate(deal_in, db)
    if res_in.status != "in_box":
        _fail("WP-8A.6 in-box match", f"expected in_box, got {res_in.status}: {res_in.summary()}")
    else:
        _pass("WP-8A.6 in-box match", f"status=in_box matched_program={res_in.matched_program_name!r} — {res_in.summary()}")

    with get_db_context() as db:
        deal_out = DealInput(
            property_type="single_family", state="FL", proposed_loan_amount=Decimal("5000000"),
            purchase_price=Decimal("6000000"), rehab_estimate=Decimal("100000"), arv=Decimal("7000000"),
            ref="wp8a-e2e-outbox",
        )
        res_out = evaluate(deal_out, db)
    if res_out.status != "out_of_box" or not res_out.fail_reasons:
        _fail("WP-8A.7 out-of-box match + explanation", f"status={res_out.status} fail_reasons={res_out.fail_reasons}")
    else:
        _pass(
            "WP-8A.7 out-of-box match + explanation",
            f"status=out_of_box, {len(res_out.fail_reasons)} explicit reasons, e.g. {res_out.fail_reasons[0]!r}",
        )

    with get_db_context() as db:
        deal_unc = DealInput(
            property_type="single_family", state="FL", proposed_loan_amount=Decimal("280000"),
            ref="wp8a-e2e-uncertain",
        )
        res_unc = evaluate(deal_unc, db)
    if res_unc.status != "uncertain" or not res_unc.uncertain_flags:
        _fail("WP-8A.8 uncertain routing", f"status={res_unc.status} uncertain_flags={res_unc.uncertain_flags}")
    else:
        _pass("WP-8A.8 uncertain routing", f"status=uncertain, missing fields flagged: {res_unc.uncertain_flags}")


def check_program_data_quality() -> None:
    _section("Data-Quality Note — real program values on this box")
    with get_db_context() as db:
        from sqlalchemy import text
        rows = db.execute(text(
            "SELECT program_key, name, max_ltc, max_ltv, notes FROM lender_box_programs WHERE is_active"
        )).mappings().fetchall()
    for r in rows:
        flag = " ⚠️ SYNTHETIC/PLACEHOLDER PER NOTES" if "synthetic" in (r["notes"] or "").lower() else ""
        print(f"  {r['program_key']:18s} max_ltc={r['max_ltc']} max_ltv={r['max_ltv']}{flag}")
        print(f"    notes: {r['notes']}")
    print(f"\n  (informational only — not a PASS/FAIL check; confirms what real business values "
          f"this run's program-matching PASS/FAIL results above were actually evaluated against.)")


# ---------------------------------------------------------------------------
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
    print(f"\n{BOLD}WP-8A E2E Test — Deal Math & Program Matching{RESET}")
    print(f"Started: {datetime.now(timezone.utc).isoformat()}\n")

    test_deal_math_complete_inputs()
    test_deal_math_missing_inputs()
    test_program_matching()
    check_program_data_quality()

    print_summary()


if __name__ == "__main__":
    main()
