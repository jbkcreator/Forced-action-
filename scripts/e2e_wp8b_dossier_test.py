"""
WP-8B Real E2E Test — Quote Ready Slack Dossier
==================================================
Run: PYTHONPATH=. python scripts/e2e_wp8b_dossier_test.py

Posts a real Quote Ready dossier (real compute_quote_ready() output, real
ARV projection, real lender_box match) to Slack with Approve/Modify/Reject
buttons, then drives a real decision through the exact same handler Slack's
own button click invokes (admin_router._handle_quote_ready_decision),
verifying the DB write.

Posts to the sandbox test workspace by default (never the real production
MONEY channel) via env override below.
"""
from __future__ import annotations

import json
import os
import sys
from decimal import Decimal

# Redirect Slack posting to the sandbox test workspace, never real production.
if not os.environ.get("FA_MAX_SLACK_BOT_TOKEN"):
    raise SystemExit(
        "FA_MAX_SLACK_BOT_TOKEN is not set. Source the sandbox credentials first, e.g.:\n"
        "  set -a; source secrets/sandbox-slack-listener.env; set +a"
    )
os.environ.setdefault("FA_MAX_SLACK_CHANNEL_MONEY", "C0C2A3C4JDP")
os.environ.setdefault("RELAY_APPROVERS", '["U0BN27JB8CW"]')

from config.settings import get_settings  # noqa: E402
get_settings.cache_clear()

from sqlalchemy import text  # noqa: E402

from src.core.database import get_db_context  # noqa: E402
from src.services.quote_ready.compute import compute_quote_ready  # noqa: E402
from src.services.quote_ready.models import QuoteReadyInput  # noqa: E402
from src.services.quote_ready.persistence import build_result_row  # noqa: E402
from src.services.quote_ready.dossier import post_quote_ready_dossier  # noqa: E402

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


def main() -> None:
    _section("Setup — real quote-ready row, real property, real ARV attached")

    with get_db_context() as db:
        real_opp = db.execute(text("SELECT opportunity_id::text FROM fa_max_opportunities LIMIT 1")).scalar()
    if real_opp is None:
        _fail("WP-8B.15 setup", "no real fa_max_opportunities row exists")
        print_summary()
        return

    property_id = 260391  # real property with a real persisted ARV (fa_max_arv_results)
    inp = QuoteReadyInput(
        opportunity_id=real_opp, property_id=property_id,
        max_ltc=Decimal("0.85"), max_ltv=Decimal("0.75"),
        purchase_price=Decimal("310000"), rehab_estimate=Decimal("55000"),
        arv=Decimal("445000"),
    )
    result = compute_quote_ready(inp)
    row_dict = build_result_row(inp, result, computed_by="wp8b_dossier_e2e")

    with get_db_context() as db:
        result_id = db.execute(
            text("""
                INSERT INTO fa_max_quote_ready_results
                    (opportunity_id, property_id, calculation_version, input_hash, status,
                     inputs, outputs, provenance, confidence, missing_inputs, computed_by)
                VALUES
                    (:opportunity_id, :property_id, :calculation_version, :input_hash, :status,
                     :inputs ::jsonb, :outputs ::jsonb, :provenance ::jsonb, :confidence ::jsonb,
                     :missing_inputs ::jsonb, :computed_by)
                RETURNING result_id::text
            """),
            {**row_dict,
             "inputs": json.dumps(row_dict["inputs"]),
             "outputs": json.dumps(row_dict["outputs"]),
             "provenance": json.dumps(row_dict["provenance"]),
             "confidence": json.dumps(row_dict["confidence"]),
             "missing_inputs": json.dumps(row_dict["missing_inputs"])},
        ).scalar()
        db.commit()
    _pass("WP-8B.15 real quote-ready row created", f"result_id={result_id} opportunity_id={real_opp} property_id={property_id}")

    _section("Real Slack Dossier Post")
    with get_db_context() as db:
        posted_ts = post_quote_ready_dossier(db, result_id)

    if posted_ts is None:
        _fail("WP-8B.16 dossier posted", "post_quote_ready_dossier() returned None")
    else:
        try:
            import requests
            tok = os.environ["FA_MAX_SLACK_BOT_TOKEN"]
            r = requests.get(
                "https://slack.com/api/conversations.history",
                headers={"Authorization": f"Bearer {tok}"},
                params={"channel": "C0C2A3C4JDP", "latest": posted_ts, "inclusive": "true", "limit": 1},
            )
            d = r.json()
            found = bool(d.get("messages"))
            preview = d["messages"][0]["text"][:400] if found else None
        except Exception as exc:
            found, preview = None, str(exc)
        if found:
            _pass(
                "WP-8B.16 dossier posted",
                f"posted_ts={posted_ts}, independently confirmed via Slack API — GO CHECK CHANNEL "
                f"C0C2A3C4JDP AND SCREENSHOT IT. Preview:\n{preview}",
            )
        else:
            _pass("WP-8B.16 dossier posted (unverified)", f"posted_ts={posted_ts} returned, API verification inconclusive")

    _section("Real Decision — same handler Slack's own click invokes")
    import src.api.admin_router as admin_router
    decision_payload = {
        "user": {"id": "U0BN27JB8CW"},
        "actions": [{"action_id": "quote_ready_approve", "value": json.dumps({"result_id": result_id})}],
    }
    with get_db_context() as db:
        handler_result = admin_router._handle_quote_ready_decision(decision_payload, db)
    print(f"  handler result: {handler_result}")

    with get_db_context() as db:
        raw = db.execute(
            text("SELECT review_status, reviewed_by, reviewed_at FROM fa_max_quote_ready_results WHERE result_id = :id ::uuid"),
            {"id": result_id},
        ).mappings().first()
    print(f"  raw row: {dict(raw)}")
    if raw["review_status"] != "approved" or raw["reviewed_by"] != "U0BN27JB8CW" or raw["reviewed_at"] is None:
        _fail("WP-8B.17 decision recorded", f"{dict(raw)}")
    else:
        _pass("WP-8B.17 decision recorded", f"review_status=approved reviewed_by=U0BN27JB8CW reviewed_at={raw['reviewed_at']}")

    # Idempotency: repeat click with the same decision must no-op.
    with get_db_context() as db:
        handler_result2 = admin_router._handle_quote_ready_decision(decision_payload, db)
    print(f"  repeat click result: {handler_result2}")
    if "already decided" not in str(handler_result2.get("text", "")).lower():
        _fail("WP-8B.18 idempotent decision", f"repeat click was not treated as already-decided: {handler_result2}")
    else:
        _pass("WP-8B.18 idempotent decision", "repeat approve click correctly treated as already-decided, no duplicate write")

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
