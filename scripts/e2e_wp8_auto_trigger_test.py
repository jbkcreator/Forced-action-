"""
WP-8A/8B Auto-Trigger Real E2E Test
=====================================
Run: PYTHONPATH=. python scripts/e2e_wp8_auto_trigger_test.py

Proves the automatic dossier trigger: no manual dossier-posting call is
made here at all — only real state_engine.transition() calls, walking a
real synthetic opportunity to 'scoping'. If a real Quote Ready dossier gets
posted, it happened entirely from the production transition() hook, exactly
as a real opportunity reaching 'scoping' in normal operation would trigger it.

Posts to the sandbox test workspace via env override (see below) — never
production.
"""
from __future__ import annotations

import os
import sys
import uuid

if not os.environ.get("FA_MAX_SLACK_BOT_TOKEN"):
    raise SystemExit(
        "FA_MAX_SLACK_BOT_TOKEN is not set. Source the sandbox credentials first, e.g.:\n"
        "  set -a; source secrets/sandbox-slack-listener.env; set +a"
    )
os.environ.setdefault("FA_MAX_SLACK_CHANNEL_MONEY", "C0C2A3C4JDP")

from config.settings import get_settings  # noqa: E402
get_settings.cache_clear()

from sqlalchemy import text  # noqa: E402

from src.core.database import get_db_context  # noqa: E402
from src.services.state_engine import (  # noqa: E402
    create_fa_max_opportunity, ensure_entity_registry, get_opportunity_state, transition,
)

BOLD, GREEN, RED, CYAN, RESET = "\033[1m", "\033[92m", "\033[91m", "\033[96m", "\033[0m"
REAL_PROPERTY_ID = 260391  # has real financials: assessed_value_mkt=248069, last_sale_price=287000


def main() -> None:
    print(f"\n{BOLD}WP-8A/8B Auto-Trigger Test — real transition(), zero manual dossier calls{RESET}\n")

    native_id = str(uuid.uuid4())
    with get_db_context() as db:
        db.execute(
            text("INSERT INTO fa_max_persons (person_id, lifecycle_state, source) "
                 "VALUES (:pid ::uuid, 'identified', 'zztest_autotrigger')"),
            {"pid": native_id},
        )
        db.commit()
    print(f"  created synthetic person person_id={native_id}")

    with get_db_context() as db:
        opp_id = create_fa_max_opportunity(
            session=db, person_id=native_id, opportunity_type="acquisition", source="zztest_autotrigger",
        )
        entity_uuid = ensure_entity_registry(session=db, entity_type="opportunity", native_id=opp_id)
        db.execute(
            text("INSERT INTO fa_max_opportunity_properties (opportunity_id, property_id, role, source) "
                 "VALUES (:oid ::uuid, :pid, 'subject', 'zztest_autotrigger')"),
            {"oid": opp_id, "pid": REAL_PROPERTY_ID},
        )
        db.commit()
    print(f"  created real opportunity opportunity_id={opp_id}, linked to REAL property_id={REAL_PROPERTY_ID}")

    with get_db_context() as db:
        row = db.execute(
            text("SELECT result_id FROM fa_max_quote_ready_results WHERE opportunity_id = :oid ::uuid"),
            {"oid": opp_id},
        ).fetchall()
    print(f"  quote_ready rows for this opportunity BEFORE any transition: {len(row)} (must be 0)")
    assert len(row) == 0

    for to_stage in ("qualifying", "scoping"):
        with get_db_context() as db:
            current = get_opportunity_state(session=db, opportunity_id=opp_id)
            res = transition(
                session=db, entity_type="opportunity", entity_uuid=entity_uuid,
                from_state=current["current_stage"], to_state=to_stage,
                actor="system:zztest_autotrigger", source_component="zztest",
                idempotency_key=f"zztest-autotrigger-{opp_id}-{to_stage}",
                state_version=current["state_version"],
            )
            db.commit()
        print(f"  transition -> {to_stage}: outcome={res.outcome.value}")
        assert res.outcome.value == "succeeded", f"FAIL: transition to {to_stage} did not succeed: {res}"

    print(f"\n{CYAN}Just transitioned to 'scoping' via the plain transition() call above — "
          f"NO dossier-posting function was called manually anywhere in this script.{RESET}\n")

    with get_db_context() as db:
        rows = db.execute(
            text("SELECT result_id, status, inputs, outputs, missing_inputs FROM fa_max_quote_ready_results "
                 "WHERE opportunity_id = :oid ::uuid ORDER BY computed_at DESC"),
            {"oid": opp_id},
        ).mappings().fetchall()

    if not rows:
        print(f"  {RED}FAIL{RESET} — no fa_max_quote_ready_results row was created automatically.")
        sys.exit(1)

    print(f"  {GREEN}PASS{RESET} — auto-trigger created {len(rows)} quote_ready row(s) with ZERO manual calls:")
    for r in rows:
        print(f"    result_id={r['result_id']} status={r['status']} missing={r['missing_inputs']}")
        print(f"    inputs={r['inputs']}")
        print(f"    outputs={r['outputs']}")

    print(f"\n  {BOLD}GO CHECK SLACK CHANNEL C0C2A3C4JDP NOW — a dossier card should have posted "
          f"itself, entirely automatically, from the real production transition() code path.{RESET}")

    print("\n--- Cleanup ---")
    with get_db_context() as db:
        db.execute(text("SET LOCAL fa_max.allow_state_write = 'on'"))
        db.execute(text("DELETE FROM fa_max_quote_ready_results WHERE opportunity_id = :oid ::uuid"), {"oid": opp_id})
        db.execute(text("DELETE FROM fa_max_opportunity_properties WHERE opportunity_id = :oid ::uuid"), {"oid": opp_id})
        db.execute(text("DELETE FROM fa_max_state_transition_events WHERE person_id = :pid ::uuid"), {"pid": native_id})
        db.execute(text("DELETE FROM fa_max_opportunities WHERE opportunity_id = :oid ::uuid"), {"oid": opp_id})
        db.execute(text("DELETE FROM fa_max_entity_registry WHERE native_id = :oid OR entity_uuid = :euid ::uuid"),
                   {"oid": opp_id, "euid": entity_uuid})
        db.execute(text("DELETE FROM fa_max_persons WHERE person_id = :pid ::uuid"), {"pid": native_id})
        db.commit()
    print("cleaned up synthetic person/opportunity/events (real property/financials untouched).")


if __name__ == "__main__":
    main()
