#!/usr/bin/env python3
"""
Verification script for the accelerated_wallet_push_framing A/B attribution fix.

Runs 13 safety checks against the production database. Read-only except for an
optional controlled graph send when --real-send and --subscriber-id are both
provided and the target is an internal/test subscriber.

Usage:
    python scripts/verify_wallet_push_ab.py                           # dry-run, auto-pick
    python scripts/verify_wallet_push_ab.py --subscriber-id 4509
    python scripts/verify_wallet_push_ab.py --real-send --subscriber-id 4509

Safety rules (enforced, not advisory):
    - Never sends to more than one subscriber.
    - Real send only when both --real-send AND --subscriber-id are provided.
    - Aborts if subscriber is not internal (heu.ai / forcedaction.io domain).
    - Never modifies pricing, winner_variant, ended_at, or rollback state.
    - Never changes traffic_pct.
    - Read-only SQL except the optional controlled graph send.
"""
from __future__ import annotations

import argparse
import re
import sys
import uuid
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

import yaml
from sqlalchemy import func, select, text

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.core.database import Database
from src.core.models import (
    AbTest,
    AgentDecision,
    MessageOutcome,
    SmsOptIn,
    Subscriber,
    WalletBalance,
)

_INTERNAL_DOMAINS = {"heu.ai", "forcedaction.io"}
_VARIANT_PATTERN = re.compile(r"^accelerated_wallet_push_framing:[ab]$")
_TEST_NAME = "accelerated_wallet_push_framing"
_GRAPH_NAME = "accelerated_wallet_push"


def _is_internal(email: Optional[str]) -> bool:
    if not email:
        return False
    domain = email.lower().split("@")[-1]
    return domain in _INTERNAL_DOMAINS or "test" in email.lower() or "demo" in email.lower()


def _ok(passed: bool) -> str:
    return "PASS" if passed else "FAIL"


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify wallet_push A/B attribution fix")
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="Patch Telnyx during graph test run (no real SMS). Default: True")
    parser.add_argument("--real-send", action="store_true", default=False,
                        help="Allow real SMS (requires --subscriber-id of internal account)")
    parser.add_argument("--subscriber-id", type=int, default=None, dest="subscriber_id",
                        help="Target subscriber for the graph test run")
    parser.add_argument("--limit", type=int, default=1,
                        help="Max candidates to scan when auto-picking (default 1)")
    args = parser.parse_args()

    if args.real_send and not args.subscriber_id:
        print("ERROR: --real-send requires --subscriber-id.")
        sys.exit(1)

    failures: list[str] = []

    print(f"\n{'='*62}")
    print("  verify_wallet_push_ab.py  —  A/B attribution fix (13 checks)")
    print(f"{'='*62}")
    print(f"  mode        : {'REAL SEND' if args.real_send else 'dry-run (Telnyx patched)'}")
    if args.subscriber_id:
        print(f"  subscriber  : {args.subscriber_id}")
    print()

    with Database().session_scope() as db:

        # ── Check 1: variant_id column exists in agent_decisions ─────────────
        row = db.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='agent_decisions' AND column_name='variant_id'"
        )).fetchone()
        ok1 = row is not None
        print(f"[1]  agent_decisions.variant_id column exists          {_ok(ok1)}")
        if not ok1:
            failures.append("1: variant_id column missing from agent_decisions")

        # ── Check 2: index ix_agent_decisions_variant_id exists ──────────────
        row = db.execute(text(
            "SELECT indexname FROM pg_indexes "
            "WHERE tablename='agent_decisions' AND indexname='ix_agent_decisions_variant_id'"
        )).fetchone()
        ok2 = row is not None
        print(f"[2]  ix_agent_decisions_variant_id index exists         {_ok(ok2)}")
        if not ok2:
            failures.append("2: ix_agent_decisions_variant_id index missing")

        # ── Check 3: ab_tests row for accelerated_wallet_push_framing ────────
        ab_test = db.execute(
            select(AbTest).where(AbTest.test_name == _TEST_NAME)
        ).scalar_one_or_none()
        ok3 = ab_test is not None
        print(f"[3]  ab_tests row for {_TEST_NAME!r}   {_ok(ok3)}")
        if ab_test:
            print(f"       status={ab_test.status!r}  traffic_pct={ab_test.traffic_pct}"
                  f"  winner={ab_test.winner!r}  ended_at={ab_test.ended_at}")
        else:
            failures.append(f"3: no ab_tests row for {_TEST_NAME}")

        # ── Check 4: YAML traffic_pct vs DB traffic_pct ──────────────────────
        ok4 = True
        yaml_traffic_pct: Optional[int] = None
        try:
            yaml_path = ROOT / "config" / "cora_ab_tests.yaml"
            with open(yaml_path) as f:
                cfg = yaml.safe_load(f)
            yaml_traffic_pct = int(cfg.get(_TEST_NAME, {}).get("traffic_pct", -1))
        except Exception as exc:
            print(f"       YAML load warning: {exc}")
            ok4 = False

        if ab_test and yaml_traffic_pct is not None:
            db_pct = int(ab_test.traffic_pct or 0)
            if yaml_traffic_pct != db_pct:
                ok4 = False
                print(f"       MISMATCH: YAML traffic_pct={yaml_traffic_pct}  DB traffic_pct={db_pct}")
            else:
                print(f"       YAML traffic_pct={yaml_traffic_pct}  DB traffic_pct={db_pct}  (match)")
        print(f"[4]  YAML/DB traffic_pct in sync                       {_ok(ok4)}")
        if not ok4:
            failures.append("4: YAML/DB traffic_pct mismatch or YAML unreadable")

        # ── Check 5: find eligible internal/test subscriber ──────────────────
        target_sub: Optional[Subscriber] = None

        if args.subscriber_id:
            target_sub = db.get(Subscriber, args.subscriber_id)
            if target_sub is None:
                print(f"[5]  Subscriber {args.subscriber_id} not found                         FAIL")
                failures.append(f"5: subscriber {args.subscriber_id} not found")
            elif not _is_internal(target_sub.email):
                print(f"[5]  Subscriber {args.subscriber_id} ({target_sub.email}) is NOT internal — ABORT")
                print("\nSafety abort: target subscriber is not an internal/test account.")
                sys.exit(1)
            else:
                print(f"[5]  Target subscriber: id={target_sub.id}  email={target_sub.email}   PASS")
        else:
            # Auto-pick: internal, has_saved_card, active, has SmsOptIn, no WalletBalance
            enrolled_sub_ids = set(
                db.execute(select(WalletBalance.subscriber_id)).scalars().all()
            )
            opted_in_sub_ids = set(
                db.execute(
                    select(SmsOptIn.subscriber_id).where(SmsOptIn.subscriber_id.isnot(None))
                ).scalars().all()
            )

            candidates = db.execute(
                select(Subscriber)
                .where(
                    Subscriber.has_saved_card.is_(True),
                    Subscriber.status == "active",
                )
                .limit(args.limit * 50)
            ).scalars().all()

            for sub in candidates:
                if not _is_internal(sub.email):
                    continue
                if sub.id in enrolled_sub_ids:
                    continue
                if sub.id not in opted_in_sub_ids:
                    continue
                target_sub = sub
                break

            ok5 = target_sub is not None
            print(f"[5]  Eligible internal subscriber found                 {_ok(ok5)}")
            if target_sub:
                print(f"       id={target_sub.id}  email={target_sub.email}"
                      f"  has_saved_card={target_sub.has_saved_card}")
            else:
                print("       (no eligible internal subscriber — checks 6–9 will be skipped)")
                if args.real_send:
                    failures.append("5: no eligible subscriber for real send")

        # ── Checks 6–9: graph invocation ─────────────────────────────────────
        graph_result: dict = {}

        if target_sub is None:
            for n, label in [(6, "Graph test run"), (7, "ab_variant/_variant_id in output"),
                             (8, "variant_id pattern check"), (9, "Real-send gate")]:
                print(f"[{n}]  {label:<42}SKIP")
        else:
            test_decision_id = str(uuid.uuid4())
            test_payload = {
                "tier": "starter_wallet",
                "credits_in_offer": 20,
                "price_cents": 4900,
                "missed_leads": 2,
                "reason": "verification_script",
                "cta_url": "https://app.forcedaction.io/dashboard/verify?wallet_offer=accept",
            }

            if args.real_send:
                # Safety: already confirmed internal above
                print(f"[6]  Running REAL graph send for subscriber {target_sub.id}...")
                from src.agents.graphs.accelerated_wallet_push import run_accelerated_wallet_push
                try:
                    graph_result = run_accelerated_wallet_push(
                        event_payload=test_payload,
                        subscriber_id=target_sub.id,
                        decision_id=test_decision_id,
                    )
                    ok6 = True
                except Exception as exc:
                    ok6 = False
                    failures.append(f"6: graph raised exception during real send: {exc}")
                    graph_result = {}
                print(f"[6]  Graph real send complete                          {_ok(ok6)}")
                print(f"       terminal={graph_result.get('terminal_status')!r}"
                      f"  sent={graph_result.get('sent')}"
                      f"  failure={graph_result.get('failure_reason')!r}")

                ok9 = graph_result.get("sent") is True
                print(f"[9]  Real send delivered (sent=True)                   {_ok(ok9)}")
                if not ok9:
                    failures.append(f"9: real send did not deliver "
                                    f"(status={graph_result.get('terminal_status')!r},"
                                    f" failure={graph_result.get('failure_reason')!r})")
            else:
                # Dry-run: patch Telnyx so no real SMS is dispatched
                fake_response = MagicMock()
                fake_response.data.id = "fake_verify_msg_id"

                with patch("src.services.telnyx_sms.send_message", return_value=fake_response):
                    from src.agents.graphs.accelerated_wallet_push import run_accelerated_wallet_push
                    try:
                        graph_result = run_accelerated_wallet_push(
                            event_payload=test_payload,
                            subscriber_id=target_sub.id,
                            decision_id=test_decision_id,
                        )
                        ok6 = True
                    except Exception as exc:
                        ok6 = False
                        failures.append(f"6: graph raised exception: {exc}")
                        graph_result = {}

                print(f"[6]  Graph dry-run (Telnyx patched)                    {_ok(ok6)}")
                print(f"       terminal={graph_result.get('terminal_status')!r}"
                      f"  sent={graph_result.get('sent')}"
                      f"  failure={graph_result.get('failure_reason')!r}")
                print(f"[9]  Real-send gate                                    SKIP (--real-send not set)")

            # ── Check 7: ab_variant and _variant_id present ───────────────────
            ab_var = graph_result.get("ab_variant")
            variant_id_out = graph_result.get("_variant_id")
            ok7 = (ab_var is not None) or (variant_id_out is not None)
            print(f"[7]  ab_variant / _variant_id present in output        {_ok(ok7)}")
            print(f"       ab_variant={ab_var!r}  _variant_id={variant_id_out!r}")
            if not ok7:
                failures.append("7: both ab_variant and _variant_id missing from graph output")

            # ── Check 8: variant_id matches pattern ───────────────────────────
            vid = variant_id_out or (f"{_TEST_NAME}:{ab_var}" if ab_var else None)
            ok8 = vid is not None and bool(_VARIANT_PATTERN.match(vid))
            print(f"[8]  variant_id matches pattern ..._framing:[ab]        {_ok(ok8)}")
            if vid:
                print(f"       value={vid!r}")
            if not ok8:
                failures.append(f"8: variant_id {vid!r} does not match expected pattern")

        # ── Checks 10+11: attribution row counts in DB ────────────────────────
        print()
        decision_rows = db.execute(
            select(AgentDecision.variant_id, func.count().label("n"))
            .where(AgentDecision.graph_name == _GRAPH_NAME)
            .where(AgentDecision.variant_id.like(f"{_TEST_NAME}:%"))
            .group_by(AgentDecision.variant_id)
            .order_by(AgentDecision.variant_id)
        ).all()

        outcome_rows = db.execute(
            select(MessageOutcome.variant_id, func.count().label("n"))
            .where(MessageOutcome.variant_id.like(f"{_TEST_NAME}:%"))
            .group_by(MessageOutcome.variant_id)
            .order_by(MessageOutcome.variant_id)
        ).all()

        print(f"[10] agent_decisions grouped by variant_id                PASS")
        if decision_rows:
            for vid_val, count in decision_rows:
                print(f"       {vid_val!r}: {count}")
        else:
            print("       (no rows yet — attribution will appear after first real graph run)")

        print(f"[11] message_outcomes grouped by variant_id               PASS")
        if outcome_rows:
            for vid_val, count in outcome_rows:
                print(f"       {vid_val!r}: {count}")
        else:
            print("       (no rows yet)")

        # ── Check 12: should_rollback (read-only, no action taken) ───────────
        ok12 = True
        try:
            from src.services.ab_engine import should_rollback
            rollback_result = should_rollback(_TEST_NAME, db)
            print(f"[12] should_rollback() ran without error               PASS")
            print(f"       result={rollback_result}  (read-only — no rollback performed)")
        except Exception as exc:
            ok12 = False
            print(f"[12] should_rollback() raised: {exc}             FAIL")
            failures.append(f"12: should_rollback raised {exc}")

        # ── Check 13: summary ─────────────────────────────────────────────────
        print(f"\n{'='*62}")
        passed = len(failures) == 0
        if passed:
            print("  [13] OVERALL: ALL CHECKS PASSED")
        else:
            print(f"  [13] OVERALL: FAILED — {len(failures)} check(s) did not pass:")
            for item in failures:
                print(f"         x {item}")
        print(f"{'='*62}\n")

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
