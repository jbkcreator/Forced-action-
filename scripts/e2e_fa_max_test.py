"""
FA Max Real E2E Test — WP-1, WP-2, WP-5B
==========================================
Run: PYTHONPATH=. python scripts/e2e_fa_max_test.py

Tests actual DB writes, real Slack messages, real profile computation.
Each section prints PASS/FAIL with evidence (IDs, Slack timestamps).
"""
from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text

from src.core.database import get_db_context

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
    print(f"\n{BOLD}{CYAN}{'='*60}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'='*60}{RESET}")


# ---------------------------------------------------------------------------
# WP-1 — Durable State Engine
# ---------------------------------------------------------------------------

def test_wp1_person_state_transition() -> Optional[str]:
    """Create person, transition identified→engaged, verify event log row."""
    _section("WP-1 · Person State Transition + Event Log")
    from src.services.state_engine import (
        transition, get_person_state, get_person_history, make_idempotency_key
    )

    person_id = str(uuid.uuid4())
    ikey = make_idempotency_key("e2e", person_id, "identified", "engaged")

    try:
        from src.services.state_engine import ensure_entity_registry

        with get_db_context() as session:
            # Register in entity registry first (required before transition)
            entity_uuid = ensure_entity_registry(
                session=session, entity_type="person", native_id=person_id
            )
            # Insert person in 'identified' state
            session.execute(
                text("""
                    INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
                    VALUES (CAST(:pid AS uuid), 'identified', 'e2e_test')
                    ON CONFLICT (person_id) DO NOTHING
                """),
                {"pid": person_id},
            )
            _pass("WP-1.1 person + entity_registry created", f"person_id={person_id} entity_uuid={entity_uuid}")

        # Read current state
        with get_db_context() as session:
            state = get_person_state(session=session, person_id=person_id)
        if state is None:
            _fail("WP-1.2 get_person_state", "returned None")
            return None
        _pass("WP-1.2 get_person_state", f"state={state['lifecycle_state']} version={state['state_version']}")

        current_version = state["state_version"]

        # Transition identified → enriched (first valid hop in the state machine)
        with get_db_context() as session:
            result = transition(
                session=session,
                entity_type="person",
                entity_uuid=entity_uuid,
                from_state="identified",
                to_state="enriched",
                actor="system:e2e_test",
                source_component="scripts.e2e_fa_max_test",
                idempotency_key=ikey,
                state_version=current_version,
                context={"reason": "e2e real test run"},
            )
            session.commit()

        if result.outcome.value not in ("succeeded", "already_advanced", "idempotent_skip"):
            _fail("WP-1.3 transition identified→enriched", f"outcome={result.outcome.value}")
            return None
        _pass("WP-1.3 transition identified→enriched", f"outcome={result.outcome.value} event_id={result.event_id}")

        # Verify state updated
        with get_db_context() as session:
            new_state = get_person_state(session=session, person_id=person_id)
        if new_state["lifecycle_state"] != "enriched":
            _fail("WP-1.4 post-transition state", f"got {new_state['lifecycle_state']}")
            return None
        _pass("WP-1.4 post-transition state", f"state=enriched version={new_state['state_version']}")

        # Verify event log
        with get_db_context() as session:
            history = get_person_history(session=session, person_id=person_id)
        events = history["events"]
        if not events:
            _fail("WP-1.5 event log", "no events found")
            return None
        ev = events[0]
        _pass(
            "WP-1.5 event log entry",
            f"from={ev['from_state']} to={ev['to_state']} actor={ev['actor']} ts={ev['occurred_at']}",
        )

        return person_id

    except Exception as exc:
        _fail("WP-1 exception", str(exc))
        return None


def test_wp1_interaction_write(person_id: Optional[str]) -> None:
    """Write an interaction log entry and verify immutability guard."""
    _section("WP-1 · Interaction Write + Immutability Guard")
    if not person_id:
        _fail("WP-1.6 interaction write", "skipped — no person_id from prior test")
        return

    from src.services.state_engine import write_interaction

    try:
        with get_db_context() as session:
            interaction_id = write_interaction(
                session=session,
                person_id=person_id,
                channel="email",
                direction="outbound",
                body_redacted="e2e test interaction",
                actor="system:e2e_test",
            )
            session.commit()
        _pass("WP-1.6 write_interaction", f"interaction_id={interaction_id}")

        # Verify immutability — UPDATE should be blocked by trigger
        blocked = False
        try:
            with get_db_context() as session:
                session.execute(
                    text("UPDATE fa_max_interactions SET content_summary='tampered' WHERE interaction_id=CAST(:iid AS uuid)"),
                    {"iid": interaction_id},
                )
                session.commit()
        except Exception:
            blocked = True

        if blocked:
            _pass("WP-1.7 immutability trigger blocks UPDATE", "exception raised on UPDATE attempt")
        else:
            _fail("WP-1.7 immutability trigger", "UPDATE succeeded — trigger not firing")

    except Exception as exc:
        _fail("WP-1.6 write_interaction exception", str(exc))


def test_wp1_work_queue() -> None:
    """Enqueue, claim, complete a work item. Verify no double-claim."""
    _section("WP-1 · Work Queue (enqueue → claim → complete)")
    from src.services.state_engine import enqueue_work_item, claim_next_work_item, complete_work_item

    run_suffix = uuid.uuid4().hex[:8]
    ikey = f"e2e-wq-{uuid.uuid4().hex}"
    queue_name = f"e2e_test_queue_{run_suffix}"
    worker = f"e2e-worker-{run_suffix}"
    try:
        with get_db_context() as session:
            work_item_id = enqueue_work_item(
                session=session,
                queue_name=queue_name,
                idempotency_key=ikey,
                payload={"test": True, "ts": datetime.now(timezone.utc).isoformat()},
            )
            session.commit()
        if not work_item_id:
            _fail("WP-1.8 enqueue_work_item", "returned None (idempotent skip on fresh key?)")
            return
        _pass("WP-1.8 enqueue_work_item", f"work_item_id={work_item_id}")

        # Claim
        with get_db_context() as session:
            claimed = claim_next_work_item(session=session, queue_name=queue_name, worker_id=worker)
            session.commit()
        if not claimed:
            _fail("WP-1.9 claim_next_work_item", "returned None")
            return
        _pass("WP-1.9 claim_next_work_item", f"work_item_id={claimed['work_item_id']} status={claimed['status']}")

        # Second claim should return None (queue is now empty — only 1 item)
        with get_db_context() as session:
            second = claim_next_work_item(session=session, queue_name=queue_name, worker_id=worker + "-2")
            session.commit()
        if second is None:
            _pass("WP-1.10 queue empty after single claim", "second claim returned None (only 1 item enqueued)")
        else:
            _fail("WP-1.10 queue empty check", f"unexpected item returned: {second['work_item_id']}")

        # Complete
        with get_db_context() as session:
            complete_work_item(
                session=session,
                work_item_id=claimed["work_item_id"],
                worker_id=worker,
                status="done",
            )
            session.commit()
        _pass("WP-1.11 complete_work_item", f"work_item_id={claimed['work_item_id']} → done")

    except Exception as exc:
        _fail("WP-1 work queue exception", str(exc))


# ---------------------------------------------------------------------------
# WP-2 — Slack Queue + Send Governance
# ---------------------------------------------------------------------------

def test_wp2_queue_and_slack_post() -> Optional[list[int]]:
    """Enqueue FA Max items per lane, post real Slack approval cards."""
    _section("WP-2 · Queue + Real Slack Card Per Lane")
    from src.services.relay.queue import enqueue
    from src.services.relay.slack_post import post_for_approval

    run_id = uuid.uuid4().hex[:8]
    item_ids = []

    for lane in ("MONEY", "EXCEPTIONS", "RELATIONSHIPS"):
        person_id = str(uuid.uuid4())
        try:
            with get_db_context() as session:
                session.execute(
                    text("""
                        INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
                        VALUES (CAST(:pid AS uuid), 'identified', 'e2e_test')
                    """),
                    {"pid": person_id},
                )
                session.execute(
                    text("""
                        INSERT INTO fa_max_person_consent (person_id, channel, consented, source)
                        VALUES (CAST(:pid AS uuid), 'email', true, 'e2e_test')
                    """),
                    {"pid": person_id},
                )
                session.execute(
                    text("""
                        INSERT INTO fa_max_backflip_campaign_feed (id, last_success_at)
                        VALUES (1, now()) ON CONFLICT (id) DO UPDATE SET last_success_at = now()
                    """),
                )

            item = enqueue(
                idempotency_key=f"e2e-wp2-{run_id}-{lane.lower()}",
                channel="email",
                recipient=f"e2e-{run_id}-{lane.lower()}@test.example",
                payload={
                    "subject": f"[E2E TEST] FA Max {lane} lane — {run_id}",
                    "body": f"Real end-to-end test. Lane: {lane}. Run: {run_id}. This is a TEST item.",
                },
                venture_key="fa_max_lending",
                lane=lane,
                agent_name="e2e_test",
                autonomy_tier_at_send="A",
                person_id=person_id,
                skip_contract_validation=True,
            )
            item_ids.append(item.id)
            _pass(f"WP-2 enqueue {lane} lane", f"item_id={item.id} idempotency_key=e2e-wp2-{run_id}-{lane.lower()}")

            # Post the real Slack card
            post_for_approval(item)

            # Verify slack_message_ts was saved
            with get_db_context() as session:
                row = session.execute(
                    text("SELECT slack_message_ts FROM relay_approval_queue WHERE id=:id"),
                    {"id": item.id},
                ).fetchone()

            if row and row.slack_message_ts:
                _pass(
                    f"WP-2 Slack card posted ({lane})",
                    f"item_id={item.id} slack_message_ts={row.slack_message_ts}",
                )
            else:
                _fail(f"WP-2 Slack card ({lane})", "slack_message_ts is NULL — check FA_MAX_SLACK_BOT_TOKEN / channel config")

        except Exception as exc:
            _fail(f"WP-2 {lane} lane exception", str(exc))

    return item_ids if item_ids else None


def test_wp2_governance_blocks() -> None:
    """Verify governance blocks: prohibited payload, missing consent, missing identity."""
    _section("WP-2 · Governance Fail-Closed Checks")
    from src.services.fa_max_send_governance import (
        validate_safe_payload, require_consent, GovernanceBlocked
    )

    # 1. Prohibited financial field
    try:
        validate_safe_payload({"subject": "hello", "credit_score": 720})
        _fail("WP-2.G1 financial field blocked", "did NOT raise GovernanceBlocked")
    except GovernanceBlocked as e:
        _pass("WP-2.G1 financial field blocked", f"GovernanceBlocked: {e.reason}")

    # 2. Prohibited text in content
    try:
        validate_safe_payload({"body": "Your interest rate is 5.5%"})
        _fail("WP-2.G2 financial text blocked", "did NOT raise GovernanceBlocked")
    except GovernanceBlocked as e:
        _pass("WP-2.G2 financial text blocked", f"GovernanceBlocked: {e.reason}")

    # 3. Consent absent
    fake_person = str(uuid.uuid4())
    with get_db_context() as session:
        result = require_consent(session=session, person_id=fake_person, channel="email")
    if not result.allowed and result.reason == "consent_absent":
        _pass("WP-2.G3 consent_absent blocks send", f"reason={result.reason}")
    else:
        _fail("WP-2.G3 consent_absent", f"allowed={result.allowed} reason={result.reason}")

    # 4. Consent withdrawn
    person_id = str(uuid.uuid4())
    with get_db_context() as session:
        session.execute(
            text("INSERT INTO fa_max_persons (person_id, lifecycle_state, source) VALUES (CAST(:pid AS uuid), 'identified', 'e2e_test')"),
            {"pid": person_id},
        )
        session.execute(
            text("INSERT INTO fa_max_person_consent (person_id, channel, consented, source) VALUES (CAST(:pid AS uuid), 'email', false, 'e2e_test')"),
            {"pid": person_id},
        )
    with get_db_context() as session:
        result = require_consent(session=session, person_id=person_id, channel="email")
    if not result.allowed and result.reason == "consent_withdrawn":
        _pass("WP-2.G4 consent_withdrawn blocks send", f"reason={result.reason}")
    else:
        _fail("WP-2.G4 consent_withdrawn", f"allowed={result.allowed} reason={result.reason}")


# ---------------------------------------------------------------------------
# WP-5B — Borrower Profile
# ---------------------------------------------------------------------------

def test_wp5b_profile_compute() -> Optional[str]:
    """Create entity-linked person, compute profile, verify DB row."""
    _section("WP-5B · Profile Compute + Read")
    from src.services.borrower_profile_service import compute_person_profile, get_person_profile

    person_id = str(uuid.uuid4())

    try:
        with get_db_context() as session:
            # Create person
            session.execute(
                text("""
                    INSERT INTO fa_max_persons (person_id, lifecycle_state, source)
                    VALUES (CAST(:pid AS uuid), 'identified', 'e2e_test')
                """),
                {"pid": person_id},
            )
            _pass("WP-5B.1 person row created", f"person_id={person_id}")

        # Compute profile (no entity link — unknown confidence)
        with get_db_context() as session:
            profile = compute_person_profile(session=session, person_id=person_id)
            session.commit()

        if profile is None:
            _fail("WP-5B.2 compute_person_profile", "returned None")
            return None

        confidence = profile.get("confidence_tier", "?")
        _pass(
            "WP-5B.2 compute_person_profile",
            f"confidence_tier={confidence} deed_count={profile.get('deed_count')} next_need={profile.get('predicted_next_need')}",
        )

        # Read back from DB
        with get_db_context() as session:
            stored = get_person_profile(session=session, person_id=person_id)

        if stored is None:
            _fail("WP-5B.3 get_person_profile", "row not found in fa_max_person_profiles")
            return None
        _pass(
            "WP-5B.3 get_person_profile (from DB)",
            f"computed_at={stored.get('computed_at')} confidence_tier={stored.get('confidence_tier')}",
        )

        # Idempotency — run again, should upsert not duplicate
        with get_db_context() as session:
            count_before = session.execute(
                text("SELECT COUNT(*) FROM fa_max_person_profiles WHERE person_id=CAST(:pid AS uuid)"),
                {"pid": person_id},
            ).scalar()

        with get_db_context() as session:
            compute_person_profile(session=session, person_id=person_id)
            session.commit()

        with get_db_context() as session:
            count_after = session.execute(
                text("SELECT COUNT(*) FROM fa_max_person_profiles WHERE person_id=CAST(:pid AS uuid)"),
                {"pid": person_id},
            ).scalar()

        if count_before == count_after == 1:
            _pass("WP-5B.4 idempotency (UPSERT, not INSERT)", f"rows={count_after} before={count_before} after={count_after}")
        else:
            _fail("WP-5B.4 idempotency", f"before={count_before} after={count_after}")

        return person_id

    except Exception as exc:
        _fail("WP-5B exception", str(exc))
        return None


def test_wp5b_work_queue_trigger(person_id: Optional[str]) -> None:
    """Verify profile_recompute work item is enqueued when write_interaction is called."""
    _section("WP-5B · Auto Work Queue Trigger via write_interaction")
    if not person_id:
        _fail("WP-5B.5 work queue trigger", "skipped — no person_id")
        return

    from src.services.state_engine import write_interaction

    try:
        with get_db_context() as session:
            write_interaction(
                session=session,
                person_id=person_id,
                channel="voice",
                direction="inbound",
                body_redacted="e2e wp5b trigger test call",
                actor="system:e2e_test",
            )
            session.commit()

        # Check work queue for profile_recompute (enqueued by write_interaction via schedule_profile_recompute)
        with get_db_context() as session:
            row = session.execute(
                text("""
                    SELECT work_item_id, status FROM fa_max_work_queue
                    WHERE queue_name = 'profile_recompute'
                      AND person_id = CAST(:pid AS uuid)
                    ORDER BY created_at DESC LIMIT 1
                """),
                {"pid": person_id},
            ).fetchone()

        if row:
            _pass("WP-5B.5 profile_recompute enqueued on write_interaction", f"work_item_id={row.work_item_id} status={row.status}")
        else:
            _fail("WP-5B.5 profile_recompute not enqueued", "no matching row in fa_max_work_queue after write_interaction")

    except Exception as exc:
        _fail("WP-5B.5 work queue trigger exception", str(exc))


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary(item_ids: Optional[list[int]]) -> None:
    _section("SUMMARY")
    total = len(_results)
    passed = sum(1 for _, ok, _ in _results if ok)
    failed = total - passed

    for label, ok, evidence in _results:
        icon = f"{GREEN}✓{RESET}" if ok else f"{RED}✗{RESET}"
        print(f"  {icon}  {label}")

    print(f"\n{BOLD}Result: {GREEN}{passed} passed{RESET} / {RED}{failed} failed{RESET} / {total} total{RESET}")

    if item_ids:
        print(f"\n{BOLD}WP-2 Slack cards posted — check Slack for approval cards in MONEY / EXCEPTIONS / RELATIONSHIPS channels.{RESET}")
        print(f"Item IDs: {item_ids}")
        print(f"\nTo verify after clicking Approve/Reject in Slack:")
        print(f"  PYTHONPATH=. python scripts/smoke_fa_max_socket_mode.py verify {' '.join(map(str, item_ids))}")

    if failed:
        sys.exit(1)


def main() -> None:
    print(f"\n{BOLD}FA Max E2E Test — WP-1 / WP-2 / WP-5B{RESET}")
    print(f"Started: {datetime.now(timezone.utc).isoformat()}\n")

    # WP-1
    person_id = test_wp1_person_state_transition()
    test_wp1_interaction_write(person_id)
    test_wp1_work_queue()

    # WP-2
    item_ids = test_wp2_queue_and_slack_post()
    test_wp2_governance_blocks()

    # WP-5B
    wp5b_person = test_wp5b_profile_compute()
    test_wp5b_work_queue_trigger(wp5b_person)

    print_summary(item_ids)


if __name__ == "__main__":
    main()
