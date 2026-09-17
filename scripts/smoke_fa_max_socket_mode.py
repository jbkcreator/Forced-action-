"""Create and verify two non-sending FA Max Socket Mode approval cards.

This is a local acceptance harness. It queues cards only; it never runs a
Relay sweep, so neither card can send email or SMS. Use ``create`` to produce
one MONEY card for approval and one RELATIONSHIPS card for rejection, then use
``verify`` with the printed IDs after clicking their Slack buttons.
"""
from __future__ import annotations

import argparse
import uuid

from sqlalchemy import text

from src.core.database import get_db_context
from src.services.relay.queue import enqueue


def create() -> int:
    run_id = uuid.uuid4().hex
    item_ids: list[int] = []
    for lane, expected in (("MONEY", "approved"), ("RELATIONSHIPS", "rejected")):
        person_id = str(uuid.uuid4())
        with get_db_context() as session:
            session.execute(
                text("INSERT INTO fa_max_persons (person_id, lifecycle_state, source) "
                     "VALUES (CAST(:person_id AS uuid), 'identified', 'socket_smoke')"),
                {"person_id": person_id},
            )
            session.execute(
                text("INSERT INTO fa_max_person_consent "
                     "(person_id, channel, consented, source) "
                     "VALUES (CAST(:person_id AS uuid), 'email', true, 'socket_smoke')"),
                {"person_id": person_id},
            )
            session.execute(
                text("INSERT INTO fa_max_backflip_campaign_feed (id, last_success_at) "
                     "VALUES (1, now()) ON CONFLICT (id) DO UPDATE "
                     "SET last_success_at = now()")
            )

        item = enqueue(
            idempotency_key=f"socket-smoke-{run_id}-{lane.lower()}",
            channel="email",
            recipient=f"socket-smoke-{run_id}-{lane.lower()}@example.test",
            payload={
                "subject": f"Socket Mode smoke test: {lane}",
                "body": "Approval test only. No send sweep will run.",
            },
            venture_key="fa_max_lending",
            lane=lane,
            agent_name="socket_smoke",
            autonomy_tier_at_send="A",
            person_id=person_id,
            skip_contract_validation=True,
        )
        item_ids.append(item.id)
        action_label = "APPROVE" if expected == "approved" else "REJECT"
        print(f"{lane}: item_id={item.id}; click {action_label} in Slack")

    print("Do not run a Relay sweep. After both clicks, run:")
    print(f"  python scripts/smoke_fa_max_socket_mode.py verify {' '.join(map(str, item_ids))}")
    return 0


def verify(item_ids: list[int]) -> int:
    with get_db_context() as session:
        rows = session.execute(
            text("SELECT id, status, decided_by, decision_interaction_id "
                 "FROM relay_approval_queue WHERE id = ANY(:item_ids) ORDER BY id"),
            {"item_ids": item_ids},
        ).mappings().all()
    for row in rows:
        print(
            f"item_id={row['id']} status={row['status']} "
            f"decided_by={row['decided_by'] or '-'} "
            f"interaction={row['decision_interaction_id'] or '-'}"
        )
    statuses = {row["status"] for row in rows}
    return 0 if len(rows) == 2 and statuses == {"approved", "rejected"} else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("create", "verify"))
    parser.add_argument("item_ids", type=int, nargs="*")
    args = parser.parse_args()
    if args.mode == "create":
        return create()
    if len(args.item_ids) != 2:
        parser.error("verify requires the two item IDs printed by create")
    return verify(args.item_ids)


if __name__ == "__main__":
    raise SystemExit(main())
