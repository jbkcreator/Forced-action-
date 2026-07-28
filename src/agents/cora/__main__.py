"""
Cora CLI entry point.

    python -m src.agents.cora --serve            # worker loop + target_producer sweep thread
    python -m src.agents.cora --produce-targets   # run one target_producer sweep and exit
    python -m src.agents.cora --status            # print queue/DLQ depth + kill switch state
"""
from __future__ import annotations

import argparse
import logging
import threading

logger = logging.getLogger(__name__)


def _cmd_serve() -> None:
    from src.agents.cora.ingestion import reply_mailbox_poller
    from src.agents.cora.ingestion.target_producer import run_periodic
    from src.agents.cora.queue import ensure_group
    from src.agents.cora.worker import Worker

    ensure_group()
    stop_event = threading.Event()
    producer_thread = threading.Thread(target=run_periodic, args=(stop_event,), daemon=True, name="cora-target-producer")
    producer_thread.start()

    # No-ops every poll if CORA_GMAIL_SERVICE_ACCOUNT_KEY_PATH/CORA_REPLY_MAILBOX_ADDRESS
    # aren't set — safe to always start.
    mailbox_thread = threading.Thread(
        target=reply_mailbox_poller.run_periodic, args=(stop_event,), daemon=True, name="cora-reply-mailbox-poller",
    )
    mailbox_thread.start()

    worker = Worker()
    worker.install_signal_handlers()
    try:
        worker.run_forever()
    finally:
        stop_event.set()
        producer_thread.join(timeout=5)
        mailbox_thread.join(timeout=5)


def _cmd_produce_targets() -> None:
    from src.agents.cora.ingestion.target_producer import produce_auction_fast_follow_targets, produce_targets
    from src.agents.cora.ingestion.win_back_producer import produce_win_back_targets
    from src.core.database import get_db_context

    with get_db_context() as db:
        produced_founder_tier_blitz = produce_targets(db)
        produced_auction_fast_follow = produce_auction_fast_follow_targets(db)
        produced_win_back = produce_win_back_targets(db)
    print(f"target_producer: founder_tier_blitz published {len(produced_founder_tier_blitz)} target.ready event(s): {produced_founder_tier_blitz}")
    print(f"target_producer: auction_fast_follow published {len(produced_auction_fast_follow)} target.ready event(s): {produced_auction_fast_follow}")
    print(f"target_producer: win_back published {len(produced_win_back)} target.ready event(s): {produced_win_back}")


def _cmd_poll_mailbox() -> None:
    from src.agents.cora.ingestion.reply_mailbox_poller import poll_once

    published = poll_once()
    print(f"reply_mailbox_poller: published {published} reply.received event(s)")


def _cmd_status() -> None:
    from src.agents.cora import queue
    from src.agents.cora.kill_switch import cora_halted

    print(f"kill_switch cora_halted: {cora_halted()}")
    print(f"queue_depth (XLEN cora:events): {queue.queue_depth()}")
    print(f"pending_count (unacked backlog): {queue.pending_count()}")
    print(f"dlq_depth: {queue.dlq_depth()}")


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(prog="python -m src.agents.cora")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--serve", action="store_true", help="Run the worker loop + target producer sweep.")
    group.add_argument("--produce-targets", action="store_true", help="Run one target_producer sweep and exit.")
    group.add_argument("--poll-mailbox", action="store_true", help="Run one reply-mailbox poll and exit.")
    group.add_argument("--status", action="store_true", help="Print queue/DLQ depth + kill switch state.")
    args = parser.parse_args()

    if args.serve:
        _cmd_serve()
    elif args.produce_targets:
        _cmd_produce_targets()
    elif args.poll_mailbox:
        _cmd_poll_mailbox()
    elif args.status:
        _cmd_status()


if __name__ == "__main__":
    main()
