"""
THROUGH-v2.2 CLI entry point.

    python -m src.services.cora_throughput --serve   # periodic batch-builder + stale-batch sweep
    python -m src.services.cora_throughput --build    # run one batch-construction pass and exit
    python -m src.services.cora_throughput --status   # print current pending-batch state

In-process periodic thread, not a crontab entry — mirrors src.agents.cora's
own scheduling pattern rather than Relay's cron-sweep convention
(src.services.relay), since crontab.txt stays off-limits pending the still-
unmerged Cora->Lifecycle rename's own crontab changes (see the THROUGH-v2.2
plan's Context section).
"""
from __future__ import annotations

import argparse
import logging
import threading

from src.services.cora_throughput import builder, standing_order_compiler

logger = logging.getLogger(__name__)


def _cmd_serve() -> None:
    stop_event = threading.Event()
    builder_thread = threading.Thread(
        target=builder.run_periodic, args=(stop_event,), daemon=True, name="cora-throughput-builder",
    )
    builder_thread.start()

    compiler_thread = threading.Thread(
        target=standing_order_compiler.run_periodic, args=(stop_event,), daemon=True, name="cora-throughput-standing-order-compiler",
    )
    compiler_thread.start()

    try:
        stop_event.wait()  # runs until SIGTERM/SIGINT kills the process
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        builder_thread.join(timeout=5)
        compiler_thread.join(timeout=5)


def _cmd_build() -> None:
    from src.core.database import get_db_context

    with get_db_context() as db:
        expired = builder.expire_stale_batches(db)
        reposted = builder.repost_unposted_batch(db)
        result = builder.build_batch(db)
    print(f"cora_throughput: expired {expired} stale batch(es)")
    print(f"cora_throughput: re-posted stranded batch: {reposted}")
    print(f"cora_throughput: {result}")


def _cmd_status() -> None:
    from sqlalchemy import text
    from src.core.database import get_db_context

    with get_db_context() as db:
        row = db.execute(
            text(
                "SELECT batch_id, status, created_at, "
                "(SELECT count(*) FROM cora_batch_items WHERE cora_batch_items.batch_id = cora_draft_batches.batch_id) AS item_count "
                "FROM cora_draft_batches ORDER BY created_at DESC LIMIT 1"
            )
        ).mappings().first()
    if row is None:
        print("cora_throughput: no batches have ever been created")
        return
    print(f"cora_throughput: most recent batch {row['batch_id']} status={row['status']} item_count={row['item_count']} created_at={row['created_at']}")


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(prog="python -m src.services.cora_throughput")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--serve", action="store_true", help="Run the periodic batch-builder + stale-batch sweep.")
    group.add_argument("--build", action="store_true", help="Run one batch-construction pass and exit.")
    group.add_argument("--status", action="store_true", help="Print the most recent batch's state.")
    args = parser.parse_args()

    if args.serve:
        _cmd_serve()
    elif args.build:
        _cmd_build()
    elif args.status:
        _cmd_status()


if __name__ == "__main__":
    main()
