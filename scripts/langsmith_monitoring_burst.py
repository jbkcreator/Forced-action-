"""
LangSmith monitoring burst — fire a paced, representative stream of REAL Claude
calls so the LangSmith Monitoring tab has enough data points to render
(volume / latency / cost / token charts).

Every call is traced (graph_name="monitoring_burst") and also written to
api_usage_logs, which is cleaned up at the end so the production cost ledger
stays pristine. Traces persist in LangSmith — that's the point.

Usage:
    python scripts/langsmith_monitoring_burst.py                 # 24 calls, ~2.5s apart (~1 min)
    python scripts/langsmith_monitoring_burst.py --count 40 --interval 2

Cost: tiny Haiku/Sonnet calls (~$0.001 total for the default 24).
"""

import argparse
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import delete

from src.core.database import Database
from src.core.models import ApiUsageLog
from src.services.claude_router import _COST_TABLE, call_claude_with_usage
from src.agents.observability.langsmith import (
    configure_tracing,
    get_langsmith_client,
    get_project_name,
)

BURST_GRAPH = "monitoring_burst"
_SONNET = _COST_TABLE["sonnet"]

# Representative mix ~ 75% Haiku / 25% Sonnet (matches the routing target).
# (task_type, graph_name, prompt-template) — {i} varies each call.
_MIX = [
    ("sms_copy", "fomo", "Write a 1-line urgency SMS about lead #{i} in the subscriber's ZIP."),
    ("sms_copy", "abandonment", "Write a 1-line SMS nudging subscriber #{i} back to checkout."),
    ("chat_response", "concierge_chat", "Answer briefly: do you cover ZIP 336{i:02d}?"),
    ("sms_copy", "wallet_to_lock_close", "Write a 1-line SMS: subscriber #{i} can lock a ZIP now."),
    ("classification", "supervisor", "One-word intent for inbound text #{i}: 'how much'."),
    ("chat_response", "concierge_chat", "Briefly: what verticals do you serve? (q#{i})"),
    ("email_copy", "stripe_recovery", "Short failed-payment recovery email for subscriber #{i}."),
    ("sms_copy", "nws_urgency", "Write a 1-line storm-urgency SMS for area #{i}."),
]  # 6/8 haiku, 2/8 sonnet -> 75% haiku


def main() -> int:
    ap = argparse.ArgumentParser(description="Fire a paced burst of traced Claude calls for LangSmith monitoring.")
    ap.add_argument("--count", type=int, default=24, help="Number of calls (default 24).")
    ap.add_argument("--interval", type=float, default=2.5, help="Seconds between calls (default 2.5).")
    args = ap.parse_args()

    enabled = configure_tracing()
    project = get_project_name()
    print("=" * 64)
    print(f"LangSmith Monitoring Burst — {args.count} calls @ {args.interval}s")
    print(f"Tracing: {enabled}  |  Project: {project}")
    print("=" * 64)
    if not enabled:
        print("FAIL: tracing not enabled (check LANGSMITH_API_KEY + LANGSMITH_TRACING).")
        return 1

    started = datetime.now(timezone.utc)
    by_model = defaultdict(int)
    total_cost = 0.0
    all_sonnet = 0.0
    errors = 0

    db = Database()
    with db.session_scope() as session:
        for i in range(args.count):
            task, graph, tmpl = _MIX[i % len(_MIX)]
            try:
                r = call_claude_with_usage(
                    task_type=task,
                    messages=[{"role": "user", "content": tmpl.format(i=i)}],
                    max_tokens=64,
                    graph_name=BURST_GRAPH,
                    db=session,
                )
                by_model[r["model"]] += 1
                total_cost += r["cost_usd"]
                all_sonnet += (r["input_tokens"] * _SONNET["input"]
                               + r["output_tokens"] * _SONNET["output"]) / 1_000_000
                print(f"  [{i+1:>2}/{args.count}] {task:<14} -> {r['model']:<6} "
                      f"${r['cost_usd']:.6f}")
            except Exception as e:
                errors += 1
                print(f"  [{i+1:>2}/{args.count}] {task:<14} -> ERROR {str(e)[:60]}")
            if i < args.count - 1:
                time.sleep(args.interval)

        session.flush()
        deleted = session.execute(
            delete(ApiUsageLog).where(ApiUsageLog.graph_name == BURST_GRAPH)
        ).rowcount

    n = sum(by_model.values())
    haiku_pct = 100.0 * by_model.get("haiku", 0) / n if n else 0.0
    sonnet_pct = 100.0 * by_model.get("sonnet", 0) / n if n else 0.0
    reduction = (1 - total_cost / all_sonnet) * 100 if all_sonnet else 0.0

    # Confirm the traces landed
    print("\nConfirming traces in LangSmith (async ingest)...")
    client = get_langsmith_client()
    time.sleep(5)
    traced = 0
    try:
        runs = list(client.list_runs(project_name=project, start_time=started, limit=args.count + 10))
        traced = len(runs)
    except Exception as e:
        print(f"  trace lookup error: {str(e)[:80]}")

    print("\n" + "=" * 64)
    print("DoD EVIDENCE")
    print("=" * 64)
    print(f"  Calls succeeded        {n}/{args.count}  (errors: {errors})")
    print(f"  Haiku                  {by_model.get('haiku',0)} ({haiku_pct:.1f}%)")
    print(f"  Sonnet                 {by_model.get('sonnet',0)} ({sonnet_pct:.1f}%)")
    print(f"  Actual cost            ${total_cost:.6f}")
    print(f"  All-Sonnet baseline    ${all_sonnet:.6f}")
    print(f"  Cost reduction         {reduction:.1f}%  vs naive all-Sonnet")
    print(f"  Traces in '{project}'   {traced}  (this window)")
    print(f"  Cost ledger            clean ({deleted} '{BURST_GRAPH}' rows removed)")
    print("=" * 64)
    print("Open LangSmith -> project '%s' -> Monitoring tab, set range to 'last hour'." % project)
    return 0


if __name__ == "__main__":
    sys.exit(main())
