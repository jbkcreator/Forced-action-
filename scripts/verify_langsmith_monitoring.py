"""
End-to-end LangSmith monitoring verification — REAL calls, no mocks.

Unlike scripts/smoke_langsmith_test.py (which mocks Anthropic and therefore
cannot emit a real trace), this script:
  1. Reports LangSmith config (project / endpoint / tracing flag).
  2. Probes LangSmith READ auth (list_runs).
  3. Makes ONE real Claude Haiku call through the router (tracing wraps it).
  4. Confirms the cost row landed in api_usage_logs.
  5. Flushes the LangSmith client and looks for the emitted run.

Run (config comes from .env via pydantic settings):
    python scripts/verify_langsmith_monitoring.py

Cost: one tiny Haiku call (~$0.0001).
"""

import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from datetime import datetime, timezone, timedelta

from sqlalchemy import select

from src.core.database import Database
from src.core.models import ApiUsageLog
from src.services.claude_router import call_claude_with_usage
from src.agents.observability.langsmith import (
    configure_tracing,
    get_endpoint,
    get_langsmith_client,
    get_project_name,
    is_tracing_enabled,
)

_MARKER = "verify_langsmith_monitoring"


def _line(label, value):
    print(f"  {label:<26} {value}")


def main() -> int:
    print("=" * 64)
    print("LangSmith Monitoring — End-to-End Verification (real calls)")
    print("=" * 64)

    # Bridge .env → os.environ exactly as the agents runtime does at startup.
    configure_tracing()

    print("\n[config]")
    _line("Project", get_project_name())
    _line("Endpoint", get_endpoint())
    _line("Tracing enabled", is_tracing_enabled())

    client = get_langsmith_client()
    if client is None:
        print("\nFAIL: LangSmith client not configured "
              "(need LANGSMITH_API_KEY + LANGSMITH_TRACING=true).")
        return 1

    # 1. READ auth probe -----------------------------------------------------
    print("\n[1] LangSmith READ auth probe (list_runs)...")
    read_ok = False
    try:
        list(client.list_runs(project_name=get_project_name(), limit=1))
        read_ok = True
        _line("READ", "OK (200)")
    except Exception as e:
        msg = str(e)
        code = "403" if "403" in msg or "Forbidden" in msg else (
            "401" if "401" in msg else "ERROR")
        _line("READ", f"BLOCKED ({code}) — {msg[:90]}")

    # 2. Real Claude call (tracing wraps the SDK call) -----------------------
    print("\n[2] Making ONE real Claude Haiku call through the router...")
    db = Database()
    cost_logged = False
    with db.session_scope() as session:
        result = call_claude_with_usage(
            task_type="sms_copy",  # → haiku
            messages=[{"role": "user", "content": "Say 'pong' and nothing else."}],
            max_tokens=16,
            graph_name=_MARKER,
            db=session,
        )
        _line("model", result["model"])
        _line("text", repr(result["text"][:40]))
        _line("tokens", f"{result['input_tokens']}/{result['output_tokens']}")
        _line("cost_usd", f"${result['cost_usd']:.6f}")

        # 3. Confirm ledger row
        row = session.execute(
            select(ApiUsageLog)
            .where(ApiUsageLog.graph_name == _MARKER)
            .order_by(ApiUsageLog.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        cost_logged = row is not None
        _line("api_usage_logs row", "WRITTEN" if cost_logged else "MISSING")

    # 4. Flush + look for the emitted trace ----------------------------------
    print("\n[3] Flushing LangSmith client and searching for the run...")
    trace_found = False
    try:
        client.flush()
        time.sleep(3)  # ingestion is async
        since = datetime.now(timezone.utc) - timedelta(minutes=5)
        runs = list(client.list_runs(project_name=get_project_name(), start_time=since, limit=20))
        trace_found = len(runs) > 0
        _line("recent runs", len(runs))
    except Exception as e:
        _line("trace lookup", f"BLOCKED — {str(e)[:90]}")

    # Verdict ----------------------------------------------------------------
    print("\n" + "=" * 64)
    print("VERDICT")
    print("=" * 64)
    _line("Claude call",        "OK" if result.get("text") else "FAIL")
    _line("Cost ledger (DB)",   "OK" if cost_logged else "FAIL")
    _line("LangSmith READ",     "OK" if read_ok else "BLOCKED")
    _line("Trace visible",      "OK" if trace_found else "NOT VISIBLE")
    print("-" * 64)
    if cost_logged and read_ok and trace_found:
        print("SUCCESS: monitoring fully working — traces landing in LangSmith.")
        return 0
    if cost_logged and not read_ok:
        print("PARTIAL: Claude + DB ledger work. LangSmith key is BLOCKED "
              "(403/401) — rotate the key with run:create + run:list scope.")
        return 2
    print("ISSUE: see rows above.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
