"""
Simulate Claude API routing to estimate Haiku/Sonnet mix and cost reduction
BEFORE live traffic.

For each representative prompt it calls the real router
(call_claude_with_usage) with db=session and graph_name="simulation_test",
captures the routed model + tokens + cost, then reports:

  - total calls, Haiku % / Sonnet % / Opus %
  - total actual cost
  - all-Sonnet counterfactual cost (the baseline from the plan)
  - estimated cost-reduction %
  - per-task breakdown of which tasks route to Haiku vs Sonnet
  - effect of any force_tier overrides

It does NOT modify production routing: `_TASK_ROUTING` is untouched, force_tier
only affects the simulated call, and every row written to api_usage_logs is
tagged graph_name="simulation_test" and DELETED at the end.

Usage:
    # Real calls (uses ANTHROPIC_API_KEY from .env; default prompt set):
    python scripts/simulate_claude_routing.py

    # Offline estimate — no API spend, no network (mocked SDK):
    python scripts/simulate_claude_routing.py --dry-run

    # Your own prompt file (JSON list, see PROMPT FORMAT below):
    python scripts/simulate_claude_routing.py --prompts path/to/prompts.json

    # Pull the repo's golden sets as the prompt set:
    python scripts/simulate_claude_routing.py --golden

PROMPT FORMAT (JSON list):
    [
      {
        "task_type": "sms_copy",          # required — drives routing
        "user_prompt": "Write an SMS...", # required
        "system_prompt": "You are...",    # optional
        "graph_name": "fomo",             # optional — for the per-graph view
        "force_tier": "sonnet"            # optional — "haiku"|"sonnet"|"opus"
      },
      ...
    ]
"""

import argparse
import json
import sys
from collections import defaultdict
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import MagicMock, patch

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import delete

from src.core.database import Database
from src.core.models import ApiUsageLog
from src.services.claude_router import _COST_TABLE, call_claude_with_usage

SIM_GRAPH = "simulation_test"
_SONNET = _COST_TABLE["sonnet"]


# ── Prompt sources ──────────────────────────────────────────────────────────

# Small synthetic set covering both tiers, used when no --prompts/--golden given.
# task_types map to tiers via the real _TASK_ROUTING (haiku vs sonnet).
_SYNTHETIC_PROMPTS = [
    {"task_type": "sms_copy", "graph_name": "fomo",
     "user_prompt": "Write a 1-line SMS: a competitor just acted on a lead in the subscriber's ZIP. Create urgency."},
    {"task_type": "sms_copy", "graph_name": "abandonment",
     "user_prompt": "Write a 1-line SMS nudging a subscriber who abandoned checkout to come back."},
    {"task_type": "sms_copy", "graph_name": "wallet_to_lock_close",
     "user_prompt": "Write a 1-line SMS telling a subscriber their wallet balance is enough to lock a ZIP now."},
    {"task_type": "chat_response", "graph_name": "concierge_chat",
     "user_prompt": "Answer briefly: do you cover ZIP 33602?"},
    {"task_type": "classification", "graph_name": "supervisor",
     "user_prompt": "Classify this inbound text intent in one word: 'how much is it'."},
    {"task_type": "email_copy", "graph_name": "stripe_recovery",
     "user_prompt": "Write a short recovery email subject + body for a failed card payment."},
    {"task_type": "retention_copy", "graph_name": "retention",
     "user_prompt": "Write a short weekly retention summary for a Gold-tier subscriber."},
    {"task_type": "lead_analysis", "graph_name": "lead_analysis",
     "user_prompt": "Summarize why a foreclosure + tax-lien stacked lead scores high, in 2 sentences."},
    # Example of an eval-gated upgrade: same task, forced to Sonnet.
    {"task_type": "sms_copy", "graph_name": "wallet_to_lock_close", "force_tier": "sonnet",
     "user_prompt": "Write a high-stakes close SMS for a deal-win moment (forced Sonnet for comparison)."},
]


def load_prompts(prompts_path: str | None, use_golden: bool) -> list[dict]:
    if prompts_path:
        data = json.loads(Path(prompts_path).read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError("--prompts file must contain a JSON list")
        return data

    if use_golden:
        golden_dir = _ROOT / "config" / "golden_sets"
        prompts: list[dict] = []
        for f in sorted(golden_dir.glob("*.json")):
            if f.stem == "eval_results":
                continue
            prompts.extend(json.loads(f.read_text(encoding="utf-8")))
        if not prompts:
            raise SystemExit(f"No golden-set prompts found in {golden_dir}")
        return prompts

    return _SYNTHETIC_PROMPTS


# ── Dry-run mock (no network, no spend) ───────────────────────────────────────

def _mock_client_factory():
    """Return a fake Anthropic client; tokens scale with prompt length."""
    from anthropic.types import TextBlock

    def _create(**kwargs):
        # crude token estimate: ~4 chars/token for input, fixed-ish output
        text_in = ""
        for m in kwargs.get("messages", []):
            text_in += str(m.get("content", ""))
        system = kwargs.get("system")
        if isinstance(system, list):
            text_in += "".join(b.get("text", "") for b in system)
        elif isinstance(system, str):
            text_in += system
        in_tok = max(8, len(text_in) // 4)
        out_tok = min(kwargs.get("max_tokens", 64), 40)

        block = MagicMock()
        block.__class__ = TextBlock
        block.text = "Simulated response."
        usage = MagicMock()
        usage.input_tokens = in_tok
        usage.output_tokens = out_tok
        resp = MagicMock()
        resp.content = [block]
        resp.usage = usage
        return resp

    client = MagicMock()
    client.messages.create.side_effect = _create
    return client


# ── Simulation ────────────────────────────────────────────────────────────────

def simulate(prompts: list[dict], dry_run: bool) -> list[dict]:
    """Run every prompt through the router; return per-call records."""
    records: list[dict] = []

    # In dry-run, replace the client builder so no real API call is made and
    # no LangSmith wrap interferes. Production routing logic is untouched.
    cm = (
        patch("src.services.claude_router._build_client", _mock_client_factory)
        if dry_run else nullcontext()
    )

    db = Database()
    with cm, db.session_scope() as session:
        for i, p in enumerate(prompts):
            task_type = p["task_type"]
            force_tier = p.get("force_tier")
            result = call_claude_with_usage(
                task_type=task_type,
                messages=[{"role": "user", "content": p["user_prompt"]}],
                system=p.get("system_prompt") or None,
                max_tokens=p.get("max_tokens", 120),
                graph_name=SIM_GRAPH,
                db=session,
                force_tier=force_tier,
            )
            blocked = result["text"].startswith("[BLOCKED]")
            records.append({
                "i": i,
                "task_type": task_type,
                "graph_name": p.get("graph_name", "-"),
                "force_tier": force_tier,
                "model": result["model"],
                "input_tokens": result["input_tokens"],
                "output_tokens": result["output_tokens"],
                "cost_usd": result["cost_usd"],
                "all_sonnet_cost": (
                    result["input_tokens"] * _SONNET["input"]
                    + result["output_tokens"] * _SONNET["output"]
                ) / 1_000_000,
                "blocked": blocked,
            })

        # Keep the production ledger clean — drop the simulation marker rows.
        session.flush()
        deleted = session.execute(
            delete(ApiUsageLog).where(ApiUsageLog.graph_name == SIM_GRAPH)
        ).rowcount

    return records, deleted


# ── Reporting ──────────────────────────────────────────────────────────────

def _pct(n, d):
    return (100.0 * n / d) if d else 0.0


def report(records: list[dict], dry_run: bool, deleted: int) -> None:
    active = [r for r in records if not r["blocked"]]
    blocked = [r for r in records if r["blocked"]]
    n = len(active)

    by_model = defaultdict(int)
    for r in active:
        by_model[r["model"]] += 1

    total_cost = sum(r["cost_usd"] for r in active)
    all_sonnet = sum(r["all_sonnet_cost"] for r in active)
    reduction = (1 - total_cost / all_sonnet) * 100 if all_sonnet else 0.0

    print("=" * 70)
    print(f"Claude Routing Simulation  ({'DRY-RUN (mocked, no spend)' if dry_run else 'REAL calls'})")
    print("=" * 70)

    # Per-task breakdown ----------------------------------------------------
    print("\nPer-task routing & cost")
    print(f"  {'task_type':<22}{'force':<8}{'model':<8}{'calls':>6}"
          f"{'actual$':>12}{'allSonnet$':>13}{'save%':>8}")
    print("  " + "-" * 75)
    grp = defaultdict(lambda: {"calls": 0, "cost": 0.0, "sonnet": 0.0})
    for r in active:
        key = (r["task_type"], r["force_tier"] or "-", r["model"])
        g = grp[key]
        g["calls"] += 1
        g["cost"] += r["cost_usd"]
        g["sonnet"] += r["all_sonnet_cost"]
    for (task, force, model), g in sorted(grp.items(), key=lambda kv: -kv[1]["cost"]):
        save = (1 - g["cost"] / g["sonnet"]) * 100 if g["sonnet"] else 0.0
        print(f"  {task:<22}{force:<8}{model:<8}{g['calls']:>6}"
              f"{g['cost']:>12.6f}{g['sonnet']:>13.6f}{save:>7.1f}%")

    # Which tasks -> which tier --------------------------------------------
    tiers = defaultdict(set)
    for r in active:
        tiers[r["model"]].add(r["task_type"])
    print("\nRouting map")
    for tier in ("haiku", "sonnet", "opus"):
        if tiers.get(tier):
            print(f"  {tier:<7} <- {', '.join(sorted(tiers[tier]))}")

    # Totals ----------------------------------------------------------------
    print("\nTotals")
    print(f"  Total calls            {n}")
    print(f"  Haiku                  {by_model.get('haiku',0)} ({_pct(by_model.get('haiku',0), n):.1f}%)")
    print(f"  Sonnet                 {by_model.get('sonnet',0)} ({_pct(by_model.get('sonnet',0), n):.1f}%)")
    if by_model.get("opus"):
        print(f"  Opus                   {by_model['opus']} ({_pct(by_model['opus'], n):.1f}%)")
    if blocked:
        print(f"  Blocked (vendor pause) {len(blocked)} (excluded from cost)")
    print(f"  Total actual cost      ${total_cost:.6f}")
    print(f"  All-Sonnet baseline    ${all_sonnet:.6f}")
    print(f"  Estimated reduction    {reduction:.1f}%  vs naive all-Sonnet routing")

    print(f"\n  (ledger kept clean: {deleted} '{SIM_GRAPH}' row(s) deleted)")
    print("=" * 70)


def main() -> int:
    ap = argparse.ArgumentParser(description="Simulate Claude routing cost/mix before live traffic.")
    ap.add_argument("--prompts", help="Path to a JSON list of prompts.")
    ap.add_argument("--golden", action="store_true", help="Use config/golden_sets/*.json as prompts.")
    ap.add_argument("--dry-run", action="store_true", help="Mock the SDK — no API spend, no network.")
    args = ap.parse_args()

    prompts = load_prompts(args.prompts, args.golden)
    print(f"Loaded {len(prompts)} prompt(s).")
    records, deleted = simulate(prompts, args.dry_run)
    report(records, args.dry_run, deleted)
    return 0


if __name__ == "__main__":
    sys.exit(main())
