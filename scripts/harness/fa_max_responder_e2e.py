"""WP-T2-12 responder — REAL-DB + REAL-LLM acceptance harness.

Unlike the unit tests (which mock the LLM and the DB), this drives the exact
production classify pipeline against a live Haiku call and runs every catalog
lookup against the real database. It exists to catch the two failure classes
unit tests structurally cannot:

  1. LLM classification quality  — does a natural phrasing map to the right
     bucket/lookup? (unit tests stub the classifier)
  2. Real schema / query validity — does the bucket CHECK accept every Bucket
     value, and does each catalog SQL actually run against the live schema?
     (unit tests mock the DB session)

It is READ-ONLY against business data. The single audit-write check runs inside
a transaction that is always rolled back, so nothing is persisted.

Usage (from repo root):
    PYTHONPATH=. python scripts/harness/fa_max_responder_e2e.py            # uses .env
    PYTHONPATH=. python scripts/harness/fa_max_responder_e2e.py test.env   # explicit env file

Exit code 0 = all assertions passed; non-zero = at least one failure.
Costs a handful of Haiku calls (cents).
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Optional


def _load_env(path: str) -> None:
    from dotenv import dotenv_values
    if not os.path.exists(path):
        print(f"env file not found: {path} (continuing with process env)")
        return
    for k, v in dotenv_values(path).items():
        os.environ[k] = v or ""


@dataclass
class Case:
    text: str
    expect_bucket: str
    expect_lookup: Optional[str] = None
    history: Optional[list] = None
    note: str = ""


# Natural-language matrix — the phrasings a human operator would actually type.
CASES: list[Case] = [
    # --- simple_lookup: must hit the right catalog query -------------------
    Case("how many green deals do we have?", "simple_lookup", "count_by_color"),
    Case("count the yellows", "simple_lookup", "count_by_color"),
    Case("how many reds today?", "simple_lookup", "count_by_color"),
    Case("biggest deal I haven't called yet", "simple_lookup", "top_uncalled_deal"),
    Case("show me the top uncalled green", "simple_lookup", "top_uncalled_deal"),
    Case("is Tracerfy stale?", "simple_lookup", "source_staleness"),
    Case("are any of our data sources going cold?", "simple_lookup", "source_staleness"),
    Case("what's the status of 4021 Bayshore?", "simple_lookup", "deal_status"),
    # --- cc_query: complex / out-of-catalog / vague follow-up -------------
    Case("which deal should I call first and why?", "cc_query"),
    Case("forecast next month's closings", "cc_query"),
    Case("give me a full scoreboard for this week", "cc_query"),
    Case(
        "what is it?", "cc_query",
        history=[
            {"role": "user", "content": "how many reds?"},
            {"role": "assistant", "content": "Open opportunities: 1 red."},
        ],
        note="vague follow-up must redirect, not guess a lookup",
    ),
    # --- social: greetings / thanks / praise ------------------------------
    Case("good morning team", "social"),
    Case("thanks!", "social"),
    Case("nice work", "social"),
    Case("great job everyone", "social"),
    # --- other: pure noise → silent ---------------------------------------
    Case("asdfghjkl", "other"),
    Case("brb", "other"),
    Case("12345", "other"),
    # --- follow-up that SHOULD resolve via history ------------------------
    Case(
        "what about reds?", "simple_lookup", "count_by_color",
        history=[
            {"role": "user", "content": "how many greens?"},
            {"role": "assistant", "content": "Open opportunities: 5 green."},
        ],
        note="follow-up resolves color from prior turn",
    ),
]


def _classify(raw_text: str, history: Optional[list]):
    """Mirror production's classify block exactly — real LLM, forced tool_use."""
    from src.services.relay.thread_fallback_responder import (
        CLASSIFY_TOOL, _CLASSIFY_SYSTEM, _validate_classify,
        _parse_classify_response, _coalesce_roles,
    )
    from src.services.claude_router import call_claude_with_usage

    messages = _coalesce_roles(
        list(history or []) + [{"role": "user", "content": f"DATA: {raw_text}"}]
    )
    resp = call_claude_with_usage(
        task_type="fa_max_thread_fallback",
        messages=messages,
        system=_CLASSIFY_SYSTEM,
        cache_system=True,
        tools=[CLASSIFY_TOOL],
        tool_choice={"type": "tool", "name": CLASSIFY_TOOL["name"]},
        max_tokens=256,
    )
    ti = resp.get("tool_input")
    if isinstance(ti, dict):
        return _validate_classify(ti)
    return _parse_classify_response(resp.get("text") or "")


def run_classification_matrix() -> int:
    from src.services.relay.thread_fallback_responder import _run_catalog_lookup
    from src.core.database import get_db_context

    print("\n=== 1. Classification + live catalog queries (real Haiku, real DB) ===")
    failures = 0
    for c in CASES:
        try:
            result = _classify(c.text, c.history)
        except Exception as exc:
            print(f"  FAIL  {c.text!r:45} classify raised: {exc}")
            failures += 1
            continue

        bucket_ok = result.bucket.value == c.expect_bucket
        lookup_ok = (c.expect_lookup is None) or (result.lookup_id == c.expect_lookup)

        # For simple_lookup, actually run the query against the live schema.
        query_ok = True
        query_note = ""
        if result.bucket.value == "simple_lookup":
            try:
                with get_db_context() as db:
                    reply = _run_catalog_lookup(result, db)
                query_note = f" -> {reply[:50]!r}"
            except Exception as exc:
                query_ok = False
                query_note = f" query raised: {exc}"

        ok = bucket_ok and lookup_ok and query_ok
        status = "PASS" if ok else "FAIL"
        got = f"{result.bucket.value}/{result.lookup_id}"
        want = f"{c.expect_bucket}/{c.expect_lookup}"
        print(f"  {status}  {c.text!r:45} got={got:32} want={want}{query_note}")
        if not ok:
            failures += 1
            if c.note:
                print(f"        note: {c.note}")
    return failures


def run_schema_audit_check() -> int:
    """Prove the real bucket CHECK accepts every Bucket enum value. Rolled back."""
    from sqlalchemy import text
    from src.core.database import get_db_context
    from src.services.relay.thread_fallback_responder import Bucket

    print("\n=== 2. Audit-write schema check (all buckets, rolled back) ===")
    failures = 0
    for b in [x.value for x in Bucket]:
        try:
            with get_db_context() as db:
                db.execute(
                    text(
                        "INSERT INTO fa_max_thread_fallback_log "
                        "(relay_item_id, slack_user_id, thread_ts, lane, bucket) "
                        "VALUES (NULL, 'E2E_HARNESS', 'e2e', 'RELATIONSHIPS', :b)"
                    ),
                    {"b": b},
                )
                db.rollback()  # never persist
            print(f"  PASS  bucket={b!r} accepted by CHECK constraint")
        except Exception as exc:
            print(f"  FAIL  bucket={b!r} rejected: {exc}")
            failures += 1
    return failures


def run_enum_constraint_consistency() -> int:
    """The DB CHECK must list exactly the Bucket enum — catches drift."""
    from sqlalchemy import text
    from src.core.database import get_db_context
    from src.services.relay.thread_fallback_responder import Bucket

    print("\n=== 3. Bucket enum <-> DB CHECK consistency ===")
    with get_db_context() as db:
        definition = db.execute(
            text(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE conname = 'ck_fa_max_thread_fallback_bucket'"
            )
        ).scalar()
    if not definition:
        print("  FAIL  constraint ck_fa_max_thread_fallback_bucket not found")
        return 1
    missing = [b.value for b in Bucket if b.value not in definition]
    if missing:
        print(f"  FAIL  enum values missing from DB CHECK: {missing}")
        print(f"        constraint: {definition}")
        return 1
    print(f"  PASS  all {len(list(Bucket))} enum values present in CHECK")
    return 0


def main() -> int:
    env_file = sys.argv[1] if len(sys.argv) > 1 else ".env"
    _load_env(env_file)

    from config.settings import get_settings
    get_settings.cache_clear()

    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ERROR: ANTHROPIC_API_KEY not set — cannot run live-LLM harness")
        return 2
    if not os.environ.get("DATABASE_URL"):
        print("ERROR: DATABASE_URL not set — cannot run real-DB harness")
        return 2

    total = 0
    total += run_classification_matrix()
    total += run_schema_audit_check()
    total += run_enum_constraint_consistency()

    print("\n" + "=" * 60)
    if total == 0:
        print("RESULT: ALL PASS — safe for production")
    else:
        print(f"RESULT: {total} FAILURE(S) — do not ship until resolved")
    print("=" * 60)
    return 1 if total else 0


if __name__ == "__main__":
    sys.exit(main())
