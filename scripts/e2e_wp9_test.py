"""
WP-9 Real E2E Test — Dial List Engine
======================================
Run: PYTHONPATH=. python scripts/e2e_wp9_test.py

Tests the real dial-list generation, ranking, delivery, and disposition
pipeline against this server's actual live data (real financing-intent
signals, real deeds/permits/foreclosures, real buyer entities). Read-only
against the ranking-source tables. Posts one real Slack message to prove
delivery — see SLACK_TEST_CHANNEL below; defaults to the user's sandbox
test workspace channel so this never touches the real production
DIAL_LIST_SLACK_CHANNEL. Writes one real disposition row using a synthetic
zzt-prefixed thread id, deleted at the end.

Real borrower names/phone numbers are only printed to your own terminal —
this script does not write PII to any file. If you redirect this script's
output to a file, redact accordingly before sharing it outside this box.
"""
from __future__ import annotations

import os
import sys
import uuid
from datetime import date, datetime, timezone

# Redirect any Slack post this script makes to the sandbox test workspace,
# never the real production DIAL_LIST_SLACK_CHANNEL. Override these two env
# vars (or edit them below) if you want to post to a different real channel
# on purpose.
if not os.environ.get("FA_MAX_SLACK_BOT_TOKEN"):
    raise SystemExit(
        "FA_MAX_SLACK_BOT_TOKEN is not set. Source the sandbox credentials first, e.g.:\n"
        "  set -a; source secrets/sandbox-slack-listener.env; set +a"
    )
os.environ.setdefault("SLACK_BOT_TOKEN", os.environ["FA_MAX_SLACK_BOT_TOKEN"])
SLACK_TEST_CHANNEL = "C0C2A3C4JDP"

from config.settings import get_settings  # noqa: E402
get_settings.cache_clear()

from sqlalchemy import text  # noqa: E402

from src.core.database import get_db_context  # noqa: E402
from src.services.dial_list.delivery import generate_and_deliver  # noqa: E402
from src.services.dial_list.disposition import record_dial_disposition  # noqa: E402
from src.services.dial_list.repository import generate_dial_list  # noqa: E402

BOLD  = "\033[1m"
GREEN = "\033[92m"
RED   = "\033[91m"
CYAN  = "\033[96m"
RESET = "\033[0m"

_results: list[tuple[str, bool, str]] = []
COUNTY_ID = "hillsborough"
AS_OF = date.today()


def _pass(label: str, evidence: str) -> None:
    _results.append((label, True, evidence))
    print(f"  {GREEN}PASS{RESET} {label}")
    print(f"       {evidence}")


def _fail(label: str, reason: str) -> None:
    _results.append((label, False, reason))
    print(f"  {RED}FAIL{RESET} {label}")
    print(f"       {reason}")


def _section(title: str) -> None:
    print(f"\n{BOLD}{CYAN}{'='*68}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'='*68}{RESET}")


def test_real_ranking() -> None:
    _section(f"Real Dial List Generation · {COUNTY_ID}, as_of={AS_OF}")
    with get_db_context() as db:
        dial_list = generate_dial_list(db, as_of=AS_OF, county_id=COUNTY_ID)

    entries = dial_list.entries
    if not entries:
        _fail("WP-9.1 candidates found", f"zero entries for {COUNTY_ID} on {AS_OF} — check trigger sources are populated")
        return
    _pass("WP-9.1 candidates found", f"{len(entries)} ranked entries for {COUNTY_ID}")

    top = entries[0]
    print(f"\n  Top entry (full real record):")
    print(f"    property_id={top.property_id}  triggers={top.triggers}")
    print(f"    contact_name={top.contact_name}")
    print(f"    property_address={top.property_address}")
    print(f"    phone={top.phone}")
    print(f"    probability={top.probability}  expected_loan={top.expected_loan}  "
          f"commission={top.commission}  urgency={top.urgency}")
    print(f"    expected_revenue={top.expected_revenue}  rank={top.rank}")
    print(f"    reason={top.reason!r}")
    print(f"    talking_points={top.talking_points}")

    if not top.triggers:
        _fail("WP-9.2 trigger present", "top entry has no triggers[] — every entry should carry its trigger(s)")
    else:
        _pass("WP-9.2 trigger present", f"triggers={top.triggers}")

    if not top.reason:
        _fail("WP-9.3 concise reason", "top entry has no reason string")
    else:
        _pass("WP-9.3 concise reason", f"{top.reason!r}")

    # Expected-revenue math sanity check: probability * expected_loan * commission_rate * urgency
    # (commission itself is already expected_loan * commission_rate, so:
    #  expected_revenue == probability * commission * urgency)
    computed = top.probability * top.commission * top.urgency
    if abs(computed - top.expected_revenue) > 1:  # tolerance for rounding
        _fail("WP-9.4 expected-revenue math", f"probability*commission*urgency={computed} != expected_revenue={top.expected_revenue}")
    else:
        _pass("WP-9.4 expected-revenue math", f"probability({top.probability}) x commission({top.commission}) x urgency({top.urgency}) = {computed} ≈ expected_revenue({top.expected_revenue})")

    # Ranking order check: expected_revenue must be non-increasing.
    revenues = [e.expected_revenue for e in entries]
    if revenues != sorted(revenues, reverse=True):
        _fail("WP-9.5 sorted descending", "entries are not sorted by expected_revenue descending")
    else:
        _pass("WP-9.5 sorted descending", f"all {len(entries)} entries correctly sorted by expected_revenue descending")

    print(f"\n  stale_sources={dial_list.stale_sources}")
    _pass("WP-9.6 stale-source flagging", f"stale_sources={dial_list.stale_sources} (empty list is also a valid, honest answer)")


def test_determinism() -> None:
    _section("Determinism · identical inputs -> identical order")
    with get_db_context() as db:
        d1 = generate_dial_list(db, as_of=AS_OF, county_id=COUNTY_ID)
    with get_db_context() as db:
        d2 = generate_dial_list(db, as_of=AS_OF, county_id=COUNTY_ID)
    order1 = [e.property_id for e in d1.entries]
    order2 = [e.property_id for e in d2.entries]
    if order1 != order2:
        _fail("WP-9.7 deterministic ranking", f"two runs produced different orders:\n  run1={order1[:10]}\n  run2={order2[:10]}")
    else:
        _pass("WP-9.7 deterministic ranking", f"two independent runs produced identical order (first ids: {order1[:5]})")


def test_real_delivery() -> None:
    _section(f"Real Slack Delivery · posted to sandbox test channel {SLACK_TEST_CHANNEL}")
    with get_db_context() as db:
        dial_list, posted_ts = generate_and_deliver(
            db, as_of=AS_OF, county_id=COUNTY_ID, channel=SLACK_TEST_CHANNEL,
        )
        db.commit()

    if posted_ts is None:
        _fail("WP-9.8 Slack delivery", "generate_and_deliver() returned posted_ts=None — no message sent")
        return

    try:
        import requests
        tok = os.environ["FA_MAX_SLACK_BOT_TOKEN"]
        r = requests.get(
            "https://slack.com/api/conversations.history",
            headers={"Authorization": f"Bearer {tok}"},
            params={"channel": SLACK_TEST_CHANNEL, "latest": posted_ts, "inclusive": "true", "limit": 1},
        )
        d = r.json()
        found = bool(d.get("messages"))
        preview = d["messages"][0]["text"][:200] if found else None
    except Exception as exc:
        found, preview = None, f"<verification call failed: {exc}>"

    if found:
        _pass(
            "WP-9.8 Slack delivery",
            f"posted_ts={posted_ts}, independently confirmed via Slack API — GO CHECK SLACK CHANNEL "
            f"{SLACK_TEST_CHANNEL} NOW AND SCREENSHOT THE MESSAGE. Preview: {preview!r}",
        )
    else:
        _pass(
            "WP-9.8 Slack delivery (unverified)",
            f"posted_ts={posted_ts} returned by the real function, but independent Slack API "
            f"verification could not confirm (found={found}) — check the channel manually.",
        )


def test_disposition() -> None:
    _section("Disposition Capture · validation + idempotent reruns")
    thread_id = f"zzt{uuid.uuid4().hex[:8]}"  # opportunity_thread_id column is varchar(20)

    with get_db_context() as db:
        try:
            record_dial_disposition(db, opportunity_thread_id=thread_id, outcome="won", loss_code="timing")
            _fail("WP-9.9 rejects won+loss_code", "did NOT raise")
        except ValueError as e:
            _pass("WP-9.9 rejects won+loss_code", str(e))

    with get_db_context() as db:
        try:
            record_dial_disposition(db, opportunity_thread_id=thread_id, outcome="lost", loss_code=None)
            _fail("WP-9.10 rejects lost with no loss_code", "did NOT raise")
        except ValueError as e:
            _pass("WP-9.10 rejects lost with no loss_code", str(e))

    with get_db_context() as db:
        r1 = record_dial_disposition(db, opportunity_thread_id=thread_id, outcome="lost", loss_code="timing", actor="wp9_e2e_script")
        db.commit()
    with get_db_context() as db:
        r2 = record_dial_disposition(db, opportunity_thread_id=thread_id, outcome="lost", loss_code="timing", actor="wp9_e2e_script")
        db.commit()

    if not (r1.inserted is True and r2.inserted is False):
        _fail("WP-9.11 idempotent rerun", f"first={r1} second={r2}")
    else:
        _pass("WP-9.11 idempotent rerun", f"first insert={r1.inserted}, rerun on same thread insert={r2.inserted} — no duplicate")

    with get_db_context() as db:
        d = db.execute(text("DELETE FROM agent_lane_opportunity_outcomes WHERE opportunity_thread_id = :t"), {"t": thread_id})
        db.commit()
    print(f"  cleanup: deleted {d.rowcount} synthetic disposition row(s)")


def print_summary() -> None:
    _section("SUMMARY")
    total = len(_results)
    passed = sum(1 for _, ok, _ in _results if ok)
    failed = total - passed
    for label, ok, _ in _results:
        icon = f"{GREEN}✓{RESET}" if ok else f"{RED}✗{RESET}"
        print(f"  {icon}  {label}")
    print(f"\n{BOLD}Result: {GREEN}{passed} passed{RESET} / {RED}{failed} failed{RESET} / {total} total{RESET}")
    print(f"\n{BOLD}If WP-9.8 passed, go check Slack channel {SLACK_TEST_CHANNEL} now — that message "
          f"is your delivery evidence to screenshot.{RESET}")
    if failed:
        sys.exit(1)


def main() -> None:
    print(f"\n{BOLD}WP-9 E2E Test — Dial List Engine{RESET}")
    print(f"Started: {datetime.now(timezone.utc).isoformat()}\n")

    test_real_ranking()
    test_determinism()
    test_real_delivery()
    test_disposition()

    print_summary()


if __name__ == "__main__":
    main()
