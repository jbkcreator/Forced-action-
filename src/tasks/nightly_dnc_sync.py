"""R1 0.11: nightly opt-out reconciliation across GHL, BatchDialer and Instantly.

Opt-outs already travel event by event (``compliance._propagate``, the opt-out poller, the GHL DND backstop, Cora's
Instantly block-list sweep). This run is the backstop that makes every platform's suppression match once a night:

1. Pull: every GHL contact with do-not-disturb (``ghl_dnd_backstop.run_backstop``) and every address on NDL's
   Instantly block list we do not hold yet become internal opt-outs (``propagate_opt_out``). BatchDialer has no
   do-not-call list API; its opt-outs arrive as DNC_REQUEST call results through the CDR poller.
2. Push: what our ledger shows as not yet done is pushed again: GHL do-not-disturb (``sync_ghl_dnd``, until it stops
   making progress), pending BatchDialer removals, and every opted-out address missing from the Instantly block-list
   queue (opt-outs only).

Only ever adds suppression, never removes it. Each step runs in its own transaction, so one platform being down does
not stop the others; any failure posts to the ops notice channel. National DNC is not re-checked here (weekly scrub).

Usage (cron 07:30 UTC = 2:30 AM ET):
    python -m src.tasks.nightly_dnc_sync
"""
from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any, Optional

from sqlalchemy import text

from src.utils.http_helpers import requests_get_with_retry

from config.lending_compliance import OptOutChannel, SuppressionReason

logger = logging.getLogger(__name__)

_LOG = "[nightly-dnc-sync]"
INSTANTLY_PAGE_SIZE = 100
INSTANTLY_MAX_PAGES = 500
GHL_DND_MAX_ROUNDS = 50

FetchEntries = Callable[[], Iterator[dict[str, Any]]]
SessionFactory = Callable[[], Any]

_OPT_OUT = SuppressionReason.OPT_OUT.value
_KNOWN_EMAILS = text(
    "SELECT lower(email) FROM lending.suppression_list WHERE lower(email) = ANY(:emails) "
    "UNION SELECT lower(email) FROM lending.instantly_block_queue WHERE lower(email) = ANY(:emails)"
)
_QUEUE_MISSING = text("""
    INSERT INTO lending.instantly_block_queue (email)
    SELECT DISTINCT lower(s.email) FROM lending.suppression_list s
     WHERE s.reason = :opt_out AND s.email IS NOT NULL AND trim(s.email) <> ''
       AND NOT EXISTS (SELECT 1 FROM lending.instantly_block_queue q WHERE q.email = lower(s.email))
    ON CONFLICT (email) DO NOTHING
""")


@dataclass
class SyncReport:
    counts: dict[str, int] = field(default_factory=dict)
    failures: list[str] = field(default_factory=list)

    def line(self) -> str:
        parts = [f"{k}={v}" for k, v in self.counts.items()] + [f"failed={','.join(self.failures) or 'none'}"]
        return " ".join(parts)


def pull_instantly(db, entries: Iterator[dict[str, Any]]) -> int:
    """Record each blocked address we do not hold yet as an email opt-out (domain entries skipped). Does not commit."""
    from src.lending.compliance import propagate_opt_out

    by_email: dict[str, str] = {}
    for entry in entries:
        value = (entry.get("bl_value") or "").strip().lower()
        if value and "@" in value and not entry.get("is_domain"):
            by_email.setdefault(value, str(entry.get("id") or value))
    if not by_email:
        return 0
    known = {row[0] for row in db.execute(_KNOWN_EMAILS, {"emails": sorted(by_email)})}
    new = 0
    for email, entry_id in by_email.items():
        if email in known:
            continue
        propagate_opt_out(db, email=email, source_ref=f"instantly:{entry_id}"[:100], channel=OptOutChannel.EMAIL)
        new += 1
    return new


def push_instantly(db) -> int:
    """Queue every opted-out address the Instantly block-list queue lacks; Cora's minute sweep sends them. A row that
    already failed stays failed (its inbox item asks for a manual add). Does not commit."""
    return db.execute(_QUEUE_MISSING, {"opt_out": _OPT_OUT}).rowcount or 0


def push_ghl(db_factory: SessionFactory, ghl_dnd: Optional[Callable[[str], bool]] = None) -> int:
    """Run ``sync_ghl_dnd`` batch after batch, each in its own transaction, until a batch makes no progress."""
    from src.lending.compliance import sync_ghl_dnd

    total = 0
    for _ in range(GHL_DND_MAX_ROUNDS):
        with db_factory() as db:
            done = sync_ghl_dnd(db, ghl_dnd)
        total += done
        if not done:
            break
    return total


def _instantly_entries() -> Iterator[dict[str, Any]]:
    """NDL's Instantly block list, page by page. UNVERIFIED against a live call: shape per the public v2 reference
    (GET /api/v2/block-lists-entries -> items[{id, bl_value, is_domain}], next_starting_after)."""
    from src.lending.instantly_client import TIMEOUT_SECONDS, lending_instantly_account

    account = lending_instantly_account()
    if account is None:
        raise RuntimeError("LENDING_INSTANTLY_API_KEY not set")
    cursor: Optional[str] = None
    for _ in range(INSTANTLY_MAX_PAGES):
        params: dict[str, Any] = {"limit": INSTANTLY_PAGE_SIZE}
        if cursor:
            params["starting_after"] = cursor
        response = requests_get_with_retry(f"{account.base_url}/api/v2/block-lists-entries", max_retries=3, params=params,
                                           headers={"Authorization": f"Bearer {account.api_key}"},
                                           timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json() or {}
        yield from payload.get("items") or []
        cursor = payload.get("next_starting_after")
        if not cursor:
            return


def _step(report: SyncReport, name: str, run: Callable[[], int]) -> None:
    try:
        report.counts[name] = run()
    except Exception as exc:  # class only: messages can carry phones or addresses
        logger.error("%s %s failed (%s)", _LOG, name, type(exc).__name__)
        report.failures.append(name)


def run_sync(db_factory: SessionFactory, *, fetch_ghl_dnd_page: Optional[Callable[[int], list[dict[str, Any]]]] = None,
             fetch_instantly: Optional[FetchEntries] = None, ghl_dnd: Optional[Callable[[str], bool]] = None,
             dialer_remover: Optional[Callable[..., None]] = None) -> SyncReport:
    """Pull, then push; each step in its own committed transaction. Fakes replace every platform in tests."""
    from src.lending import compliance
    from src.lending.ghl_account import lending_ghl_account
    from src.lending.ghl_dnd_backstop import fetch_ghl_dnd_page as live_ghl_page, run_backstop
    from src.lending.instantly_client import lending_instantly_account

    report = SyncReport()

    def in_tx(work: Callable[[Any], int]) -> Callable[[], int]:
        def run() -> int:
            with db_factory() as db:
                return work(db)
        return run

    if fetch_ghl_dnd_page is not None or lending_ghl_account() is not None:
        _step(report, "ghl_pulled", in_tx(lambda db: run_backstop(db, fetch_dnd_page=fetch_ghl_dnd_page or live_ghl_page)))
    if fetch_instantly is not None or lending_instantly_account() is not None:
        _step(report, "instantly_pulled", in_tx(lambda db: pull_instantly(db, (fetch_instantly or _instantly_entries)())))
    _step(report, "ghl_pushed", lambda: push_ghl(db_factory, ghl_dnd))
    _step(report, "dialer_pushed", in_tx(lambda db: compliance._retry_pending_dialer_removals(db, dialer_remover)))
    _step(report, "instantly_queued", in_tx(push_instantly))
    return report


def main() -> int:
    from src.core.database import get_db_context
    from src.lending.ops_alerts import post_ops_notice

    report = run_sync(get_db_context)
    logger.info("%s %s", _LOG, report.line())
    if report.failures:
        post_ops_notice(f"Nightly opt-out sync: step(s) failed: {', '.join(report.failures)}. "
                        "They retry tomorrow night; the event-driven paths keep running.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
