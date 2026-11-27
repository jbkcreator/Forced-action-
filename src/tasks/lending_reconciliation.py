"""Nightly reconcile of lost inbound webhooks (R1 0.5 T7, grill D9 / D10 / D12).

    python -m src.tasks.lending_reconciliation [--dry-run] [--hours 48] [--force]

One job, a list of source slots. A slot finds the events of the trailing window that never reached us
(``find_missing(since)`` -> payloads) and heals each one through the normal journal handler (``heal(payload)``,
raises when the event was not healed). The GHL stage slot fetches records directly from GHL.

Cron fires at 06:00 and 07:00 UTC; only the run that is 2:xx ET acts (``--force`` for any time). Off until
``LENDING_RECONCILE_ENABLED``. One post to ``LENDING_OPS_CHANNEL`` only when something was healed or failed; a failure
also raises a Cora inbox item and exits 1. The job sends nothing to borrowers itself: healed events go through the
same handlers as a webhook (their sends keep their own hour rules). Logs and posts carry ids and error classes only.
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Optional, Protocol, Sequence

import requests

from config.lending_reminders import TIMEZONE
from config.settings import get_settings
from src.lending.ghl_account import GhlAccount, ghl_headers, lending_ghl_account
from src.lending.inbound import ghl_stage, journal
from src.services import ghl_webhook

logger = logging.getLogger(__name__)

RUN_HOUR_ET = 2
DEFAULT_HOURS = 48


# ---- slot protocol ---------------------------------------------------------------------------------------------

class Slot(Protocol):
    """One inbound source. ``find_missing`` raises on failure (``SlotWarning`` = note it, not a failure);
    ``heal`` raises when the payload was not healed. Messages never carry contact details."""

    name: str

    def find_missing(self, since: datetime) -> Iterable[dict[str, Any]]: ...

    def heal(self, payload: dict[str, Any]) -> None: ...


class SlotWarning(Exception):
    """The slot could not look (e.g. its source is not there yet) but nothing is known to be lost."""


class ReconcileError(RuntimeError):
    """A slot failed. The message is safe for logs and Slack (ids, HTTP status, error class)."""


@dataclass
class SlotReport:
    name: str
    found: int = 0
    healed: int = 0
    heal_failures: int = 0
    error: Optional[str] = None
    warning: Optional[str] = None

    @property
    def failed(self) -> bool:
        return bool(self.error or self.heal_failures)


def run_slot(slot: Slot, since: datetime, *, dry_run: bool) -> SlotReport:
    report = SlotReport(slot.name)
    try:
        payloads = list(slot.find_missing(since))
    except SlotWarning as exc:
        report.warning = str(exc)
        logger.warning("[lending-reconcile] %s: %s", slot.name, exc)
        return report
    except Exception as exc:
        report.error = str(exc) if isinstance(exc, ReconcileError) else type(exc).__name__
        logger.error("[lending-reconcile] %s failed: %s", slot.name, report.error)
        return report
    report.found = len(payloads)
    if dry_run:
        return report
    for payload in payloads:
        try:
            slot.heal(payload)
            report.healed += 1
        except Exception as exc:  # class only
            report.heal_failures += 1
            logger.error("[lending-reconcile] %s heal failed (%s)", slot.name, type(exc).__name__)
    return report


def format_summary(reports: Sequence[SlotReport], *, hours: int) -> str:
    lines = [f"*Nightly reconcile (last {hours} h)*"]
    for r in reports:
        line = f"• {r.name}: found {r.found} missing, healed {r.healed}"
        if r.heal_failures:
            line += f", {r.heal_failures} not healed (saved for retry)"
        if r.error:
            line += f", FAILED: {r.error}"
        if r.warning:
            line += f", warning: {r.warning}"
        lines.append(line)
    return "\n".join(lines)


# ---- GHL stage slot --------------------------------------------------------------------------------------------

Request = Callable[..., Any]

SEARCH_PAGE_SIZE = 500  # GHL's maximum per page
SEARCH_MAX_RECORDS = 10_000  # page * limit may not exceed this (searchAfter beyond; not needed for 24 h of one pipeline)
SEARCH_VERSION = "v3"  # the only Version the advanced search accepts
_TIMEOUT = 30


def _single_attempt(method: str, url: str, **kwargs: Any) -> requests.Response:
    return requests.request(method, url, timeout=_TIMEOUT, **kwargs)


def _db_current_stages(opportunity_ids: Sequence[str]) -> dict[str, str]:
    from src.lending.closing.blockers import current_stages
    from src.lending.db import lending_session

    with lending_session() as db:
        return current_stages(db, opportunity_ids)


class GhlStageSlot:
    """NDL-pipeline opportunities GHL updated in the window whose stage differs from (or is missing in) our current
    stage, healed through the ``ghl_stage`` journal handler with a reconcile payload."""

    name = "GHL stages"

    def __init__(self, account: Optional[GhlAccount], pipeline_ids: Sequence[str], *,
                 request: Optional[Request] = None,
                 current_stages: Callable[[Sequence[str]], dict[str, str]] = _db_current_stages,
                 ingest: Callable[..., Any] = journal.ingest) -> None:
        self._account = account
        self._pipeline_ids = [p for p in pipeline_ids if p]
        self._request = request or _single_attempt
        self._current_stages = current_stages
        self._ingest = ingest

    def _call(self, what: str, method: str, path: str, *, version: str = "2021-07-28", **kwargs: Any) -> dict:
        try:
            response = self._request(method, f"{ghl_webhook._GHL_BASE}{path}",
                                     headers=ghl_headers(self._account.api_key, version=version), **kwargs)
        except Exception as exc:  # class only: the message can carry request detail
            raise ReconcileError(f"GHL {what} request error ({type(exc).__name__})") from None
        if response.status_code >= 400:
            raise ReconcileError(f"GHL {what} failed: HTTP {response.status_code}")
        try:
            return response.json() or {}
        except ValueError:
            raise ReconcileError(f"GHL {what} returned no JSON") from None

    def _stage_names(self) -> dict[str, str]:
        pipelines = self._call("pipeline list", "GET", "/opportunities/pipelines",
                               params={"locationId": self._account.location_id}).get("pipelines") or []
        return {s["id"]: (s.get("name") or "").strip() for p in pipelines for s in p.get("stages") or ()
                if s.get("id")}

    def _search(self, pipeline_id: str, since: datetime) -> Iterable[dict[str, Any]]:
        page = 1
        while True:
            body = {"locationId": self._account.location_id, "limit": SEARCH_PAGE_SIZE, "page": page,
                    "filters": [{"field": "pipeline_id", "operator": "eq", "value": pipeline_id},
                                {"field": "date_updated", "operator": "range",
                                 "value": {"gte": since.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}}],
                    "sort": [{"field": "date_updated", "direction": "asc"}]}
            found = self._call("opportunity search", "POST", "/opportunities/search", version=SEARCH_VERSION,
                               json=body).get("opportunities") or []
            yield from found
            if len(found) < SEARCH_PAGE_SIZE:
                return
            if (page + 1) * SEARCH_PAGE_SIZE > SEARCH_MAX_RECORDS:
                logger.warning("[lending-reconcile] pipeline %s: over %s updated opportunities, rest not checked",
                               pipeline_id, SEARCH_MAX_RECORDS)
                return
            page += 1

    def find_missing(self, since: datetime) -> list[dict[str, Any]]:
        if self._account is None:
            raise ReconcileError("LENDING_GHL_API_KEY / LENDING_GHL_LOCATION_ID unset")
        if not self._pipeline_ids:
            raise ReconcileError("LENDING_RECONCILE_GHL_PIPELINE_IDS unset")
        names = self._stage_names()
        ghl: dict[str, dict[str, Any]] = {}
        for pipeline_id in self._pipeline_ids:
            for opp in self._search(pipeline_id, since):
                stage = names.get(opp.get("pipelineStageId") or "")
                if not opp.get("id") or not stage:
                    logger.warning("[lending-reconcile] opportunity %s: stage id not in its pipeline, skipped",
                                   opp.get("id"))
                    continue
                ghl[opp["id"]] = {"opportunity_id": opp["id"], "stage_name": stage,
                                  "pipeline_id": opp.get("pipelineId") or pipeline_id,
                                  "phone": (opp.get("contact") or {}).get("phone") or None,
                                  "source": ghl_stage.RECONCILE}
        ours = self._current_stages(list(ghl))
        return [payload for opp_id, payload in ghl.items()
                if (ours.get(opp_id) or "").strip().lower() != payload["stage_name"].lower()]

    def heal(self, payload: dict[str, Any]) -> None:
        key = "reconcile:" + ghl_stage.dedup_key(payload["opportunity_id"], payload["stage_name"])
        result = self._ingest(ghl_stage.SOURCE, key, payload, rerun_duplicates=True)
        if result.outcome == "failed":
            raise ReconcileError("ghl_stage handler failed (saved for retry)")


def _pipeline_ids() -> list[str]:
    return [p.strip() for p in get_settings().lending_reconcile_ghl_pipeline_ids.split(",") if p.strip()]


def default_slots() -> list[Slot]:
    return [GhlStageSlot(lending_ghl_account(), _pipeline_ids())]


# ---- job -------------------------------------------------------------------------------------------------------

def _post_ops_alert(message: str) -> bool:
    from src.lending.ops_alerts import post_ops_alert

    return post_ops_alert(message)


def _add_inbox(**item: Any) -> Optional[int]:
    from src.lending.cora_inbox import add_to_inbox_in_own_session

    return add_to_inbox_in_own_session(**item)


def main(argv: Optional[Sequence[str]] = None, *, now: Optional[datetime] = None,
         slots: Optional[Sequence[Slot]] = None, post: Callable[[str], Any] = _post_ops_alert,
         add_inbox: Callable[..., Any] = _add_inbox) -> int:
    parser = argparse.ArgumentParser(prog="python -m src.tasks.lending_reconciliation")
    parser.add_argument("--dry-run", action="store_true", help="find and report missing events, heal nothing")
    parser.add_argument("--hours", type=int, default=DEFAULT_HOURS, help="window to check (default 48)")
    parser.add_argument("--force", action="store_true", help="run regardless of the ET hour")
    args = parser.parse_args(argv)
    if not get_settings().lending_reconcile_enabled:
        logger.info("[lending-reconcile] LENDING_RECONCILE_ENABLED is off")
        return 0
    now = now or datetime.now(timezone.utc)
    now_et = now.astimezone(TIMEZONE)
    if now_et.hour != RUN_HOUR_ET and not args.force:
        return 0
    since = now - timedelta(hours=args.hours)
    reports = [run_slot(slot, since, dry_run=args.dry_run) for slot in (default_slots() if slots is None else slots)]
    summary = format_summary(reports, hours=args.hours)
    failed = [r for r in reports if r.failed]
    logger.info("[lending-reconcile] %s", summary.replace("\n", " | "))
    if args.dry_run:
        return 1 if failed else 0
    if failed or any(r.healed for r in reports):
        post(summary)
    if failed:
        add_inbox(event_type="lending_reconcile_failed", dedup_key=f"{now_et:%Y-%m-%d}",
                  summary="The nightly reconcile of lost webhooks failed: "
                          + "; ".join(f"{r.name}: {r.error or f'{r.heal_failures} not healed'}" for r in failed)
                          + ". Lost GHL stage changes or LendingFlow leads may not be in our records.",
                  payload={"slots": [r.name for r in failed]})
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
