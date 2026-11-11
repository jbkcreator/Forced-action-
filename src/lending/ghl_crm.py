"""Cora's approved CRM writes to the Next Deal Lending GoHighLevel sub-account: pipeline stage moves,
contact tags and contact notes.

Each write runs only after an approver clicked Approve (the relay calls the executor). One HTTP attempt
per call, never retried here, and error text carries the HTTP status only (never a response body,
phone or email). Every write is refused while the lending emergency halt is on, because a stage move or
tag can start a GHL workflow that messages the borrower.

Stage moves resolve the stage NAME Cora was given against the live pipeline in GHL
(``GET /opportunities/pipelines``), case-insensitive exact match, inside ``LENDING_GHL_PIPELINE_ID``
only, and move the contact's open card there. No card or no matching stage fails without writing.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Optional

import requests

from config.settings import get_settings
from src.lending.global_halt import is_globally_halted
from src.lending.ghl_account import GhlAccount, ghl_headers, lending_ghl_account
from src.services import ghl_webhook

logger = logging.getLogger(__name__)

Request = Callable[..., Any]

_TIMEOUT = 15  # seconds
_CLOSED_STATUSES = frozenset({"won", "lost", "abandoned"})


class GhlCrmError(RuntimeError):
    """A CRM write was not made (or its outcome is unknown). The message never holds contact details."""


def _single_attempt(method: str, url: str, **kwargs: Any) -> requests.Response:
    return requests.request(method, url, timeout=_TIMEOUT, **kwargs)


def _pipeline_id() -> Optional[str]:
    return get_settings().lending_ghl_pipeline_id or None


class GhlCrmWriter:
    _log_prefix = "[lending-ghl-crm]"

    def __init__(self, account: GhlAccount, request: Optional[Request] = None) -> None:
        self._account = account
        self._request = request or _single_attempt

    def _call(self, what: str, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        try:
            response = self._request(method, f"{ghl_webhook._GHL_BASE}{path}",
                                     headers=ghl_headers(self._account.api_key), **kwargs)
        except Exception as exc:  # class only: the message can carry request detail
            logger.warning("%s %s request error: %s", self._log_prefix, what, type(exc).__name__)
            raise GhlCrmError(f"GHL {what} request error ({type(exc).__name__})") from None
        if response.status_code >= 400:
            logger.warning("%s %s failed: HTTP %s", self._log_prefix, what, response.status_code)
            raise GhlCrmError(f"GHL {what} failed: HTTP {response.status_code}")
        try:
            return response.json() or {}
        except ValueError:
            return {}

    def _refuse_if_halted(self, what: str) -> None:
        if is_globally_halted():
            logger.warning("%s %s refused: lending emergency halt is on", self._log_prefix, what)
            raise GhlCrmError("lending emergency halt is on; nothing changed in GHL")

    def move_stage(self, contact_id: str, stage_name: str) -> str:
        """Move the contact's open card in the lending pipeline to ``stage_name``; returns the opportunity id."""
        self._refuse_if_halted("stage move")
        pipeline_id = _pipeline_id()
        if not pipeline_id:
            raise GhlCrmError("LENDING_GHL_PIPELINE_ID is not set; no pipeline to move cards in")
        pipelines = self._call("pipeline list", "GET", "/opportunities/pipelines",
                               params={"locationId": self._account.location_id}).get("pipelines") or []
        pipeline = next((p for p in pipelines if p.get("id") == pipeline_id), None)
        if pipeline is None:
            raise GhlCrmError("the configured lending pipeline was not found in GHL")
        stages = pipeline.get("stages") or []
        wanted = stage_name.strip().casefold()
        stage = next((s for s in stages if (s.get("name") or "").strip().casefold() == wanted), None)
        if stage is None:
            names = ", ".join(s.get("name") or "?" for s in stages)
            raise GhlCrmError(f"no stage named '{stage_name.strip()}' in {pipeline.get('name') or 'the pipeline'}; "
                              f"stages are: {names}")
        cards = self._call("opportunity search", "GET", "/opportunities/search",
                           params={"location_id": self._account.location_id, "contact_id": contact_id,
                                   "pipeline_id": pipeline_id}).get("opportunities") or []
        card = next((c for c in cards if (c.get("status") or "open").lower() not in _CLOSED_STATUSES), None)
        if card is None:
            raise GhlCrmError("the contact has no card in the lending pipeline to move")
        self._call("stage move", "PUT", f"/opportunities/{card['id']}", json={"pipelineStageId": stage["id"]})
        logger.info("%s moved opportunity %s to stage %s", self._log_prefix, card["id"], stage["id"])
        return str(card["id"])

    def add_tag(self, contact_id: str, tag: str) -> str:
        """Add one tag (GHL's add-tags endpoint keeps the contact's other tags); returns a receipt reference."""
        self._refuse_if_halted("tag add")
        tag = tag.strip()
        self._call("tag add", "POST", f"/contacts/{contact_id}/tags", json={"tags": [tag]})
        logger.info("%s tagged contact %s", self._log_prefix, contact_id)
        return f"{contact_id}:tag:{tag}"

    def add_note(self, contact_id: str, body: str) -> str:
        """Add a note to the contact; returns the GHL note id (or a receipt reference when none is returned)."""
        self._refuse_if_halted("note add")
        data = self._call("note add", "POST", f"/contacts/{contact_id}/notes", json={"body": body})
        note_id = (data.get("note") or {}).get("id") or data.get("id")
        logger.info("%s added a note to contact %s", self._log_prefix, contact_id)
        return str(note_id) if note_id else f"{contact_id}:note"


def get_crm_writer() -> Optional[GhlCrmWriter]:
    """None until the Next Deal Lending GHL account is configured."""
    account = lending_ghl_account()
    return GhlCrmWriter(account) if account is not None else None
