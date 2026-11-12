"""Cora's approved CRM writes to the Next Deal Lending GoHighLevel sub-account: pipeline stage moves,
contact tags, contact notes and contact tasks; plus the contact read the P1.1 agent job checks use.

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
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import requests

from config.lending_compliance import GHL_DND_CHANNELS
from config.settings import get_settings
from src.lending.global_halt import is_globally_halted
from src.lending.ghl_account import GhlAccount, ghl_headers, lending_ghl_account
from src.services import ghl_webhook
from src.services.phone_utils import normalize

logger = logging.getLogger(__name__)

Request = Callable[..., Any]

_TIMEOUT = 15  # seconds
_CLOSED_STATUSES = frozenset({"won", "lost", "abandoned"})


class GhlCrmError(RuntimeError):
    """A CRM write was not made (or its outcome is unknown). The message never holds contact details."""


@dataclass(frozen=True)
class GhlContact:
    """What an agent job needs from a GHL contact: where to send, and what GHL itself blocks (DND, tags)."""

    contact_id: str
    phone: Optional[str]
    email: Optional[str]
    dnd_channels: frozenset[str]
    tags: frozenset[str]


def _contact_from(raw: dict[str, Any]) -> GhlContact:
    if raw.get("dnd"):
        dnd = frozenset(GHL_DND_CHANNELS)
    else:
        dnd = frozenset(channel for channel, entry in (raw.get("dndSettings") or {}).items()
                        if isinstance(entry, dict) and (entry.get("status") or "").lower() == "active")
    return GhlContact(contact_id=str(raw.get("id")), phone=normalize(raw.get("phone")),
                      email=(raw.get("email") or "").strip().lower() or None, dnd_channels=dnd,
                      tags=frozenset(str(tag) for tag in raw.get("tags") or ()))


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
        stage_id = self._resolve_stage(pipeline_id, stage_name)
        card_id = self._open_card(pipeline_id, contact_id)
        self._refuse_if_halted("stage move")  # again: a halt can be set while the lookups run
        self._call("stage move", "PUT", f"/opportunities/{card_id}", json={"pipelineStageId": stage_id})
        logger.info("%s moved opportunity %s to stage %s", self._log_prefix, card_id, stage_id)
        return card_id

    def _resolve_stage(self, pipeline_id: str, stage_name: str) -> str:
        """The id of the pipeline stage named ``stage_name`` (case-insensitive, exact), read live from GHL."""
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
        return str(stage["id"])

    def _open_card(self, pipeline_id: str, contact_id: str) -> str:
        """The id of the contact's open card in the pipeline. Closed cards (won, lost, abandoned) are not moved."""
        cards = self._call("opportunity search", "GET", "/opportunities/search",
                           params={"location_id": self._account.location_id, "contact_id": contact_id,
                                   "pipeline_id": pipeline_id}).get("opportunities") or []
        card = next((c for c in cards if (c.get("status") or "open").lower() not in _CLOSED_STATUSES), None)
        if card is None:
            raise GhlCrmError("the contact has no card in the lending pipeline to move")
        return str(card["id"])

    def add_tag(self, contact_id: str, tag: str) -> str:
        """Add one tag (GHL's add-tags endpoint keeps the contact's other tags). GHL returns no id for a tag, so the
        receipt stored on the action is ``<contact id>:tag:<tag>``."""
        self._refuse_if_halted("tag add")
        tag = tag.strip()
        self._call("tag add", "POST", f"/contacts/{contact_id}/tags", json={"tags": [tag]})
        logger.info("%s tagged contact %s", self._log_prefix, contact_id)
        return f"{contact_id}:tag:{tag}"

    def add_note(self, contact_id: str, body: str) -> str:
        """Add a note to the contact; returns the GHL note id, or ``<contact id>:note`` when GHL returns none."""
        self._refuse_if_halted("note add")
        data = self._call("note add", "POST", f"/contacts/{contact_id}/notes", json={"body": body})
        note_id = (data.get("note") or {}).get("id") or data.get("id")
        logger.info("%s added a note to contact %s", self._log_prefix, contact_id)
        return str(note_id) if note_id else f"{contact_id}:note"

    def read_contact(self, contact_id: str) -> Optional[GhlContact]:
        """The contact in the Next Deal Lending sub-account, or None when GHL has no such contact there. Read only,
        so it works while the emergency halt is on."""
        try:
            response = self._request("GET", f"{ghl_webhook._GHL_BASE}/contacts/{contact_id}",
                                     headers=ghl_headers(self._account.api_key))
        except Exception as exc:  # class only: the message can carry request detail
            logger.warning("%s contact read request error: %s", self._log_prefix, type(exc).__name__)
            raise GhlCrmError(f"GHL contact read request error ({type(exc).__name__})") from None
        if response.status_code in (400, 404, 422):
            return None
        if response.status_code >= 400:
            logger.warning("%s contact read failed: HTTP %s", self._log_prefix, response.status_code)
            raise GhlCrmError(f"GHL contact read failed: HTTP {response.status_code}")
        try:
            raw = (response.json() or {}).get("contact") or {}
        except ValueError:
            raw = {}
        if not raw or raw.get("locationId") != self._account.location_id:
            return None
        return _contact_from(raw)

    def create_task(self, contact_id: str, title: str, due_at: datetime, assignee: Optional[str]) -> str:
        """Create a task on the contact; returns the GHL task id, or ``<contact id>:task`` when GHL returns none."""
        self._refuse_if_halted("task create")
        body: dict[str, Any] = {"title": title.strip(),
                                "dueDate": due_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                                "completed": False}
        if assignee:
            body["assignedTo"] = assignee
        data = self._call("task create", "POST", f"/contacts/{contact_id}/tasks", json=body)
        task_id = (data.get("task") or {}).get("id") or data.get("id")
        logger.info("%s created a task on contact %s", self._log_prefix, contact_id)
        return str(task_id) if task_id else f"{contact_id}:task"


def get_crm_writer() -> Optional[GhlCrmWriter]:
    """None until the Next Deal Lending GHL account is configured."""
    account = lending_ghl_account()
    return GhlCrmWriter(account) if account is not None else None
