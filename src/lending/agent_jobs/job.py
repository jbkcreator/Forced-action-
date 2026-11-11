"""Parse one agent job file (brief section 6) into an :class:`AgentJob`, or raise :class:`JobFormatError`.

The format is the brief's, plus three optional fields agreed in the P1.1 plan: ``subject`` (required for
``send_email``, decision D10) and ``task_due_at`` / ``task_assignee`` for ``create_task`` (D9). Unknown fields are
ignored. ``lane`` is kept as written: a wrong lane is a ``blocked_lane`` outcome (checks), not a format error.

An error's ``reason`` names the failing fields and the kind of problem only, never a value from the file, so it can
be logged and written to a receipt without leaking message text or contact details.
"""
from __future__ import annotations

import json
import re
from typing import Literal, Optional
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError, model_validator

from config.lending_agent_jobs import MAX_FILE_BYTES

_UUID_AT_END = re.compile(r"_([0-9a-fA-F-]{36})\.json$")


class JobFormatError(ValueError):
    """The file is not a usable job. ``job_id`` is set when it could be read, so a receipt can still be written."""

    def __init__(self, reason: str, job_id: Optional[str] = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.job_id = job_id


class AgentJob(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    job_id: UUID
    lane: str = Field(max_length=20)
    created_by: str = Field(min_length=1, max_length=120)
    created_at: AwareDatetime
    type: Literal["send_sms", "send_email", "create_task", "add_tag", "move_stage", "add_note"]
    ghl_contact_id: str = Field(min_length=1, max_length=64)
    contact_label: str = Field(default="", max_length=120)
    template_version: str = Field(min_length=1, max_length=80)
    body: str = Field(min_length=1, max_length=2000)
    reason: str = Field(default="", max_length=500)
    requires_approval: bool
    not_before: Optional[AwareDatetime] = None
    expires_at: AwareDatetime
    subject: Optional[str] = Field(default=None, min_length=1, max_length=200)
    task_due_at: Optional[AwareDatetime] = None
    task_assignee: Optional[str] = Field(default=None, max_length=64)

    @model_validator(mode="after")
    def _consistent(self) -> "AgentJob":
        if self.not_before is not None and self.expires_at <= self.not_before:
            raise ValueError("expires_at must be after not_before")
        if self.type == "send_email" and not self.subject:
            raise ValueError("subject is required for send_email")
        return self


def _id_from_name(file_name: str) -> Optional[str]:
    match = _UUID_AT_END.search(file_name)
    if not match:
        return None
    try:
        return str(UUID(match.group(1)))
    except ValueError:
        return None


def _id_from_json(data: object) -> Optional[str]:
    if not isinstance(data, dict):
        return None
    try:
        return str(UUID(str(data.get("job_id"))))
    except ValueError:
        return None


def _describe(error: ValidationError) -> str:
    """Field names and error kinds only (never ``input``, which holds the file's values)."""
    parts = []
    for item in error.errors(include_url=False, include_input=False, include_context=False):
        where = ".".join(str(part) for part in item["loc"]) or "job"
        detail = item["type"] if item["type"] != "value_error" else item["msg"].removeprefix("Value error, ")
        parts.append(f"{where}: {detail}")
    return "invalid job file (" + "; ".join(parts) + ")"


def parse_job(raw: bytes, file_name: str) -> AgentJob:
    fallback_id = _id_from_name(file_name)
    if len(raw) > MAX_FILE_BYTES:
        raise JobFormatError(f"job file is larger than {MAX_FILE_BYTES} bytes", fallback_id)
    try:
        data = json.loads(raw)
    except ValueError:
        raise JobFormatError("job file is not valid JSON", fallback_id) from None
    job_id = _id_from_json(data) or fallback_id
    try:
        return AgentJob.model_validate(data)
    except ValidationError as exc:
        raise JobFormatError(_describe(exc), job_id) from None
