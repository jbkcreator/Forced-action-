"""Structured fields extracted from a lending call transcript.

The caller still chooses a disposition code for every call; this adds the
detail behind it, extracted from the transcript with a forced Claude tool call
through the router (cost tracking and the vendor-cost pause apply) and
validated with Pydantic before anything is stored.

The brief freezes twelve fields and names seven; the other five are pending
client confirmation, so the model holds the seven and new fields are added
here. Two guards keep the model from inventing facts: a deadline must be a
plausible future date, and the verbatim kill reason must appear word for word
in the transcript, otherwise it is dropped.
"""
from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from typing import Optional

from pydantic import BaseModel, Field, ValidationError, field_validator

logger = logging.getLogger(__name__)

EXTRACTION_TASK_TYPE = "lending_call_extraction"
TOOL_NAME = "record_call_fields"
MAX_DEADLINE_AHEAD = timedelta(days=366)
MAX_LIST_ITEMS = 10


class CallExtraction(BaseModel):
    """The frozen extraction fields for one call."""

    objection: Optional[str] = Field(default=None, max_length=280)
    missing_file_items: list[str] = Field(default_factory=list)
    next_action: Optional[str] = Field(default=None, max_length=200)
    deadline: Optional[date] = None
    referral_names: list[str] = Field(default_factory=list)
    decision_maker: Optional[str] = Field(default=None, max_length=120)
    kill_reason_verbatim: Optional[str] = Field(default=None, max_length=500)

    @field_validator("missing_file_items", "referral_names")
    @classmethod
    def _clean_list(cls, items: list[str]) -> list[str]:
        cleaned = [" ".join(str(item).split()) for item in items if str(item).strip()]
        return list(dict.fromkeys(cleaned))[:MAX_LIST_ITEMS]

    @field_validator("deadline", mode="before")
    @classmethod
    def _unparseable_deadline_to_none(cls, value):
        """A malformed date loses only the deadline, not the rest of the extraction."""
        if value in (None, "") or isinstance(value, date):
            return value or None
        try:
            return date.fromisoformat(str(value))
        except ValueError:
            return None

    @field_validator("objection", "next_action", "decision_maker", "kill_reason_verbatim")
    @classmethod
    def _blank_to_none(cls, value: Optional[str]) -> Optional[str]:
        return value.strip() or None if value else None


_TOOL = {
    "name": TOOL_NAME,
    "description": "Record the structured fields of a lending phone call.",
    "input_schema": {
        "type": "object",
        "properties": {
            "objection": {"type": "string", "description": "The borrower's main objection, if any. Max 280 chars."},
            "missing_file_items": {
                "type": "array", "items": {"type": "string"},
                "description": "Documents or file items the borrower still needs to provide.",
            },
            "next_action": {"type": "string", "description": "The agreed next step, if any. Max 200 chars."},
            "deadline": {
                "type": "string",
                "description": "ISO date (YYYY-MM-DD) of any deadline mentioned, resolved against today's date. "
                               "Omit if no date or day was given.",
            },
            "referral_names": {
                "type": "array", "items": {"type": "string"},
                "description": "Names of people the borrower referred or mentioned as contacts.",
            },
            "decision_maker": {"type": "string", "description": "Who the borrower says makes the financing decision."},
            "kill_reason_verbatim": {
                "type": "string",
                "description": "If the borrower ended the opportunity, their exact words copied from the transcript. "
                               "Omit otherwise.",
            },
        },
    },
}


def _normalize_words(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()


def _verify_against_transcript(extraction: CallExtraction, transcript: str, today: date) -> CallExtraction:
    updates: dict = {}
    if extraction.deadline and not (today <= extraction.deadline <= today + MAX_DEADLINE_AHEAD):
        logger.warning("call_extraction: dropped implausible deadline (%s days from today)",
                       (extraction.deadline - today).days)
        updates["deadline"] = None
    reason = extraction.kill_reason_verbatim
    if reason and _normalize_words(reason) not in _normalize_words(transcript):
        logger.warning("call_extraction: dropped kill reason not found verbatim in the transcript")
        updates["kill_reason_verbatim"] = None
    return extraction.model_copy(update=updates) if updates else extraction


def extract_call_fields(transcript: str, *, today: date, db=None) -> CallExtraction:
    """Extract and validate the call fields from a transcript.

    Raises RuntimeError when the model returns no structured output (for
    example while the vendor-cost pause blocks calls), so a blocked call is
    never stored as an empty extraction.
    """
    from src.services.claude_router import call_claude_with_usage

    if not transcript or not transcript.strip():
        return CallExtraction()

    result = call_claude_with_usage(
        task_type=EXTRACTION_TASK_TYPE,
        messages=[{"role": "user", "content": f"TRANSCRIPT:\n{transcript}"}],
        system=(
            "You extract structured fields from lending phone call transcripts. "
            "Use only what was said on the call; leave a field out rather than guess. "
            "Never infer rates, income or credit figures. "
            f"Today is {today:%A, %Y-%m-%d}; resolve relative days against today."
        ),
        tools=[_TOOL],
        tool_choice={"type": "tool", "name": TOOL_NAME},
        db=db,
    )
    tool_input = result.get("tool_input")
    if not tool_input:
        raise RuntimeError("call extraction returned no structured output")
    try:
        extraction = CallExtraction.model_validate(tool_input)
    except ValidationError as exc:
        logger.warning("call_extraction: output failed validation (%d errors)", exc.error_count())
        raise
    return _verify_against_transcript(extraction, transcript, today)
