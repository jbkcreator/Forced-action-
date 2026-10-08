"""Structured fields extracted from a lending call transcript.

The caller still chooses a disposition code for every call; this adds the
detail behind it, extracted from the transcript with a forced Claude tool call
through the router (cost tracking and the vendor-cost pause apply) and
validated with Pydantic before anything is stored.

The twelve frozen fields: the seven named in the brief (objection, missing
file items, next action, deadline, referral names, decision maker, verbatim
kill reason) and the five the client named for the booking qualification
(completed projects, credit above or below 640, liquidity, deal status,
property address or target market). Credit is only ever a band; an exact
score is never recorded.

Guards keep the model from inventing facts: a deadline must be a plausible
future date, the verbatim kill reason must appear word for word in the
transcript, and a value outside an allowed set is dropped rather than stored.
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
MAX_COMPLETED_PROJECTS = 500

CREDIT_BANDS = ("at_or_above_640", "below_640")
DEAL_STATUSES = ("under_contract", "actively_looking", "no_deal")


class CallExtraction(BaseModel):
    """The twelve frozen extraction fields for one call."""

    objection: Optional[str] = Field(default=None, max_length=280)
    missing_file_items: list[str] = Field(default_factory=list)
    next_action: Optional[str] = Field(default=None, max_length=200)
    deadline: Optional[date] = None
    referral_names: list[str] = Field(default_factory=list)
    decision_maker: Optional[str] = Field(default=None, max_length=120)
    kill_reason_verbatim: Optional[str] = Field(default=None, max_length=500)
    completed_projects_3y: Optional[int] = None
    credit_band: Optional[str] = None
    has_liquidity: Optional[bool] = None
    deal_status: Optional[str] = None
    property_address: Optional[str] = Field(default=None, max_length=200)
    target_market: Optional[str] = Field(default=None, max_length=120)

    @field_validator("completed_projects_3y", mode="before")
    @classmethod
    def _implausible_count_to_none(cls, value):
        """A negative, absurd or non-numeric count is dropped, not stored."""
        if value is None or isinstance(value, bool):
            return None
        try:
            count = int(value)
        except (TypeError, ValueError):
            return None
        return count if 0 <= count <= MAX_COMPLETED_PROJECTS else None

    @field_validator("credit_band", mode="before")
    @classmethod
    def _unknown_credit_band_to_none(cls, value):
        return value if value in CREDIT_BANDS else None

    @field_validator("deal_status", mode="before")
    @classmethod
    def _unknown_deal_status_to_none(cls, value):
        return value if value in DEAL_STATUSES else None

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

    @field_validator("objection", "next_action", "decision_maker", "kill_reason_verbatim",
                     "property_address", "target_market")
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
            "completed_projects_3y": {
                "type": "integer",
                "description": "How many fix-and-flip or new-construction projects the borrower says they completed "
                               "in the last 3 years. Omit if not stated.",
            },
            "credit_band": {
                "type": "string", "enum": list(CREDIT_BANDS),
                "description": "Only whether the borrower said their credit is at or above 640, or below 640. "
                               "Never an exact score. Omit if not stated.",
            },
            "has_liquidity": {
                "type": "boolean",
                "description": "Whether the borrower says they have reserves to carry a project and handle a surprise. "
                               "Omit if not discussed.",
            },
            "deal_status": {
                "type": "string", "enum": list(DEAL_STATUSES),
                "description": "under_contract: has a live deal now; actively_looking: actively in the market; "
                               "no_deal: no deal and not looking. Omit if not discussed.",
            },
            "property_address": {"type": "string", "description": "The property address discussed, if any."},
            "target_market": {
                "type": "string",
                "description": "The area or market the borrower is buying in, when they are looking without an address.",
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
            "Never infer rates, income or an exact credit score; record credit only as at or above 640 "
            "or below 640, and only if the borrower said it. "
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
