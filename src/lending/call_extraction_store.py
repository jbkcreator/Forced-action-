"""Store the twelve extracted call fields, one row per dialer call.

``extract_and_store_call`` is the single call the call-record intake makes once
a call's transcript is available: it extracts the fields and upserts them, so
a re-run for the same call replaces the earlier extraction instead of adding a
second row. A blank transcript stores nothing. Does not commit.
"""
from __future__ import annotations

import json
from datetime import date, datetime
from typing import Optional

from sqlalchemy import text as sa_text

from src.lending.call_extraction import CallExtraction, extract_call_fields
from src.lending.first_contact import LENDING_TIMEZONE
from src.services.phone_utils import normalize as normalize_phone


def save_call_extraction(
    session,
    *,
    dialer_call_id: str,
    phone: Optional[str],
    extraction: CallExtraction,
    extracted_at: datetime,
) -> None:
    """Upsert the extraction for one dialer call."""
    if not dialer_call_id or not dialer_call_id.strip():
        raise ValueError("dialer_call_id is required")
    if extracted_at.tzinfo is None:
        raise ValueError("extracted_at must be timezone-aware")
    session.execute(
        sa_text(
            """
            INSERT INTO lending.call_extractions (dialer_call_id, phone, fields, extracted_at)
            VALUES (:dialer_call_id, :phone, CAST(:fields AS jsonb), :extracted_at)
            ON CONFLICT (dialer_call_id) DO UPDATE SET
                phone = EXCLUDED.phone,
                fields = EXCLUDED.fields,
                extracted_at = EXCLUDED.extracted_at
            """
        ),
        {
            "dialer_call_id": dialer_call_id.strip(),
            "phone": normalize_phone(phone) if phone else None,
            "fields": extraction.model_dump_json(),
            "extracted_at": extracted_at,
        },
    )


def extract_and_store_call(
    session,
    *,
    dialer_call_id: str,
    phone: Optional[str],
    transcript: Optional[str],
    extracted_at: datetime,
    today: Optional[date] = None,
) -> Optional[CallExtraction]:
    """Extract the fields from a call's transcript and store them. None for a blank transcript."""
    if not transcript or not transcript.strip():
        return None
    if extracted_at.tzinfo is None:
        raise ValueError("extracted_at must be timezone-aware")
    call_day = today or extracted_at.astimezone(LENDING_TIMEZONE).date()
    extraction = extract_call_fields(transcript, today=call_day, db=session)
    save_call_extraction(
        session, dialer_call_id=dialer_call_id, phone=phone,
        extraction=extraction, extracted_at=extracted_at,
    )
    return extraction


def load_call_extraction(session, dialer_call_id: str) -> Optional[CallExtraction]:
    """The stored extraction for a call, or None."""
    row = session.execute(
        sa_text("SELECT fields FROM lending.call_extractions WHERE dialer_call_id = :id"),
        {"id": dialer_call_id},
    ).scalar()
    if row is None:
        return None
    return CallExtraction.model_validate(row if isinstance(row, dict) else json.loads(row))
