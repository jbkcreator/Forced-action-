"""Facts a borrower confirms on a call, kept so later scoring treats them as known.

Public records only estimate a loan's maturity (recording date plus term), so
an estimated maturity becomes a question for the caller. When the borrower
confirms the date on the call, it is recorded here and the next scoring pass
reads it as known. The decision maker being on the call is confirmed the same
way. Both are among the inputs rank 10 requires.

One row per property, holding the latest confirmation of each fact. A later
call that confirms only one fact leaves the other as it was. Does not commit.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlalchemy import text as sa_text


def record_call_confirmation(
    session,
    *,
    property_id: int,
    confirmed_at: datetime,
    caller_seat: Optional[str],
    source_call_ref: Optional[str],
    maturity_date: Optional[date] = None,
    decision_maker_on_call: Optional[bool] = None,
) -> None:
    """Store what the borrower confirmed on a call. Unconfirmed facts are left untouched."""
    if maturity_date is None and decision_maker_on_call is None:
        raise ValueError("nothing was confirmed on the call")
    if confirmed_at.tzinfo is None:
        raise ValueError("confirmed_at must be timezone-aware")
    session.execute(
        sa_text(
            """
            INSERT INTO lending.lead_call_confirmations
                (property_id, maturity_date, maturity_confirmed_at,
                 decision_maker_on_call, decision_maker_confirmed_at,
                 caller_seat, source_call_ref, updated_at)
            VALUES
                (:property_id, :maturity_date,
                 CASE WHEN CAST(:maturity_date AS date) IS NULL THEN NULL ELSE :confirmed_at END,
                 :decision_maker_on_call,
                 CASE WHEN CAST(:decision_maker_on_call AS boolean) IS NULL THEN NULL ELSE :confirmed_at END,
                 :caller_seat, :source_call_ref, :confirmed_at)
            ON CONFLICT (property_id) DO UPDATE SET
                maturity_date = COALESCE(EXCLUDED.maturity_date, lead_call_confirmations.maturity_date),
                maturity_confirmed_at = COALESCE(EXCLUDED.maturity_confirmed_at,
                                                 lead_call_confirmations.maturity_confirmed_at),
                decision_maker_on_call = COALESCE(EXCLUDED.decision_maker_on_call,
                                                  lead_call_confirmations.decision_maker_on_call),
                decision_maker_confirmed_at = COALESCE(EXCLUDED.decision_maker_confirmed_at,
                                                       lead_call_confirmations.decision_maker_confirmed_at),
                caller_seat = EXCLUDED.caller_seat,
                source_call_ref = EXCLUDED.source_call_ref,
                updated_at = EXCLUDED.updated_at
            """
        ),
        {
            "property_id": property_id,
            "maturity_date": maturity_date,
            "decision_maker_on_call": decision_maker_on_call,
            "confirmed_at": confirmed_at,
            "caller_seat": caller_seat,
            "source_call_ref": source_call_ref,
        },
    )
