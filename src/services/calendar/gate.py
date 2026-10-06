"""
WP-GL-5: Booking gate evaluation and enforcement.

The gate is filled by a caller (internal form), not the borrower. A booking
can only proceed once the most-recent gate row for the tracked_link has
result='pass' and the list_key is not currently blocked.

Seven fields per Josh's locked bar (Oct 1 email D2, reconfirmed Oct 4 email
"Booking bar stays all seven from D2"): experience (completed_projects),
deal_status (real deal / actively looking), credit_band, liquidity, occupancy,
decision maker, and property_address-or-target_market. exit_strategy is an
eighth, optional field — captured, never validated or gated.

Financial terms must never appear in gate answers — all fields are stored as
short enum codes (e.g. "cash", "at_or_above_640"), not free text or a real
number. credit_band specifically is a caller-asked estimate, never a pulled
score — see config/booking_gate.py's module docstring for why that keeps it
out of the borrower-financial-data prohibition. This keeps gate data clear
of both the relay payload CHECK constraint and the voice-intake
_FINANCIAL_TERMS regex. See config/booking_gate.py for vocabularies.

The List 4 block is decided by our own records, not by the caller.
resolve_list_key() reads lending.calling_pool_staging.source_tag by phone and
wins over any list_key the caller sent. A contact we have no list for (a hand
loaded CSV, an inbound caller not in the pool) is not blocked and books as
usual. Per Josh's Oct 4 email, only List 4 (brokers/LOs) blocks; List 2 (cash
buyers) no longer does.
"""
from __future__ import annotations

import logging
import secrets
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text as sa_text

from config.calendar import CALENDAR_TIMEZONE
from src.services.phone_utils import normalize as normalize_phone
from config.booking_gate import (
    BLOCKED_LIST_KEYS,
    CALENDAR_DAILY_CAP,
    COMPLETED_PROJECTS,
    CREDIT_BAND_QUALIFYING_VALUES,
    CREDIT_BANDS,
    DAILY_CAP_ADVISORY_KEY,
    DEAL_STATUS_QUALIFYING_VALUES,
    DEAL_STATUS_VALUES,
    DECISION_MAKER_KILL_VALUES,
    DECISION_MAKER_VALUES,
    EXIT_STRATEGIES,
    GATE_LIST_UNBLOCK_DATE,
    GATE_RULES_VERSION,
    LIQUIDITY_KILL_VALUES,
    LIQUIDITY_SOURCES,
    OCCUPANCY_KILL_VALUES,
    OCCUPANCY_TYPES,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GateAnswers:
    """Typed, code-only representation of a caller's gate form answers.

    property_address is optional when deal_status == "actively_looking" —
    D2: "property address or target market" — in which case target_market
    is the field that must be populated instead.

    liquidity_amount is the brief's "rough amount" (p.1) — a caller-reported
    ballpark, not a verified figure. No format is specified, so it is stored
    as free-form text and is not validated or gated on; it never fails the
    gate on its own.

    exit_strategy is captured but fully optional per D2 — it is never
    validated and never fails the gate, regardless of value (including
    blank/None).
    """

    liquidity_source: str
    completed_projects: str
    occupancy: str
    decision_maker: str
    deal_status: str
    credit_band: str
    exit_strategy: Optional[str] = None
    property_address: Optional[str] = None
    target_market: Optional[str] = None
    liquidity_amount: Optional[str] = None


@dataclass(frozen=True)
class GateResult:
    passed: bool
    failed_field: Optional[str]
    reason: Optional[str]


def evaluate_gate(answers: GateAnswers) -> GateResult:
    """Pure evaluation — no DB. Returns pass/fail + first failing field.

    Auto-kill conditions (fail regardless of anything else):
      - liquidity_source in LIQUIDITY_KILL_VALUES ("none")
      - occupancy in OCCUPANCY_KILL_VALUES ("homestead")
      - decision_maker in DECISION_MAKER_KILL_VALUES ("no")
      - neither property_address nor target_market present

    Unknown codes fail validation with reason='invalid_code'.

    Qualification (book vs nurture) per Josh's Oct 4 email §2 — not a kill,
    a routing decision: PASS requires (deal_status is real_deal or
    actively_looking) AND (completed_projects != "0") AND
    (credit_band == "at_or_above_640"). Anything short of all three routes
    to nurture with reason='insufficient_qualification', same as the
    original brief's "loose for launch" bar — 0 experience or sub-640 credit
    does not kill the record, it defers it.

    exit_strategy is never checked here — D2 makes it optional.
    """
    if answers.liquidity_source not in LIQUIDITY_SOURCES:
        return GateResult(passed=False, failed_field="liquidity_source", reason="invalid_code")
    if answers.liquidity_source in LIQUIDITY_KILL_VALUES:
        return GateResult(passed=False, failed_field="liquidity_source", reason="no_liquidity")

    if answers.occupancy not in OCCUPANCY_TYPES:
        return GateResult(passed=False, failed_field="occupancy", reason="invalid_code")
    if answers.occupancy in OCCUPANCY_KILL_VALUES:
        return GateResult(passed=False, failed_field="occupancy", reason="homestead")

    if answers.decision_maker not in DECISION_MAKER_VALUES:
        return GateResult(passed=False, failed_field="decision_maker", reason="invalid_code")
    if answers.decision_maker in DECISION_MAKER_KILL_VALUES:
        return GateResult(passed=False, failed_field="decision_maker", reason="not_decision_maker")

    if answers.deal_status not in DEAL_STATUS_VALUES:
        return GateResult(passed=False, failed_field="deal_status", reason="invalid_code")

    if answers.credit_band not in CREDIT_BANDS:
        return GateResult(passed=False, failed_field="credit_band", reason="invalid_code")

    if answers.completed_projects not in COMPLETED_PROJECTS:
        return GateResult(passed=False, failed_field="completed_projects", reason="invalid_code")

    has_address = bool(answers.property_address and answers.property_address.strip())
    has_target_market = bool(answers.target_market and answers.target_market.strip())
    if not has_address and not has_target_market:
        return GateResult(
            passed=False, failed_field="property_address", reason="missing_address_or_market"
        )

    qualifies = (
        answers.deal_status in DEAL_STATUS_QUALIFYING_VALUES
        and answers.completed_projects != "0"
        and answers.credit_band in CREDIT_BAND_QUALIFYING_VALUES
    )
    if not qualifies:
        return GateResult(
            passed=False, failed_field=None, reason="insufficient_qualification"
        )

    return GateResult(passed=True, failed_field=None, reason=None)


def store_gate(
    session,
    *,
    answers: GateAnswers,
    tracked_link_id: Optional[int] = None,
    person_id: Optional[int] = None,
    list_key: Optional[str] = None,
    captured_by: Optional[str] = None,
) -> tuple[str, "GateResult"]:
    """Evaluate and durably store a gate attempt. Returns (gate_id, result).

    Always writes a row regardless of pass/fail so the caller-bonus
    calculation has a complete audit trail. ``list_key`` is stored trimmed and
    lower-cased; blank is stored as NULL (list unknown, not blocked).
    """
    list_key = (list_key or "").strip().lower() or None
    result = evaluate_gate(answers)
    gate_id = secrets.token_urlsafe(16)

    # Store codes only — property_address, target_market and
    # liquidity_amount are the three free-text fields, and all live in
    # answers JSONB on the gate table only, never in relay payload.
    answers_dict = {
        "liquidity_source": answers.liquidity_source,
        "liquidity_amount": answers.liquidity_amount,
        "completed_projects": answers.completed_projects,
        "deal_status": answers.deal_status,
        "credit_band": answers.credit_band,
        "exit_strategy": answers.exit_strategy,
        "occupancy": answers.occupancy,
        "decision_maker": answers.decision_maker,
        "property_address": answers.property_address,
        "target_market": answers.target_market,
    }

    import json

    session.execute(
        sa_text(
            """
            INSERT INTO fa_max_booking_gates
                (gate_id, tracked_link_id, person_id, answers, result,
                 failed_field, list_key, rules_version, captured_by, evaluated_at)
            VALUES
                (:gate_id, :tracked_link_id, :person_id, CAST(:answers AS jsonb), :result,
                 :failed_field, :list_key, :rules_version, :captured_by, NOW())
            """
        ),
        {
            "gate_id": gate_id,
            "tracked_link_id": tracked_link_id,
            "person_id": person_id,
            "answers": json.dumps(answers_dict),
            "result": "pass" if result.passed else "fail",
            "failed_field": result.failed_field,
            "list_key": list_key,
            "rules_version": GATE_RULES_VERSION,
            "captured_by": captured_by,
        },
    )
    session.commit()

    logger.info(
        "gate.store: gate_id=%s tracked_link_id=%s result=%s failed_field=%s",
        gate_id, tracked_link_id, "pass" if result.passed else "fail", result.failed_field,
    )

    if not result.passed:
        from src.services.calendar.nurture import enqueue_nurture

        enqueue_nurture(
            session,
            gate_id=gate_id,
            tracked_link_id=tracked_link_id,
            person_id=person_id,
            failed_field=result.failed_field,
            fail_reason=result.reason,
            list_key=list_key,
        )

    return gate_id, result


def get_passed_gate_for_link(session, tracked_link_id: int) -> Optional[str]:
    """Return the gate_id of the link's gate, if its most recent attempt passed.

    Takes the single latest row for the link regardless of result, then
    requires that row to be a pass — a stale pass from an earlier call must
    never outrank a later re-screening that failed (e.g. a second call
    discovers the property is a homestead). Filtering on result='pass' before
    ordering would let an old pass win over a fresh disqualification.

    Also enforces the list-block: a passed gate whose list_key is currently
    blocked (BLOCKED_LIST_KEYS, not yet unblocked by GATE_LIST_UNBLOCK_DATE)
    is treated as no valid gate.

    Returns gate_id or None.
    """
    row = session.execute(
        sa_text(
            """
            SELECT gate_id, list_key, result
            FROM fa_max_booking_gates
            WHERE tracked_link_id = :link_id
            ORDER BY evaluated_at DESC
            LIMIT 1
            """
        ),
        {"link_id": tracked_link_id},
    ).mappings().first()

    if row is None or row["result"] != "pass":
        return None

    if _is_list_blocked(row["list_key"]):
        logger.info(
            "gate: tracked_link_id=%s blocked — list_key=%s blocked until %s",
            tracked_link_id, row["list_key"], GATE_LIST_UNBLOCK_DATE,
        )
        return None

    return row["gate_id"]


def get_passed_gate_by_id(session, gate_id: str) -> Optional[Any]:
    """Return the gate row if gate_id exists and passed. None otherwise.

    Includes answers/captured_by so book() can forward property_address and
    booked_by to the booking-confirmed payload without a second query.
    """
    row = session.execute(
        sa_text(
            "SELECT gate_id, list_key, result, answers, captured_by "
            "FROM fa_max_booking_gates WHERE gate_id = :gate_id"
        ),
        {"gate_id": gate_id},
    ).mappings().first()

    if row is None or row["result"] != "pass":
        return None

    if _is_list_blocked(row["list_key"]):
        return None

    return row


def _is_list_blocked(list_key: Optional[str]) -> bool:
    """True if this list_key is blocked and the unblock date has not passed.

    Compared trimmed and case-insensitively. An unknown (empty) list is not blocked.
    """
    key = (list_key or "").strip().lower()
    if not key or key not in BLOCKED_LIST_KEYS:
        return False
    return date.today() < GATE_LIST_UNBLOCK_DATE


def resolve_list_key(session, phone: Optional[str]) -> Optional[str]:
    """The contact's source list, from lending.calling_pool_staging.source_tag by phone.

    A number staged under a blocked list resolves to that list even if it also
    appears under another, so a broker cannot book because the same number is
    also in a builder pool. None when the number is not staged or has no tag
    (the contact is then not blocked).

    ponytail: matches any staging run, so a number once staged as List 4 stays
    blocked after it moves lists. Narrow to the latest run if that over-blocks.
    """
    e164 = normalize_phone(phone)
    if not e164:
        return None
    tags = session.execute(
        sa_text(
            """
            SELECT DISTINCT lower(btrim(source_tag)) FROM lending.calling_pool_staging
            WHERE normalized_phone = :phone AND btrim(source_tag) <> ''
            """
        ),
        {"phone": e164},
    ).scalars().all()
    found = sorted(tag for tag in tags if tag)
    blocked = [tag for tag in found if tag in BLOCKED_LIST_KEYS]
    if blocked:
        return blocked[0]
    return found[0] if found else None


def enforce_daily_cap(session, slot_start: datetime) -> bool:
    """Returns True if the calendar day *slot_start* falls on still has capacity.

    The cap is per calendar day of the booking being made, not per day the
    request happens to arrive — a booking for next Tuesday must be checked
    against next Tuesday's count, not today's. The day boundary is computed
    in CALENDAR_TIMEZONE (the business's wall-clock day), not UTC, so it
    doesn't shift at the wrong moment relative to "6 to 8 held calls a day."

    Uses pg_advisory_xact_lock so concurrent requests serialize rather than
    racing. Counts 'pending' and 'confirmed' bookings only — cancelled slots
    free their slot for the day.

    Must be called inside a transaction that is committed before book() is
    called, or the lock is held across the provider round-trip.
    """
    session.execute(
        sa_text("SELECT pg_advisory_xact_lock(:key)"),
        {"key": DAILY_CAP_ADVISORY_KEY},
    )

    day_start, day_end = _calendar_day_bounds(slot_start)
    count_row = session.execute(
        sa_text(
            """
            SELECT COUNT(*) AS n
            FROM fa_max_bookings
            WHERE starts_at >= :day_start
              AND starts_at < :day_end
              AND status IN ('pending', 'confirmed')
            """
        ),
        {"day_start": day_start, "day_end": day_end},
    ).mappings().first()

    held = count_row["n"] if count_row else 0
    return held < CALENDAR_DAILY_CAP


def _calendar_day_bounds(moment: datetime) -> tuple[datetime, datetime]:
    """[start, end) of *moment*'s calendar day in CALENDAR_TIMEZONE, as UTC-aware bounds."""
    tz = ZoneInfo(CALENDAR_TIMEZONE)
    local_date = moment.astimezone(tz).date()
    local_start = datetime(local_date.year, local_date.month, local_date.day, tzinfo=tz)
    return local_start, local_start + timedelta(days=1)
