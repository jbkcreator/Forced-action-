"""PropertyRadar calling lists: the search criteria for each list, and how a bought record is stored.

Pure functions, no database and no network: the pull task (src/tasks/lending_pr_calling_list_pull.py)
buys records with these criteria and stores what ``shape_record`` returns. List definitions and
thresholds are in config/lending_calling_lists.py.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Optional

from config.lending_calling_lists import (
    CASH_BUYER,
    CASH_BUYER_PORTFOLIO_RANGE,
    COMPANY_OWNED_ONLY_STATES,
    CONFIRMED,
    DEFAULT_PULL_STATE,
    HARD_MONEY_LENDER_NAMES,
    HOLD_UNKNOWN,
    INVESTOR_PURCHASE,
    LIKELY,
    MATURITY_DAYS_OUT,
    MATURITY_OUTSIDE_WINDOW,
    MATURITY_TRIGGER_DAYS,
    NIGHTLY_LOOKBACK_DAYS,
    PULL_STATES,
    RESOLD_AFTER_HOLD_LIMIT,
    SELLER_UNKNOWN,
    VERIFIED_MATURITY,
    WHOLESALER,
    WHOLESALER_FLIP_TYPES,
    WHOLESALER_MAX_HOLD_MONTHS,
    WINDOWED_LISTS,
    date_range_value,
    maturity_recording_windows,
)
from src.services.property_radar_normalizer import compute_maturity, extract_principal, parse_date, parse_term

_NON_ALNUM = re.compile(r"[^A-Z0-9]+")


@dataclass(frozen=True)
class CriteriaSet:
    """One PropertyRadar search. A list can need several: criteria are AND-ed, so "private OR named
    lender" and each assumed maturity term are separate searches."""

    label: str
    criteria: list[dict]


@dataclass(frozen=True)
class PullWindow:
    start: date
    end: date


def pull_window(list_key: str, *, as_of: date, start: Optional[date], end: Optional[date]) -> PullWindow:
    """The window recorded on the pull: the maturity dates for Verified maturity (90-180 days out unless given),
    else the recording dates."""
    if list_key == VERIFIED_MATURITY and start is None and end is None:
        near, far = (as_of + timedelta(days=d) for d in MATURITY_DAYS_OUT)
        return PullWindow(near, far)
    if start is None or end is None or start > end:
        raise ValueError(f"{list_key} needs a window with start <= end")
    return PullWindow(start, end)


def nightly_window(list_key: str, *, as_of: date) -> PullWindow:
    """The nightly search window, ``NIGHTLY_LOOKBACK_DAYS`` long. Verified maturity: maturity dates ending
    ``MATURITY_TRIGGER_DAYS`` after ``as_of``. Other lists: recording dates ending yesterday."""
    if list_key == VERIFIED_MATURITY:
        end = as_of + timedelta(days=MATURITY_TRIGGER_DAYS)
    elif list_key in WINDOWED_LISTS:
        end = as_of - timedelta(days=1)
    else:
        raise ValueError(f"{list_key} has no nightly window")
    return PullWindow(end - timedelta(days=NIGHTLY_LOOKBACK_DAYS - 1), end)


def _investor_owner(state: str) -> dict:
    """Florida: not owner-occupied. A company-owned-only state: company ownership instead (its owner-occupied
    flag is unreliable, and only companies may be called there)."""
    if state in COMPANY_OWNED_ONLY_STATES:
        return {"name": "OwnershipType", "value": ["Corporate"]}
    return {"name": "isSameMailingOrExempt", "value": [0]}


def build_criteria_sets(list_key: str, *, as_of: date, window: PullWindow,
                        state: str = DEFAULT_PULL_STATE) -> list[CriteriaSet]:
    """The searches for one list in one state. Verified maturity's ``window`` holds maturity dates: each assumed
    term searches the recording dates that mature inside it."""
    if state not in PULL_STATES:
        raise ValueError(f"unsupported pull state: {state!r}")
    in_state = {"name": "State", "value": [state]}
    investor = _investor_owner(state)
    if list_key == VERIFIED_MATURITY:
        common = [in_state, investor, {"name": "isListedForSale", "value": [0]}]
        private = [{"name": "FirstLoanType", "value": ["P"]}]
        lender = [{"name": "FirstLenderOriginal", "value": list(HARD_MONEY_LENDER_NAMES)}]
        sets = []
        for term, (rec_start, rec_end) in maturity_recording_windows(window.start, window.end).items():
            recorded = {"name": "FirstDate", "value": [date_range_value(rec_start, rec_end)]}
            sets.append(CriteriaSet(f"{term}yr_private", common + [recorded] + private))
            sets.append(CriteriaSet(f"{term}yr_hard_money_lender", common + [recorded] + lender))
        return sets
    recorded = {"name": "LastTransferRecDate", "value": [date_range_value(window.start, window.end)]}
    low, high = CASH_BUYER_PORTFOLIO_RANGE
    portfolio = {"name": "NumberPropertiesOwned", "value": [[low, high]]}
    if list_key == CASH_BUYER:
        return [CriteriaSet("cash_buyers", [
            in_state, recorded, {"name": "LastTransferDownPaymentPercent", "value": [[100, None]]}, investor, portfolio,
        ])]
    if list_key == INVESTOR_PURCHASE:
        return [CriteriaSet("investor_purchases", [in_state, recorded, investor, portfolio])]
    if list_key == WHOLESALER:
        # The hold limit is also a search criterion, so records resold too late to qualify are never bought;
        # shape_record still applies it to every stored record.
        return [CriteriaSet("flips", [
            in_state, recorded, {"name": "LastTransferType", "value": list(WHOLESALER_FLIP_TYPES)},
            {"name": "PriorTransferMonthsSince", "value": [[0, WHOLESALER_MAX_HOLD_MONTHS]]},
        ])]
    raise ValueError(f"unknown calling list: {list_key!r}")


def contact_key(*parts: Optional[str]) -> Optional[str]:
    """Stable key for one person: the name parts normalized (case, punctuation, spacing) and hashed.
    None when the first part (the name) is missing."""
    if not parts or not (parts[0] or "").strip():
        return None
    normalized = "|".join(_NON_ALNUM.sub(" ", (p or "").upper()).strip() for p in parts)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _decimal(value: Any) -> Optional[Decimal]:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _small_int(value: Any) -> Optional[int]:
    number = _decimal(value)
    return int(number) if number is not None else None


def _text(raw: Mapping[str, Any], key: str) -> Optional[str]:
    value = raw.get(key)
    return str(value).strip() or None if value is not None else None


def _qualification(list_key: str, raw: Mapping[str, Any], *, as_of: date,
                   maturity: Optional[date], term: Optional[int]) -> tuple[Optional[str], Optional[str]]:
    """(match_strength, exclusion_reason): exactly one of them is set."""
    if list_key == VERIFIED_MATURITY:
        if not term:
            return LIKELY, None
        near, far = (as_of + timedelta(days=d) for d in MATURITY_DAYS_OUT)
        if maturity is not None and near <= maturity <= far:
            return CONFIRMED, None
        return None, MATURITY_OUTSIDE_WINDOW
    if list_key == WHOLESALER:
        if not _text(raw, "LastTransferSeller"):
            return None, SELLER_UNKNOWN
        months = _small_int(raw.get("PriorTransferMonthsSince"))
        if months is None:
            return None, HOLD_UNKNOWN
        if months > WHOLESALER_MAX_HOLD_MONTHS:
            return None, RESOLD_AFTER_HOLD_LIMIT
        return CONFIRMED, None
    return CONFIRMED, None


def shape_record(list_key: str, raw: Mapping[str, Any], *, as_of: date) -> dict[str, Any]:
    """The ``lending.pr_list_records`` row for one bought record (without ids and timestamps)."""
    recorded = parse_date(raw.get("FirstDate"))
    term = parse_term(raw.get("FirstTermInYears"))
    maturity = compute_maturity(recorded, term)
    strength, exclusion = _qualification(list_key, raw, as_of=as_of, maturity=maturity, term=term)
    seller = _text(raw, "LastTransferSeller")
    owner = _text(raw, "Owner")
    if list_key == WHOLESALER:
        key = contact_key(seller)
    else:
        key = contact_key(owner, _text(raw, "OwnerAddress"), _text(raw, "OwnerZipFive"))
    return {
        "list_key": list_key,
        "radar_id": str(raw["RadarID"]),
        "state": _text(raw, "State"),
        "county": _text(raw, "County"),
        "apn": _text(raw, "APN"),
        "property_address": _text(raw, "Address"),
        "city": _text(raw, "City"),
        "zip": _text(raw, "ZipFive"),
        "property_type": _text(raw, "PType"),
        "owner_name": owner,
        "ownership_type": _text(raw, "OwnershipType"),
        "owner_mailing_address": _text(raw, "OwnerAddress"),
        "owner_mailing_city": _text(raw, "OwnerCity"),
        "owner_mailing_state": _text(raw, "OwnerState"),
        "owner_mailing_zip": _text(raw, "OwnerZipFive"),
        "principal_name": extract_principal(raw.get("Persons") or []),
        "loan_type": _text(raw, "FirstLoanType"),
        "lender_name": _text(raw, "FirstLenderOriginal"),
        "loan_recorded_date": recorded,
        "loan_amount": _decimal(raw.get("FirstAmount")),
        "loan_term_years": term,
        "est_maturity_date": maturity,
        "last_transfer_date": parse_date(raw.get("LastTransferRecDate")),
        "last_transfer_value": _decimal(raw.get("LastTransferValue")),
        "last_transfer_seller": seller,
        "last_transfer_type": _text(raw, "LastTransferType"),
        "down_payment_pct": _decimal(raw.get("LastTransferDownPaymentPercent")),
        "prior_transfer_months": _small_int(raw.get("PriorTransferMonthsSince")),
        "qualifies": exclusion is None,
        "match_strength": strength,
        "exclusion_reason": exclusion,
        "contact_key": key,
        "raw": dict(raw),
    }
