"""Lending Wave 0 compliance floor (WP-W0-2 / WP-W0-3 / WP-W0-8).

- ``filter_loadable`` — list-build gate: "is this number clean to load?"
  Order (spec §3 pipeline): phone valid → Georgia entity stop → internal
  suppression → fresh Tracerfy scrub (national DNC, state DNC, litigator).
- ``can_dial_now`` — call-time gate: calling window + rolling attempt cap.
- ``propagate_opt_out`` / ``poll_fa_opt_outs`` — global stop-propagation.
- ``reconcile_suppression`` — schema-parameterised backfill used by the migration.

Batch only: one query per gate for the whole list. Never commits — the caller
owns the transaction.
"""
from __future__ import annotations

import hashlib
from collections import defaultdict
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

from src.lending.dialer_removal import DialerRemovalUndecided
from config.lending_compliance import (
    AREA_CODE_TZ,
    ATTEMPT_HISTORY_BUSINESS_DAYS,
    ATTEMPT_PERIOD_HOURS,
    CALL_WINDOW_END,
    CALL_WINDOW_START,
    ET_WINDOW_END,
    ET_WINDOW_START,
    SHIFT_GROUPS,
    DEFAULT_TZ,
    DIALER_SWEEP_LOCK_KEY,
    DIALER_OPT_OUT_SOURCE,
    DNC_SCRUB_MAX_AGE_DAYS,
    SCRUB_STALE_BREAKER_MIN_POOL,
    SCRUB_STALE_BREAKER_PCT,
    GHL_DND_BATCH,
    GEORGIA_ALLOWED_ENTITY_TYPES,
    HOMESTEAD_GATE_EXCLUDED_SOURCE_TAGS,
    INVESTOR_ENTITY_TYPES,
    MAX_ATTEMPTS_PER_PERIOD,
    MAX_ATTEMPTS_TOTAL,
    OPT_OUT_EXCLUDED_SOURCES,
    OPT_OUT_POLL_LOCK_KEY,
    SMS_OPT_OUT_SOURCES,
    STOP_PROPAGATION_SLA_SECONDS,
    TRACERFY_DNC_SOURCE,
    OptOutChannel,
    OptOutStatus,
    RemovalReason,
    ReasonCode,
    SuppressionReason,
)
from src.services.phone_utils import normalize as normalize_phone
from src.utils.logger import get_logger

logger = get_logger(__name__)

Scrubber = Callable[[list[str]], list[dict]]
DialerRemover = Callable[..., None]  # remover(phone, *, reason: str) — dialer_port.Dialer.remove
DialerRestorer = Callable[[str], None]  # restorer(phone) — dialer_port.Dialer.restore
LoadedPhones = Callable[[object], list[str]]


@dataclass(frozen=True)
class GateResult:
    phone: str
    allowed: bool
    reason: Optional[ReasonCode] = None


@dataclass(frozen=True)
class ScrubResult:
    national_dnc: bool
    litigator: bool
    state_dnc: bool
    checked_at: datetime
    line_type: Optional[str] = None


def _blocked(phone: str, reason: ReasonCode) -> GateResult:
    return GateResult(phone=phone, allowed=False, reason=reason)


def _is_yes(value: Optional[str]) -> bool:
    return (value or "").strip().upper() in ("Y", "YES", "TRUE", "1")


def phone_hash(phone: str) -> str:
    return hashlib.sha256(phone.encode()).hexdigest()


# ── List-build filter (WP-W0-2 + Georgia stop) ───────────────────────────────


def _georgia_blocked(record: dict) -> bool:
    if (record.get("state") or "").strip().upper() != "GA":
        return False
    return (record.get("entity_status") or "").strip().upper() not in GEORGIA_ALLOWED_ENTITY_TYPES


def _homestead_blocked(record: dict) -> bool:
    """F8 (Josh, Oct 4 §2): "Owner is an LLC, LP or corporation, or a non owner
    occupied investor. Homestead is out." Does not apply to List 4 (brokers/LOs)
    — a professional referral list, never screened as a property owner.

    Blocks only a *confirmed* homestead-exempt property (``homestead_exempt is
    True``). Josh's rule describes two allowed categories and says nothing about
    an unverified status, and financials.homestead_exempt has few confirmed values
    yet — nearly every current record is NULL. Treating NULL as blocked would gate
    out virtually the entire pool, directly against his #1 stated priority (lead
    volume). Unknown passes through like every other unscored field here; this
    narrows automatically as real homestead data backfills in.
    """
    if (record.get("source_tag") or "").strip().lower() in HOMESTEAD_GATE_EXCLUDED_SOURCE_TAGS:
        return False
    if (record.get("entity_status") or "").strip().upper() in INVESTOR_ENTITY_TYPES:
        return False
    return record.get("homestead_exempt") is True


def _suppressed_phones(db, phones: list[str]) -> set[str]:
    rows = db.execute(
        text(
            "SELECT phone FROM lending.suppression_list WHERE phone = ANY(:phones) "
            "UNION "
            "SELECT phone FROM sms_opt_outs WHERE phone = ANY(:phones) AND source <> :tracerfy"
        ),
        {"phones": phones, "tracerfy": TRACERFY_DNC_SOURCE},
    ).fetchall()
    return {r[0] for r in rows}


def suppress_warm_network_phones(db, phones: list[str], *, source_ref: Optional[str] = None) -> int:
    """Permanently suppress Josh's warm network from the cold dialer queue (Oct 4
    §2: "not in my warm network, permanently suppressed from the cold queue"). Not
    an opt-out — he still works these relationships himself — so it is its own
    SuppressionReason, but it is checked by the exact same ``_suppressed_phones``
    gate every other suppression_list row already goes through: no second filter
    path to keep in sync. Idempotent (ON CONFLICT on the unique phone column); does
    not commit."""
    normalized = sorted({p for p in (normalize_phone(x) for x in phones) if p})
    if not normalized:
        return 0
    db.execute(
        text(
            "INSERT INTO lending.suppression_list (phone, reason, source_channel, source_ref) "
            "VALUES (:phone, :reason, 'warm_network', :source_ref) ON CONFLICT (phone) DO NOTHING"
        ),
        [{"phone": p, "reason": SuppressionReason.WARM_NETWORK.value, "source_ref": source_ref} for p in normalized],
    )
    return len(normalized)


def _stored_scrubs(db, phones: list[str]) -> dict[str, ScrubResult]:
    """Freshest scrub per phone across FA's cache (read-only) and lending's own."""
    rows = db.execute(
        text(
            "SELECT DISTINCT ON (phone) phone, national_dnc, litigator, state_dnc, checked_at, line_type FROM ("
            "  SELECT phone, national_dnc, litigator, raw_result->>'state_dnc' AS state_dnc, checked_at, "
            "         raw_result->>'phone_type' AS line_type "
            "  FROM dnc_phone_checks WHERE phone = ANY(:phones) "
            "  UNION ALL "
            "  SELECT phone, national_dnc, litigator, CASE WHEN state_dnc THEN 'Y' ELSE 'N' END, checked_at, line_type "
            "  FROM lending.dnc_scrubs WHERE phone = ANY(:phones)"
            ") s ORDER BY phone, checked_at DESC"
        ),
        {"phones": phones},
    ).fetchall()
    return {r[0]: ScrubResult(r[1], r[2], _is_yes(r[3]), r[4], r[5]) for r in rows}


def _verdict(phone: str, scrub: ScrubResult) -> GateResult:
    if scrub.litigator:
        return _blocked(phone, ReasonCode.LITIGATOR)
    if scrub.national_dnc:
        return _blocked(phone, ReasonCode.NATIONAL_DNC)
    if scrub.state_dnc:
        return _blocked(phone, ReasonCode.STATE_DNC)
    return GateResult(phone=phone, allowed=True)


def filter_loadable(
    records: list[dict],
    db,
    *,
    now: Optional[datetime] = None,
    scrubber: Scrubber,
    run_id: Optional[str] = None,
) -> list[GateResult]:
    """One result per input record, same order. With ``run_id``, every block is
    written to lending.load_exclusions (A2 evidence)."""
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=DNC_SCRUB_MAX_AGE_DAYS)

    early: dict[int, GateResult] = {}
    phones: list[Optional[str]] = []
    for i, record in enumerate(records):
        raw = record.get("normalized_phone") or record.get("phone") or ""
        phone = normalize_phone(raw)
        phones.append(phone)
        if not phone:
            early[i] = _blocked(raw, ReasonCode.INVALID_PHONE)
        elif _georgia_blocked(record):
            early[i] = _blocked(phone, ReasonCode.GA_NATURAL_PERSON)
        elif _homestead_blocked(record):
            early[i] = _blocked(phone, ReasonCode.HOMESTEAD_OWNER_OCCUPIED)

    candidates = sorted({p for i, p in enumerate(phones) if p and i not in early})
    by_phone: dict[str, GateResult] = {}

    suppressed = _suppressed_phones(db, candidates) if candidates else set()
    by_phone.update({p: _blocked(p, ReasonCode.SUPPRESSED) for p in suppressed})
    candidates = [p for p in candidates if p not in suppressed]

    exhausted = _attempt_history_exhausted_phones(db, candidates, now) if candidates else set()
    by_phone.update({p: _blocked(p, ReasonCode.ATTEMPT_HISTORY_EXCEEDED) for p in exhausted})
    candidates = [p for p in candidates if p not in exhausted]

    scrubs = _stored_scrubs(db, candidates) if candidates else {}
    fresh = {p: s for p, s in scrubs.items() if s.checked_at >= cutoff}
    stale = [p for p in candidates if p not in fresh]
    if stale:
        fresh.update(_scrub(db, stale, scrubber))
    for phone in candidates:
        scrub = fresh.get(phone)
        by_phone[phone] = _verdict(phone, scrub) if scrub else _blocked(phone, ReasonCode.SCRUB_FAILED)

    _stamp_contacts(db, fresh)
    _flag_nurture(db, [p for p, g in by_phone.items() if g.reason in NURTURE_REASONS])
    results = [early.get(i) or by_phone[phones[i]] for i in range(len(records))]
    if run_id:
        _record_exclusions(db, run_id, results)
    return results


def _scrub(db, phones: list[str], scrubber: Scrubber) -> dict[str, ScrubResult]:
    """One Tracerfy batch for every stale/missing phone. Unreturned phones are absent."""
    try:
        rows = scrubber(phones)
    except Exception as exc:
        logger.error("[lending-compliance] Tracerfy scrub failed for %d phones: %s", len(phones), _error_kind(exc))
        return {}

    wanted = set(phones)
    checked_at = datetime.now(timezone.utc)
    scrubbed: dict[str, ScrubResult] = {}
    raw_by_phone: dict[str, dict] = {}
    for row in rows:
        phone = normalize_phone(str(row.get("phone") or "").strip())
        if phone not in wanted:
            continue
        scrubbed[phone] = ScrubResult(
            national_dnc=_is_yes(row.get("national_dnc")),
            litigator=_is_yes(row.get("litigator")),
            state_dnc=_is_yes(row.get("state_dnc")),
            checked_at=checked_at,
            line_type=row.get("phone_type") or None,
        )
        raw_by_phone[phone] = row
    if not scrubbed:
        return {}

    db.execute(
        text(
            "INSERT INTO lending.dnc_scrubs "
            "(phone, national_dnc, litigator, state_dnc, line_type, checked_at, raw_result) "
            "VALUES (:phone, :national_dnc, :litigator, :state_dnc, :line_type, :checked_at, "
            "CAST(:raw_result AS jsonb)) "
            "ON CONFLICT (phone) DO UPDATE SET national_dnc = EXCLUDED.national_dnc, "
            "litigator = EXCLUDED.litigator, state_dnc = EXCLUDED.state_dnc, line_type = EXCLUDED.line_type, "
            "checked_at = EXCLUDED.checked_at, raw_result = EXCLUDED.raw_result"
        ),
        [
            {
                "phone": p,
                "national_dnc": s.national_dnc,
                "litigator": s.litigator,
                "state_dnc": s.state_dnc,
                "line_type": s.line_type,
                "checked_at": s.checked_at,
                "raw_result": json.dumps(raw_by_phone[p]),
            }
            for p, s in scrubbed.items()
        ],
    )
    litigators = [p for p, s in scrubbed.items() if s.litigator]
    if litigators:
        db.execute(
            text(
                "INSERT INTO lending.suppression_list (phone, reason, source_channel) "
                "SELECT unnest(CAST(:phones AS varchar[])), :reason, 'tracerfy_scrub' "
                "ON CONFLICT (phone) DO NOTHING"
            ),
            {"phones": litigators, "reason": SuppressionReason.LITIGATOR.value},
        )
    return scrubbed


def _stamp_contacts(db, scrubs: dict[str, ScrubResult]) -> None:
    """Every scrubbed number carries its scrub time (spec §4.2) and line type."""
    if not scrubs:
        return
    phones = list(scrubs)
    db.execute(
        text(
            "INSERT INTO lending.contacts (phone, phone_hash, last_dnc_scrub, line_type) "
            "SELECT * FROM unnest(CAST(:phones AS varchar[]), CAST(:hashes AS varchar[]), "
            "CAST(:checked AS timestamptz[]), CAST(:line_types AS varchar[])) "
            "ON CONFLICT (phone) DO UPDATE SET last_dnc_scrub = EXCLUDED.last_dnc_scrub, "
            "phone_hash = EXCLUDED.phone_hash, "
            "line_type = COALESCE(EXCLUDED.line_type, lending.contacts.line_type)"
        ),
        {
            "phones": phones,
            "hashes": [phone_hash(p) for p in phones],
            "checked": [scrubs[p].checked_at for p in phones],
            "line_types": [scrubs[p].line_type for p in phones],
        },
    )


NURTURE_REASONS = frozenset({ReasonCode.NATIONAL_DNC, ReasonCode.STATE_DNC, ReasonCode.ATTEMPT_HISTORY_EXCEEDED})


def _flag_nurture(db, phones: list[str]) -> None:
    """A DNC block is a phone rule only: the contact stays eligible for the shared
    (email) nurture destination. Litigators and opt-outs are never flagged."""
    if phones:
        db.execute(text("UPDATE lending.contacts SET nurture = true WHERE phone = ANY(:phones)"), {"phones": phones})


def _record_exclusions(db, run_id: str, results: list[GateResult]) -> None:
    blocked = [r for r in results if not r.allowed]
    if not blocked:
        return
    db.execute(
        text(
            "INSERT INTO lending.load_exclusions (run_id, phone_hash, reason) "
            "VALUES (:run_id, :phone_hash, :reason)"
        ),
        [{"run_id": run_id, "phone_hash": phone_hash(r.phone), "reason": r.reason.value} for r in blocked],
    )


def tracerfy_scrub(phones: list[str]) -> list[dict]:
    """Live Tracerfy DNC batch via FA's existing submit/poll helpers (ADR 0002)."""
    from config.settings import get_settings
    from src.tasks.dnc_refresh import _poll_queue, _submit_scrub_batch

    key = get_settings().tracerfy_api_key
    if not key:
        raise RuntimeError("TRACERFY_API_KEY not configured")
    api_key = key.get_secret_value()
    return _poll_queue(_submit_scrub_batch(phones, api_key), api_key)


# ── A2 coverage check (spec §12) ──────────────────────────────────────────────


@dataclass(frozen=True)
class CoverageGap:
    phone: str
    reason: ReasonCode


@dataclass(frozen=True)
class A2Report:
    checked: int
    gaps: list[CoverageGap]
    gap_counts: dict[str, int]

    @property
    def passed(self) -> bool:
        return not self.gaps


def coverage_gaps(db, phones: list[str], *, now: Optional[datetime] = None) -> list[CoverageGap]:
    """Every number in ``phones`` that would violate A2: no scrub within 31 days
    (FA or lending cache), on any suppression store, or a positive DNC/litigator
    result. Read-only and never calls Tracerfy — it re-checks stored evidence with
    the same lookups filter_loadable uses. An empty list means A2 passes."""
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=DNC_SCRUB_MAX_AGE_DAYS)

    gaps: list[CoverageGap] = []
    normalized: dict[str, str] = {}
    for raw in dict.fromkeys(phones):
        phone = normalize_phone(raw)
        if phone:
            normalized.setdefault(phone, raw)
        else:
            gaps.append(CoverageGap(phone=raw, reason=ReasonCode.INVALID_PHONE))

    candidates = sorted(normalized)
    if not candidates:
        return gaps
    suppressed = _suppressed_phones(db, candidates)
    scrubs = _stored_scrubs(db, candidates)
    for phone in candidates:
        scrub = scrubs.get(phone)
        if phone in suppressed:
            gaps.append(CoverageGap(phone, ReasonCode.SUPPRESSED))
        elif scrub is None or scrub.checked_at < cutoff:
            gaps.append(CoverageGap(phone, ReasonCode.NO_FRESH_SCRUB))
        elif not (verdict := _verdict(phone, scrub)).allowed:
            gaps.append(CoverageGap(phone, verdict.reason))
    return gaps


def a2_coverage_report(db, phones: list[str], *, now: Optional[datetime] = None) -> A2Report:
    """Summary for the A2 evidence file: distinct numbers checked + gaps by reason."""
    gaps = coverage_gaps(db, phones, now=now)
    counts: dict[str, int] = {}
    for gap in gaps:
        counts[gap.reason.value] = counts.get(gap.reason.value, 0) + 1
    report = A2Report(checked=len(dict.fromkeys(phones)), gaps=gaps, gap_counts=counts)
    logger.info("[lending-compliance] A2 coverage checked=%d gaps=%s passed=%s",
                report.checked, counts, report.passed)
    return report


# ── Call-time gate (WP-W0-3) ─────────────────────────────────────────────────


def recipient_timezone(phone: str, zip_code: Optional[str] = None) -> Optional[ZoneInfo]:
    """Wave 0 pools are Hillsborough/Pinellas: a ZIP in either is Eastern. Otherwise
    area code: ``AREA_CODE_TZ``'s deliberate FL/GA overrides first (850 is kept
    Central even though NANP assigns it Eastern, the over-suppressing direction),
    then the full NANP table. ``None`` means the timezone could not be determined —
    the caller must fail closed (never treat an unmapped code as Eastern)."""
    if zip_code:
        from src.utils.zip_centroids import get_zip_centroid

        if get_zip_centroid(zip_code, "hillsborough") or get_zip_centroid(zip_code, "pinellas"):
            return ZoneInfo(DEFAULT_TZ)
    digits = "".join(c for c in phone if c.isdigit())
    if digits.startswith("1"):
        digits = digits[1:]
    mapped = AREA_CODE_TZ.get(digits[:3])
    if mapped:
        return ZoneInfo(mapped)
    tz_name = _nanp_timezone(phone)
    return ZoneInfo(tz_name) if tz_name else None


def _nanp_timezone(phone: str) -> Optional[str]:
    """One unambiguous IANA zone for a NANP number via libphonenumber's own area-code
    table (already a project dependency), or None if the number is invalid or maps to
    more than one zone (an area code spanning zones must fail closed, not guess)."""
    import phonenumbers
    from phonenumbers import timezone as phonenumbers_timezone

    try:
        parsed = phonenumbers.parse(phone if phone.startswith("+") else f"+1{phone}", None)
    except phonenumbers.NumberParseException:
        return None
    if not phonenumbers.is_valid_number(parsed):
        return None
    zones = phonenumbers_timezone.time_zones_for_number(parsed)
    if len(zones) != 1 or zones[0] == "Etc/Unknown":
        return None
    return zones[0]


def can_dial_now(
    phone: str,
    db,
    *,
    now: Optional[datetime] = None,
    zip_code: Optional[str] = None,
    seat_group: Optional[str] = None,
) -> GateResult:
    """Calling window (09:00-19:15 ET and 8-20 recipient local, narrowed by the
    seat's shift group), then the rolling attempt cap.

    Attempts are outbound rows in lending.call_dispositions (one per dialer
    call.ended, with or without a disposition) — owned by WP-W0-6. A NULL
    direction counts: a missing value must not let a 4th call through.
    """
    now = now or datetime.now(timezone.utc)
    normalized = normalize_phone(phone)
    if not normalized:
        return _blocked(phone, ReasonCode.INVALID_PHONE)

    if _outside_call_window(normalized, now, zip_code, seat_group):
        return _blocked(normalized, ReasonCode.OUTSIDE_CALL_WINDOW)

    cap_result = _attempt_cap(db, normalized, now)
    if not cap_result.allowed:
        return cap_result
    if _attempt_history_exhausted_phones(db, [normalized], now):
        return _blocked(normalized, ReasonCode.ATTEMPT_HISTORY_EXCEEDED)
    return cap_result


def _outside_call_window(
    phone: str, now: datetime, zip_code: Optional[str] = None, seat_group: Optional[str] = None
) -> bool:
    tz = recipient_timezone(phone, zip_code)
    if tz is None:
        logger.warning("[lending-compliance] no timezone for phone_hash=%s; call window fails closed",
                       phone_hash(phone)[:12])
        return True
    local = now.astimezone(tz).time()
    if not (CALL_WINDOW_START <= local < CALL_WINDOW_END):
        return True
    start, end = SHIFT_GROUPS.get(seat_group, (ET_WINDOW_START, ET_WINDOW_END)) if seat_group else (
        ET_WINDOW_START, ET_WINDOW_END)
    eastern = now.astimezone(ZoneInfo(DEFAULT_TZ)).time()
    return not (start <= eastern < end)


def _attempt_cap(db, phone: str, now: datetime) -> GateResult:
    attempts = db.execute(
        text(
            "SELECT count(*) FROM lending.call_dispositions "
            "WHERE phone = :phone AND (direction = 'outbound' OR direction IS NULL) "
            "AND call_ended_at > :since AND call_ended_at <= :now"
        ),
        {"phone": phone, "since": now - timedelta(hours=ATTEMPT_PERIOD_HOURS), "now": now},
    ).scalar()
    if attempts >= MAX_ATTEMPTS_PER_PERIOD:
        return _blocked(phone, ReasonCode.ATTEMPT_CAP_REACHED)
    return GateResult(phone=phone, allowed=True)


def _business_days_ago(now: datetime, business_days: int) -> datetime:
    """``now`` minus N business days, skipping Sat/Sun — a rolling compliance window,
    not a trading-holiday calendar."""
    remaining = business_days
    cursor = now
    while remaining > 0:
        cursor -= timedelta(days=1)
        if cursor.weekday() < 5:  # Mon=0 .. Fri=4
            remaining -= 1
    return cursor


def _total_attempt_counts(db, phones: list[str], since: datetime, now: datetime) -> dict[str, int]:
    """All outbound attempts per phone since ``since`` — unlike ``_attempt_counts``'s
    rolling 24h, this spans the full ``ATTEMPT_HISTORY_BUSINESS_DAYS`` window (Josh,
    Oct 4 answers §6: 3 per 24h, 6 total over 10 business days, then nurture)."""
    if not phones:
        return {}
    rows = db.execute(
        text(
            "SELECT phone, count(*) FROM lending.call_dispositions "
            "WHERE phone = ANY(:phones) AND (direction = 'outbound' OR direction IS NULL) "
            "AND call_ended_at > :since AND call_ended_at <= :now GROUP BY phone"
        ),
        {"phones": phones, "since": since, "now": now},
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _attempt_history_exhausted_phones(db, phones: list[str], now: datetime) -> set[str]:
    since = _business_days_ago(now, ATTEMPT_HISTORY_BUSINESS_DAYS)
    counts = _total_attempt_counts(db, phones, since, now)
    return {p for p, n in counts.items() if n >= MAX_ATTEMPTS_TOTAL}


def _close_exhausted_load_row(db, phone: str) -> None:
    """Permanently closes the load row once total attempt history crosses the cap.
    Unlike a CALL_WINDOW/ATTEMPT_CAP/SCRUB_STALE hold, this can never un-do itself
    (the count only grows), so the contact must leave the active pool for good and
    move to nurture — the same disposition as a DNC/litigator verdict, not a
    temporary sweep hold."""
    db.execute(
        text(
            "UPDATE lending.dialer_load_records SET active = false, deactivated_at = now(), "
            "deactivation_reason = 'attempt_history_exhausted' WHERE active AND phone = :phone"
        ),
        {"phone": phone},
    )


def on_attempt_recorded(
    db,
    phone: Optional[str],
    *,
    now: Optional[datetime] = None,
    dialer_remover: Optional[DialerRemover] = None,
) -> Optional[GateResult]:
    """WP-W0-6 hook, called after every call.ended row commits. At the 24h cap, pull
    the contact from the dialer pool (restoring after 24h is the step-5 sweep's job).
    At the total-history cap, the move is permanent: flag nurture, remove from the
    dialer and close the load row, rather than a temporary hold. Idempotent: a replay
    re-counts the same rows and re-issues the same removal (a no-op for a contact
    already out of the pool). Does not commit."""
    normalized = normalize_phone(phone) if phone else None
    if not normalized:
        return None
    now = now or datetime.now(timezone.utc)
    result = _attempt_cap(db, normalized, now)
    if not result.allowed:
        if _remove_from_dialer([normalized], dialer_remover, RemovalReason.ATTEMPT_CAP):
            _open_holds(db, {normalized: RemovalReason.ATTEMPT_CAP})
        return result
    if _attempt_history_exhausted_phones(db, [normalized], now):
        _flag_nurture(db, [normalized])
        _remove_from_dialer([normalized], dialer_remover, RemovalReason.ATTEMPT_HISTORY)
        _close_exhausted_load_row(db, normalized)
        return _blocked(normalized, ReasonCode.ATTEMPT_HISTORY_EXCEEDED)
    return result


# ── Dialer enforcement sweep (WP-W0-3) ────────────────────────────────────────
# Callers dial from the dialer queue, so the rules must change what is dialable:
# a contact leaves the queue outside its calling window or at the attempt cap,
# and returns when the rule allows it again. Holds record every temporary pull.


@dataclass(frozen=True)
class SweepResult:
    pulled: int
    restored: int
    skipped_locked: bool = False


SWEEP_LOCK_KEY = DIALER_SWEEP_LOCK_KEY


def _default_dialer_restorer() -> Optional[DialerRestorer]:
    from src.lending import dialer_port

    dialer = dialer_port.get_dialer()
    return dialer.restore if dialer is not None else None


def _default_loaded_phones(db) -> list[str]:
    """Active contacts in the dialer, from the load records."""
    if db.execute(text("SELECT to_regclass('lending.dialer_load_records')")).scalar() is None:
        return []
    return [r[0] for r in db.execute(
        text("SELECT DISTINCT phone FROM lending.dialer_load_records WHERE active AND phone IS NOT NULL")
    ).fetchall()]


def _attempt_counts(db, phones: list[str], now: datetime) -> dict[str, int]:
    rows = db.execute(
        text(
            "SELECT phone, count(*) FROM lending.call_dispositions "
            "WHERE phone = ANY(:phones) AND (direction = 'outbound' OR direction IS NULL) "
            "AND call_ended_at > :since AND call_ended_at <= :now GROUP BY phone"
        ),
        {"phones": phones, "since": now - timedelta(hours=ATTEMPT_PERIOD_HOURS), "now": now},
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _stale_scrub_phones(db, phones: list[str], now: datetime) -> set[str]:
    """Phones with no scrub, or one older than the freshness window — the weekly job's
    README says these are "blocked from dialing until rescrubbed"; this is what
    actually enforces that, since the weekly job itself only checks freshness at load
    time, not at every dial."""
    if not phones:
        return set()
    cutoff = now - timedelta(days=DNC_SCRUB_MAX_AGE_DAYS)
    scrubs = _stored_scrubs(db, phones)
    return {p for p in phones if p not in scrubs or scrubs[p].checked_at < cutoff}


def _scrub_stale_breaker_tripped(*, stale: int, pool: int) -> bool:
    """True when so much of the loaded pool looks stale that the weekly rescrub (or Tracerfy)
    is down. Pulling it all would empty the dialer; the per-dial gate still blocks each
    stale number at dial time, so the sweep alerts instead."""
    return pool >= SCRUB_STALE_BREAKER_MIN_POOL and stale * 100 > pool * SCRUB_STALE_BREAKER_PCT


def dial_blocks(db, phones: list[str], *, now: Optional[datetime] = None) -> dict[str, ReasonCode]:
    """``can_dial_now`` for many phones in one query: phone -> reason for each one that
    cannot be dialed now (attempt cap, total attempt history, the calling window, then
    scrub freshness)."""
    if not phones:
        return {}
    now = now or datetime.now(timezone.utc)
    counts = _attempt_counts(db, phones, now)
    stale = _stale_scrub_phones(db, phones, now)
    exhausted = _attempt_history_exhausted_phones(db, phones, now)
    blocks: dict[str, ReasonCode] = {}
    for phone in phones:
        if counts.get(phone, 0) >= MAX_ATTEMPTS_PER_PERIOD:
            blocks[phone] = ReasonCode.ATTEMPT_CAP_REACHED
        elif phone in exhausted:
            blocks[phone] = ReasonCode.ATTEMPT_HISTORY_EXCEEDED
        elif _outside_call_window(phone, now):
            blocks[phone] = ReasonCode.OUTSIDE_CALL_WINDOW
        elif phone in stale:
            blocks[phone] = ReasonCode.NO_FRESH_SCRUB
    return blocks


def _hold_reason(
    phone: str, attempts: int, now: datetime, *, stale_scrub: bool = False, history_exhausted: bool = False,
) -> Optional[RemovalReason]:
    if attempts >= MAX_ATTEMPTS_PER_PERIOD:
        return RemovalReason.ATTEMPT_CAP
    if history_exhausted:
        # Defense in depth: on_attempt_recorded already closes the load row and
        # removes the contact the moment the total-history cap is crossed, so the
        # sweep should never actually find one of these still active. If it does
        # (a missed disposition event, say), this never releases — the count can
        # only grow — matching on_attempt_recorded's permanent-move-to-nurture intent.
        return RemovalReason.ATTEMPT_HISTORY
    if _outside_call_window(phone, now):
        return RemovalReason.CALL_WINDOW
    if stale_scrub:
        return RemovalReason.SCRUB_STALE
    return None


def _open_holds(db, reasons: dict[str, RemovalReason]) -> None:
    db.execute(
        text(
            "INSERT INTO lending.dialer_holds (phone, reason) VALUES (:phone, :reason) "
            "ON CONFLICT (phone) WHERE released_at IS NULL DO NOTHING"
        ),
        [{"phone": p, "reason": r.value} for p, r in reasons.items()],
    )


def sweep_dialer_pool(
    db,
    *,
    now: Optional[datetime] = None,
    dialer_remover: Optional[DialerRemover] = None,
    dialer_restorer: Optional[DialerRestorer] = None,
    loaded_phones: Optional[LoadedPhones] = None,
) -> SweepResult:
    """One enforcement cycle. Idempotent; one cycle at a time (advisory lock). Does not commit."""
    if not db.execute(text("SELECT pg_try_advisory_xact_lock(:k)"), {"k": SWEEP_LOCK_KEY}).scalar():
        return SweepResult(pulled=0, restored=0, skipped_locked=True)
    now = now or datetime.now(timezone.utc)

    held = {r[0] for r in db.execute(text("SELECT phone FROM lending.dialer_holds WHERE released_at IS NULL"))}
    loaded = sorted({p for p in (normalize_phone(x) for x in (loaded_phones or _default_loaded_phones)(db)) if p})
    active = [p for p in loaded if p not in held]
    counts = _attempt_counts(db, active, now) if active else {}
    stale = _stale_scrub_phones(db, active, now) if active else set()
    if _scrub_stale_breaker_tripped(stale=len(stale), pool=len(active)):
        logger.error("[lending-compliance] %d of %d loaded phone(s) have a stale scrub (> %d%%): the weekly "
                     "rescrub looks down; not mass-pulling them this cycle", len(stale), len(active),
                     SCRUB_STALE_BREAKER_PCT)
        stale = set()
    exhausted = _attempt_history_exhausted_phones(db, active, now) if active else set()
    to_pull = {
        p: r for p in active
        if (r := _hold_reason(p, counts.get(p, 0), now, stale_scrub=p in stale, history_exhausted=p in exhausted))
    }

    pulled = 0
    for reason in (RemovalReason.ATTEMPT_CAP, RemovalReason.CALL_WINDOW, RemovalReason.SCRUB_STALE,
                   RemovalReason.ATTEMPT_HISTORY):
        phones = [p for p, r in to_pull.items() if r is reason]
        done = _remove_from_dialer(phones, dialer_remover, reason)
        if done:
            _open_holds(db, {p: reason for p in done})
            pulled += len(done)

    restored = _release_holds(db, sorted(held), now, dialer_restorer)
    if pulled or restored:
        logger.info("[lending-compliance] dialer sweep pulled=%d restored=%d", pulled, restored)
    return SweepResult(pulled=pulled, restored=restored)


def _release_holds(db, held: list[str], now: datetime, dialer_restorer: Optional[DialerRestorer]) -> int:
    """Restore holds whose rule now allows dialling; close suppressed ones without restoring."""
    if not held:
        return 0
    suppressed = _suppressed_phones(db, held)
    counts = _attempt_counts(db, held, now)
    stale = _stale_scrub_phones(db, held, now)
    exhausted = _attempt_history_exhausted_phones(db, held, now)
    closing: list[dict] = [{"phone": p, "why": "suppressed"} for p in held if p in suppressed]
    ready = [p for p in held if p not in suppressed
             and _hold_reason(p, counts.get(p, 0), now, stale_scrub=p in stale,
                              history_exhausted=p in exhausted) is None]

    restorer = dialer_restorer or _default_dialer_restorer()
    restored = 0
    if ready and restorer is None:
        logger.warning("[lending-compliance] %d hold(s) ready but no dialer restorer configured", len(ready))
    for phone in ready if restorer else []:
        try:
            restorer(phone)
        except Exception as exc:
            logger.error("[lending-compliance] dialer restore failed phone_hash=%s: %s",
                         phone_hash(phone)[:12], _error_kind(exc))
            continue
        closing.append({"phone": phone, "why": "restored"})
        restored += 1

    if closing:
        db.execute(
            text(
                "UPDATE lending.dialer_holds SET released_at = :now, release_reason = :why "
                "WHERE phone = :phone AND released_at IS NULL"
            ),
            [{**c, "now": now} for c in closing],
        )
    return restored


# ── Global stop-propagation (WP-W0-8, spec §3.2) ─────────────────────────────
# FA code is not touched (least privilege, Option B):
#   - Dialer DNC_REQUEST → propagate_opt_out: writes FA's SMS/email stores through
#     suppress_contact (the spec requires the dialer opt-out to block SMS + email),
#     then the lending stores and the dialer pool.
#   - SMS STOP / email UNSUBSCRIBE → FA's own handlers write FA's stores;
#     poll_fa_opt_outs picks them up every OPT_OUT_POLL_SECONDS.


@dataclass(frozen=True)
class _OptOut:
    channel: OptOutChannel
    phone: Optional[str]
    email: Optional[str]
    source_ref: Optional[str]
    actor: Optional[str]
    received_at: datetime
    fa_table: Optional[str] = None
    fa_row_id: Optional[int] = None


@dataclass(frozen=True)
class PollResult:
    new_opt_outs: int
    dialer_retried: int
    skipped_locked: bool = False


POLL_LOCK_KEY = OPT_OUT_POLL_LOCK_KEY


def _error_kind(exc: Exception) -> str:
    """Exception class only: messages from SQL/Tracerfy/dialer can echo phones."""
    return type(exc).__name__


def _default_dialer_remover() -> Optional[DialerRemover]:
    """The configured dialer (BatchDialer), or None: removals then stay pending."""
    from src.lending import dialer_port

    dialer = dialer_port.get_dialer()
    return dialer.remove if dialer is not None else None


def _channel_for(source: str, fa_table: str) -> OptOutChannel:
    if source in SMS_OPT_OUT_SOURCES:
        return OptOutChannel.SMS
    if fa_table == "sms_opt_outs" and "sms" in source:
        logger.warning("[lending-compliance] unmapped SMS opt-out source=%s; recording as sms", source)
        return OptOutChannel.SMS
    return OptOutChannel.EMAIL


def propagate_opt_out(
    db,
    *,
    phone: Optional[str] = None,
    email: Optional[str] = None,
    source_ref: Optional[str] = None,
    actor: Optional[str] = None,
    dialer_remover: Optional[DialerRemover] = None,
    channel: OptOutChannel = OptOutChannel.DIALER,
) -> Optional[int]:
    """Verbal decline (DNC_REQUEST) or GHL STOP/DND entry point. Returns the opt_out_events
    id. Does not commit.

    Idempotent per ``channel`` + ``source_ref`` (dialer call id / GHL contact): a redelivered
    event returns the existing event id and writes nothing."""
    from src.services.email_suppression import suppress_contact

    if source_ref:
        existing = db.execute(
            text(
                "SELECT id FROM lending.opt_out_events "
                "WHERE channel = :channel AND source_ref = :ref ORDER BY id LIMIT 1"
            ),
            {"channel": channel.value, "ref": source_ref},
        ).scalar()
        if existing:
            return existing

    phone = normalize_phone(phone) if phone else None
    email = email.strip().lower() if email else None
    if not phone and not email:
        return None
    suppress_contact(db, email=email, phone=phone, source=DIALER_OPT_OUT_SOURCE)
    opt_out = _OptOut(channel, phone, email, source_ref, actor, datetime.now(timezone.utc))
    event_id = _propagate(db, [opt_out], dialer_remover)[0]
    if channel is OptOutChannel.GHL:  # GHL already holds the DND: never echo it back
        db.execute(text("UPDATE lending.opt_out_events SET ghl_dnd_at = now() WHERE id = :id"), {"id": event_id})
    return event_id


def poll_fa_opt_outs(db, *, dialer_remover: Optional[DialerRemover] = None,
                     ghl_dnd: Optional[Callable[[str], bool]] = None) -> PollResult:
    """Mirror each FA opt-out row exactly once (keyed on its FA row id, so a raw or
    padded FA value can never loop), and retry pending dialer removals.

    One cycle at a time across processes (transaction-scoped advisory lock).
    Does not commit."""
    if not db.execute(text("SELECT pg_try_advisory_xact_lock(:k)"), {"k": POLL_LOCK_KEY}).scalar():
        return PollResult(new_opt_outs=0, dialer_retried=0, skipped_locked=True)

    rows = db.execute(
        text(
            "SELECT 'sms_opt_outs' AS fa_table, o.id, o.phone AS value, o.source, "
            "       o.opted_out_at AT TIME ZONE 'UTC' "
            "FROM sms_opt_outs o "
            "WHERE o.source <> ALL(:excluded) AND NOT EXISTS ("
            "  SELECT 1 FROM lending.opt_out_events e WHERE e.fa_table = 'sms_opt_outs' AND e.fa_row_id = o.id) "
            "UNION ALL "
            "SELECT 'email_opt_outs', o.id, o.email, o.source, o.opted_out_at AT TIME ZONE 'UTC' "
            "FROM email_opt_outs o "
            "WHERE o.source <> ALL(:excluded) AND NOT EXISTS ("
            "  SELECT 1 FROM lending.opt_out_events e WHERE e.fa_table = 'email_opt_outs' AND e.fa_row_id = o.id)"
        ),
        {"excluded": sorted(OPT_OUT_EXCLUDED_SOURCES)},
    ).fetchall()

    opt_outs: list[_OptOut] = []
    for fa_table, row_id, value, source, opted_out_at in rows:
        phone = normalize_phone(value) if fa_table == "sms_opt_outs" else None
        email = ((value or "").strip().lower() or None) if fa_table == "email_opt_outs" else None
        if phone or email:
            opt_outs.append(_OptOut(
                _channel_for(source, fa_table), phone, email, source, None, opted_out_at, fa_table, row_id,
            ))
    if opt_outs:
        _propagate(db, opt_outs, dialer_remover)

    retried = _retry_pending_dialer_removals(db, dialer_remover)
    _sync_ghl_dnd(db, ghl_dnd)
    if opt_outs or retried:
        logger.info("[lending-compliance] poll new_opt_outs=%d dialer_retried=%d", len(opt_outs), retried)
    return PollResult(new_opt_outs=len(opt_outs), dialer_retried=retried)


def _sync_ghl_dnd(db, ghl_dnd: Optional[Callable[[str], bool]]) -> int:
    """Write every opt-out not yet in GHL as do-not-disturb (new ones and retries alike),
    a bounded batch per poll. Returns how many GHL accepted."""
    if ghl_dnd is None:
        from src.lending.ghl_dnd import get_ghl_dnd
        ghl_dnd = get_ghl_dnd()
        if ghl_dnd is None:
            return 0
    pending = db.execute(
        text(
            "SELECT e.id, c.phone FROM lending.opt_out_events e "
            "JOIN lending.contacts c ON c.phone_hash = e.phone_hash "
            "WHERE e.ghl_dnd_at IS NULL AND e.phone_hash IS NOT NULL ORDER BY e.id LIMIT :n"
        ),
        {"n": GHL_DND_BATCH},
    ).fetchall()
    ids_by_phone: dict[str, list[int]] = defaultdict(list)
    for event_id, phone in pending:
        ids_by_phone[phone].append(event_id)
    done: list[int] = []
    for phone, event_ids in ids_by_phone.items():
        if ghl_dnd(phone):
            done.extend(event_ids)
        else:
            logger.warning("[lending-compliance] GHL DND pending phone_hash=%s", phone_hash(phone)[:12])
    if done:
        db.execute(text("UPDATE lending.opt_out_events SET ghl_dnd_at = now() WHERE id = ANY(:ids)"), {"ids": done})
    return len(done)


def _propagate(db, opt_outs: list[_OptOut], dialer_remover: Optional[DialerRemover]) -> list[int]:
    """Batch: one statement per lending table, then one dialer call per phone."""
    event_ids = [
        r[0]
        for r in db.execute(
            text(
                "INSERT INTO lending.opt_out_events "
                "(channel, source_ref, phone_hash, actor, received_at, fa_table, fa_row_id, status) "
                "SELECT channel, source_ref, phone_hash, actor, received_at, fa_table, fa_row_id, :pending "
                "FROM unnest(CAST(:channels AS varchar[]), CAST(:refs AS varchar[]), CAST(:hashes AS varchar[]), "
                "CAST(:actors AS varchar[]), CAST(:received AS timestamptz[]), CAST(:fa_tables AS varchar[]), "
                "CAST(:fa_ids AS integer[])) WITH ORDINALITY "
                "AS t(channel, source_ref, phone_hash, actor, received_at, fa_table, fa_row_id, ord) ORDER BY ord "
                "RETURNING id"
            ),
            {
                "channels": [o.channel.value for o in opt_outs],
                "refs": [o.source_ref for o in opt_outs],
                "hashes": [phone_hash(o.phone) if o.phone else None for o in opt_outs],
                "actors": [o.actor for o in opt_outs],
                "received": [o.received_at for o in opt_outs],
                "fa_tables": [o.fa_table for o in opt_outs],
                "fa_ids": [o.fa_row_id for o in opt_outs],
                "pending": OptOutStatus.PENDING.value,
            },
        ).fetchall()
    ]

    db.execute(
        text(
            "INSERT INTO lending.suppression_list (phone, email, reason, source_channel, source_ref) "
            "VALUES (:phone, :email, :reason, :channel, :source_ref) ON CONFLICT DO NOTHING"
        ),
        [
            {
                "phone": value if kind == "phone" else None,
                "email": value if kind == "email" else None,
                "reason": SuppressionReason.OPT_OUT.value,
                "channel": o.channel.value,
                "source_ref": o.source_ref,
            }
            for o in opt_outs
            for kind, value in (("phone", o.phone), ("email", o.email))
            if value
        ],
    )
    phones = sorted({o.phone for o in opt_outs if o.phone})
    if phones:
        db.execute(
            text(
                "INSERT INTO lending.contacts (phone, phone_hash, do_not_contact) "
                "SELECT p, h, true FROM unnest(CAST(:phones AS varchar[]), CAST(:hashes AS varchar[])) AS t(p, h) "
                "ON CONFLICT (phone) DO UPDATE SET do_not_contact = true, phone_hash = EXCLUDED.phone_hash"
            ),
            {"phones": phones, "hashes": [phone_hash(p) for p in phones]},
        )
    stores_at = datetime.now(timezone.utc)

    removed_at = _remove_from_dialer(phones, dialer_remover, RemovalReason.OPT_OUT)
    updates = []
    for event_id, o in zip(event_ids, opt_outs):
        dialer_at = removed_at.get(o.phone) if o.phone else None
        status = OptOutStatus.COMPLETE if (o.phone is None or dialer_at) else OptOutStatus.DIALER_PENDING
        updates.append({
            "id": event_id,
            "stores_at": stores_at,
            # FA's store already holds this identifier: FA wrote it before the poll,
            # or suppress_contact wrote it inside propagate_opt_out.
            "sms_at": stores_at if o.phone else None,
            "email_at": stores_at if o.email else None,
            "dialer_at": dialer_at,
            "status": status.value,
        })
        _log_propagation(event_id, o.channel, o.received_at, dialer_at if o.phone else stores_at, status)
    db.execute(
        text(
            "UPDATE lending.opt_out_events SET suppression_at = :stores_at, sms_at = :sms_at, "
            "email_at = :email_at, dialer_removed_at = :dialer_at, status = :status WHERE id = :id"
        ),
        updates,
    )
    return event_ids


def _remove_from_dialer(
    phones: list[str],
    dialer_remover: Optional[DialerRemover],
    reason: RemovalReason,
    *,
    retry: bool = False,
) -> dict[str, datetime]:
    """Removal time per phone dialer confirmed. Failures stay pending for the next poll."""
    remover = dialer_remover or _default_dialer_remover()
    if remover is None:
        if phones and not retry:
            logger.warning("[lending-compliance] no dialer remover configured; %d removal(s) pending", len(phones))
        return {}
    done: dict[str, datetime] = {}
    for phone in phones:
        try:
            remover(phone, reason=reason.value)
            done[phone] = datetime.now(timezone.utc)
        except Exception as exc:
            kind = _error_kind(exc)
            # The dialer adapter raises DialerRemovalUndecided (e.g. UnconfirmedCapability)
            # while an endpoint is unconfirmed. Expected state, not a fault: warn on the
            # first attempt, stay quiet on 15 s retries. Checked on the exception instance,
            # not the stringified class name, so subclasses are still recognized.
            if isinstance(exc, DialerRemovalUndecided):
                level = logger.debug if retry else logger.warning
            else:
                level = logger.error
            level(
                "[lending-compliance] dialer removal failed reason=%s phone_hash=%s: %s",
                reason.value, phone_hash(phone)[:12], kind,
            )
    return done


def _retry_pending_dialer_removals(db, dialer_remover: Optional[DialerRemover]) -> int:
    pending = db.execute(
        text(
            "SELECT e.id, c.phone, e.channel, e.received_at FROM lending.opt_out_events e "
            "JOIN lending.contacts c ON c.phone_hash = e.phone_hash "
            "WHERE e.status = :pending"
        ),
        {"pending": OptOutStatus.DIALER_PENDING.value},
    ).fetchall()
    if not pending:
        return 0
    removed_at = _remove_from_dialer(
        sorted({row[1] for row in pending}), dialer_remover, RemovalReason.OPT_OUT, retry=True,
    )
    updates = []
    for event_id, phone, channel, received_at in pending:
        if phone in removed_at:
            updates.append({"id": event_id, "at": removed_at[phone], "status": OptOutStatus.COMPLETE.value})
            _log_propagation(event_id, OptOutChannel(channel), received_at, removed_at[phone], OptOutStatus.COMPLETE)
    if updates:
        db.execute(
            text("UPDATE lending.opt_out_events SET dialer_removed_at = :at, status = :status WHERE id = :id"),
            updates,
        )
    return len(updates)


def _log_propagation(event_id, channel, received_at, finished_at: Optional[datetime], status) -> None:
    """``finished_at`` = when the last store (incl. the dialer pool) was cleared;
    None while the dialer removal is still pending, so the SLA is not yet met."""
    if finished_at is None:
        logger.warning(
            "[lending-compliance] opt-out event=%s channel=%s status=%s sla_met=no (dialer removal pending)",
            event_id, channel.value, status.value,
        )
        return
    seconds = (finished_at - received_at).total_seconds()
    met = seconds <= STOP_PROPAGATION_SLA_SECONDS
    (logger.info if met else logger.warning)(
        "[lending-compliance] opt-out event=%s channel=%s status=%s propagated_in=%.3fs sla_met=%s (sla=%ss)",
        event_id, channel.value, status.value, seconds, "yes" if met else "no", STOP_PROPAGATION_SLA_SECONDS,
    )


_RECONCILE_BATCH = 1000


def _stream(db, sql: str, params: dict):
    """First column of every row, read with a server-side cursor in pages of _RECONCILE_BATCH."""
    result = db.execute(text(sql).execution_options(yield_per=_RECONCILE_BATCH), params)
    for row in result:
        yield row[0]


def reconcile_suppression(db, source_schema: str = "public", target_schema: str = "lending") -> int:
    """Copy FA opt-outs (and Tracerfy litigators) missing from lending.suppression_list.

    Idempotent. Used by the migration backfill (no events, no dialer calls —
    live opt-outs go through poll_fa_opt_outs). Phones are
    normalized in Python (phone_utils rule) before insert. Returns rows added.
    """
    s, t = source_schema, target_schema
    excluded = sorted(OPT_OUT_EXCLUDED_SOURCES)
    insert = text(
        f'INSERT INTO "{t}".suppression_list (phone, email, reason, source_channel) '
        "VALUES (:phone, :email, :reason, :channel) ON CONFLICT DO NOTHING"
    )
    before = db.execute(text(f'SELECT count(*) FROM "{t}".suppression_list')).scalar()
    batch: list[dict] = []

    def flush() -> None:
        if batch:
            db.execute(insert, batch)
            batch.clear()

    def add(row: dict) -> None:
        batch.append(row)
        if len(batch) >= _RECONCILE_BATCH:
            flush()

    seen_phones: set[str] = set()
    for sql, params, reason, channel in (
        (f'SELECT phone FROM "{s}".sms_opt_outs WHERE source <> ALL(:excluded)', {"excluded": excluded},
         SuppressionReason.OPT_OUT, "backfill:sms_opt_outs"),
        (f'SELECT phone FROM "{s}".dnc_phone_checks WHERE litigator', {},
         SuppressionReason.LITIGATOR, "backfill:dnc_phone_checks"),
    ):
        for raw in _stream(db, sql, params):
            phone = normalize_phone(raw)
            if phone and phone not in seen_phones:
                seen_phones.add(phone)
                add({"phone": phone, "email": None, "reason": reason.value, "channel": channel})

    seen_emails: set[str] = set()
    for raw in _stream(db, f'SELECT email FROM "{s}".email_opt_outs WHERE source <> ALL(:excluded)',
                       {"excluded": excluded}):
        email = (raw or "").strip().lower()
        if email and email not in seen_emails:
            seen_emails.add(email)
            add({"phone": None, "email": email, "reason": SuppressionReason.OPT_OUT.value,
                 "channel": "backfill:email_opt_outs"})
    flush()

    added = db.execute(text(f'SELECT count(*) FROM "{t}".suppression_list')).scalar() - before
    logger.info("[lending-compliance] reconcile_suppression added=%d", added)
    return added
