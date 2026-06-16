"""
Tracerfy skip trace — Tier 1 in the waterfall.

Uses POST /trace/ (batch async, 1 credit/hit = $0.02) rather than
POST /trace/lookup/ (instant sync, 5 credits/hit = $0.10). We send
owner name + address so the normal tier applies.

Flow:
  1. Collect candidates in batches of up to 100
  2. POST to /trace/ with trace_type="normal" → queue_id
  3. Poll GET /queue/:id until pending=False
  4. Download results CSV, match rows back to owners by label (owner_id)
  5. Persist EnrichedContact, update Owner fields, suppress DNC phones

DNC side-effect (free): phones in the trace response carry dnc/litigator
flags. Hits are written to sms_opt_outs immediately; dnc_checked_at is
stored in phone_metadata for the monthly re-scrub.

API:  POST https://tracerfy.com/v1/api/trace/
Auth: Bearer token
Cost: 1 credit/hit ($0.02), 0 on miss

Rate limit: 10 POST /trace/ per 5-minute window → 30s delay between batches.

Usage:
  python -m src.services.tracerfy_fallback --limit 50
  python -m src.services.tracerfy_fallback --dry-run --limit 5
  python -m src.services.tracerfy_fallback --balance
"""

import re
import time
import traceback
from datetime import datetime, timezone
from typing import Optional

import requests

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import (
    DistressScore, EnrichedContact, EnrichmentUsageLog, Owner, Property, SmsOptOut,
)
from src.services.email import send_alert
from src.services.enrichment_log import log_usage
from src.services.phone_utils import normalize as normalize_phone
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

_TRACERFY_BASE        = "https://tracerfy.com/v1/api"
_TRACE_ENDPOINT       = f"{_TRACERFY_BASE}/trace/"
_TRACE_QUEUE_ENDPOINT = f"{_TRACERFY_BASE}/queue/"
_ANALYTICS_ENDPOINT   = f"{_TRACERFY_BASE}/analytics/"
_BATCH_SIZE           = 3000  # records per POST /trace/ queue submission
_POLL_INTERVAL        = 5     # seconds between queue status polls
_BATCH_DELAY          = 31    # seconds between batch POSTs (rate limit: 10/5min)

# Tokens that indicate a name is a corporate entity rather than a traceable person.
_ENTITY_TOKENS = frozenset({
    "LLC", "LLP", "LLLP", "LP", "INC", "CORP", "CORPORATION", "LTD", "COMPANY", "CO",
    "PLLC", "PA", "PL", "ENTERPRISES", "HOLDINGS", "PROPERTIES", "TRUST", "ESTATE",
    "BANK", "NA", "N.A.", "GROUP", "SERVICES", "SOLUTIONS", "MANAGEMENT",
    "ASSOCIATES", "PARTNERS", "INVESTMENTS", "REALTY", "VENTURES", "LAW", "OFFICE",
    # Government / institutional entities often mis-classified as Individual in assessor data
    "UNION", "FEDERAL", "AUTHORITY", "DEPARTMENT", "COUNTY", "CITY", "STATE",
    "GOVERNMENT", "MUNICIPAL", "DISTRICT", "FOUNDATION", "ASSOCIATION", "CREDIT",
    "HOUSING", "AUTHORITY", "AGENCY", "BOARD", "COMMITTEE", "COMMISSION",
})

_NAME_SUFFIXES = frozenset({"JR", "JR.", "SR", "SR.", "II", "III", "IV", "ESQ", "ESQ."})

# Strips "ET AL", "ET ALS", "ET AL." from the end of assessor owner names.
_ET_AL_RE = re.compile(r"\s+ET\s+AL[S.]?\s*$", re.IGNORECASE)


def _looks_corporate(name: str) -> bool:
    return bool(_ENTITY_TOKENS.intersection(name.upper().split()))


def _parse_name(full_name: str) -> tuple[str, str]:
    """
    Parse a name string → (first, last).

    Handles both 'FIRST LAST' and Sunbiz 'LAST, FIRST [MIDDLE] [SUFFIX]' formats.
    Strips name suffixes (Jr., Sr., II, etc.) from the first component.
    """
    name = full_name.strip()
    if "," in name:
        # Sunbiz format: "Last, First [Middle] [Suffix]"
        last_part, rest = name.split(",", 1)
        tokens = rest.strip().split()
        # Drop trailing suffixes to get the actual first name
        first_tokens = [t for t in tokens if t.upper().rstrip(".") not in _NAME_SUFFIXES]
        first = first_tokens[0] if first_tokens else ""
        return first, last_part.strip()
    parts = name.split(None, 1)
    return (parts[0], parts[1]) if len(parts) > 1 else ("", parts[0] if parts else "")


def _parse_ra_address(addr: str) -> dict | None:
    """
    Parse Sunbiz multi-line RA address into {address, city, state, zip}.

    Sunbiz format:
      Line 1: street address
      Line N: City, ST 00000

    Returns None if parsing fails (caller falls back to property address).
    """
    import re
    lines = [ln.strip() for ln in addr.strip().splitlines() if ln.strip()]
    if len(lines) < 2:
        return None
    street = lines[0]
    city_state_zip = lines[-1]
    m = re.match(r"^(.*?),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\s*$", city_state_zip, re.IGNORECASE)
    if not m:
        return None
    return {
        "address": street,
        "city":    m.group(1).strip(),
        "state":   m.group(2).upper(),
        "zip":     m.group(3)[:5],
    }


def _resolve_trace_subject(owner: "Owner") -> tuple[str, str, dict | None, bool]:
    """
    Return (first, last, address_override, is_traceable).

    is_traceable=False when the owner is an entity with no individual contact
    found — the caller should skip this record rather than submit a corporate
    name as a person's name (which returns 0% hit rate and wastes a queue slot).

    For entity owners: managing member → registered agent (individual only).
    For individuals: strip ET AL, handle joint-owner AND patterns (including
    shared-last-name variants like "JOHN AND JANE SMITH"), strip middle initials.
    """
    is_entity = (
        (owner.owner_type and owner.owner_type != "Individual")
        or _looks_corporate(owner.owner_name or "")
    )
    if is_entity:
        for member in (owner.managing_members or []):
            name = (member.get("name") or "").strip()
            if name and not _looks_corporate(name):
                return (*_parse_name(name), None, True)
        ra_name = (owner.registered_agent_name or "").strip()
        if ra_name and not _looks_corporate(ra_name):
            ra_addr = _parse_ra_address(owner.registered_agent_address or "")
            return (*_parse_name(ra_name), ra_addr, True)
        # Entity with no traceable individual — skip rather than submit LLC name
        return "", "", None, False

    # ── Individual path ────────────────────────────────────────────────────
    raw = _ET_AL_RE.sub("", owner.owner_name or "").strip()

    for sep in (" AND ", " & "):
        idx = raw.upper().find(sep)
        if idx != -1:
            person_part = raw[:idx].strip()
            tokens = person_part.split()
            # Shared-last-name pattern: "JOHN AND JANE SMITH" → person_part="JOHN"
            # or "PETER S AND JISLAYNE HARRISON" → person_part="PETER S".
            # If no usable last name remains (1 token, or 2 tokens where the
            # second looks like a middle initial), borrow the last word of the
            # full original name as the shared family name.
            missing_last = len(tokens) <= 1 or (
                len(tokens) == 2 and len(tokens[-1]) <= 2
            )
            if missing_last:
                all_tokens = raw.split()
                shared_last = all_tokens[-1] if all_tokens else ""
                if shared_last:
                    person_part = f"{person_part} {shared_last}"
            raw = person_part
            break

    first, last = _parse_name(raw)
    # Strip a leading middle initial from the last-name slot:
    # "KURT W JOHNSON" → last="W JOHNSON" → ["W", "JOHNSON"] → drop "W"
    last_parts = last.split(None, 1)
    if len(last_parts) == 2 and len(last_parts[0]) == 1:
        last = last_parts[1]
    return first, last, None, True


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }


def _submit_trace_batch(records: list[dict], api_key: str, trace_type: str = "normal") -> tuple[str, int]:
    """
    POST /trace/ as multipart/form-data.
    Returns (queue_id, estimated_wait_seconds).

    trace_type='normal'   — standard name+address trace (1 credit/hit = $0.02)
    trace_type='advanced' — address-only trace (2 credits/hit = $0.04); name fields optional

    Confirmed response shape:
      {"queue_id": 94858, "status": "pending", "rows_uploaded": 3,
       "estimated_wait_seconds": 30, "credits_per_lead": 1, ...}
    """
    import json as _json

    fields = {
        "address_column":     "address",
        "city_column":        "city",
        "state_column":       "state",
        "zip_column":         "zip",
        "first_name_column":  "first_name",
        "last_name_column":   "last_name",
        "label_column":       "label",   # owner_id echoed back for reliable result matching
        "mail_address_column": "address",
        "mail_city_column":   "city",
        "mail_state_column":  "state",
        "mailing_zip_column": "zip",
        "trace_type":         trace_type,
        "json_data":          _json.dumps(records),
    }
    multipart = {k: (None, v) for k, v in fields.items()}

    resp = requests.post(
        _TRACE_ENDPOINT,
        headers={"Authorization": f"Bearer {api_key}"},
        files=multipart,
        timeout=30,
    )
    if resp.status_code in (401, 403):
        raise RuntimeError(f"Tracerfy: invalid API key ({resp.status_code})")
    if resp.status_code == 429:
        raise RuntimeError("Tracerfy: rate limited (429) — slow down batch submissions")
    if not resp.ok:
        raise RuntimeError(f"Tracerfy POST /trace/ HTTP {resp.status_code}: {resp.text[:400]}")

    data = resp.json()
    logger.info("[Tracerfy] Batch submit: %s", data)

    queue_id = data.get("queue_id") or data.get("id")
    if not queue_id:
        raise RuntimeError(f"Tracerfy /trace/: no queue_id in response: {data}")
    estimated_wait = int(data.get("estimated_wait_seconds") or 30)
    return str(queue_id), estimated_wait


def _poll_trace_queue(queue_id: str, api_key: str, estimated_wait: int = 30) -> list[dict]:
    """
    Poll GET /queue/:id until it returns a non-empty list, then return the results.

    Confirmed response shape (2026-06-03):
      [{"address": "...", "city": "...", "state": "...",
        "first_name": "...", "last_name": "...",
        "primary_phone": "5125550100", "primary_phone_type": "Mobile",
        "mobile_1".."mobile_5": "...",
        "landline_1".."landline_3": "...",
        "email_1".."email_5": "...",
        "mail_address": "...", "mail_city": "...", "mail_state": "..."}, ...]

    Returns [] when still processing. After initial sleep we retry up to
    60 × 5s = 5 minutes before giving up (treating remaining submitted
    records as misses).
    """
    url = f"{_TRACE_QUEUE_ENDPOINT}{queue_id}"

    logger.info("[Tracerfy] Waiting %ds for queue=%s...", estimated_wait, queue_id)
    time.sleep(estimated_wait)

    last_count = -1
    stable_rounds = 0

    for attempt in range(120):
        resp = requests.get(url, headers=_headers(api_key), timeout=30)
        if not resp.ok:
            raise RuntimeError(f"Tracerfy GET /queue/ HTTP {resp.status_code}: {resp.text[:300]}")
        results = resp.json()
        current_count = len(results)

        if current_count == 0:
            logger.info("[Tracerfy] queue=%s still empty attempt=%d", queue_id, attempt + 1)
            time.sleep(_POLL_INTERVAL)
            continue

        # Tracerfy streams results as it processes — wait until count stabilises
        # across two consecutive polls before accepting as complete.
        if current_count == last_count:
            stable_rounds += 1
            if stable_rounds >= 2:
                logger.info("[Tracerfy] queue=%s stable at %d rows after %d attempts",
                            queue_id, current_count, attempt + 1)
                return results
        else:
            stable_rounds = 0
            logger.info("[Tracerfy] queue=%s growing: %d rows (attempt=%d)",
                        queue_id, current_count, attempt + 1)

        last_count = current_count
        time.sleep(_POLL_INTERVAL)

    logger.warning("[Tracerfy] queue=%s did not stabilise after timeout — returning %d rows",
                   queue_id, last_count)
    return results if results else []


def _parse_trace_row(row: dict) -> dict:
    """
    Parse one result row from GET /queue/:id into the standard contact dict.

    Confirmed field names (2026-06-03):
      primary_phone, primary_phone_type
      mobile_1..mobile_5, landline_1..landline_3
      email_1..email_5
      mail_address, mail_city, mail_state
      address, city, state (property address echoed back)

    Note: the batch trace endpoint does NOT return DNC flags. DNC checking
    is handled separately by the monthly dnc_refresh task.
    """
    def _col(*keys) -> str:
        for k in keys:
            v = (row.get(k) or "").strip()
            if v:
                return v
        return ""

    # ── Phones ────────────────────────────────────────────────────────────
    # Use primary_phone first (Tracerfy's best pick), then mobile_1..5,
    # then landline_1..3.
    mobile_phone: Optional[str] = None
    landline:     Optional[str] = None

    primary_raw = _col("primary_phone")
    primary_type = _col("primary_phone_type").lower()
    if primary_raw:
        primary_num = normalize_phone(primary_raw)
        if primary_num:
            if "mobile" in primary_type or "wireless" in primary_type:
                mobile_phone = primary_num
            else:
                landline = primary_num

    for i in range(1, 6):
        raw = _col(f"mobile_{i}")
        if not raw:
            break
        num = normalize_phone(raw)
        if num and not mobile_phone:
            mobile_phone = num

    for i in range(1, 4):
        raw = _col(f"landline_{i}")
        if not raw:
            break
        num = normalize_phone(raw)
        if num and not landline:
            landline = num

    # ── Email ─────────────────────────────────────────────────────────────
    email: Optional[str] = None
    for i in range(1, 6):
        addr = _col(f"email_{i}").lower()
        if addr and "@" in addr:
            email = addr
            break

    # ── Mailing address ───────────────────────────────────────────────────
    parts = [_col("mail_address"), _col("mail_city"), _col("mail_state")]
    mailing_address = ", ".join(p for p in parts if p) or None

    return {
        "mobile_phone":    mobile_phone,
        "landline":        landline,
        "email":           email,
        "mailing_address": mailing_address,
        "match_success":   bool(mobile_phone or landline or email),
        "dnc_flags":       None,   # not returned by batch trace; handled by dnc_refresh
        "all_dnc_phones":  [],
    }


# ---------------------------------------------------------------------------
# Account
# ---------------------------------------------------------------------------

def get_tracerfy_balance() -> dict:
    """GET /analytics/ — returns credit balance and account summary."""
    settings = get_settings()
    if not settings.tracerfy_api_key:
        return {"error": "TRACERFY_API_KEY not set"}
    api_key = settings.tracerfy_api_key.get_secret_value()
    resp = requests.get(_ANALYTICS_ENDPOINT, headers=_headers(api_key), timeout=15)
    if not resp.ok:
        raise RuntimeError(f"Tracerfy analytics HTTP {resp.status_code}: {resp.text[:300]}")
    return resp.json()


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_tracerfy_fallback(
    limit: int = 100,
    county_id: str = "hillsborough",
    owner_ids: Optional[list] = None,
    dry_run: bool = False,
    retrace_misses: bool = False,
    individual_only: bool = False,
    entity_only: bool = False,
    trace_type: str = "normal",
) -> dict:
    """
    Run Tracerfy batch skip-trace for Gold+ leads.

    Standard mode (owner_ids=None, trace_type='normal'):
        Selects Gold+ candidates not yet Tracerfy-traced (1 credit/hit = $0.02).

    Waterfall mode (owner_ids=[...]):
        Processes a specific list supplied by the cascade orchestrator.

    Address-Only mode (trace_type='advanced'):
        Submits address only — skips entity-classification, uses empty names.
        2 credits/hit ($0.04). Used for Standard misses + entity-skips (ADR 0016).
        entity_skip_ids is always [] in this mode (already skipping the skip-check).

    retrace_misses=True:
        Re-submits properties with existing tracerfy miss EC rows, updating them
        in-place on a hit. When owner_ids is ALSO given, restricts to those IDs.

    Returns stats dict including entity_skip_ids (list of owner_ids skipped because
    no traceable individual was found — these become Address-Only candidates).
    """
    settings     = get_settings()
    cost_per_hit = (
        settings.tracerfy_advanced_cost_cents if trace_type == "advanced"
        else settings.tracerfy_cost_cents
    )

    if not settings.tracerfy_api_key:
        logger.warning("TRACERFY_API_KEY not set — Tracerfy skip-trace skipped")
        return {"skipped": True, "reason": "TRACERFY_API_KEY not configured"}

    api_key = settings.tracerfy_api_key.get_secret_value()
    stats   = {
        "total": 0, "success": 0, "failed": 0,
        "no_address": 0, "already_done": 0, "skipped": False,
        "skipped_entity": 0,
        "entity_skip_ids": [],   # owner_ids skipped due to no traceable individual
    }

    with get_db_context() as session:
        from sqlalchemy import or_ as sa_or, func as sa_func
        from sqlalchemy import text as sa_text

        if retrace_misses:
            # Re-submit properties whose prior Tracerfy run returned no match.
            # The existing miss EC row is updated in-place on a hit; continued
            # misses are left unchanged (no duplicate rows inserted).
            # When owner_ids is also provided, restrict to those specific IDs
            # (cascade uses this for Address-Only on specific miss owners).
            type_filter = ""
            if individual_only:
                type_filter = " AND o.owner_type = 'Individual'"
            elif entity_only:
                type_filter = " AND o.owner_type != 'Individual'"

            owner_filter = ""
            params: dict = {"county_id": county_id, "limit": limit}
            if owner_ids is not None:
                owner_filter = " AND o.id = ANY(:owner_ids)"
                params["owner_ids"] = owner_ids

            miss_owner_ids = [
                r[0]
                for r in session.execute(sa_text(f"""
                    SELECT DISTINCT o.id
                    FROM enriched_contacts ec
                    JOIN owners o ON o.property_id = ec.property_id
                      AND o.county_id = :county_id
                    WHERE ec.source = 'tracerfy'
                      AND ec.match_success = FALSE
                    {type_filter}
                    {owner_filter}
                    ORDER BY o.id
                    LIMIT :limit
                """), params).fetchall()
            ]
            rows = (
                session.query(Owner, Property)
                .join(Property, Owner.property_id == Property.id)
                .filter(Owner.id.in_(miss_owner_ids))
                .all()
            )

        elif owner_ids is not None:
            already_tracerfy = (
                session.query(EnrichedContact.property_id)
                .filter(EnrichedContact.source == "tracerfy")
                .subquery()
            )
            rows = (
                session.query(Owner, Property)
                .join(Property, Owner.property_id == Property.id)
                .filter(
                    Owner.id.in_(owner_ids),
                    Owner.property_id.notin_(session.query(already_tracerfy)),
                )
                .all()
            )

        else:
            no_phone = sa_or(
                Owner.phone_1.is_(None),
                sa_func.length(sa_func.trim(Owner.phone_1)) == 0,
            )
            already_tracerfy = (
                session.query(EnrichedContact.property_id)
                .filter(EnrichedContact.source == "tracerfy")
                .subquery()
            )
            rows = (
                session.query(Owner, Property)
                .join(Property, Owner.property_id == Property.id)
                .join(DistressScore, DistressScore.property_id == Property.id)
                .filter(
                    Owner.county_id == county_id,
                    no_phone,
                    Owner.skip_trace_success.is_not(True),
                    Owner.property_id.notin_(session.query(already_tracerfy)),
                    DistressScore.lead_tier.in_(("Gold", "Platinum", "Ultra Platinum")),
                    Property.address.isnot(None),
                    Property.address != "",
                    Property.zip.isnot(None),
                    Property.zip != "",
                )
                .limit(limit)
                .all()
            )

        # Post-query type filter (applies to all modes except retrace, which already filtered in SQL)
        if not retrace_misses:
            if individual_only:
                rows = [(o, p) for o, p in rows if o.owner_type == "Individual" and not _looks_corporate(o.owner_name or "")]
            elif entity_only:
                rows = [(o, p) for o, p in rows if o.owner_type != "Individual" or _looks_corporate(o.owner_name or "")]

    if not rows:
        logger.info("[Tracerfy] No candidates found.")
        return stats

    logger.info("[Tracerfy] Found %d candidates to enrich", len(rows))
    stats["total"] = len(rows)

    if dry_run:
        for owner, prop in rows[:5]:
            logger.info(
                "[Tracerfy DRY RUN] Would trace: property_id=%d | %s | %s",
                prop.id, prop.address, owner.owner_name,
            )
        logger.info("[Tracerfy DRY RUN] Would process %d records. No API call made.", len(rows))
        return stats

    # ── Build batches ─────────────────────────────────────────────────────
    # label = owner_id (str) so we can match results back without address parsing
    for batch_start in range(0, len(rows), _BATCH_SIZE):
        batch = rows[batch_start: batch_start + _BATCH_SIZE]
        batch_num = batch_start // _BATCH_SIZE + 1

        records:    list[dict]        = []
        label_map:  dict[str, tuple] = {}  # str(owner_id) → (owner_snap, prop_snap)
        address_map: dict[str, tuple] = {}  # UPPER(submitted_address) → (owner_snap, prop_snap)

        for owner, prop in batch:
            if not prop.address or not prop.zip:
                stats["no_address"] += 1
                continue
            if trace_type == "advanced":
                # Address-Only: skip entity classification; name fields are
                # optional for the advanced trace type.
                first, last, addr_override = "", "", None
            else:
                first, last, addr_override, is_traceable = _resolve_trace_subject(owner)
                if not is_traceable:
                    stats["skipped_entity"] += 1
                    stats["entity_skip_ids"].append(owner.id)
                    continue
            addr = addr_override or {
                "address": prop.address,
                "city":    prop.city or "Tampa",
                "state":   prop.state or "FL",
                "zip":     (prop.zip or "")[:5],
            }
            label = str(owner.id)
            records.append({
                "label":      label,
                "first_name": first,
                "last_name":  last,
                "address":    addr["address"],
                "city":       addr["city"],
                "state":      addr["state"],
                "zip":        addr["zip"],
            })
            label_map[label] = (owner, prop)
            # Secondary fallback key — use the address we actually submitted
            # (addr_override for entities, property address for individuals).
            address_map[addr["address"].upper().strip()] = (owner, prop)

        if not records:
            continue

        logger.info("[Tracerfy] Batch %d: submitting %d records...", batch_num, len(records))

        try:
            queue_id, estimated_wait = _submit_trace_batch(records, api_key, trace_type)
            logger.info("[Tracerfy] Batch %d queued — queue_id=%s est_wait=%ds",
                        batch_num, queue_id, estimated_wait)
            results = _poll_trace_queue(queue_id, api_key, estimated_wait)
        except RuntimeError as e:
            err_msg = str(e)
            logger.error("[Tracerfy] Batch %d failed: %s", batch_num, err_msg)
            if "401" in err_msg or "403" in err_msg or "429" in err_msg:
                send_alert(
                    subject="[Forced Action] Tracerfy API ERROR",
                    body=(
                        f"Tracerfy skip-trace halted: {err_msg}\n\n"
                        f"Check TRACERFY_API_KEY in .env and credits at tracerfy.com\n\n"
                        f"Forced Action Ops Alert — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
                    ),
                )
                break
            stats["failed"] += len(records)
            continue

        # ── Persist results ───────────────────────────────────────────────
        hit_ids: set[str] = set()   # owner_id strings that returned a hit
        with get_db_context() as session:
            from sqlalchemy import insert as sa_insert

            # ── Bulk pre-loads — eliminates N round trips down to 2 ──────────
            all_owner_ids = [int(lbl) for lbl in label_map]
            owners_by_id: dict[int, Owner] = {
                o.id: o
                for o in session.query(Owner).filter(Owner.id.in_(all_owner_ids)).all()
            }
            all_property_ids = [o.property_id for o in owners_by_id.values()]
            existing_ecs: dict[int, EnrichedContact] = {
                ec.property_id: ec
                for ec in session.query(EnrichedContact).filter(
                    EnrichedContact.property_id.in_(all_property_ids),
                    EnrichedContact.source == "tracerfy",
                ).all()
            }

            # Accumulate usage log dicts — one bulk insert at end, no per-row flush.
            usage_log_entries: list[dict] = []
            now = datetime.now(timezone.utc)

            for row in results:
                label = str(row.get("label") or "").strip()
                if label and label in label_map:
                    owner_snap, prop_snap = label_map[label]
                elif (row.get("address") or "").upper().strip() in address_map:
                    addr_echo = (row.get("address") or "").upper().strip()
                    owner_snap, prop_snap = address_map[addr_echo]
                    label = str(owner_snap.id)
                else:
                    logger.debug(
                        "[Tracerfy] Unmatched result row: label=%r addr=%r",
                        row.get("label"), row.get("address"),
                    )
                    continue

                hit_ids.add(label)
                parsed = _parse_trace_row(row)

                try:
                    owner = owners_by_id.get(owner_snap.id)
                    if owner is None:
                        continue

                    existing = existing_ecs.get(owner.property_id)

                    if existing and not retrace_misses:
                        stats["already_done"] += 1
                        continue

                    usage_log_entries.append({
                        "vendor":      "tracerfy",
                        "purpose":     "skip_trace",
                        "success":     parsed["match_success"],
                        "cost_cents":  cost_per_hit if parsed["match_success"] else 0,
                        "property_id": owner.property_id,
                        "request_ref": queue_id,
                        "created_at":  now,
                    })

                    # For Address-Only runs, stamp trace_type in raw_response
                    # so analytics can distinguish advanced from normal hits.
                    raw_row = dict(row)
                    if trace_type == "advanced":
                        raw_row["trace_type"] = "advanced"

                    if existing and retrace_misses:
                        existing.mobile_phone    = parsed["mobile_phone"]
                        existing.landline        = parsed["landline"]
                        existing.email           = parsed["email"]
                        existing.mailing_address = parsed["mailing_address"]
                        existing.match_success   = parsed["match_success"]
                        existing.raw_response    = raw_row
                        existing.enriched_at     = now
                    else:
                        session.add(EnrichedContact(
                            property_id=owner.property_id,
                            county_id=owner.county_id or county_id,
                            mobile_phone=parsed["mobile_phone"],
                            landline=parsed["landline"],
                            email=parsed["email"],
                            mailing_address=parsed["mailing_address"],
                            source="tracerfy",
                            match_success=parsed["match_success"],
                            raw_response=raw_row,
                            enriched_at=now,
                        ))

                    if parsed["mobile_phone"] and not owner.phone_1:
                        owner.phone_1 = parsed["mobile_phone"]
                    elif parsed["landline"] and not owner.phone_1:
                        owner.phone_1 = parsed["landline"]
                    if parsed["email"] and not owner.email_1:
                        owner.email_1 = parsed["email"]
                    if parsed["match_success"]:
                        owner.skip_trace_success = True

                    dnc_flags = parsed.get("dnc_flags")
                    if dnc_flags:
                        meta = dict(owner.phone_metadata or {})
                        meta["phone_1"] = {**(meta.get("phone_1") or {}), **dnc_flags}
                        owner.phone_metadata = meta

                    for dnc_num in parsed.get("all_dnc_phones") or []:
                        already = session.query(SmsOptOut).filter_by(phone=dnc_num).first()
                        if not already:
                            session.add(SmsOptOut(
                                phone=dnc_num,
                                keyword_used="DNC",
                                source="tracerfy_dnc",
                                opted_out_at=now,
                            ))
                            logger.info(
                                "[Tracerfy] DNC suppression: property_id=%d phone=%s",
                                owner.property_id, dnc_num,
                            )

                    if parsed["match_success"]:
                        stats["success"] += 1
                    else:
                        stats["failed"] += 1

                except Exception as e:
                    logger.error("[Tracerfy] Persist error owner_id=%d: %s", owner_snap.id, e)
                    logger.debug(traceback.format_exc())
                    stats["failed"] += 1

            # ── Bulk-write usage logs for hits ───────────────────────────────
            if usage_log_entries:
                session.execute(sa_insert(EnrichmentUsageLog), usage_log_entries)

            # ── Bulk-write misses ────────────────────────────────────────────
            if not retrace_misses:
                miss_snaps = [
                    owner_snap
                    for lbl, (owner_snap, _) in label_map.items()
                    if lbl not in hit_ids
                ]
                if miss_snaps:
                    session.execute(
                        sa_insert(EnrichedContact),
                        [
                            {
                                "property_id":   snap.property_id,
                                "county_id":     snap.county_id or county_id,
                                "source":        "tracerfy",
                                "match_success": False,
                                "enriched_at":   now,
                            }
                            for snap in miss_snaps
                        ],
                    )
                    session.execute(
                        sa_insert(EnrichmentUsageLog),
                        [
                            {
                                "vendor":      "tracerfy",
                                "purpose":     "skip_trace",
                                "success":     False,
                                "cost_cents":  0,
                                "property_id": snap.property_id,
                                "request_ref": queue_id,
                                "created_at":  now,
                            }
                            for snap in miss_snaps
                        ],
                    )
                    stats["failed"] += len(miss_snaps)

            session.commit()

        if batch_start + _BATCH_SIZE < len(rows):
            logger.info("[Tracerfy] Waiting %ds before next batch (rate limit)...", _BATCH_DELAY)
            time.sleep(_BATCH_DELAY)

    logger.info("=" * 60)
    logger.info("TRACERFY SKIP TRACE COMPLETE")
    logger.info("  Total processed  : %d", stats["total"])
    logger.info("  Success          : %d", stats["success"])
    logger.info("  No contact found : %d", stats["failed"])
    logger.info("  No address       : %d", stats["no_address"])
    logger.info("  Skipped (entity) : %d", stats["skipped_entity"])
    logger.info("=" * 60)

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys
    import json as _json

    parser = argparse.ArgumentParser(description="Tracerfy batch skip-trace (Tier 1, $0.02/hit)")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--county-id", dest="county_id", default="hillsborough")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--balance", action="store_true",
                        help="Print account credit balance and exit")
    parser.add_argument("--retrace-misses", dest="retrace_misses", action="store_true",
                        help="Re-submit properties with existing tracerfy miss EC rows")
    parser.add_argument("--individual-only", dest="individual_only", action="store_true",
                        help="Only process Individual owner_type records")
    parser.add_argument("--entity-only", dest="entity_only", action="store_true",
                        help="Only process non-Individual (LLC/Corp/Trust/Estate) records")
    args = parser.parse_args()

    try:
        if args.balance:
            print(_json.dumps(get_tracerfy_balance(), indent=2))
            sys.exit(0)

        stats = run_tracerfy_fallback(
            limit=args.limit,
            county_id=args.county_id,
            dry_run=args.dry_run,
            retrace_misses=args.retrace_misses,
            individual_only=args.individual_only,
            entity_only=args.entity_only,
        )
        sys.exit(0)
    except Exception as e:
        logger.error("Tracerfy fallback failed: %s", e)
        logger.debug(traceback.format_exc())
        sys.exit(1)
