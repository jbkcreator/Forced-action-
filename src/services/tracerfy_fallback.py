"""
Tracerfy skip trace — Tier 1 in the waterfall.

Uses POST /trace/ (batch async, 1 credit/hit = $0.02) rather than
POST /trace/lookup/ (instant sync, 5 credits/hit = $0.10). We send
owner name + address so the normal tier applies.

Address-only fallback (address_only_fallback=True): re-submits existing
miss rows as advanced traces without owner name (2 credits = $0.04). Bypasses name
parsing failures — useful when AND patterns / ET AL / entity names caused
the original run to miss.

Flow:
  1. Collect candidates in batches of up to 100
  2. POST to /trace/ with trace_type="normal" → queue_id
  3. Poll GET /queue/:id until pending=False
  4. Match result rows back to owners by NORMALIZED ADDRESS (the queue endpoint
     does not echo our `label`/owner_id, so a canonical street-address key is the
     only reliable join — see src.services.skip_trace_ledger.trace_key)
  5. Persist EnrichedContact, update Owner fields, suppress DNC phones

Duplicate-charge guardrail: Tracerfy bills per hit with no server-side dedup.
Before any submission, every candidate is gated by skip_trace_ledger against the
addresses we have already traced (keyed on submission history, not on the
hit/miss flag we recorded), so an address is never paid for twice. A per-run
spend cap (SKIP_TRACE_MAX_RUN_COST_CENTS) is a second backstop.

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

import json
import re
import time
import traceback
from datetime import datetime, timezone
from types import SimpleNamespace
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
from src.services.skip_trace_ledger import (
    ADVANCED, NORMAL, BillingModel, RunSpendCap, already_traced, should_submit, trace_key,
)
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


def _submit_trace_batch(
    records: list[dict],
    api_key: str,
    address_only: bool = False,
) -> tuple[str, int]:
    """
    POST /trace/ as multipart/form-data.
    Returns (queue_id, estimated_wait_seconds).

    address_only=True uses trace_type="advanced", where Tracerfy identifies
    the owner from address/city/state without first_name/last_name columns.

    Confirmed response shape:
      {"queue_id": 94858, "status": "pending", "rows_uploaded": 3,
       "estimated_wait_seconds": 30, "credits_per_lead": 1, ...}
    """
    import json as _json

    fields = {
        "address_column":      "address",
        "city_column":         "city",
        "state_column":        "state",
        "zip_column":          "zip",
        "label_column":        "label",   # sent for traceability only; queue results do NOT echo it back (see module docstring)
        "mail_address_column": "address",
        "mail_city_column":    "city",
        "mail_state_column":   "state",
        "mailing_zip_column":  "zip",
        "trace_type":          "advanced" if address_only else "normal",
        "json_data":           _json.dumps(records),
    }
    if not address_only:
        fields["first_name_column"] = "first_name"
        fields["last_name_column"]  = "last_name"
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


def _poll_trace_queue(
    queue_id: str,
    api_key: str,
    estimated_wait: int = 30,
    stable_rounds_required: int = 2,
    max_empty_attempts: int = 120,
) -> list[dict]:
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
        try:
            resp = requests.get(url, headers=_headers(api_key), timeout=30)
        except requests.RequestException as e:
            logger.warning(
                "[Tracerfy] queue=%s poll failed attempt=%d/%d: %s",
                queue_id,
                attempt + 1,
                max_empty_attempts,
                e,
            )
            if attempt + 1 >= max_empty_attempts:
                logger.warning(
                    "[Tracerfy] queue=%s did not return results after poll errors — treating as no-result queue",
                    queue_id,
                )
                return []
            time.sleep(_POLL_INTERVAL)
            continue
        if not resp.ok:
            raise RuntimeError(f"Tracerfy GET /queue/ HTTP {resp.status_code}: {resp.text[:300]}")
        results = resp.json()
        current_count = len(results)

        if current_count == 0:
            logger.info("[Tracerfy] queue=%s still empty attempt=%d", queue_id, attempt + 1)
            if attempt + 1 >= max_empty_attempts:
                logger.warning(
                    "[Tracerfy] queue=%s stayed empty after %d attempts — treating as no-result queue",
                    queue_id,
                    attempt + 1,
                )
                return []
            time.sleep(_POLL_INTERVAL)
            continue

        # Tracerfy streams results as it processes — wait until count stabilises
        # across two consecutive polls before accepting as complete.
        if current_count == last_count:
            stable_rounds += 1
            if stable_rounds >= stable_rounds_required:
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
    address_only_fallback: bool = False,
    force_retrace: bool = False,
) -> dict:
    """
    Run Tracerfy batch skip-trace (POST /trace/, 1 credit/hit = $0.02) for Gold+ leads.

    Standard mode (owner_ids=None): selects Gold+ candidates not yet Tracerfy-traced.
    Waterfall mode (owner_ids=[...]): processes a specific list supplied by the orchestrator.
    retrace_misses=True: re-submits properties with existing tracerfy miss EC rows,
      updating them in-place on a hit. Ignores owner_ids when active.
    address_only_fallback=True: re-submits existing miss rows without owner name
      (2 credits = $0.04). Bypasses name parsing failures. Ignores owner_ids.
    individual_only / entity_only: restrict to individual or entity owner types.
    force_retrace=True: bypass the address dedup ledger (deliberate re-verification
      only — normally an address is never submitted twice).

    Duplicate-charge guardrail: every candidate is gated by skip_trace_ledger so a
    normalized address is submitted at most once per mode (one normal, one advanced
    retry), regardless of which selection path fed it in. retrace_misses re-submits
    are therefore no-ops unless force_retrace=True — use address_only_fallback for
    miss recovery.

    DNC side-effect: phones with dnc=True or litigator=True are written to
    sms_opt_outs(source="tracerfy_dnc") so can_send() blocks them immediately.

    Returns stats dict.
    """
    settings     = get_settings()
    # Address-only traces use advanced mode: 2 credits ($0.04) vs 1 credit for name+address.
    cost_per_hit = 4 if address_only_fallback else settings.tracerfy_cost_cents

    if not settings.tracerfy_api_key:
        logger.warning("TRACERFY_API_KEY not set — Tracerfy skip-trace skipped")
        return {"skipped": True, "reason": "TRACERFY_API_KEY not configured"}

    api_key = settings.tracerfy_api_key.get_secret_value()
    stats   = {
        "total": 0, "success": 0, "failed": 0,
        "no_address": 0, "already_done": 0, "skipped": False,
        "skipped_entity": 0, "skipped_already_traced": 0, "aborted_cost_cap": False,
    }

    with get_db_context() as session:
        from sqlalchemy import text as sa_text

        if address_only_fallback and owner_ids is not None:
            raw_rows = session.execute(sa_text("""
                WITH gold_plus AS MATERIALIZED (
                    SELECT property_id
                    FROM (
                        SELECT DISTINCT ON (property_id)
                            property_id,
                            lead_tier
                        FROM distress_scores
                        WHERE county_id = :county_id
                        ORDER BY property_id, score_date DESC NULLS LAST, id DESC
                    ) latest
                    WHERE lead_tier IN ('Gold', 'Platinum', 'Ultra Platinum')
                )
                SELECT DISTINCT ON (o.id)
                    o.id AS owner_id,
                    o.property_id,
                    o.county_id AS owner_county_id,
                    p.id AS prop_id,
                    p.address,
                    p.city,
                    p.state,
                    p.zip
                FROM owners o
                JOIN properties p ON p.id = o.property_id
                JOIN gold_plus gp ON gp.property_id = o.property_id
                WHERE o.id = ANY(:owner_ids)
                  AND o.county_id = :county_id
                  AND (o.phone_1 IS NULL OR length(trim(o.phone_1)) = 0)
                  AND (o.phone_2 IS NULL OR length(trim(o.phone_2)) = 0)
                  AND (o.phone_3 IS NULL OR length(trim(o.phone_3)) = 0)
                  AND (o.email_1 IS NULL OR length(trim(o.email_1)) = 0)
                  AND (o.email_2 IS NULL OR length(trim(o.email_2)) = 0)
                  AND p.address IS NOT NULL
                  AND p.address != ''
                  AND p.zip IS NOT NULL
                  AND p.zip != ''
                  AND NOT EXISTS (
                      SELECT 1
                      FROM enriched_contacts ec_success
                      WHERE ec_success.property_id = o.property_id
                        AND ec_success.match_success = TRUE
                  )
                ORDER BY o.id
            """), {
                "county_id": county_id,
                "owner_ids": owner_ids,
            }).mappings().all()
            rows = [
                (
                    SimpleNamespace(
                        id=r["owner_id"],
                        property_id=r["property_id"],
                        county_id=r["owner_county_id"],
                    ),
                    SimpleNamespace(
                        id=r["prop_id"],
                        address=r["address"],
                        city=r["city"],
                        state=r["state"],
                        zip=r["zip"],
                    ),
                )
                for r in raw_rows
            ]

        elif address_only_fallback:
            # Re-submit properties whose prior Tracerfy run returned no match.
            # retrace_misses: resubmit with same name+address (1 credit/hit).
            # address_only_fallback: resubmit address only in advanced mode, no name.
            # In both modes the existing miss EC row is updated in-place on a hit.
            type_filter = ""
            if individual_only:
                type_filter = " AND o.owner_type = 'Individual'"
            elif entity_only:
                type_filter = " AND o.owner_type != 'Individual'"

            # Address-only mode requires a valid property address — guard in SQL
            # since there is no name fallback if address is blank.
            addr_filter = (
                " AND p.address IS NOT NULL AND p.address != ''"
                " AND p.zip IS NOT NULL AND p.zip != ''"
            ) if address_only_fallback else ""

            logger.info(
                "[Tracerfy] Selecting Gold+ tracerfy misses county=%s limit=%d...",
                county_id, limit,
            )
            select_started = time.monotonic()
            raw_rows = session.execute(sa_text(f"""
                WITH gold_plus AS MATERIALIZED (
                    SELECT property_id
                    FROM (
                        SELECT DISTINCT ON (property_id)
                            property_id,
                            lead_tier
                        FROM distress_scores
                        WHERE county_id = :county_id
                        ORDER BY property_id, score_date DESC NULLS LAST, id DESC
                    ) latest
                    WHERE lead_tier IN ('Gold', 'Platinum', 'Ultra Platinum')
                )
                SELECT DISTINCT ON (o.id)
                    o.id AS owner_id,
                    o.property_id,
                    o.county_id AS owner_county_id,
                    p.id AS prop_id,
                    p.address,
                    p.city,
                    p.state,
                    p.zip
                FROM enriched_contacts ec
                JOIN owners o ON o.property_id = ec.property_id
                  AND o.county_id = :county_id
                JOIN gold_plus gp ON gp.property_id = o.property_id
                JOIN properties p ON p.id = o.property_id
                WHERE ec.source = 'tracerfy'
                  AND ec.match_success = FALSE
                  AND (o.phone_1 IS NULL OR length(trim(o.phone_1)) = 0)
                  AND (o.phone_2 IS NULL OR length(trim(o.phone_2)) = 0)
                  AND (o.phone_3 IS NULL OR length(trim(o.phone_3)) = 0)
                  AND (o.email_1 IS NULL OR length(trim(o.email_1)) = 0)
                  AND (o.email_2 IS NULL OR length(trim(o.email_2)) = 0)
                  AND p.address IS NOT NULL
                  AND p.address != ''
                  AND p.zip IS NOT NULL
                  AND p.zip != ''
                  AND NOT EXISTS (
                      SELECT 1
                      FROM enriched_contacts ec_success
                      WHERE ec_success.property_id = o.property_id
                        AND ec_success.match_success = TRUE
                  )
                {type_filter}
                {addr_filter}
                ORDER BY o.id
                LIMIT :limit
            """), {"county_id": county_id, "limit": limit}).mappings().all()
            logger.info(
                "[Tracerfy] Found %d Gold+ tracerfy misses in %.1fs",
                len(raw_rows), time.monotonic() - select_started,
            )
            rows = [
                (
                    SimpleNamespace(
                        id=r["owner_id"],
                        property_id=r["property_id"],
                        county_id=r["owner_county_id"],
                    ),
                    SimpleNamespace(
                        id=r["prop_id"],
                        address=r["address"],
                        city=r["city"],
                        state=r["state"],
                        zip=r["zip"],
                    ),
                )
                for r in raw_rows
            ]

        elif retrace_misses:
            type_filter = ""
            if individual_only:
                type_filter = " AND o.owner_type = 'Individual'"
            elif entity_only:
                type_filter = " AND o.owner_type != 'Individual'"

            logger.info(
                "[Tracerfy] Selecting Gold+ tracerfy misses county=%s limit=%d...",
                county_id, limit,
            )
            select_started = time.monotonic()
            raw_rows = session.execute(sa_text(f"""
                WITH gold_plus AS MATERIALIZED (
                    SELECT property_id
                    FROM (
                        SELECT DISTINCT ON (property_id)
                            property_id,
                            lead_tier
                        FROM distress_scores
                        WHERE county_id = :county_id
                        ORDER BY property_id, score_date DESC NULLS LAST, id DESC
                    ) latest
                    WHERE lead_tier IN ('Gold', 'Platinum', 'Ultra Platinum')
                )
                SELECT DISTINCT ON (o.id)
                    o.id AS owner_id,
                    o.property_id,
                    o.county_id AS owner_county_id,
                    o.owner_name,
                    o.owner_type,
                    o.phone_1,
                    o.email_1,
                    o.phone_metadata,
                    o.managing_members,
                    o.registered_agent_name,
                    o.registered_agent_address,
                    p.id AS prop_id,
                    p.address,
                    p.city,
                    p.state,
                    p.zip
                FROM enriched_contacts ec
                JOIN owners o ON o.property_id = ec.property_id
                  AND o.county_id = :county_id
                JOIN gold_plus gp ON gp.property_id = o.property_id
                JOIN properties p ON p.id = o.property_id
                WHERE ec.source = 'tracerfy'
                  AND ec.match_success = FALSE
                  AND (o.phone_1 IS NULL OR length(trim(o.phone_1)) = 0)
                  AND (o.phone_2 IS NULL OR length(trim(o.phone_2)) = 0)
                  AND (o.phone_3 IS NULL OR length(trim(o.phone_3)) = 0)
                  AND (o.email_1 IS NULL OR length(trim(o.email_1)) = 0)
                  AND (o.email_2 IS NULL OR length(trim(o.email_2)) = 0)
                  AND p.address IS NOT NULL
                  AND p.address != ''
                  AND p.zip IS NOT NULL
                  AND p.zip != ''
                  AND NOT EXISTS (
                      SELECT 1
                      FROM enriched_contacts ec_success
                      WHERE ec_success.property_id = o.property_id
                        AND ec_success.match_success = TRUE
                  )
                {type_filter}
                ORDER BY o.id
                LIMIT :limit
            """), {"county_id": county_id, "limit": limit}).mappings().all()
            logger.info(
                "[Tracerfy] Found %d Gold+ tracerfy misses in %.1fs",
                len(raw_rows), time.monotonic() - select_started,
            )
            rows = [
                (
                    SimpleNamespace(
                        id=r["owner_id"],
                        property_id=r["property_id"],
                        county_id=r["owner_county_id"],
                        owner_name=r["owner_name"],
                        owner_type=r["owner_type"],
                        phone_1=r["phone_1"],
                        email_1=r["email_1"],
                        phone_metadata=r["phone_metadata"],
                        managing_members=r["managing_members"],
                        registered_agent_name=r["registered_agent_name"],
                        registered_agent_address=r["registered_agent_address"],
                    ),
                    SimpleNamespace(
                        id=r["prop_id"],
                        address=r["address"],
                        city=r["city"],
                        state=r["state"],
                        zip=r["zip"],
                    ),
                )
                for r in raw_rows
            ]

        elif owner_ids is not None:
            raw_rows = session.execute(sa_text("""
                SELECT
                    o.id AS owner_id,
                    o.property_id,
                    o.county_id AS owner_county_id,
                    o.owner_name,
                    o.owner_type,
                    o.phone_1,
                    o.email_1,
                    o.phone_metadata,
                    o.managing_members,
                    o.registered_agent_name,
                    o.registered_agent_address,
                    p.id AS prop_id,
                    p.address,
                    p.city,
                    p.state,
                    p.zip
                FROM owners o
                JOIN properties p ON p.id = o.property_id
                WHERE o.id = ANY(:owner_ids)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM enriched_contacts ec
                      WHERE ec.property_id = o.property_id
                        AND ec.source = 'tracerfy'
                  )
                ORDER BY o.id
            """), {"owner_ids": owner_ids}).mappings().all()
            rows = [
                (
                    SimpleNamespace(
                        id=r["owner_id"],
                        property_id=r["property_id"],
                        county_id=r["owner_county_id"],
                        owner_name=r["owner_name"],
                        owner_type=r["owner_type"],
                        phone_1=r["phone_1"],
                        email_1=r["email_1"],
                        phone_metadata=r["phone_metadata"],
                        managing_members=r["managing_members"],
                        registered_agent_name=r["registered_agent_name"],
                        registered_agent_address=r["registered_agent_address"],
                    ),
                    SimpleNamespace(
                        id=r["prop_id"],
                        address=r["address"],
                        city=r["city"],
                        state=r["state"],
                        zip=r["zip"],
                    ),
                )
                for r in raw_rows
            ]

        else:
            raw_rows = session.execute(sa_text("""
                WITH gold_plus AS MATERIALIZED (
                    SELECT property_id
                    FROM (
                        SELECT DISTINCT ON (property_id)
                            property_id,
                            lead_tier
                        FROM distress_scores
                        WHERE county_id = :county_id
                        ORDER BY property_id, score_date DESC NULLS LAST, id DESC
                    ) latest
                    WHERE lead_tier IN ('Gold', 'Platinum', 'Ultra Platinum')
                )
                SELECT
                    o.id AS owner_id,
                    o.property_id,
                    o.county_id AS owner_county_id,
                    o.owner_name,
                    o.owner_type,
                    o.phone_1,
                    o.email_1,
                    o.phone_metadata,
                    o.managing_members,
                    o.registered_agent_name,
                    o.registered_agent_address,
                    p.id AS prop_id,
                    p.address,
                    p.city,
                    p.state,
                    p.zip
                FROM owners o
                JOIN properties p ON p.id = o.property_id
                JOIN gold_plus gp ON gp.property_id = o.property_id
                WHERE o.county_id = :county_id
                  AND (o.phone_1 IS NULL OR length(trim(o.phone_1)) = 0)
                  AND o.skip_trace_success IS NOT TRUE
                  AND p.address IS NOT NULL
                  AND p.address != ''
                  AND p.zip IS NOT NULL
                  AND p.zip != ''
                  AND NOT EXISTS (
                      SELECT 1
                      FROM enriched_contacts ec
                      WHERE ec.property_id = o.property_id
                        AND ec.source = 'tracerfy'
                  )
                ORDER BY o.id
                LIMIT :limit
            """), {"county_id": county_id, "limit": limit}).mappings().all()
            rows = [
                (
                    SimpleNamespace(
                        id=r["owner_id"],
                        property_id=r["property_id"],
                        county_id=r["owner_county_id"],
                        owner_name=r["owner_name"],
                        owner_type=r["owner_type"],
                        phone_1=r["phone_1"],
                        email_1=r["email_1"],
                        phone_metadata=r["phone_metadata"],
                        managing_members=r["managing_members"],
                        registered_agent_name=r["registered_agent_name"],
                        registered_agent_address=r["registered_agent_address"],
                    ),
                    SimpleNamespace(
                        id=r["prop_id"],
                        address=r["address"],
                        city=r["city"],
                        state=r["state"],
                        zip=r["zip"],
                    ),
                )
                for r in raw_rows
            ]

        # Post-query type filter (applies to default and owner_ids modes — retrace modes already filter in SQL)
        if not retrace_misses and not address_only_fallback:
            if individual_only:
                rows = [(o, p) for o, p in rows if o.owner_type == "Individual" and not _looks_corporate(o.owner_name or "")]
            elif entity_only:
                rows = [(o, p) for o, p in rows if o.owner_type != "Individual" or _looks_corporate(o.owner_name or "")]

    if not rows:
        logger.info("[Tracerfy] No candidates found.")
        return stats

    logger.info("[Tracerfy] Found %d candidates to enrich", len(rows))
    stats["total"] = len(rows)

    submit_mode = ADVANCED if address_only_fallback else NORMAL

    # ── Resolve each candidate to the address we would submit + its dedup key ──
    address_fallback_skipped_owner_ids: list[int] = []
    address_fallback_missed_owner_ids: list[int] = []
    resolved: list[tuple] = []  # (owner, prop, first, last, addr, key)
    for owner, prop in rows:
        if not prop.address or not prop.zip:
            stats["no_address"] += 1
            continue

        if address_only_fallback:
            first = last = ""
            addr = {
                "address": prop.address,
                "city":    prop.city or "Tampa",
                "state":   prop.state or "FL",
                "zip":     (prop.zip or "")[:5],
            }
        else:
            first, last, addr_override, is_traceable = _resolve_trace_subject(owner)
            if not is_traceable:
                stats["skipped_entity"] += 1
                address_fallback_skipped_owner_ids.append(owner.id)
                continue
            addr = addr_override or {
                "address": prop.address,
                "city":    prop.city or "Tampa",
                "state":   prop.state or "FL",
                "zip":     (prop.zip or "")[:5],
            }
        resolved.append((owner, prop, first, last, addr, trace_key(addr["address"], addr["zip"])))

    # ── Duplicate-charge guardrail: load the dedup ledger once, gate every ──
    # candidate. The ledger keys on whether an address was already SUBMITTED
    # (per mode) — never on our recorded hit/miss flag — so a paid address can
    # never be re-submitted regardless of which selection path produced it.
    with get_db_context() as session:
        ledger = already_traced(session, "tracerfy", [r[5] for r in resolved if r[5]])

    records: list[dict] = []
    record_keys: list[str] = []
    key_map: dict[str, list[tuple]] = {}  # trace_key → [(owner_snap, prop_snap), ...]
    for owner, prop, first, last, addr, key in resolved:
        if not key:
            logger.warning(
                "[Tracerfy] Unmatchable address — not submitted: owner_id=%s addr=%r",
                owner.id, addr["address"],
            )
            stats["no_address"] += 1
            continue
        if not force_retrace and not should_submit(key, submit_mode, ledger, BillingModel.PER_HIT):
            stats["skipped_already_traced"] += 1
            continue
        if key in key_map:
            # Co-owner or duplicate address — one paid trace, fanned out on persist.
            key_map[key].append((owner, prop))
            continue
        key_map[key] = [(owner, prop)]
        rec = {
            "label":   str(owner.id),
            "address": addr["address"],
            "city":    addr["city"],
            "state":   addr["state"],
            "zip":     addr["zip"],
        }
        if not address_only_fallback:
            rec["first_name"] = first
            rec["last_name"]  = last
        records.append(rec)
        record_keys.append(key)

    logger.info(
        "[Tracerfy] %d unique addresses to submit (skipped: already_traced=%d entity=%d no_address=%d)",
        len(records), stats["skipped_already_traced"], stats["skipped_entity"], stats["no_address"],
    )

    if dry_run:
        for rec in records[:5]:
            logger.info("[Tracerfy DRY RUN] Would trace: %s, %s %s %s",
                        rec["address"], rec.get("city"), rec.get("state"), rec.get("zip"))
        logger.info(
            "[Tracerfy DRY RUN] Would submit %d unique addresses (%d blocked by dedup ledger). No API call made.",
            len(records), stats["skipped_already_traced"],
        )
        return stats

    # ── Submit in batches, with a hard per-run spend ceiling as a backstop ──
    cap = RunSpendCap(settings.skip_trace_max_run_cost_cents)
    for batch_start in range(0, len(records), _BATCH_SIZE):
        batch_records = records[batch_start: batch_start + _BATCH_SIZE]
        batch_keys = set(record_keys[batch_start: batch_start + _BATCH_SIZE])
        batch_num = batch_start // _BATCH_SIZE + 1

        projected_cents = len(batch_records) * cost_per_hit
        if cap.would_exceed(projected_cents):
            logger.error(
                "[Tracerfy] Per-run spend cap %d¢ would be exceeded by batch %d "
                "(worst-case +%d¢, already %d¢) — stopping to protect spend.",
                cap.ceiling_cents, batch_num, projected_cents, cap.projected_cents,
            )
            send_alert(
                subject="[Forced Action] Tracerfy per-run spend cap hit",
                body=(
                    f"Tracerfy skip-trace stopped before batch {batch_num} "
                    f"({len(batch_records)} addresses, worst-case {projected_cents}¢): it would "
                    f"exceed the per-run ceiling of {cap.ceiling_cents}¢ "
                    f"(SKIP_TRACE_MAX_RUN_COST_CENTS). Raise it only if intended.\n\n"
                    f"Forced Action Ops Alert — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
                ),
            )
            stats["aborted_cost_cap"] = True
            break

        logger.info("[Tracerfy] Batch %d: submitting %d records...", batch_num, len(batch_records))

        try:
            queue_id, estimated_wait = _submit_trace_batch(batch_records, api_key, address_only=address_only_fallback)
            logger.info("[Tracerfy] Batch %d queued — queue_id=%s est_wait=%ds",
                        batch_num, queue_id, estimated_wait)
            stable_rounds_required = 8 if address_only_fallback else 2
            results = _poll_trace_queue(
                queue_id,
                api_key,
                estimated_wait,
                stable_rounds_required=stable_rounds_required,
                max_empty_attempts=36 if address_only_fallback else 120,
            )
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
            stats["failed"] += len(batch_records)
            continue

        # The batch was accepted by Tracerfy — count its worst-case cost against the cap.
        cap.add(projected_cents)

        # ── Persist results — matched by normalized-address key, fanned to co-owners ──
        hit_keys: set[str] = set()      # trace_keys that returned a row
        success_keys: set[str] = set()  # trace_keys that produced a contact
        with get_db_context() as session:
            from sqlalchemy import insert as sa_insert

            # ── Bulk pre-loads — eliminates N round trips down to 2 ──────────
            batch_owner_ids = [o.id for k in batch_keys for (o, _) in key_map[k]]
            preload_rows = session.execute(sa_text("""
                SELECT
                    o.id AS owner_id,
                    o.property_id,
                    o.county_id,
                    o.phone_1,
                    o.email_1,
                    o.phone_metadata,
                    ec.id AS ec_id,
                    ec.match_success AS ec_match_success
                FROM owners o
                LEFT JOIN LATERAL (
                    SELECT id, property_id, match_success
                    FROM enriched_contacts
                    WHERE property_id = o.property_id
                      AND source = 'tracerfy'
                    ORDER BY enriched_at DESC NULLS LAST, id DESC
                    LIMIT 1
                ) ec ON TRUE
                WHERE o.id = ANY(:ids)
            """), {"ids": batch_owner_ids}).mappings().all()
            owners_by_id = {}
            existing_ecs = {}
            for r in preload_rows:
                owners_by_id[r["owner_id"]] = SimpleNamespace(
                    id=r["owner_id"],
                    property_id=r["property_id"],
                    county_id=r["county_id"],
                    phone_1=r["phone_1"],
                    email_1=r["email_1"],
                    phone_metadata=r["phone_metadata"],
                )
                if r["ec_id"] is not None:
                    existing_ecs[r["property_id"]] = SimpleNamespace(
                        id=r["ec_id"],
                        property_id=r["property_id"],
                        match_success=r["ec_match_success"],
                    )

            # Accumulate write dicts — one bulk insert/update each at the end.
            usage_log_entries: list[dict] = []
            ec_update_entries: list[dict] = []
            owner_update_entries: list[dict] = []
            now = datetime.now(timezone.utc)

            for row in results:
                rkey = trace_key(row.get("address"), row.get("zip"))
                targets = key_map.get(rkey)
                if not targets or rkey not in batch_keys:
                    logger.warning(
                        "[Tracerfy] Unmatched result row (no address-key match): addr=%r zip=%r",
                        row.get("address"), row.get("zip"),
                    )
                    continue

                hit_keys.add(rkey)
                parsed = _parse_trace_row(row)

                # Fan the single paid result out to every owner sharing this address.
                for owner_snap, prop_snap in targets:
                    try:
                        owner = owners_by_id.get(owner_snap.id)
                        if owner is None:
                            continue

                        existing = existing_ecs.get(owner.property_id)

                        if existing and not retrace_misses and not address_only_fallback:
                            stats["already_done"] += 1
                            continue

                        usage_log_entries.append({
                            "vendor":         "tracerfy",
                            "purpose":        "skip_trace",
                            "success":        parsed["match_success"],
                            "cost_cents":     cost_per_hit if parsed["match_success"] else 0,
                            "property_id":    owner.property_id,
                            "target_address": rkey,
                            "request_ref":    queue_id,
                            "created_at":     now,
                        })

                        if existing and (retrace_misses or address_only_fallback):
                            ec_update_entries.append({
                                "mobile_phone": parsed["mobile_phone"],
                                "landline": parsed["landline"],
                                "email": parsed["email"],
                                "mailing_address": parsed["mailing_address"],
                                "match_success": parsed["match_success"],
                                "raw_response": json.dumps(row),
                                "enriched_at": now,
                                "trace_type": "advanced" if address_only_fallback else "normal",
                                "ec_id": existing.id,
                            })
                        else:
                            session.add(EnrichedContact(
                                property_id=owner.property_id,
                                county_id=owner.county_id or county_id,
                                mobile_phone=parsed["mobile_phone"],
                                landline=parsed["landline"],
                                email=parsed["email"],
                                mailing_address=parsed["mailing_address"],
                                source="tracerfy",
                                trace_type="advanced" if address_only_fallback else "normal",
                                match_success=parsed["match_success"],
                                raw_response=row,
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
                            owner_update_entries.append({
                                "phone_1": owner.phone_1,
                                "email_1": owner.email_1,
                                "owner_id": owner.id,
                            })

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
                            success_keys.add(rkey)
                            stats["success"] += 1
                        else:
                            stats["failed"] += 1

                    except Exception as e:
                        logger.error("[Tracerfy] Persist error owner_id=%d: %s", owner_snap.id, e)
                        logger.debug(traceback.format_exc())
                        stats["failed"] += 1

            # ── Bulk-write hit usage logs, EC updates, owner updates ──────────
            if usage_log_entries:
                session.execute(sa_insert(EnrichmentUsageLog), usage_log_entries)

            if ec_update_entries:
                session.execute(sa_text("""
                    UPDATE enriched_contacts
                    SET mobile_phone = :mobile_phone,
                        landline = :landline,
                        email = :email,
                        mailing_address = :mailing_address,
                        match_success = :match_success,
                        raw_response = CAST(:raw_response AS JSONB),
                        enriched_at = :enriched_at,
                        trace_type = :trace_type
                    WHERE id = :ec_id
                """), ec_update_entries)

            if owner_update_entries:
                session.execute(sa_text("""
                    UPDATE owners
                    SET phone_1 = :phone_1,
                        email_1 = :email_1,
                        skip_trace_success = TRUE
                    WHERE id = :owner_id
                """), owner_update_entries)

            # ── Misses: batch keys with no returned row, fanned to all co-owners ──
            # retrace_misses (normal) historically records nothing on a miss — the
            # existing EC row already seeds the ledger — so skip writing there.
            if not (retrace_misses and not address_only_fallback):
                miss_ec_entries: list[dict] = []
                miss_usage_entries: list[dict] = []
                for k in batch_keys:
                    if k in hit_keys:
                        continue
                    for owner_snap, _prop_snap in key_map[k]:
                        miss_usage_entries.append({
                            "vendor":         "tracerfy",
                            "purpose":        "skip_trace",
                            "success":        False,
                            "cost_cents":     0,
                            "property_id":    owner_snap.property_id,
                            "target_address": k,
                            "request_ref":    queue_id,
                            "error":          "address_only_no_row" if address_only_fallback else None,
                            "created_at":     now,
                        })
                        if owner_snap.property_id not in existing_ecs:
                            miss_ec_entries.append({
                                "property_id":   owner_snap.property_id,
                                "county_id":     owner_snap.county_id or county_id,
                                "source":        "tracerfy",
                                "trace_type":    "advanced" if address_only_fallback else "normal",
                                "match_success": False,
                                "enriched_at":   now,
                            })
                        stats["failed"] += 1
                if miss_ec_entries:
                    session.execute(sa_insert(EnrichedContact), miss_ec_entries)
                if miss_usage_entries:
                    session.execute(sa_insert(EnrichmentUsageLog), miss_usage_entries)

            session.commit()

        if retrace_misses and not address_only_fallback:
            address_fallback_missed_owner_ids.extend(
                o.id
                for k in batch_keys if k not in success_keys
                for (o, _) in key_map[k]
            )

        if batch_start + _BATCH_SIZE < len(records):
            logger.info("[Tracerfy] Waiting %ds before next batch (rate limit)...", _BATCH_DELAY)
            time.sleep(_BATCH_DELAY)

    if not address_only_fallback and (
        address_fallback_skipped_owner_ids or address_fallback_missed_owner_ids
    ):
        seen_fallback_ids = set()
        fallback_ids = []
        for owner_id in address_fallback_skipped_owner_ids + address_fallback_missed_owner_ids:
            if owner_id not in seen_fallback_ids:
                fallback_ids.append(owner_id)
                seen_fallback_ids.add(owner_id)
        logger.info(
            "[Tracerfy] Normal trace left %d owner(s); running address-only fallback "
            "(skipped_name=%d normal_miss=%d)...",
            len(fallback_ids),
            len(set(address_fallback_skipped_owner_ids)),
            len(set(address_fallback_missed_owner_ids)),
        )
        fallback_stats = run_tracerfy_fallback(
            limit=len(fallback_ids),
            county_id=county_id,
            owner_ids=fallback_ids,
            dry_run=False,
            retrace_misses=False,
            individual_only=False,
            entity_only=False,
            address_only_fallback=True,
        )
        stats["success"] += fallback_stats.get("success", 0)
        stats["failed"] += fallback_stats.get("failed", 0)
        stats["no_address"] += fallback_stats.get("no_address", 0)

    logger.info("=" * 60)
    logger.info("TRACERFY SKIP TRACE COMPLETE")
    logger.info("  Total processed       : %d", stats["total"])
    logger.info("  Success               : %d", stats["success"])
    logger.info("  No contact found      : %d", stats["failed"])
    logger.info("  No address            : %d", stats["no_address"])
    logger.info("  Skipped (entity)      : %d", stats["skipped_entity"])
    logger.info("  Skipped (already traced): %d", stats["skipped_already_traced"])
    if stats["aborted_cost_cap"]:
        logger.warning("  ABORTED on per-run spend cap — some candidates were not submitted")
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
    parser.add_argument("--address-only-fallback", dest="address_only_fallback", action="store_true",
                        help="Re-submit existing miss rows using address-only advanced trace (2 credits = $0.04)")
    parser.add_argument("--force-retrace", dest="force_retrace", action="store_true",
                        help="Bypass the address dedup ledger (deliberate re-verification — may re-charge)")
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
            address_only_fallback=args.address_only_fallback,
            force_retrace=args.force_retrace,
        )
        sys.exit(0)
    except Exception as e:
        logger.error("Tracerfy fallback failed: %s", e)
        logger.debug(traceback.format_exc())
        sys.exit(1)
