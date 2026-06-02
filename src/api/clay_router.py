"""
Clay HTTP API enrichment endpoints.

Clay's "HTTP API" enrichment column POSTs a row to these endpoints and maps the
JSON response back into Clay table columns. Auth is a static shared bearer
secret (CLAY_HTTP_API_SECRET) — Clay sends:

    Authorization: Bearer <CLAY_HTTP_API_SECRET>

Endpoints:
    POST /api/clay/resolve-linkedin-url
        Given company_name / domain / location + Google search results, ask the
        LLM to pick the single most likely LinkedIn *company page* URL and
        return structured JSON. Rejects people profiles, jobs, posts, pulse,
        school, and search URLs.
"""

from __future__ import annotations

import hmac
import json
import logging
import re
from types import SimpleNamespace
from typing import Any, Optional, Union

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from config.settings import get_settings
from src.services.claude_router import call_claude

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/clay", tags=["clay"])

# Cap on how much of the (possibly huge) Google results blob we feed the LLM.
_MAX_SEARCH_CHARS = 12_000
# Below this, we never surface a company_linkedin_url (safety rule 9).
_MIN_CONFIDENCE = 0.70

# A valid LinkedIn *company* page: linkedin.com/company/<slug>. Accepts optional
# locale subdomain (e.g. uk.linkedin.com) and trailing path/query.
_COMPANY_URL_RE = re.compile(
    r"^https?://([a-z0-9-]+\.)?linkedin\.com/company/[^/\s?#]+",
    re.IGNORECASE,
)


# ── Auth ────────────────────────────────────────────────────────────────────

def verify_clay_secret(request: Request) -> None:
    """Validate the static bearer secret Clay sends.

    - 503 when the secret is not configured (refuse to run unauthenticated).
    - 401 when the header is missing or does not match.
    Uses hmac.compare_digest for constant-time comparison.
    """
    settings = get_settings()
    secret = settings.clay_http_api_secret
    if not secret:
        raise HTTPException(status_code=503, detail="Clay HTTP API not configured")
    secret_value = secret.get_secret_value()

    auth = request.headers.get("Authorization", "")
    token = auth.removeprefix("Bearer ").strip()
    if not token or not hmac.compare_digest(token, secret_value):
        raise HTTPException(status_code=401, detail="Invalid Clay API token")


# ── Schemas ─────────────────────────────────────────────────────────────────

class ResolveLinkedInRequest(BaseModel):
    lead_id: Optional[str] = None
    company_name: Optional[str] = None
    domain: Optional[str] = None
    # Clay may send a string, a list of result objects, or a single object.
    google_search_results: Union[str, list, dict, None] = None
    city: Optional[str] = None
    state: Optional[str] = None


class LinkedInCandidate(BaseModel):
    url: str
    title: Optional[str] = None
    confidence: float = Field(ge=0.0, le=1.0)


class ResolveLinkedInResponse(BaseModel):
    company_linkedin_url: Optional[str] = None
    linkedin_confidence: float = Field(ge=0.0, le=1.0)
    linkedin_match_type: str  # "company_page" | "uncertain" | "no_match"
    linkedin_reason: str
    linkedin_candidates: list[LinkedInCandidate] = Field(default_factory=list)


# ── Helpers ─────────────────────────────────────────────────────────────────

def _normalize_search_results(raw: Union[str, list, dict, None]) -> str:
    """Coerce google_search_results into a single bounded string.

    Never raises — malformed/missing data returns "" or a best-effort str().
    """
    if raw is None:
        return ""
    if isinstance(raw, str):
        text = raw
    else:
        try:
            text = json.dumps(raw, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            text = str(raw)
    return text[:_MAX_SEARCH_CHARS]


def _is_company_url(url: Optional[str]) -> bool:
    return bool(url) and bool(_COMPANY_URL_RE.match(url.strip()))


def _no_match(reason: str, candidates: Optional[list] = None) -> dict:
    return {
        "company_linkedin_url": None,
        "linkedin_confidence": 0.0,
        "linkedin_match_type": "no_match",
        "linkedin_reason": reason,
        "linkedin_candidates": candidates or [],
    }


_SYSTEM_PROMPT = (
    "You identify the official LinkedIn COMPANY PAGE for a business from Google "
    "search results. A valid answer is a URL of the form "
    "https://www.linkedin.com/company/<slug> (a locale subdomain like "
    "uk.linkedin.com is acceptable).\n\n"
    "REJECT and never choose: LinkedIn people/profiles (/in/...), jobs (/jobs/...), "
    "posts (/posts/...), pulse articles (/pulse/...), school pages (/school/...), "
    "search pages (/search/...), and any non-linkedin.com URL.\n\n"
    "Use the company name, domain, and location to disambiguate between "
    "similarly named companies. Only return high confidence when the match is "
    "clearly the same company (name and/or domain align).\n\n"
    "Respond with ONLY a JSON object, no prose, in exactly this shape:\n"
    "{\n"
    '  "company_linkedin_url": string or null,\n'
    '  "linkedin_confidence": number 0..1,\n'
    '  "linkedin_match_type": "company_page" | "uncertain" | "no_match",\n'
    '  "linkedin_reason": string,\n'
    '  "linkedin_candidates": [{"url": string, "title": string or null, "confidence": number 0..1}]\n'
    "}"
)


def _build_user_prompt(req: ResolveLinkedInRequest, search_text: str) -> str:
    location = ", ".join(p for p in (req.city, req.state) if p) or "unknown"
    return (
        f"Company name: {req.company_name or 'unknown'}\n"
        f"Domain/website: {req.domain or 'unknown'}\n"
        f"Location: {location}\n\n"
        f"Google search results:\n{search_text or '(none provided)'}"
    )


def _parse_llm_json(text: str) -> Optional[dict]:
    """Extract the first JSON object from the LLM response. Returns None on failure."""
    if not text:
        return None
    # Strip markdown fences if present.
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n?|\n?```$", "", text).strip()
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        # Fall back to grabbing the outermost {...} block.
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            return None
        try:
            return json.loads(match.group(0))
        except (json.JSONDecodeError, ValueError):
            return None


def _coerce_confidence(value: Any) -> float:
    try:
        c = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, c))


def _sanitize_candidates(raw: Any) -> list[dict]:
    out: list[dict] = []
    if not isinstance(raw, list):
        return out
    for item in raw:
        if not isinstance(item, dict):
            continue
        url = item.get("url")
        if not isinstance(url, str) or not url.strip():
            continue
        title = item.get("title")
        out.append({
            "url": url.strip(),
            "title": title if isinstance(title, str) else None,
            "confidence": _coerce_confidence(item.get("confidence")),
        })
    return out


# ── Endpoint ────────────────────────────────────────────────────────────────

@router.post("/resolve-linkedin-url", response_model=ResolveLinkedInResponse)
async def resolve_linkedin_url(
    request: Request,
    _auth: None = Depends(verify_clay_secret),
):
    """Resolve the most likely LinkedIn company-page URL for a Clay row.

    Body validation is intentionally removed: Clay's payload is read raw and
    logged verbatim (debug visibility into what Clay actually sends), then
    resolution runs off a plain dict. Always returns 200 with a structured
    result (including the no_match shape) unless auth fails (401/503) or an
    unexpected server error occurs (500).
    """
    # ── Read + log the raw body exactly as received ──────────────────────────
    raw_body = await request.body()
    try:
        data = json.loads(raw_body) if raw_body else {}
    except (json.JSONDecodeError, ValueError):
        data = {}
    if not isinstance(data, dict):
        # Clay could (mis)send a bare array/string — wrap so .get() is safe.
        data = {"google_search_results": data}

    logger.info(
        "[clay] resolve-linkedin-url received: content_type=%s raw=%r parsed=%s",
        request.headers.get("content-type"),
        raw_body[:2000],
        json.dumps(data, default=str)[:2000],
    )

    # Plain-dict accessor — no Pydantic validation, never raises on bad types.
    req = SimpleNamespace(
        lead_id=data.get("lead_id"),
        company_name=data.get("company_name"),
        domain=data.get("domain"),
        google_search_results=data.get("google_search_results"),
        city=data.get("city"),
        state=data.get("state"),
    )

    search_text = _normalize_search_results(req.google_search_results)

    # Nothing to reason over and no identifiers → deterministic no_match.
    if not search_text and not (req.company_name or req.domain):
        return _no_match("No company identifiers or search results provided.")

    try:
        user_prompt = _build_user_prompt(req, search_text)
        raw = call_claude(
            task_type="classification",
            messages=[{"role": "user", "content": user_prompt}],
            system=_SYSTEM_PROMPT,
            max_tokens=700,
            force_tier="haiku",
        )
    except Exception as exc:  # unexpected LLM/transport failure
        logger.error("[clay] resolve-linkedin-url LLM call failed (lead_id=%s): %s",
                     req.lead_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="LLM resolution failed")

    parsed = _parse_llm_json(raw)
    if not parsed or not isinstance(parsed, dict):
        logger.warning("[clay] unparseable LLM response (lead_id=%s): %r", req.lead_id, raw[:200])
        return _no_match("Could not parse a LinkedIn match from search results.")

    chosen_url = parsed.get("company_linkedin_url")
    confidence = _coerce_confidence(parsed.get("linkedin_confidence"))
    match_type = parsed.get("linkedin_match_type")
    if match_type not in ("company_page", "uncertain", "no_match"):
        match_type = "uncertain"
    reason = parsed.get("linkedin_reason")
    if not isinstance(reason, str) or not reason:
        reason = "LLM did not provide a reason."
    candidates = _sanitize_candidates(parsed.get("linkedin_candidates"))

    # Safety rules (9): drop the URL if it isn't a company page or is low-confidence.
    if not _is_company_url(chosen_url):
        chosen_url = None
        if match_type == "company_page":
            match_type = "no_match"
        reason = f"{reason} | Rejected: not a linkedin.com/company URL."
    elif confidence < _MIN_CONFIDENCE:
        chosen_url = None
        match_type = "uncertain"
        reason = f"{reason} | Suppressed: confidence {confidence:.2f} < {_MIN_CONFIDENCE}."

    return {
        "company_linkedin_url": chosen_url,
        "linkedin_confidence": confidence,
        "linkedin_match_type": match_type,
        "linkedin_reason": reason,
        "linkedin_candidates": candidates,
    }
