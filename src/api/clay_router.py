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
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from src.core.database import get_db_context

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/clay", tags=["clay"])

# Cap on how much of the (possibly huge) Google results blob we feed the LLM.
_MAX_SEARCH_CHARS = 12_000
# Below this, we never surface a company_linkedin_url (safety rule 9).
_MIN_CONFIDENCE = 0.70


def get_db():
    with get_db_context() as db:
        yield db

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
    owner_name: Optional[str] = None
    company_name: Optional[str] = None
    domain: Optional[str] = None
    linkedin_url: Optional[str] = None
    # Clay may send a string, a list of result objects, or a single object.
    google_search_results: Union[str, list, dict, None] = None
    city: Optional[str] = None
    state: Optional[str] = None


class VerifyLinkedInResponse(BaseModel):
    linkedin_url: Optional[str] = None
    profile_type: str  # "person" | "company" | "no_match"
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str


class ClayWebhookResponse(BaseModel):
    status: str
    message: str
    lead_id: int
    updated: bool
    fields_received: list[str]


# ── Helpers ─────────────────────────────────────────────────────────────────

def _clean_optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _coerce_lead_id(value: Any) -> int:
    try:
        lead_id = int(str(value).strip())
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="lead_id must be a valid integer")
    if lead_id <= 0:
        raise HTTPException(status_code=422, detail="lead_id must be a positive integer")
    return lead_id


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



def _no_match(reason: str) -> dict:
    return {
        "linkedin_url": None,
        "profile_type": "no_match",
        "confidence": 0.0,
        "reason": reason,
    }

_SYSTEM_PROMPT = (
    "You find the best LinkedIn URL to contact the owner of a business from Google search results.\n\n"
    "You will receive: owner name, company name, domain, location, and Google search results.\n\n"
    "Priority order for picking a URL:\n"
    "1. Owner's personal LinkedIn profile (/in/...) — preferred, enables direct contact.\n"
    "2. Company LinkedIn page (/company/...) — fallback if no clear personal profile is found.\n\n"
    "Rules:\n"
    "- Only choose a URL that appears in the search results — never fabricate one.\n"
    "- For a person profile: owner name must clearly match (allow nickname variations, e.g. 'Zach' for 'Zachary').\n"
    "- For a company page: company name or domain must clearly match.\n"
    "- If multiple person profiles exist, prefer the one whose title/snippet aligns with the industry or company.\n"
    "- If no confident match exists, return null for linkedin_url.\n\n"
    "Respond with ONLY a JSON object, no prose, in exactly this shape:\n"
    "{\n"
    '  "linkedin_url": string or null,\n'
    '  "profile_type": "person" | "company" | "no_match",\n'
    '  "confidence": number 0..1,\n'
    '  "reason": string\n'
    "}"
)



def _build_user_prompt(req, search_text: str) -> str:
    location = ", ".join(p for p in (req.city, req.state) if p) or "unknown"
    return (
        f"Owner name: {req.owner_name or 'unknown'}\n"
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




# ── Endpoint ────────────────────────────────────────────────────────────────

@router.post("/webhook/clay", response_model=ClayWebhookResponse)
async def clay_webhook(
    request: Request,
    _auth: None = Depends(verify_clay_secret),
    db=Depends(get_db),
):
    """
    Receive Clay enrichment data and update the matching DBPR contact.

    Blank or missing enrichment fields are ignored so existing contact data is
    not overwritten by partial webhook payloads.
    """
    # ── Read the raw body ────────────────────────────────────────────────────
    raw_body = await request.body()
    try:
        data = json.loads(raw_body) if raw_body else {}
    except (json.JSONDecodeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    if not isinstance(data, dict):
        raise HTTPException(status_code=422, detail="JSON body must be an object")

    # ── Extract requested fields ─────────────────────────────────────────────
    lead_id = _coerce_lead_id(data.get("lead_id"))
    fields = {
        "work_email": _clean_optional_str(data.get("work_email")),
        "linkedin_url": _clean_optional_str(data.get("linkedin_url")),
        "personal_email": _clean_optional_str(data.get("personal_email")),
        "domain": _clean_optional_str(data.get("domain")),
    }
    fields_received = [name for name, value in fields.items() if value is not None]

    logger.info(
        "[clay] webhook received: lead_id=%s, fields_received=%s",
        lead_id,
        fields_received,
    )

    if not fields_received:
        return ClayWebhookResponse(
            status="accepted",
            message="No enrichment fields provided; no database update performed.",
            lead_id=lead_id,
            updated=False,
            fields_received=[],
        )

    try:
        result = db.execute(
            text(
                """
                UPDATE dbpr_contacts
                SET
                    work_email = COALESCE(:work_email, work_email),
                    linkedin_url = COALESCE(:linkedin_url, linkedin_url),
                    email = COALESCE(:personal_email, email),
                    domain = COALESCE(:domain, domain),
                    clay_synced_at = NOW(),
                    clay_synced = TRUE,
                    updated_at = NOW()
                WHERE id = :lead_id
                """
            ),
            {
                "lead_id": lead_id,
                "work_email": fields["work_email"],
                "linkedin_url": fields["linkedin_url"],
                "personal_email": fields["personal_email"],
                "domain": fields["domain"],
            },
        )
    except SQLAlchemyError:
        logger.exception("[clay] failed to update dbpr_contacts: lead_id=%s", lead_id)
        raise HTTPException(status_code=503, detail="Database update failed")

    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"DBPR contact {lead_id} not found")

    return ClayWebhookResponse(
        status="success",
        message="Clay enrichment data updated successfully.",
        lead_id=lead_id,
        updated=True,
        fields_received=fields_received,
    )
