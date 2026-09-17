"""Self-serve pre-fill path — WP-7 (WI-1 through WI-6).

  GET  /go/{slug}                          — public, no auth. Resolve the
                                              tracked link, log the click,
                                              create the session (WI-2/WI-3),
                                              302 into the flow.
  GET  /selfserve/{token}                  — public, no auth. The borrower
                                              screen: pre-fill facts +
                                              corrections + the 7 confirmation
                                              questions + consent checkbox,
                                              one HTML form (WI-5). v1
                                              simplification: the plan
                                              describes 3 screens; this ships
                                              as one page with three visual
                                              sections rather than three
                                              server round-trips — there is
                                              no JS framework in this repo to
                                              carry state between page loads
                                              cheaply, and splitting it adds
                                              no functional value at v1 scope.
  POST /api/selfserve/{token}/submit       — persist + handoff to Backflip
                                              (WI-3 + WI-6 combined).
  POST /api/admin/selfserve/tracked-links  — admin JWT. Mint a tracked link.
  GET  /api/admin/selfserve/tracked-links  — admin JWT. List tracked links.

Link generation without an engineer (the client's "own outreach lane"
requirement) ships as a Slack slash command, not a route in this file — the
FA Max Slack app is Socket Mode, so slash commands arrive over a WebSocket,
not an HTTP POST. See src/services/tracked_links.py
(handle_tracked_link_socket_request) and src/services/relay/socket_listener.py
(PR #276) for where it actually lives.

An unknown or inactive slug redirects to the generic flow and logs a
warning — it never 404s a borrower (a dead link in a printed mailer must not
be a dead end). An unknown *session* token does 404 — that can only happen
from a mistyped/expired URL, not a borrower's own click.
"""
from __future__ import annotations

import hashlib
import html
import json
import logging
import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.selfserve_questions import CONSENT_COPY, SELFSERVE_QUESTIONS
from config.settings import settings
from src.api.admin_router import get_current_admin
from src.api.deps import get_db
from src.core.models import TrackedLink
from src.core.redis_client import rincr
from src.services.backflip_port import HandoffPayload, get_backflip_port
from src.services.prefill_assembly import assemble_prefill, find_property_by_address
from src.services.selfserve_sessions import (
    create_session,
    flag_suppressed_handoff,
    get_session_by_token,
    is_backflip_suppressed,
    submit_session,
)
from src.services.tracked_links import (
    VALID_TRACKED_LINK_KINDS,
    mint_link,
    record_click,
    resolve_slug,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["selfserve"])

# WI-4 — Redis rate limit on session creation and submit per IP. Fails open
# (never blocks) if Redis is unavailable, same convention as every other
# rincr() caller in this codebase (src/core/redis_client.py:122).
_RATE_LIMIT_WINDOW_SECONDS = 60
_CLICK_LIMIT_PER_MINUTE = 30
_SUBMIT_LIMIT_PER_MINUTE = 10


def _hash_ip(ip: Optional[str]) -> Optional[str]:
    """Salted IP hash — never store the raw IP. Salt is the admin JWT secret,
    reused rather than provisioning a new one for a non-cryptographic salt."""
    if not ip:
        return None
    salt = settings.admin_jwt_secret.get_secret_value() if settings.admin_jwt_secret else "no-salt"
    return hashlib.sha256(f"{salt}:{ip}".encode()).hexdigest()


def _rate_limited(bucket: str, limit: int) -> bool:
    """True if this bucket has exceeded `limit` hits in the current window.
    No ip_hash (unknown client, e.g. behind a proxy misconfig) never blocks —
    an IP-less request can't be rate-limited by IP without false-positiving
    every anonymized caller onto one shared counter."""
    return rincr(f"selfserve:rl:{bucket}", ttl_seconds=_RATE_LIMIT_WINDOW_SECONDS) > limit


# ---------------------------------------------------------------------------
# GET /go/{slug} — click, attribute, create the session
# ---------------------------------------------------------------------------


@router.get("/go/{slug}")
def click_tracked_link(slug: str, request: Request, db: Session = Depends(get_db)):
    ip_hash = _hash_ip(request.client.host if request.client else None)
    if ip_hash and _rate_limited(f"click:{ip_hash}", _CLICK_LIMIT_PER_MINUTE):
        raise HTTPException(status_code=429, detail="Too many requests.")

    link = resolve_slug(db, slug)
    token = str(uuid.uuid4())

    link_id = None
    property_id = None
    if link is None:
        logger.warning("selfserve: unknown or inactive slug=%r — degrading to generic flow", slug)
    else:
        link_id = link.id
        property_id = link.property_id
        record_click(
            db,
            tracked_link_id=link.id,
            session_token=token,
            ip_hash=ip_hash,
            user_agent=request.headers.get("user-agent"),
            referer=request.headers.get("referer"),
        )

    prefill = assemble_prefill(db, property_id).to_dict() if property_id else {"property_id": None, "fields": {}}
    session_row = create_session(db, prefill_snapshot=prefill, tracked_link_id=link_id, property_id=property_id)
    # The session's own token IS the click's attribution token — one UUID,
    # not two — so a plain 302 (no cookie/JWT round-trip) is sufficient.
    session_row.token = token
    db.commit()

    return RedirectResponse(url=f"/selfserve/{token}", status_code=302)


# ---------------------------------------------------------------------------
# GET /selfserve/{token} — the borrower screen
# ---------------------------------------------------------------------------


def _esc(value) -> str:
    """Escape before interpolating into HTML. Property/owner values are scraped
    from county portals — untrusted input on a public page."""
    return html.escape(str(value), quote=True)


def _render_field_row(key: str, value) -> str:
    display = value if not isinstance(value, (dict, list)) else str(value)
    return (
        f'<div class="field"><label>{_esc(key.replace("_", " ").title())}</label>'
        f'<input type="text" name="correction__{_esc(key)}" value="{_esc(display)}"></div>'
    )


def _render_question_row(q: dict) -> str:
    key, label = _esc(q["key"]), _esc(q["label"])
    if q["type"] == "select":
        options = "".join(f'<option value="{_esc(o)}">{_esc(o)}</option>' for o in q.get("options", []))
        return f'<div class="field"><label>{label}</label><select name="q__{key}">{options}</select></div>'
    if q["type"] == "boolean":
        return (
            f'<div class="field"><label>{label}</label>'
            f'<select name="q__{key}"><option value="true">Yes</option><option value="false">No</option></select></div>'
        )
    return f'<div class="field"><label>{label}</label><input type="text" name="q__{key}"></div>'


@router.get("/selfserve/{token}", response_class=HTMLResponse)
def selfserve_screen(token: str, db: Session = Depends(get_db)):
    session_row = get_session_by_token(db, token)
    if session_row is None:
        raise HTTPException(status_code=404, detail="Session not found or expired.")
    if session_row.status in ("handed_off",):
        raise HTTPException(status_code=409, detail="This session has already been submitted.")

    fields = (session_row.prefill_snapshot or {}).get("fields", {})
    address_prompt = ""
    if session_row.property_id is None:
        address_prompt = (
            '<div class="field"><label>Property address (we could not recognize it automatically)</label>'
            '<input type="text" name="manual_address"></div>'
        )
        prefill_html = "<p>We could not automatically recognize your property. Please enter the address below.</p>"
    else:
        prefill_html = "".join(_render_field_row(k, v["value"]) for k, v in fields.items()) or "<p>No public records found for this property.</p>"

    questions_html = "".join(_render_question_row(q) for q in SELFSERVE_QUESTIONS)

    return HTMLResponse(f"""
    <html><body>
    <form method="post" action="/api/selfserve/{token}/submit">
      <h2>Is this your property?</h2>
      {prefill_html}
      {address_prompt}
      <h2>A few more details</h2>
      {questions_html}
      <h2>Your contact info</h2>
      <div class="field"><label>Name</label><input type="text" name="contact_name" required></div>
      <div class="field"><label>Email</label><input type="email" name="contact_email" required></div>
      <div class="field"><label>Phone</label><input type="tel" name="contact_phone"></div>
      <div class="field">
        <label><input type="checkbox" name="consent"> {CONSENT_COPY}</label>
      </div>
      <button type="submit">Continue to Backflip</button>
    </form>
    </body></html>
    """)


# ---------------------------------------------------------------------------
# POST /api/selfserve/{token}/submit — persist + handoff
# ---------------------------------------------------------------------------


@router.post("/api/selfserve/{token}/submit")
async def submit_selfserve(token: str, request: Request, db: Session = Depends(get_db)):
    ip_hash = _hash_ip(request.client.host if request.client else None)
    if ip_hash and _rate_limited(f"submit:{ip_hash}", _SUBMIT_LIMIT_PER_MINUTE):
        raise HTTPException(status_code=429, detail="Too many requests.")

    session_row = get_session_by_token(db, token)
    if session_row is None:
        raise HTTPException(status_code=404, detail="Session not found or expired.")
    if session_row.status == "handed_off":
        raise HTTPException(status_code=409, detail="This session has already been submitted.")

    form = await request.form()

    if session_row.property_id is None and form.get("manual_address"):
        prop_id, _confidence = find_property_by_address(db, str(form["manual_address"]))
        if prop_id:
            prefill = assemble_prefill(db, prop_id).to_dict()
            db.execute(
                text(
                    "UPDATE selfserve_sessions SET property_id = :pid, "
                    "prefill_snapshot = CAST(:snap AS JSONB) WHERE token = :token"
                ),
                {"pid": prop_id, "snap": json.dumps(prefill), "token": token},
            )
            session_row.property_id = prop_id

    def _str_field(name: str) -> Optional[str]:
        # Form fields are plain text inputs — never file uploads — but a
        # crafted multipart request could send an UploadFile under this
        # field name. Reject rather than silently stringifying a file object.
        val = form.get(name)
        if val is None:
            return None
        if not isinstance(val, str):
            raise HTTPException(status_code=400, detail=f"{name} must be text.")
        return val

    corrections = {k[len("correction__"):]: v for k, v in form.items() if k.startswith("correction__") and isinstance(v, str)}
    confirmations = {k[len("q__"):]: v for k, v in form.items() if k.startswith("q__") and isinstance(v, str)}
    contact = {
        "name": _str_field("contact_name"),
        "email": _str_field("contact_email"),
        "phone": _str_field("contact_phone"),
    }
    consent_channels = ["email", "sms"] if form.get("consent") else []

    updated = submit_session(
        db,
        token=token,
        corrections=corrections or None,
        confirmations=confirmations,
        contact=contact,
        consent_channels=consent_channels,
    )

    # WI-4 / client-answered attribution rule (client-response doc Q5): if a
    # prospect already has an active Backflip campaign touch in motion,
    # Forced Action holds off rather than sending a competing outreach. The
    # session's own attribution redirect to Backflip *is* that outreach, so
    # it is what gets held — the borrower's answers are still saved, nothing
    # is lost, we just don't inject our own attribution on top of Backflip's
    # existing touch.
    if is_backflip_suppressed(db, email=contact.get("email"), phone=contact.get("phone")):
        logger.warning(
            "selfserve: session token=%s suppressed — active Backflip touch on this contact, holding off",
            token,
        )
        if updated.person_id:
            # Spec's failure-behavior section: "A send fails suppression.
            # Blocked, logged with reason, surfaced to me." A log line alone
            # doesn't satisfy "surfaced to me" — Josh needs to see this.
            flag_suppressed_handoff(db, person_id=updated.person_id, session_token=token, contact=contact)
        db.commit()
        return HTMLResponse(
            "<html><body><p>Thanks — you're already connected with Backflip on this. "
            "No further action needed from you; we'll stay out of the way.</p></body></html>"
        )

    port = get_backflip_port()
    result = port.handoff(HandoffPayload(
        session_token=token,
        prefill_fields={k: v.get("value") for k, v in (updated.prefill_snapshot or {}).get("fields", {}).items()},
        confirmations=confirmations,
        contact=contact,
    ))

    db.execute(
        text(
            "UPDATE selfserve_sessions SET status = 'handed_off', handoff_ref = :ref, "
            "handed_off_at = now() WHERE token = :token"
        ),
        {"ref": result.handoff_ref, "token": token},
    )
    db.commit()

    logger.info("selfserve: session token=%s handed off, ref=%s", token, result.handoff_ref)
    return RedirectResponse(url=result.redirect_url, status_code=302)


# ---------------------------------------------------------------------------
# Admin: mint + list tracked links
# ---------------------------------------------------------------------------


class _MintTrackedLinkRequest(BaseModel):
    kind: str = Field(..., description="partner | campaign | source | property_mailer")
    label: str = Field(..., min_length=1)
    partner_ref: Optional[str] = None
    campaign_ref: Optional[str] = None
    property_id: Optional[int] = None
    destination: Optional[str] = None


class _TrackedLinkResponse(BaseModel):
    id: int
    slug: str
    kind: str
    label: str
    url: str
    is_active: bool


def _to_response(link: TrackedLink) -> _TrackedLinkResponse:
    base = settings.app_base_url.rstrip("/") if settings.app_base_url else ""
    return _TrackedLinkResponse(
        id=link.id,
        slug=link.slug,
        kind=link.kind,
        label=link.label,
        url=f"{base}/go/{link.slug}",
        is_active=link.is_active,
    )


@router.post("/api/admin/selfserve/tracked-links", response_model=_TrackedLinkResponse, status_code=201)
def create_tracked_link(
    body: _MintTrackedLinkRequest,
    admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    if body.kind not in VALID_TRACKED_LINK_KINDS:
        raise HTTPException(status_code=422, detail=f"kind must be one of {VALID_TRACKED_LINK_KINDS}")

    link = mint_link(
        db,
        kind=body.kind,
        label=body.label,
        created_by=admin.get("sub", "admin"),
        partner_ref=body.partner_ref,
        campaign_ref=body.campaign_ref,
        property_id=body.property_id,
        destination=body.destination,
    )
    db.commit()
    return _to_response(link)


@router.get("/api/admin/selfserve/tracked-links", response_model=list[_TrackedLinkResponse])
def list_tracked_links(
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    rows = db.execute(
        text(
            "SELECT id, slug, kind, label, partner_ref, campaign_ref, "
            "property_id, destination, is_active, created_by, created_at "
            "FROM tracked_links ORDER BY created_at DESC LIMIT 200"
        )
    ).mappings().all()
    return [_to_response(TrackedLink(**dict(row))) for row in rows]

