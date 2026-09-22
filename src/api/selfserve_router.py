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
import json
import logging
import re
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
from src.api.public_page import escape_html, render_page
from src.core.models import TrackedLink
from src.core.redis_client import rincr
from src.services.backflip_port import HandoffPayload, get_backflip_port
from src.services.phone_utils import normalize as normalize_phone
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

# Client-side hint (HTML pattern attribute) and server-side enforcement
# share this one definition rather than drifting apart. A raw constant
# keeps the regex escapes literal instead of fighting the outer f-string
# that renders the page. Digits-only (no parens/dashes) because the field
# itself strips non-digit keystrokes live (see the oninput handler) --
# exactly 10 digits, the US number without a leading country code.
_US_PHONE_PATTERN = r"^\d{10}$"
_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


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
    buyer_entity_id = None
    if link is None:
        logger.warning("selfserve: unknown or inactive slug=%r — degrading to generic flow", slug)
    else:
        link_id = link.id
        property_id = link.property_id
        buyer_entity_id = link.buyer_entity_id
        record_click(
            db,
            tracked_link_id=link.id,
            session_token=token,
            ip_hash=ip_hash,
            user_agent=request.headers.get("user-agent"),
            referer=request.headers.get("referer"),
        )

    prefill = assemble_prefill(db, property_id).to_dict() if property_id else {"property_id": None, "fields": {}}
    session_row = create_session(
        db, prefill_snapshot=prefill, tracked_link_id=link_id, property_id=property_id,
        buyer_entity_id=buyer_entity_id,
    )
    # The session's own token IS the click's attribution token — one UUID,
    # not two — so a plain 302 (no cookie/JWT round-trip) is sufficient.
    session_row.token = token
    db.commit()

    return RedirectResponse(url=f"/selfserve/{token}", status_code=302)


# ---------------------------------------------------------------------------
# GET /selfserve/{token} — the borrower screen
# ---------------------------------------------------------------------------


# The page shell lives in src/api/public_page.py: a borrower can meet
# this page and the booking page in one conversation, and two copies of
# the stylesheet would drift into two different-looking products.
_esc = escape_html
_render_page = render_page


def _render_field_row(key: str, value) -> str:
    display = value if not isinstance(value, (dict, list)) else str(value)
    field_id = f"correction__{_esc(key)}"
    label = _esc(key.replace("_", " ").title())
    return (
        f'<div class="field"><label for="{field_id}">{label}</label>'
        f'<input type="text" id="{field_id}" name="{field_id}" value="{_esc(display)}" required></div>'
    )


def _render_question_row(q: dict, default: Optional[str] = None) -> str:
    """default pre-fills a known answer (e.g. entity_name/prior_flip_count
    from a buyer_entity resolved at link-mint time) as an editable value —
    never hidden, so the borrower still confirms it rather than it being
    silently assumed (same principle as the property section)."""
    key, label = _esc(q["key"]), _esc(q["label"])
    field_id = f"q__{key}"
    if q["type"] == "select":
        options = "".join(
            f'<option value="{_esc(o)}"{" selected" if default == o else ""}>{_esc(o.title())}</option>'
            for o in q.get("options", [])
        )
        return f'<div class="field"><label for="{field_id}">{label}</label><select id="{field_id}" name="{field_id}">{options}</select></div>'
    if q["type"] == "boolean":
        true_sel = " selected" if default == "true" else ""
        false_sel = " selected" if default == "false" else ""
        return (
            f'<div class="field"><label for="{field_id}">{label}</label>'
            f'<select id="{field_id}" name="{field_id}"><option value="true"{true_sel}>Yes</option>'
            f'<option value="false"{false_sel}>No</option></select></div>'
        )
    if q["type"] == "date":
        # Native date picker — full day/month/year, either picked from a
        # calendar or typed directly; the browser enforces the format.
        value_attr = f' value="{_esc(default)}"' if default is not None else ""
        return f'<div class="field"><label for="{field_id}">{label}</label><input type="date" id="{field_id}" name="{field_id}"{value_attr}></div>'
    value_attr = f' value="{_esc(default)}"' if default is not None else ""
    return f'<div class="field"><label for="{field_id}">{label}</label><input type="text" id="{field_id}" name="{field_id}"{value_attr}></div>'


@router.get("/selfserve/{token}", response_class=HTMLResponse)
def selfserve_screen(token: str, db: Session = Depends(get_db)):
    session_row = get_session_by_token(db, token)
    if session_row is None:
        raise HTTPException(status_code=404, detail="Session not found or expired.")
    if session_row.status in ("handed_off",):
        raise HTTPException(status_code=409, detail="This session has already been submitted.")

    # A borrower still reading the page is still "mid-flow" -- without this,
    # the abandonment sweep (list_stale_session_tokens) can mislabel them
    # abandoned purely because submit hasn't happened yet.
    db.execute(
        text("UPDATE selfserve_sessions SET last_activity_at = now() WHERE token = :token"),
        {"token": token},
    )
    db.commit()

    # Borrower-level recognition (WI-1 follow-up, 2026-09-18) — independent
    # of property recognition: an exact canonical_name match at link-mint
    # time (tracked_links.find_buyer_entity_by_name), never derived from the
    # property, since a property's current owner-of-record is not assumed
    # to be the borrower financing it next. Resolved before the property
    # section below so its copy can acknowledge a recognized borrower
    # without a bound property, instead of a generic "we don't know you"
    # message right next to a "Borrower recognized" badge.
    known_answers: dict[str, str] = {}
    borrower_badge = ""
    borrower_name: Optional[str] = None
    if session_row.buyer_entity_id is not None:
        entity = db.execute(
            text("SELECT canonical_name, total_purchase_count FROM buyer_entities WHERE id = :id"),
            {"id": session_row.buyer_entity_id},
        ).mappings().first()
        if entity:
            borrower_name = str(entity["canonical_name"])
            known_answers["entity_name"] = borrower_name
            known_answers["prior_flip_count"] = str(entity["total_purchase_count"])
            borrower_badge = '<span class="found-badge">&#10003; Borrower recognized</span>'

    fields = (session_row.prefill_snapshot or {}).get("fields", {})
    if session_row.property_id is None:
        no_property_copy = (
            f"We recognize you, {_esc(borrower_name.title())} — we just need the address of the property "
            "you're financing this time."
            if borrower_name else
            "We could not automatically recognize this property from the link. "
            "Enter the address below and we'll pull what public records we have."
        )
        property_section = (
            f'<p class="help-text">{no_property_copy}</p>'
            '<div class="field"><label for="manual_address">Property address</label>'
            '<input type="text" id="manual_address" name="manual_address" placeholder="123 Main St, Tampa, FL 33602" required></div>'
        )
    else:
        rows = "".join(_render_field_row(k, v["value"]) for k, v in fields.items())
        property_section = (
            '<span class="found-badge">&#10003; Property recognized</span>'
            + (rows or '<p class="help-text">No public records found for this property — you can still continue.</p>')
        )

    questions_html = borrower_badge + "".join(
        _render_question_row(q, known_answers.get(q["key"])) for q in SELFSERVE_QUESTIONS
    )

    body = f"""
    <div class="wrap">
      <div class="brand">
        <div class="brand-badge">FA</div>
        <span class="brand-name">Forced <span class="accent">Action</span></span>
      </div>
      <span class="eyebrow">&#9679; Fast-Track Application</span>
      <h1>Is this your property?</h1>
      <p class="sub">We pulled public records for this address. Confirm what looks right and correct anything that's off,
        then answer a few quick questions.</p>
      <form method="post" action="/api/selfserve/{token}/submit">
        <div class="card">
          <h2><span class="step-num">1</span>Property details</h2>
          {property_section}
        </div>
        <div class="card">
          <h2><span class="step-num">2</span>A few more details</h2>
          {questions_html}
        </div>
        <div class="card">
          <h2><span class="step-num">3</span>Your contact info</h2>
          <div class="field"><label for="contact_name">Name</label>
            <input type="text" id="contact_name" name="contact_name" autocomplete="name" required></div>
          <div class="field"><label for="contact_email">Email</label>
            <input type="email" id="contact_email" name="contact_email" autocomplete="email"
              placeholder="name@example.com" required></div>
          <div class="field"><label for="contact_phone">Phone</label>
            <input type="tel" id="contact_phone" name="contact_phone" autocomplete="tel" inputmode="numeric"
              placeholder="8135551234" pattern="{_US_PHONE_PATTERN}" maxlength="10"
              oninput="this.value=this.value.replace(/[^0-9]/g,'').slice(0,10)"
              title="Enter a 10-digit US phone number, digits only, e.g. 8135551234" required></div>
          <div class="consent">
            <input type="checkbox" id="consent" name="consent">
            <label for="consent">{CONSENT_COPY}</label>
          </div>
        </div>
        <div id="form-error" class="form-error" role="alert" hidden></div>
        <button type="submit" class="btn">Continue &rarr;</button>
        <p class="footnote">Your information is kept private and used only to process this inquiry.</p>
      </form>
    </div>
    <script>
    (function () {{
      var form = document.querySelector("form");
      var errorBox = document.getElementById("form-error");
      var btn = form.querySelector("button[type=submit]");
      var btnDefaultText = btn.textContent;

      function showError(msg) {{
        errorBox.textContent = msg;
        errorBox.hidden = false;
        errorBox.scrollIntoView({{behavior: "smooth", block: "center"}});
      }}

      form.addEventListener("submit", function (event) {{
        event.preventDefault();
        errorBox.hidden = true;
        btn.disabled = true;
        btn.textContent = "Submitting…";

        fetch(form.action, {{method: "POST", body: new FormData(form)}})
          .then(function (resp) {{
            if (resp.ok) {{
              if (resp.redirected) {{
                window.location.href = resp.url;
                return;
              }}
              return resp.text().then(function (html) {{
                document.open();
                document.write(html);
                document.close();
              }});
            }}
            return resp.json().catch(function () {{ return {{}}; }}).then(function (data) {{
              throw new Error(data.detail || "Something went wrong. Please try again.");
            }});
          }})
          .catch(function (err) {{
            showError(err.message || "Something went wrong. Please try again.");
            btn.disabled = false;
            btn.textContent = btnDefaultText;
          }});
      }});
    }})();
    </script>
    """
    return HTMLResponse(_render_page("Confirm Your Property", body))


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

    # Property details are mandatory: a borrower with no recognized property
    # must at least type an address — it doesn't have to successfully
    # resolve (plan §7 Q6 — an unmatched address still gets captured as a
    # lead), but it can't be skipped entirely.
    manual_address_raw = (_str_field("manual_address") or "").strip()
    if session_row.property_id is None and not manual_address_raw:
        raise HTTPException(status_code=400, detail="Property address is required.")

    if session_row.property_id is None and manual_address_raw:
        prop_id, _confidence = find_property_by_address(db, manual_address_raw)
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

    corrections = {k[len("correction__"):]: v for k, v in form.items() if k.startswith("correction__") and isinstance(v, str)}
    confirmations = {k[len("q__"):]: v for k, v in form.items() if k.startswith("q__") and isinstance(v, str)}

    email = (_str_field("contact_email") or "").strip()
    if not _EMAIL_PATTERN.match(email):
        raise HTTPException(status_code=400, detail="Enter a valid email address.")

    # Every phone read/write goes through phone_utils.normalize (codebase-
    # wide rule) — storing raw human-typed text here would both violate
    # that and desync from is_backflip_suppressed's own normalized lookup
    # and any future SMS send, which need strict E.164. Contact info is
    # mandatory, so phone is required same as name/email.
    raw_phone = (_str_field("contact_phone") or "").strip()
    if not raw_phone:
        raise HTTPException(status_code=400, detail="Phone number is required.")
    normalized_phone = normalize_phone(raw_phone)
    if normalized_phone is None:
        raise HTTPException(status_code=400, detail="Enter a valid US phone number.")

    contact = {
        "name": _str_field("contact_name"),
        "email": email,
        "phone": normalized_phone,
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
            "selfserve: session token=...%s suppressed — active Backflip touch on this contact, holding off",
            token[-8:],
        )
        if updated.person_id:
            # Spec's failure-behavior section: "A send fails suppression.
            # Blocked, logged with reason, surfaced to me." A log line alone
            # doesn't satisfy "surfaced to me" — Josh needs to see this.
            flag_suppressed_handoff(db, person_id=updated.person_id, session_token=token, contact=contact)
        db.commit()
        return HTMLResponse(_render_page(
            "Thanks",
            '<div class="notice-wrap">'
            '<div class="brand"><div class="brand-badge">FA</div>'
            '<span class="brand-name">Forced <span class="accent">Action</span></span></div>'
            '<div class="notice-card"><p>Thanks — you\'re already connected with Backflip on this. '
            "No further action needed from you; we'll stay out of the way.</p></div>"
            "</div>",
        ))

    # The status transition itself is the concurrency gate: two concurrent
    # POSTs (double-click, client retry) can both pass the "handed_off" check
    # above before either writes. Claim the row first (uncommitted); only the
    # request that actually flips it calls the external handoff. If handoff()
    # raises, roll back so the claim never lands and the session stays
    # retryable instead of being stuck "handed_off" with no ref.
    claim = db.execute(
        text(
            "UPDATE selfserve_sessions SET status = 'handed_off' "
            "WHERE token = :token AND status <> 'handed_off' RETURNING id"
        ),
        {"token": token},
    ).first()
    if claim is None:
        db.commit()
        raise HTTPException(status_code=409, detail="This session has already been submitted.")

    port = get_backflip_port()
    try:
        result = port.handoff(HandoffPayload(
            session_token=token,
            prefill_fields={k: v.get("value") for k, v in (updated.prefill_snapshot or {}).get("fields", {}).items()},
            confirmations=confirmations,
            contact=contact,
        ))
    except Exception:
        db.rollback()
        raise

    db.execute(
        text(
            "UPDATE selfserve_sessions SET handoff_ref = :ref, "
            "handed_off_at = now() WHERE token = :token"
        ),
        {"ref": result.handoff_ref, "token": token},
    )
    db.commit()

    logger.info("selfserve: session token=...%s handed off, ref=%s", token[-8:], result.handoff_ref)
    return RedirectResponse(url=result.redirect_url, status_code=302)


# ---------------------------------------------------------------------------
# Admin: mint + list tracked links
# ---------------------------------------------------------------------------


class _MintTrackedLinkRequest(BaseModel):
    kind: str = Field(..., description="partner | campaign | source")
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

