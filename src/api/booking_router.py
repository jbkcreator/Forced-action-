"""The borrower-facing booking page.

A borrower who books directly converts better than one who has to email back
and negotiate a time, so every outbound can carry a link to this page. No
agent is involved on this path: the visitor picks a slot and the booking is
written straight to the calendar.

Identity comes from a tracked link rather than a login. The visitor has no
account and never will, and putting a sign-up in front of a booking would
cost the conversion the page exists to win. The slug is opaque and
non-enumerable, so it identifies without authenticating, and the click is
attributed before anything is typed.
"""
from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from config.calendar import BOOKING_HORIZON_DAYS, CALENDAR_TIMEZONE
from config.settings import settings
from src.api.deps import get_db
from src.api.public_page import BRAND_MARKUP, escape_html, render_page
from src.core.redis_client import rincr
from src.services.calendar import book, get_slots, has_live_booking
from src.services.calendar.client import get_calendar_client, get_calendar_id
from src.services.tracked_links import record_click, resolve_slug

logger = logging.getLogger(__name__)

router = APIRouter(tags=["booking"])

_TZ = ZoneInfo(CALENDAR_TIMEZONE)
_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

_RATE_LIMIT_WINDOW_SECONDS = 60
_VIEW_LIMIT_PER_MINUTE = 30
_BOOK_LIMIT_PER_MINUTE = 5

# How far ahead the page offers. The availability rules cap this anyway; a
# shorter page window just keeps the grid readable.
_PAGE_WINDOW_DAYS = min(14, BOOKING_HORIZON_DAYS)

MEETING_TOPIC = "Call with Forced Action"


def _hash_ip(ip: Optional[str]) -> Optional[str]:
    """Salted IP hash — never store the raw IP.

    Same salt as the self-serve page so one visitor hashes identically across
    both, which is what makes a shared rate-limit meaningful.
    """
    if not ip:
        return None
    salt = settings.admin_jwt_secret.get_secret_value() if settings.admin_jwt_secret else "no-salt"
    return hashlib.sha256(f"{salt}:{ip}".encode()).hexdigest()


def _rate_limited(bucket: str, limit: int) -> bool:
    return rincr(f"booking:rl:{bucket}", ttl_seconds=_RATE_LIMIT_WINDOW_SECONDS) > limit


def _notice(message: str) -> HTMLResponse:
    """A dead end the visitor can understand, rendered as a normal page."""
    body = (
        f'<div class="notice-wrap">{BRAND_MARKUP}'
        f'<div class="notice-card"><p>{escape_html(message)}</p></div></div>'
    )
    return HTMLResponse(render_page("Booking", body))


# ---------------------------------------------------------------------------
# GET /book/{slug} — the picker
# ---------------------------------------------------------------------------


@router.get("/book/{slug}", response_class=HTMLResponse)
def booking_page(slug: str, request: Request, db: Session = Depends(get_db)):
    ip_hash = _hash_ip(request.client.host if request.client else None)
    if ip_hash and _rate_limited(f"view:{ip_hash}", _VIEW_LIMIT_PER_MINUTE):
        raise HTTPException(status_code=429, detail="Too many requests.")

    link = resolve_slug(db, slug)
    if link is None:
        logger.info("booking: unknown or inactive slug=%r", slug)
        return _notice("This booking link is no longer active. Please reply to the email and we'll send a new one.")

    record_click(
        db,
        tracked_link_id=link.id,
        session_token=slug,
        ip_hash=ip_hash,
        user_agent=request.headers.get("user-agent"),
        referer=request.headers.get("referer"),
    )
    db.commit()

    if has_live_booking(db, tracked_link_id=link.id):
        return _notice(
            "You already have a call booked with us. Check your inbox for the "
            "invitation, or reply to the email if you need to move it."
        )

    try:
        slots = _available_slots()
    except Exception:
        logger.exception("booking: could not read availability for slug=%r", slug)
        return _notice("We can't show times right now. Please reply to the email and we'll sort it out.")

    if not slots:
        return _notice("There are no open times in the next couple of weeks. Reply to the email and we'll find one.")

    return HTMLResponse(render_page("Book a call", _render_picker(slug, slots)))


def _available_slots() -> list:
    now = datetime.now(timezone.utc)
    return get_slots(
        client=get_calendar_client(),
        calendar_id=get_calendar_id(),
        window_start=now,
        window_end=now + timedelta(days=_PAGE_WINDOW_DAYS),
        now=now,
        # Display only — book() re-reads live before it writes.
        use_cache=True,
    )


def _render_picker(slug: str, slots: list) -> str:
    by_day: dict[str, list] = {}
    for slot in slots:
        local = slot.start.astimezone(_TZ)
        by_day.setdefault(local.strftime("%A %d %B"), []).append(slot)

    sections = []
    for day, day_slots in by_day.items():
        buttons = "".join(
            '<button type="button" class="slot" aria-pressed="false" '
            f'data-start="{escape_html(s.start.isoformat())}" '
            f'data-end="{escape_html(s.end.isoformat())}">'
            f'{escape_html(s.start.astimezone(_TZ).strftime("%H:%M"))}</button>'
            for s in day_slots
        )
        sections.append(
            f'<div class="day-label">{escape_html(day)}</div>'
            f'<div class="slot-grid">{buttons}</div>'
        )

    return f"""
    <div class="wrap">
      {BRAND_MARKUP}
      <h1>Book a call</h1>
      <p class="sub">Pick a time that suits you. It takes about 30 minutes.</p>

      <div class="card">
        <h2><span class="step-num">1</span> Choose a time</h2>
        <p class="help-text" id="tz-note">Times shown in Eastern Time.</p>
        {"".join(sections)}
      </div>

      <div class="card">
        <h2><span class="step-num">2</span> Your details</h2>
        <div class="field">
          <label for="name">Your name</label>
          <input type="text" id="name" autocomplete="name" required>
        </div>
        <div class="field">
          <label for="email">Email for the invitation</label>
          <input type="email" id="email" autocomplete="email" required>
        </div>
      </div>

      <div id="error" class="form-error" style="display:none"></div>
      <button type="button" class="btn" id="submit" disabled>Select a time first</button>
      <p class="footnote">You'll get a calendar invitation by email.</p>
    </div>
    {_PICKER_SCRIPT.replace("__SLUG__", escape_html(slug))}
    """


# Times render in Eastern server-side so the page is correct without
# JavaScript, then shift to the visitor's own zone when it runs — a borrower
# in California books far more readily against "11:00 your time" than
# against "14:00 ET".
_PICKER_SCRIPT = """
<script>
(function () {
  var chosen = null;
  var submit = document.getElementById('submit');
  var errorBox = document.getElementById('error');

  try {
    var zone = Intl.DateTimeFormat().resolvedOptions().timeZone;
    document.querySelectorAll('.slot').forEach(function (btn) {
      var when = new Date(btn.dataset.start);
      btn.textContent = when.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
    });
    document.getElementById('tz-note').textContent = 'Times shown in your local time (' + zone + ').';
  } catch (e) { /* server-rendered Eastern times stand */ }

  document.querySelectorAll('.slot').forEach(function (btn) {
    btn.addEventListener('click', function () {
      document.querySelectorAll('.slot').forEach(function (b) { b.setAttribute('aria-pressed', 'false'); });
      btn.setAttribute('aria-pressed', 'true');
      chosen = {starts_at: btn.dataset.start, ends_at: btn.dataset.end};
      submit.disabled = false;
      submit.textContent = 'Confirm this time';
    });
  });

  function fail(message) {
    errorBox.textContent = message;
    errorBox.style.display = 'block';
    submit.disabled = false;
    submit.textContent = 'Confirm this time';
  }

  submit.addEventListener('click', function () {
    if (!chosen) { return; }
    var name = document.getElementById('name').value.trim();
    var email = document.getElementById('email').value.trim();
    if (!name || !email) { fail('Please add your name and email.'); return; }

    errorBox.style.display = 'none';
    submit.disabled = true;
    submit.textContent = 'Booking...';

    fetch('/api/book/__SLUG__', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        starts_at: chosen.starts_at, ends_at: chosen.ends_at, name: name, email: email
      })
    }).then(function (resp) {
      return resp.json().catch(function () { return {}; });
    }).then(function (data) {
      if (data && data.booked) {
        document.querySelector('.wrap').innerHTML =
          '<div class="notice-card"><p><strong>You\\'re booked.</strong><br>' +
          'A calendar invitation is on its way to ' + (data.email || 'your inbox') + '.</p></div>';
        return;
      }
      if (data && data.reason === 'slot_taken') {
        fail('Sorry, that time was just taken. Please pick another.');
        return;
      }
      fail('That didn\\'t go through. Please try again, or reply to the email.');
    }).catch(function () {
      fail('That didn\\'t go through. Please try again, or reply to the email.');
    });
  });
})();
</script>
"""


# ---------------------------------------------------------------------------
# POST /api/book/{slug} — the write
# ---------------------------------------------------------------------------


class BookingRequest(BaseModel):
    starts_at: str
    ends_at: str
    name: str = Field(min_length=1, max_length=120)
    email: str = Field(min_length=3, max_length=320)


@router.post("/api/book/{slug}")
def submit_booking(
    slug: str, payload: BookingRequest, request: Request, db: Session = Depends(get_db)
):
    ip_hash = _hash_ip(request.client.host if request.client else None)
    if ip_hash and _rate_limited(f"book:{ip_hash}", _BOOK_LIMIT_PER_MINUTE):
        raise HTTPException(status_code=429, detail="Too many requests.")

    if not _EMAIL_PATTERN.match(payload.email):
        raise HTTPException(status_code=400, detail="That email address doesn't look right.")

    link = resolve_slug(db, slug)
    if link is None:
        raise HTTPException(status_code=404, detail="This booking link is no longer active.")

    if has_live_booking(db, tracked_link_id=link.id):
        # Enforced here as well as on the page: the page check only stops a
        # visitor who reloads, not one replaying the request directly.
        logger.info("booking: refused second booking for slug=%r", slug)
        return {"booked": False, "reason": "already_booked"}

    try:
        slot = _requested_slot(payload)
    except ValueError:
        raise HTTPException(status_code=400, detail="That time isn't valid.")

    result = book(
        client=get_calendar_client(),
        session=db,
        calendar_id=get_calendar_id(),
        slot=slot,
        attendee_email=payload.email,
        topic=MEETING_TOPIC,
        description=f"Booked by {payload.name} via the Forced Action booking page.",
        tracked_link_id=link.id,
    )

    if not result.booked:
        # A refusal is the page's answer, not an error: "that slot just went"
        # and "we can't contact you" are both outcomes the visitor acts on.
        logger.info("booking: refused slug=%r reason=%s", slug, result.reason)
        return {"booked": False, "reason": result.reason}

    logger.info("booking: booked slug=%r booking_ref=%s", slug, result.booking_ref)
    return {"booked": True, "booking_ref": result.booking_ref, "email": payload.email}


def _requested_slot(payload: BookingRequest):
    from src.services.calendar import Slot

    start = datetime.fromisoformat(payload.starts_at)
    end = datetime.fromisoformat(payload.ends_at)
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("timestamps must carry an offset")
    if end <= start:
        raise ValueError("end must follow start")
    return Slot(start=start, end=end)
