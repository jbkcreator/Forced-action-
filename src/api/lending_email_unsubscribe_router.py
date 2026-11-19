"""NDL's cold-email unsubscribe link: ``GET|POST /api/lending/email/unsubscribe?token=...``, served by ``lending-api``.

The footer link added to every NDL cold email (``instantly_email.NdlInstantlySender.footer_body``) points here, on NDL's
own host (``LENDING_UNSUBSCRIBE_BASE_URL``), not at the Forced Action app. The signed token is the auth (the same
stateless token as the Forced Action link, ``src.services.email_unsubscribe``).

* **GET** only shows a confirmation page with an Unsubscribe button. It changes nothing, so an email scanner or link
  prefetcher that opens the link cannot unsubscribe anyone (an opt-out here also stops calls and texts to every phone
  that address is on file with).
* **POST** records the opt-out (the button, and RFC 8058 one-click from mailbox providers): on every channel through
  ``compliance.propagate_opt_out`` (FA's and lending's stores, the dialer, GHL do-not-disturb) and Instantly's block
  list. Idempotent: the same address repeated returns the same event. Rate limited per IP.
"""
from __future__ import annotations

import hashlib
import html
import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from config.lending_compliance import UNSUBSCRIBE_PATH, OptOutChannel
from src.api.deps import get_db
from src.lending.compliance import propagate_opt_out
from src.services.email_unsubscribe import verify_unsubscribe_token
from src.services.rate_limit import enforce_or_429

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/lending/email", tags=["lending"])

SOURCE_REF_PREFIX = "unsubscribe-link:"
RATE_LIMIT_SCOPE = "lending_unsubscribe"
RATE_LIMIT_PER_WINDOW = 20
RATE_LIMIT_WINDOW_SECONDS = 60
_CONFIRMED = ("<html><body><p>You have been unsubscribed and will not receive further emails from Next Deal Lending."
              "</p></body></html>")
_INVALID = "<html><body><p>This unsubscribe link is invalid or has expired.</p></body></html>"
_FAILED = ("<html><body><p>We could not record your request right now. Please try the button again in a few minutes, "
           "or email optout@nextdeallending.com.</p></body></html>")


def _verified_email(token: str) -> Optional[str]:
    try:
        return verify_unsubscribe_token(token)
    except RuntimeError:  # no signing secret configured: nothing can be verified
        logger.error("[lending-unsubscribe] no unsubscribe signing secret is configured")
        return None


def _confirm_page(token: str) -> str:
    action = html.escape(f"{UNSUBSCRIBE_PATH}?token={token}", quote=True)
    return ("<html><body><p>Unsubscribe from Next Deal Lending emails?</p>"
            f'<form method="post" action="{action}"><button type="submit">Unsubscribe</button></form></body></html>')


@router.get("/unsubscribe")
def unsubscribe_page(token: str = Query(...)):
    """Shows the confirmation button for a valid link; never changes anything."""
    if not _verified_email(token):
        return HTMLResponse(_INVALID, status_code=400)
    return HTMLResponse(_confirm_page(token), status_code=200)


@router.post("/unsubscribe")
def unsubscribe(request: Request, token: str = Query(...), db: Session = Depends(get_db)):
    enforce_or_429(request, RATE_LIMIT_SCOPE, RATE_LIMIT_PER_WINDOW, RATE_LIMIT_WINDOW_SECONDS)
    email = _verified_email(token)
    if not email:
        return HTMLResponse(_INVALID, status_code=400)
    try:
        propagate_opt_out(db, email=email, channel=OptOutChannel.EMAIL, actor="unsubscribe_link",
                          source_ref=SOURCE_REF_PREFIX + hashlib.sha256(email.encode()).hexdigest()[:40])
        db.commit()
    except Exception as exc:  # class only: SQL errors embed the address
        db.rollback()
        logger.error("[lending-unsubscribe] opt-out not recorded (%s)", type(exc).__name__)
        return HTMLResponse(_FAILED, status_code=503)
    return HTMLResponse(_CONFIRMED, status_code=200)
