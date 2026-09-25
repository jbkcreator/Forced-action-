"""
Public one-click email unsubscribe endpoint.

No auth — the signed token itself is the auth. Suppression is all-or-nothing
(see docs/adr/00XX-cross-channel-suppression-block-all.md): a hit here
cascades to every channel via suppress_contact(), not just email.

Endpoints:
  GET  /api/email/unsubscribe?token=<signed>  — human clicking the link
  POST /api/email/unsubscribe?token=<signed>  — RFC 8058 one-click unsubscribe
       (mailbox providers POST here because our List-Unsubscribe-Post header
       advertises `List-Unsubscribe=One-Click` support; without this route
       those requests 405 and the provider treats the header as a lie)
Both run the same suppression logic and are idempotent.
"""
from fastapi import APIRouter, Depends, Query
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from src.api.deps import get_db
from src.services.email_suppression import suppress_contact
from src.services.email_unsubscribe import verify_unsubscribe_token

router = APIRouter(prefix="/api/email", tags=["email"])

_CONFIRMED_HTML = "<html><body><p>You have been unsubscribed and will not receive further emails from Forced Action.</p></body></html>"
_INVALID_HTML = "<html><body><p>This unsubscribe link is invalid or has expired.</p></body></html>"


def _cascade_fa_max_opt_out(email: str, db: Session) -> None:
    """WP-T3-4 (plan Section 6.6): if this email belongs to a resolved FA Max
    person, run the same handle_opt_out() the concierge's reply-opt-out path
    uses — moves the person to do_not_contact and cancels any active
    campaign enrollment. Best-effort: never blocks the unsubscribe response."""
    try:
        from sqlalchemy import text as _text

        row = db.execute(
            _text(
                "SELECT person_id::text FROM fa_max_persons "
                "WHERE lower(email) = lower(:email) AND merged_into_id IS NULL LIMIT 1"
            ),
            {"email": email},
        ).fetchone()
        if not row:
            return
        from src.agents.reply_concierge.opt_out import handle_opt_out

        handle_opt_out(
            person_id=row[0], contact_email=email,
            inbound_text="unsubscribe_link", channel="email", db=db,
        )
    except Exception:
        import logging

        logging.getLogger(__name__).warning(
            "email_unsubscribe: fa_max cascade failed for email=%s", email, exc_info=True,
        )


def _do_unsubscribe(token: str, db: Session) -> HTMLResponse:
    email = verify_unsubscribe_token(token)
    if not email:
        return HTMLResponse(_INVALID_HTML, status_code=400)

    suppress_contact(db, email=email, source="unsubscribe_link")
    _cascade_fa_max_opt_out(email, db)
    return HTMLResponse(_CONFIRMED_HTML, status_code=200)


@router.get("/unsubscribe")
def unsubscribe(token: str = Query(...), db: Session = Depends(get_db)):
    return _do_unsubscribe(token, db)


@router.post("/unsubscribe")
def unsubscribe_one_click(token: str = Query(...), db: Session = Depends(get_db)):
    """RFC 8058 one-click unsubscribe — mailbox providers POST here, no body
    parsing needed (the token is in the query string on both verbs)."""
    return _do_unsubscribe(token, db)
