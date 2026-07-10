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


def _do_unsubscribe(token: str, db: Session) -> HTMLResponse:
    email = verify_unsubscribe_token(token)
    if not email:
        return HTMLResponse(_INVALID_HTML, status_code=400)

    suppress_contact(db, email=email, source="unsubscribe_link")
    return HTMLResponse(_CONFIRMED_HTML, status_code=200)


@router.get("/unsubscribe")
def unsubscribe(token: str = Query(...), db: Session = Depends(get_db)):
    return _do_unsubscribe(token, db)


@router.post("/unsubscribe")
def unsubscribe_one_click(token: str = Query(...), db: Session = Depends(get_db)):
    """RFC 8058 one-click unsubscribe — mailbox providers POST here, no body
    parsing needed (the token is in the query string on both verbs)."""
    return _do_unsubscribe(token, db)
