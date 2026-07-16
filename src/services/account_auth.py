"""
Resolves a subscriber bearer token to the caller's CustomerAccount row (fa B1-02).

CustomerAccount is "bridged to legacy Subscriber" via subscriber_id (models.py).
There is no separate contractor login flow yet, so this reuses the existing
subscriber JWT (src/services/subscriber_auth.py) rather than building a new
auth system — decode the token's subscriber id, then look up the linked
CustomerAccount by subscriber_id.
"""
from __future__ import annotations

from typing import Optional

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import text as sa_text

from src.core.database import get_db
from src.services.subscriber_auth import verify_access_token

_bearer = HTTPBearer(auto_error=False)


def get_current_account(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
    db=Depends(get_db),
):
    """FastAPI dependency: returns the authenticated CustomerAccount row.

    401 if the bearer token is missing/invalid/expired.
    404 if the token's subscriber has no linked CustomerAccount.
    403 if the account is churned.
    """
    if not credentials:
        raise HTTPException(status_code=401, detail="Authentication required")

    payload = verify_access_token(credentials.credentials)
    subscriber_id = int(payload["sub"])

    row = db.execute(
        sa_text("""
            SELECT account_id, subscriber_id, status, plan_tier
              FROM customer_accounts
             WHERE subscriber_id = :sid
        """),
        {"sid": subscriber_id},
    ).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="No contractor account for this subscriber")
    if row.status == "churned":
        raise HTTPException(status_code=403, detail="Account is inactive")

    return row
