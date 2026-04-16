"""
Admin API router.

Provides JWT-authenticated endpoints for internal operations:
  POST /api/admin/login                   — issue a 24-hour bearer token
  POST /api/admin/upload/tax-delinquency  — upload a tax delinquency CSV

Auth pattern:
  - Single credential pair from env (ADMIN_USERNAME / ADMIN_PASSWORD)
  - HS256 JWT signed with ADMIN_JWT_SECRET, 24-hour expiry
  - Every protected endpoint uses Depends(get_current_admin)
  - Returns 503 if admin env vars are not configured
"""

import io
import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Depends, Form, HTTPException, UploadFile
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from pydantic import BaseModel
from sqlalchemy.orm import Session

from config.settings import settings
from src.core.database import get_db_context
from src.loaders.tax import TaxDelinquencyLoader

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["admin"])

_ALGORITHM = "HS256"
_TOKEN_EXPIRE_HOURS = 24
_bearer = HTTPBearer()

# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------

def _jwt_secret() -> str:
    """Return the JWT secret or raise 503 if not configured."""
    if not settings.admin_jwt_secret:
        raise HTTPException(status_code=503, detail="Admin not configured")
    return settings.admin_jwt_secret.get_secret_value()


def create_access_token(data: dict) -> str:
    payload = data.copy()
    payload["exp"] = datetime.now(timezone.utc) + timedelta(hours=_TOKEN_EXPIRE_HOURS)
    return jwt.encode(payload, _jwt_secret(), algorithm=_ALGORITHM)


def verify_token(token: str) -> dict:
    try:
        return jwt.decode(token, _jwt_secret(), algorithms=[_ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


def get_current_admin(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
) -> dict:
    return verify_token(credentials.credentials)


# ---------------------------------------------------------------------------
# DB dependency
# ---------------------------------------------------------------------------

def get_db():
    with get_db_context() as db:
        yield db


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/login", response_model=TokenResponse)
def admin_login(body: LoginRequest):
    """
    Exchange admin credentials for a 24-hour JWT bearer token.
    Returns 503 if ADMIN_PASSWORD / ADMIN_JWT_SECRET are not set in env.
    Returns 401 on wrong credentials.
    """
    if not settings.admin_password:
        raise HTTPException(status_code=503, detail="Admin not configured")

    username_ok = secrets.compare_digest(body.username, settings.admin_username)
    password_ok = secrets.compare_digest(
        body.password,
        settings.admin_password.get_secret_value(),
    )
    if not (username_ok and password_ok):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_access_token({"sub": body.username})
    logger.info("[Admin] Login successful for user: %s", body.username)
    return TokenResponse(access_token=token)


@router.post("/upload/tax-delinquency")
def upload_tax_delinquency(
    file: UploadFile,
    county_id: str = Form("hillsborough"),
    tax_year: Optional[int] = Form(None),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Upload a tax delinquency CSV and run it through TaxDelinquencyLoader.

    Expected CSV columns: Account Number, Tax Yr, Owner Name
    Optional enrichment columns: years_delinquent_scraped, total_amount_due, Cert Status, Deed Status

    If tax_year is provided and the CSV lacks a 'Tax Yr' column, it is injected automatically.

    Returns matched/unmatched/skipped counts.
    """
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a .csv")

    content = file.file.read().decode("utf-8", errors="replace")

    try:
        df = pd.read_csv(io.StringIO(content))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not parse CSV: {exc}")

    # Validate required column
    if "Account Number" not in df.columns:
        raise HTTPException(
            status_code=400,
            detail="CSV must contain 'Account Number' column. "
                   f"Found columns: {list(df.columns)}",
        )

    # Inject Tax Yr column if caller provided it and CSV doesn't have one
    if "Tax Yr" not in df.columns:
        if tax_year is not None:
            df["Tax Yr"] = tax_year
        else:
            raise HTTPException(
                status_code=400,
                detail="CSV must contain 'Tax Yr' column, or pass tax_year as a form field.",
            )

    total_rows = len(df)
    logger.info(
        "[Admin] Tax delinquency upload: %d rows, county=%s, tax_year=%s, user=%s",
        total_rows, county_id, tax_year, _admin.get("sub"),
    )

    loader = TaxDelinquencyLoader(db, county_id=county_id)
    matched, unmatched, skipped = loader.load_from_dataframe(df)

    logger.info(
        "[Admin] Upload complete: matched=%d unmatched=%d skipped=%d",
        matched, unmatched, skipped,
    )
    return {
        "matched": matched,
        "unmatched": unmatched,
        "skipped": skipped,
        "total_rows": total_rows,
    }
