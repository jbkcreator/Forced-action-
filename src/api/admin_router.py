"""
Admin API router.

Provides JWT-authenticated endpoints for internal operations:
  POST /api/admin/login                   — issue a 24-hour bearer token
  POST /api/admin/upload/tax-delinquency  — upload a tax delinquency CSV
  POST /api/admin/fa-max/agent-tasks      — manually dispatch a bounded FA Max
                                             agent task (WP-T2-2; see that
                                             section below for why this is a
                                             manual trigger, not automatic)

Auth pattern:
  - Single credential pair from env (ADMIN_USERNAME / ADMIN_PASSWORD)
  - HS256 JWT signed with ADMIN_JWT_SECRET, 24-hour expiry
  - Every protected endpoint uses Depends(get_current_admin)
  - Returns 503 if admin env vars are not configured
"""

import csv
import io
import json
import logging
import secrets
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Literal

import pandas as pd
import stripe
from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, Query, Response, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import and_, case, distinct, func, or_, select, text
from sqlalchemy.orm import Session

from config.settings import get_settings, settings
from config.venture_template import DEFAULT_VENTURE_KEY
from src.api.deps import get_db, VALID_TIERS, VALID_VERTICALS, ZIP_RE
from src.core.database import get_db_context
from src.core.models import (
    County,
    CountyColumnMapping,
    CountyLaunchAudit,
    CountySource,
    DistressScore,
    EnrichmentUsageLog,
    ExpansionCandidate,
    Owner,
    PremiumPurchase,
    Property,
    SentLead,
    MessageOutcome,
    SmsOptIn,
    Subscriber,
)
from src.loaders.tax import TaxDelinquencyLoader
from src.loaders.voter_registry import VoterRegistryLoader
from src.services.relay.slack_post import open_log_submission_modal
from src.services.zip_territory import claim_zip_territory
from src.utils.county_config import invalidate_cache
from src.utils.test_account import is_test_subscriber
from src.utils.quora_attribution import campaign_slug, clamp_cooldown as quora_clamp_cooldown

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
    # A valid signature is not enough: demo tokens (scope="demo") are signed
    # with this same secret, so require an explicit admin scope or a demo user
    # could authorize against every /api/admin/* route. Allowlist, not denylist.
    claims = verify_token(credentials.credentials)
    if claims.get("scope") != "admin":
        raise HTTPException(status_code=403, detail="Admin scope required")
    return claims


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

    token = create_access_token({"sub": body.username, "scope": "admin"})
    logger.info("[Admin] Login successful for user: %s", body.username)
    return TokenResponse(access_token=token)


def _run_tax_enrichment(county_id: str, df: "pd.DataFrame") -> None:
    """Run tax-collector absentee/contact enrichment + rescore off the request.

    Opens its own DB session (the request session is closed once the HTTP
    response is sent). Scheduled via BackgroundTasks so a 16k-property rescore
    never blocks the upload response past nginx's proxy timeout.
    """
    from src.core.database import get_db_context
    from src.services.tax_collector_enrichment import TaxCollectorEnrichment
    try:
        with get_db_context() as session:
            result = TaxCollectorEnrichment(session, county_id=county_id).process_upload(df)
        logger.info("[Admin] Tax enrichment (background) complete: %s", result)
    except Exception:
        logger.exception("[Admin] Tax enrichment (background) failed for county=%s", county_id)


@router.post("/entitlements/resync")
def resync_entitlements(
    plan_id: Optional[str] = None,
    dry_run: bool = False,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """Re-propagate `plans.entitlements` onto stale per-account snapshots.

    The snapshot is only written at subscription-activation time, so a plan-catalog
    edit leaves existing accounts stale and silently excluded from lead delivery.
    Run this after any catalog change made outside the seed scripts. Scope with
    `plan_id` to limit the blast radius; `dry_run` reports drift without writing.
    Returns {drifted, updated, dry_run, plans[]}.
    """
    from src.services.entitlement_sync import resync_lead_entitlements

    try:
        result = resync_lead_entitlements(
            db, plan_ids=[plan_id] if plan_id else None, dry_run=dry_run
        )
        if not dry_run:
            db.commit()
    except Exception:
        db.rollback()
        logger.error("[Admin] entitlement resync failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Entitlement resync failed")

    return {
        "drifted": len(result.drifted),
        "updated": result.updated,
        "dry_run": result.dry_run,
        "plans": sorted(result.plan_ids),
    }


@router.post("/import/founder-portfolio")
def import_founder_portfolio(
    file: UploadFile,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """B0-01 — import the founder's personal deal history CSV.

    Each row is matched to a known parcel (parcel-id exact, else address >=92);
    unmatched rows are reported, never attached (ADR 0026). Rows land as
    founder_verified / founder_import DealOutcomes (subscriber_id NULL) and fire
    no side-effects. Idempotent by source_ref. Returns
    {imported, updated, matched, unmatched[], errors[]}.
    """
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a .csv")

    content = file.file.read().decode("utf-8", errors="replace")

    from src.services.founder_portfolio_import import import_portfolio
    try:
        return import_portfolio(db, content)
    except Exception:
        logger.error("[FounderImport] import failed", exc_info=True)
        raise HTTPException(status_code=400, detail="Founder portfolio import failed")


@router.post("/upload/tax-delinquency")
def upload_tax_delinquency(
    file: UploadFile,
    background_tasks: BackgroundTasks,
    county_id: str = Form("hillsborough"),
    tax_year: Optional[int] = Form(None),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Upload a tax delinquency CSV and run it through TaxDelinquencyLoader.

    Column mapping is applied automatically if an approved/pending mapping exists for this
    county in county_column_mappings. Raw source headers are renamed to model-style
    canonical names before the loader sees them.

    Required canonical columns (after mapping): source_account_number/account_number, tax_year
    Optional canonical columns include: parcel_number, owner_name, property_address,
                                       certificate_status, deed_status, total_amount_due,
                                       years_delinquent, certificate_number, etc.
    Enrichment overrides (bypass mapping): years_delinquent_scraped, total_amount_due

    If tax_year is provided and the CSV lacks a tax year column, it is injected automatically.

    Returns matched/updated/unmatched counts.
    """
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a .csv")

    content = file.file.read().decode("utf-8", errors="replace")

    try:
        df = pd.read_csv(io.StringIO(content), dtype=str)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not parse CSV: {exc}")

    # Apply column mapping if a CountySource + approved/pending mapping exists.
    # This renames raw county-specific headers to the canonical names the loader
    # expects ("source_account_number", "parcel_number", "owner_name", "property_address", ...).
    from src.loaders.column_mapper import ColumnMapper, SkipMapping, NeedsMappingError
    from src.core.models import CountySource
    src = db.query(CountySource).filter_by(
        county_id=county_id, signal_type="tax_delinquency"
    ).first()
    if src is not None:
        try:
            sample_df = df.head(5)
            mapper = ColumnMapper()
            col_mapping = mapper.get_or_create("tax_delinquency", src.id, sample_df)
            df = ColumnMapper.apply(df, col_mapping)
        except SkipMapping:
            pass
        except NeedsMappingError as e:
            raise HTTPException(
                status_code=422,
                detail=f"Column mapping required but LLM failed — create a manual mapping in Admin > Col Mappings first. ({e})",
            )

    # Validate required column (after mapping so canonical/model names are expected)
    account_cols = {"source_account_number", "account_number", "Account Number"}
    if not account_cols.intersection(df.columns):
        raise HTTPException(
            status_code=400,
            detail="CSV must contain an account/certificate column mapped to "
                   "'source_account_number' or 'account_number'. "
                   f"Found columns: {list(df.columns)}",
        )

    # Inject tax_year if caller provided it and CSV doesn't have one.
    tax_year_cols = {"tax_year", "Tax Yr", "Tax Year"}
    if not tax_year_cols.intersection(df.columns):
        if tax_year is not None:
            df["tax_year"] = str(tax_year)
        else:
            raise HTTPException(
                status_code=400,
                detail="CSV must contain a tax year column mapped to 'tax_year', "
                       "or pass tax_year as a form field.",
            )

    total_rows = len(df)
    logger.info(
        "[Admin] Tax delinquency upload: %d rows, county=%s, tax_year=%s, user=%s",
        total_rows, county_id, tax_year, _admin.get("sub"),
    )

    # Bulk admin uploads are parcel-keyed county files — match on parcel/account
    # only and skip the per-row address/owner+LLM cascade (it dominates runtime
    # at 28k+ rows). Unmatched rows go to unmatched_records for later re-match.
    loader = TaxDelinquencyLoader(db, county_id=county_id)
    matched, updated, unmatched = loader.load_from_dataframe(df, parcel_only=True)
    db.commit()

    # Phase 5: absentee + billing-contact enrichment (and the CDS rescore it
    # triggers) runs in the BACKGROUND. On a full county file it touches ~16k
    # properties and would otherwise push the request past nginx's proxy
    # timeout (→ a 504 in the browser even though the load succeeded).
    background_tasks.add_task(_run_tax_enrichment, county_id, df)

    logger.info(
        "[Admin] Upload complete: inserted=%d updated=%d unmatched=%d (enrichment queued)",
        matched, updated, unmatched,
    )
    return {
        "matched": matched,
        "updated": updated,
        "unmatched": unmatched,
        "total_rows": total_rows,
        "enrichment": "processing_in_background",
    }


def _select_voter_data_bytes(raw_bytes: bytes, fname: str) -> bytes:
    """Return the raw voter-file bytes, extracting from a .zip if needed.

    SOE zips can carry several files (e.g. Pinellas ships ReportCodes.txt +
    FieldDescriptions.pdf alongside the real ActiveVoterData.txt); pick the
    LARGEST .txt/.csv entry, which is always the voter table.
    """
    import io as _io
    import zipfile

    if not fname.endswith(".zip"):
        return raw_bytes
    try:
        with zipfile.ZipFile(_io.BytesIO(raw_bytes)) as zf:
            data_entries = [
                i for i in zf.infolist()
                if i.filename.lower().endswith((".txt", ".csv"))
            ]
            if not data_entries:
                raise HTTPException(status_code=400, detail="Zip contains no .txt/.csv files")
            target = max(data_entries, key=lambda i: i.file_size)
            return zf.read(target.filename)
    except zipfile.BadZipFile:
        raise HTTPException(status_code=400, detail="Invalid zip archive")


def _run_voter_bulk_load(county_id: str, raw_bytes: bytes, filename: str) -> None:
    """Background: bulk-load a voter file (set-based path, its own DB sessions).

    Decodes the (already zip-extracted) bytes and streams them through
    bulk_load_voters_csv — full-county files (~hundreds of k rows) load in a
    few minutes without blocking the HTTP response (which would otherwise hit
    nginx's proxy timeout → 504).
    """
    import io as _io

    from src.loaders.voter_registry import bulk_load_voters_csv
    try:
        content = raw_bytes.decode("utf-8", errors="replace")
        rows, upserted, unmatched = bulk_load_voters_csv(
            _io.StringIO(content), county_id=county_id,
        )
        logger.info(
            "[Admin] Voter bulk load (background) complete county=%s file=%s: "
            "rows=%d matched=%d unmatched=%d",
            county_id, filename, rows, upserted, unmatched,
        )
    except Exception:
        logger.exception(
            "[Admin] Voter bulk load (background) failed county=%s file=%s",
            county_id, filename,
        )


@router.post("/upload/voter-registry")
def upload_voter_registry(
    file: UploadFile,
    background_tasks: BackgroundTasks,
    county_id: str = Form("hillsborough"),
    _admin: dict = Depends(get_current_admin),
):
    """
    Upload a Supervisor of Elections voter file (.csv, .txt, or .zip).

    Accepts the FlexRep SOE export (quoted CSV with header, used by both
    Hillsborough and Pinellas). Large county files (~hundreds of MB / k of rows)
    are loaded via the set-based bulk path in a BACKGROUND task, so the request
    returns immediately instead of blocking past nginx's timeout. Voters are
    matched to properties by residential address; contacts are isolated from
    auto-send paths per ADR 0013.

    Returns 202-style {status: "processing"} — the load finishes server-side;
    verify with: SELECT COUNT(*) FROM voters WHERE county_id = '<county>'.
    """
    fname = (file.filename or "").lower()
    if not any(fname.endswith(ext) for ext in (".csv", ".txt", ".zip")):
        raise HTTPException(status_code=400, detail="File must be .csv, .txt, or .zip")

    raw_bytes = file.file.read()
    data_bytes = _select_voter_data_bytes(raw_bytes, fname)

    logger.info(
        "[Admin] Voter registry upload received: file=%s (%d MB) county=%s user=%s — queued",
        file.filename, len(data_bytes) // (1024 * 1024), county_id, _admin.get("sub"),
    )
    background_tasks.add_task(_run_voter_bulk_load, county_id, data_bytes, file.filename or fname)

    return {
        "status": "processing",
        "county_id": county_id,
        "filename": file.filename,
        "message": (
            "Upload received. Voter file is loading in the background "
            f"(county={county_id}). This typically takes 1-4 minutes for a full "
            "county file; refresh contact stats shortly to see results."
        ),
    }


# ---------------------------------------------------------------------------
# GET  /api/admin/refunds/unlocks     — list lead-unlock purchases
# POST /api/admin/refunds/unlock/{id} — issue Stripe refund + log reason
# ---------------------------------------------------------------------------

class RefundRequest(BaseModel):
    reason: str


@router.get("/refunds/unlocks")
def list_unlock_refunds(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Return recent lead-unlock purchases with refund status.
    Joins SentLead → Subscriber (email) and Property (address).
    """
    rows = db.execute(
        select(SentLead, Subscriber.email, Property.address)
        .join(Subscriber, Subscriber.id == SentLead.subscriber_id)
        .join(Property, Property.id == SentLead.property_id)
        .where(SentLead.source == "lead_unlock_payment")
        .order_by(SentLead.sent_at.desc())
        .limit(limit)
        .offset(offset)
    ).all()

    return [
        {
            "id": sl.id,
            "subscriber_email": email,
            "property_address": address,
            "sent_at": sl.sent_at.isoformat() if sl.sent_at else None,
            "stripe_payment_intent_id": sl.stripe_payment_intent_id,
            "refunded_at": sl.refunded_at.isoformat() if sl.refunded_at else None,
            "refund_reason": sl.refund_reason,
            "stripe_refund_id": sl.stripe_refund_id,
        }
        for sl, email, address in rows
    ]


@router.post("/refunds/unlock/{sent_lead_id}")
def issue_unlock_refund(
    sent_lead_id: int,
    body: RefundRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Issue a full Stripe refund for a $4 lead-unlock and log the reason.
    Returns 404 if the SentLead doesn't exist or isn't an unlock purchase.
    Returns 409 if already refunded.
    Returns 400 if no payment intent ID was stored (pre-migration unlock).
    """
    sl = db.execute(
        select(SentLead).where(
            SentLead.id == sent_lead_id,
            SentLead.source == "lead_unlock_payment",
        )
    ).scalar_one_or_none()

    if not sl:
        raise HTTPException(status_code=404, detail="Lead unlock purchase not found")
    if sl.refunded_at:
        raise HTTPException(status_code=409, detail="Already refunded")
    if not sl.stripe_payment_intent_id:
        raise HTTPException(
            status_code=400,
            detail="No payment intent ID on record — refund manually in Stripe dashboard",
        )

    if not settings.active_stripe_secret_key:
        raise HTTPException(status_code=503, detail="Stripe not configured")

    stripe.api_key = settings.active_stripe_secret_key.get_secret_value()
    try:
        refund = stripe.Refund.create(payment_intent=sl.stripe_payment_intent_id)
    except stripe.error.StripeError as exc:
        logger.error("[Admin] Stripe refund failed for SentLead %s: %s", sent_lead_id, exc)
        raise HTTPException(status_code=502, detail=f"Stripe error: {exc.user_message or str(exc)}")

    sl.refunded_at = datetime.now(timezone.utc)
    sl.refund_reason = body.reason[:255]
    sl.stripe_refund_id = refund.id
    db.commit()

    logger.info(
        "[Admin] Refund issued: sent_lead=%s pi=%s refund=%s reason=%r admin=%s",
        sent_lead_id, sl.stripe_payment_intent_id, refund.id, body.reason, _admin.get("sub"),
    )
    return {
        "refund_id": refund.id,
        "status": refund.status,
        "sent_lead_id": sent_lead_id,
    }


# ---------------------------------------------------------------------------
# POST /api/admin/manual-invoice/provision — provision a customer who paid
# via a manually-sent Stripe Invoice, before self-serve checkout ships.
# ---------------------------------------------------------------------------

class ManualInvoiceProvisionRequest(BaseModel):
    stripe_invoice_id: str
    tier: str
    vertical: str
    county_id: str
    zip_codes: list[str]
    is_founding: bool = False
    founding_price_id: Optional[str] = None


@router.post("/manual-invoice/provision")
def provision_from_manual_invoice(
    body: ManualInvoiceProvisionRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Provision a subscriber from a manually-sent Stripe Invoice paid outside
    Checkout (e.g. a customer wired money before self-serve checkout shipped).

    Verifies the invoice is actually paid in Stripe before creating anything.
    Idempotent on stripe_customer_id — re-posting the same invoice returns the
    already-provisioned subscriber instead of erroring or duplicating.

    Mirrors _on_checkout_completed's new-subscriber field set and ZIP-lock
    all-or-nothing semantics (src/services/stripe_webhooks.py) so a manually
    invoiced customer ends up in the same state a Checkout customer would.
    """
    if body.tier not in VALID_TIERS:
        raise HTTPException(status_code=400, detail=f"Invalid tier: {body.tier}")
    if body.vertical not in VALID_VERTICALS:
        raise HTTPException(status_code=400, detail=f"Invalid vertical: {body.vertical}")
    if not body.zip_codes or not all(ZIP_RE.match(z) for z in body.zip_codes):
        raise HTTPException(status_code=400, detail="zip_codes must be a non-empty list of 5-digit ZIPs")

    if not settings.active_stripe_secret_key:
        raise HTTPException(status_code=503, detail="Stripe not configured")
    stripe.api_key = settings.active_stripe_secret_key.get_secret_value()

    try:
        invoice = stripe.Invoice.retrieve(body.stripe_invoice_id)
    except stripe.error.StripeError as exc:
        raise HTTPException(status_code=502, detail=f"Stripe error: {exc.user_message or str(exc)}")

    if invoice.get("status") != "paid" or invoice.get("amount_remaining", 0) != 0:
        raise HTTPException(
            status_code=402,
            detail=f"Invoice {body.stripe_invoice_id} is not fully paid (status={invoice.get('status')})",
        )

    stripe_customer_id = invoice.get("customer")
    if not stripe_customer_id:
        raise HTTPException(status_code=422, detail="Invoice has no customer attached")

    existing = db.execute(
        select(Subscriber).where(Subscriber.stripe_customer_id == stripe_customer_id)
    ).scalar_one_or_none()
    if existing:
        return {
            "already_provisioned": True,
            "subscriber_id": existing.id,
            "event_feed_uuid": existing.event_feed_uuid,
        }

    try:
        customer = stripe.Customer.retrieve(stripe_customer_id)
    except stripe.error.StripeError as exc:
        raise HTTPException(status_code=502, detail=f"Stripe error fetching customer: {exc.user_message or str(exc)}")

    now = datetime.now(timezone.utc)
    subscriber = Subscriber(
        stripe_customer_id=stripe_customer_id,
        stripe_subscription_id=invoice.get("subscription"),
        tier=body.tier,
        vertical=body.vertical,
        county_id=body.county_id,
        founding_member=body.is_founding,
        founding_price_id=body.founding_price_id if body.is_founding else None,
        rate_locked_at=now if body.is_founding else None,
        status="active",
        event_feed_uuid=str(uuid.uuid4()),
        email=(customer.get("email") or "").lower().strip() or None,
        name=customer.get("name"),
        phone=customer.get("phone"),
        ghl_stage=5,
        signup_source="admin",
        is_test=is_test_subscriber(
            (customer.get("email") or "").lower().strip() or None,
            stripe_livemode=invoice.get("livemode"),
        ),
    )
    db.add(subscriber)
    db.flush()  # need subscriber.id before ZIP claims

    unclaimed = [
        zip_code for zip_code in body.zip_codes
        if not claim_zip_territory(
            db, zip_code=zip_code, vertical=body.vertical, county_id=body.county_id,
            subscriber_id=subscriber.id, now=now,
        )
    ]
    if unclaimed:
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail=(
                f"ZIP(s) already claimed by another subscriber: {', '.join(unclaimed)}. "
                "Nothing was provisioned — retry with different ZIPs."
            ),
        )

    db.commit()

    logger.info(
        "[Admin] Manual-invoice provisioning: subscriber=%s invoice=%s customer=%s tier=%s zips=%s admin=%s",
        subscriber.id, body.stripe_invoice_id, stripe_customer_id, body.tier, body.zip_codes, _admin.get("sub"),
    )

    # ── Welcome email + first-leads email (inline — admin path, no BackgroundTasks needed) ──
    if subscriber.email:
        from src.services.email import send_welcome_email
        from src.services import subscriber_auth
        from src.services.activation_tracking import stamp_welcome_email_sent

        magic_url = None
        try:
            magic_url = subscriber_auth.issue_magic_link_url_with_retry(
                subscriber, db, context="paid_checkout_welcome"
            )
        except Exception:
            logger.warning(
                "[Admin] Magic-link issuance failed for subscriber %s — sending welcome without it",
                subscriber.id, exc_info=True,
            )
        try:
            if send_welcome_email(subscriber, magic_link_url=magic_url, db=db):
                stamp_welcome_email_sent(subscriber.id, db)
            else:
                logger.warning(
                    "[Admin] Welcome email not sent for subscriber %s", subscriber.id
                )
        except Exception:
            logger.error(
                "[Admin] Welcome email failed for subscriber %s", subscriber.id, exc_info=True
            )

    if subscriber.email and body.zip_codes:
        try:
            from src.tasks.subscriber_email import query_top_leads, send_subscriber_lead_email
            leads = query_top_leads(db, subscriber, body.zip_codes, limit=10)
            if leads:
                send_subscriber_lead_email(
                    subscriber,
                    leads,
                    subject_prefix="Here are your first leads",
                    zip_codes=body.zip_codes,
                )
            else:
                logger.info(
                    "[Admin] No existing leads for subscriber %s (zips=%s) — skipping first-leads email",
                    subscriber.id, body.zip_codes,
                )
        except Exception:
            logger.error(
                "[Admin] First-leads email failed for subscriber %s", subscriber.id, exc_info=True
            )

    return {
        "already_provisioned": False,
        "subscriber_id": subscriber.id,
        "event_feed_uuid": subscriber.event_feed_uuid,
        "zip_codes_locked": body.zip_codes,
    }


# ---------------------------------------------------------------------------
# GET /api/admin/dbpr-contacts/export — ranked contractor lead export (item 39)
#
# There is no distress-style score for DBPR contractor rows (unlike
# properties/CDS) — "ranked" here is an explicit, invented completeness/
# outreach-readiness proxy (0-4: has phone, has email, company name resolved,
# not DNC-flagged), not a predictive score. Labeled as such in the CSV header
# so it's never mistaken for something it isn't.
# ---------------------------------------------------------------------------

@router.get("/dbpr-contacts/export")
def export_dbpr_contacts(
    vertical: Optional[str] = Query(default=None),
    county: Optional[str] = Query(default=None),
    min_score: Optional[int] = Query(default=None, ge=0, le=4),
    exclude_suppressed: bool = Query(default=True, description="Exclude opted-out/bounced/already-signed-up contacts"),
    limit: int = Query(default=5000, ge=1, le=20000),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """CSV export of DBPR contractors with an outreach-readiness proxy score.

    Score (0-4), one point each for: has a phone, has an email, company name
    resolved (company_name_status='found'), not DNC/litigator-flagged. A
    contact with no DNC check on file scores the point (unchecked is not the
    same as confirmed bad) — dnc_status is reported separately so that
    distinction stays visible rather than baked silently into the number.
    """
    rows = db.execute(
        text("""
            WITH scored AS (
                SELECT
                    d.id, d.full_name, d.company_name, d.company_name_status,
                    d.vertical, d.zip_code, d.county_id, d.data_source,
                    d.is_opted_out, d.is_hard_bounced, d.is_signed_up,
                    COALESCE(d.mobile_phone, d.landline_phone, d.phone) AS phone,
                    COALESCE(d.work_email, d.email) AS email,
                    CASE
                        WHEN dnc.phone IS NULL THEN 'unchecked'
                        WHEN dnc.national_dnc OR dnc.litigator THEN 'flagged'
                        ELSE 'clean'
                    END AS dnc_status,
                    (
                        (CASE WHEN COALESCE(d.mobile_phone, d.landline_phone, d.phone) IS NOT NULL THEN 1 ELSE 0 END) +
                        (CASE WHEN COALESCE(d.work_email, d.email) IS NOT NULL THEN 1 ELSE 0 END) +
                        (CASE WHEN d.company_name_status = 'found' THEN 1 ELSE 0 END) +
                        (CASE WHEN dnc.phone IS NOT NULL AND (dnc.national_dnc OR dnc.litigator) THEN 0 ELSE 1 END)
                    ) AS completeness_score
                FROM dbpr_contacts d
                LEFT JOIN dnc_phone_checks dnc
                       ON dnc.phone = COALESCE(d.mobile_phone, d.landline_phone, d.phone)
            )
            SELECT * FROM scored
            WHERE (:vertical IS NULL OR vertical = :vertical)
              AND (:county IS NULL OR county_id = :county)
              AND (:min_score IS NULL OR completeness_score >= :min_score)
              AND (:exclude_suppressed = false OR (NOT is_opted_out AND NOT is_hard_bounced AND NOT is_signed_up))
            ORDER BY completeness_score DESC, full_name
            LIMIT :limit
        """),
        {
            "vertical": vertical,
            "county": county,
            "min_score": min_score,
            "exclude_suppressed": exclude_suppressed,
            "limit": limit,
        },
    ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        f"# DBPR Contractor Export — {datetime.now().strftime('%Y-%m-%d')} — {len(rows)} row(s) — "
        "'Score' is an outreach-readiness proxy (phone + email + company resolved + not DNC-flagged), "
        "not a predictive/distress score"
    ])
    writer.writerow([
        "Name", "Company", "Phone", "Email", "ZIP", "County", "Vertical",
        "Score", "DNC Status", "Company Name Status", "Data Source",
    ])
    for r in rows:
        writer.writerow([
            r.full_name, r.company_name, r.phone, r.email, r.zip_code, r.county_id, r.vertical,
            r.completeness_score, r.dnc_status, r.company_name_status, r.data_source,
        ])

    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="dbpr_contacts_export.csv"'},
    )


# ---------------------------------------------------------------------------
# GET /api/admin/stats/contact-coverage — dark-pool visibility
# ---------------------------------------------------------------------------

_GOLD_PLUS_TIERS = ("Gold", "Platinum", "Ultra Platinum")

_HAS_CONTACT = or_(
    Owner.phone_1.isnot(None),
    Owner.phone_2.isnot(None),
    Owner.phone_3.isnot(None),
    Owner.email_1.isnot(None),
    Owner.email_2.isnot(None),
)

# fa079 / item 9.2 — single uniform enrichment definition used across all
# dashboards: a Gold+ lead is "enriched" only when BOTH a phone AND an email
# are present. (The OR-based _HAS_CONTACT above stays for the dark-pool view.)
_HAS_PHONE = or_(
    Owner.phone_1.isnot(None),
    Owner.phone_2.isnot(None),
    Owner.phone_3.isnot(None),
)
_HAS_EMAIL = or_(
    Owner.email_1.isnot(None),
    Owner.email_2.isnot(None),
)
_FULLY_ENRICHED = and_(_HAS_PHONE, _HAS_EMAIL)


@router.get("/stats/contact-coverage")
def contact_coverage_stats(
    county_id: str = Query("hillsborough"),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Returns the size of the 'dark pool' — Gold+ scored properties with no
    owner contact data, broken down by ZIP. Used to quantify the upstream
    revenue leak before deciding on skip-tracing investment.

    Uses the most recent DistressScore per property (DISTINCT ON property_id
    ordered by score_date DESC).
    """
    # Subquery: latest score per property for Gold+ tiers in this county
    latest_sq = (
        select(
            DistressScore.property_id,
            DistressScore.final_cds_score,
            DistressScore.lead_tier,
        )
        .distinct(DistressScore.property_id)
        .where(
            DistressScore.lead_tier.in_(_GOLD_PLUS_TIERS),
            DistressScore.county_id == county_id,
        )
        .order_by(DistressScore.property_id, DistressScore.score_date.desc())
        .subquery()
    )

    has_contact_col = case((_HAS_CONTACT, 1), else_=0).label("has_contact")

    # Per-ZIP aggregation
    zip_rows = db.execute(
        select(
            Property.zip.label("zip"),
            func.count().label("total"),
            func.sum(has_contact_col).label("with_contact"),
            func.sum(case((~_HAS_CONTACT, 1), else_=0)).label("contactless"),
        )
        .join(latest_sq, latest_sq.c.property_id == Property.id)
        .outerjoin(Owner, Owner.property_id == Property.id)
        .group_by(Property.zip)
        .order_by(func.sum(case((~_HAS_CONTACT, 1), else_=0)).desc())
    ).all()

    total_qualified  = sum(r.total for r in zip_rows)
    total_contactless = sum(r.contactless for r in zip_rows)
    total_with_contact = sum(r.with_contact for r in zip_rows)

    # Uniform enrichment metric (phone AND email), broken down by tier.
    enriched_col = case((_FULLY_ENRICHED, 1), else_=0).label("enriched")
    tier_rows = db.execute(
        select(
            latest_sq.c.lead_tier.label("tier"),
            func.count().label("total"),
            func.sum(enriched_col).label("enriched"),
        )
        .select_from(Property)
        .join(latest_sq, latest_sq.c.property_id == Property.id)
        .outerjoin(Owner, Owner.property_id == Property.id)
        .group_by(latest_sq.c.lead_tier)
    ).all()

    total_enriched = sum(r.enriched for r in tier_rows)

    return {
        "county_id": county_id,
        # Uniform enrichment definition: phone AND email on a Gold+ lead.
        "enrichment": {
            "definition": "phone_and_email",
            "total_gold_plus": total_qualified,
            "enriched": total_enriched,
            "enriched_pct": round(100 * total_enriched / total_qualified, 1) if total_qualified else 0,
            "by_tier": {
                r.tier: {
                    "total": r.total,
                    "enriched": int(r.enriched or 0),
                    "enriched_pct": round(100 * (r.enriched or 0) / r.total, 1) if r.total else 0,
                }
                for r in tier_rows
            },
        },
        # Dark-pool view (any contact present) — kept for skip-trace targeting.
        "summary": {
            "total_gold_plus": total_qualified,
            "with_contact": total_with_contact,
            "contactless": total_contactless,
            "contactless_pct": round(100 * total_contactless / total_qualified, 1) if total_qualified else 0,
        },
        "by_zip": [
            {
                "zip": r.zip or "unknown",
                "total": r.total,
                "with_contact": r.with_contact,
                "contactless": r.contactless,
                "contactless_pct": round(100 * r.contactless / r.total, 1) if r.total else 0,
            }
            for r in zip_rows
        ],
    }


# ---------------------------------------------------------------------------
# GET /api/admin/enrichment-health — A4 degraded-provider visibility
# ---------------------------------------------------------------------------

@router.get("/enrichment-health")
def enrichment_health(
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """Per-provider live hit rate vs floor (degraded flag) plus recent
    degraded-provider anomalies. Read model for the Ops 'Provider Health' tab."""
    from config.settings import get_settings
    from src.tasks import match_rate_monitor as mrm

    s = get_settings()
    providers = []
    for provider, floor in s.enrichment_provider_floors.items():
        rate, n = mrm.recent_hit_rate(db, provider, s.enrichment_window_hours)
        health = mrm.evaluate_provider_health(
            provider, rate, n, floor, min_sample=s.enrichment_min_sample
        )
        providers.append({
            "provider": provider,
            "hit_rate": rate,
            "floor": floor,
            "sample_size": n,
            "degraded": health["degraded"],
            "skipped": health["skipped"],
        })

    rows = db.execute(
        text(
            """
            SELECT provider, detected_at, observed_hit_rate, floor_hit_rate, records_affected
            FROM enrichment_anomaly_log
            ORDER BY detected_at DESC
            LIMIT 20
            """
        )
    ).mappings().all()
    recent = [
        {
            "provider": r["provider"],
            "detected_at": r["detected_at"].isoformat() if r["detected_at"] else None,
            "observed_hit_rate": float(r["observed_hit_rate"]),
            "floor_hit_rate": float(r["floor_hit_rate"]),
            "records_affected": r["records_affected"],
        }
        for r in rows
    ]
    return {"providers": providers, "recent_anomalies": recent}


# ---------------------------------------------------------------------------
# GET /api/admin/synthflow/config — Synthflow dashboard data
# ---------------------------------------------------------------------------

_CONFIG_DIR = Path(__file__).parent.parent.parent / "config"


@router.get("/synthflow/config")
def synthflow_config(_admin: dict = Depends(get_current_admin)):
    """
    Return campaign list, agent settings, and prompt scripts read from the
    config/ directory JSON/YAML files. Powers the Synthflow dashboard tab.
    """
    import yaml

    agents = []
    for fname in [
        "finetuner_roofing_agent.json",
        "finetuner_remediation_agent.json",
        "finetuner_revenue_recovery_agent.json",
    ]:
        p = _CONFIG_DIR / fname
        if not p.exists():
            continue
        data = json.loads(p.read_text(encoding="utf-8"))
        cfg = data.get("configuration", {})
        meta = data.get("source_metadata", {})
        if "roofing" in fname:
            vertical = "roofing"
        elif "remediation" in fname:
            vertical = "remediation"
        else:
            vertical = "revenue_recovery"
        agents.append({
            "vertical": vertical,
            "agent_id": meta.get("agent_id"),
            "status": "Draft" if meta.get("is_draft") else "Published",
            "voice_name": cfg.get("voice_name"),
            "voice_provider": cfg.get("voice_provider"),
            "llm": cfg.get("llm"),
            "webhook_url": cfg.get("external_webhook_url"),
            "greeting": cfg.get("greeting_message"),
            "max_duration_seconds": cfg.get("max_duration_seconds"),
            "language": cfg.get("language"),
            "agent_type": cfg.get("agent_type"),
        })

    campaigns = []
    prompts = []
    for fname in [
        "synthflow_roofing_agent.yaml",
        "synthflow_remediation_agent.yaml",
        "synthflow_revenue_recovery_agent.yaml",
    ]:
        p = _CONFIG_DIR / "prompts" / fname
        if not p.exists():
            continue
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
        camp = data.get("campaign", {})
        campaigns.append({
            "name": camp.get("name"),
            "vertical": data.get("vertical"),
            "area_codes": data.get("area_codes", []),
            "total_volume": camp.get("total_volume"),
            "daily_cap": camp.get("daily_cap_per_number"),
            "launch_date": str(camp.get("launch_date", "")),
            "prospect_sources": camp.get("prospect_sources", []),
            "webhook_url": data.get("webhook", {}).get("url", ""),
        })
        prompts.append({
            "vertical": data.get("vertical"),
            "agent_name": data.get("agent_name"),
            "system_prompt": data.get("system_prompt", ""),
            "first_message": data.get("first_message", ""),
            "voicemail_script": data.get("voicemail_script", ""),
        })

    return {"agents": agents, "campaigns": campaigns, "prompts": prompts}


# ---------------------------------------------------------------------------
# GET /api/admin/stats/sku-margin — Premium SKU revenue / cost / margin
# ---------------------------------------------------------------------------

# Retail prices in cents — must mirror config.revenue_ladder.PREMIUM_CREDITS.
_SKU_RETAIL_CENTS = {"report": 700, "brief": 1200, "transfer": 6500, "byol": 500}
_PREMIUM_SKUS = ["report", "brief", "transfer", "byol"]
_WINDOW_DAYS = {"7d": 7, "30d": 30, "90d": 90}


@router.get("/stats/sku-margin")
def sku_margin_stats(
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """Per-SKU revenue / cost / margin / refund / dispute summary across
    rolling 7-day, 30-day, and 90-day windows.

    Cost is the sum of EnrichmentUsageLog.cost_cents joined to PremiumPurchase
    via property_id within the window. For SKUs that don't trigger an
    enrichment lookup (report, brief — artifact-only today), cost stays at 0
    and gross margin is 100% minus Stripe processing.
    """
    now = datetime.now(timezone.utc)
    out = {"as_of": now.isoformat(), "windows": {}}

    for window_key, days in _WINDOW_DAYS.items():
        cutoff = now - timedelta(days=days)
        per_sku = {}
        for sku in _PREMIUM_SKUS:
            # Counts by paid_via
            counts = db.execute(
                select(
                    PremiumPurchase.paid_via,
                    PremiumPurchase.status,
                    func.count().label("n"),
                    func.coalesce(func.sum(PremiumPurchase.amount_cents), 0).label("amount_cents"),
                )
                .where(
                    PremiumPurchase.sku == sku,
                    PremiumPurchase.purchased_at >= cutoff,
                )
                .group_by(PremiumPurchase.paid_via, PremiumPurchase.status)
            ).all()

            total = 0
            delivered = 0
            refunded = 0
            disputed = 0
            failed = 0
            gross_revenue_cents = 0
            for r in counts:
                total += r.n
                if r.status == "delivered":
                    delivered += r.n
                    gross_revenue_cents += int(r.amount_cents or 0) if r.paid_via == "card" else 0
                elif r.status == "refunded":
                    refunded += r.n
                elif r.status == "disputed":
                    disputed += r.n
                elif r.status == "failed":
                    failed += r.n

            # Cost — sum of enrichment usage logs joined by property_id, within window
            cost_cents = db.execute(
                select(func.coalesce(func.sum(EnrichmentUsageLog.cost_cents), 0))
                .select_from(EnrichmentUsageLog)
                .join(
                    PremiumPurchase,
                    PremiumPurchase.property_id == EnrichmentUsageLog.property_id,
                )
                .where(
                    PremiumPurchase.sku == sku,
                    PremiumPurchase.purchased_at >= cutoff,
                    EnrichmentUsageLog.created_at >= cutoff,
                )
            ).scalar() or 0

            margin_pct = None
            if gross_revenue_cents > 0:
                margin_pct = round(
                    100.0 * (gross_revenue_cents - cost_cents) / gross_revenue_cents, 1
                )

            refund_rate = round(100.0 * refunded / total, 1) if total else 0.0
            dispute_rate = round(100.0 * disputed / total, 1) if total else 0.0

            per_sku[sku] = {
                "label": sku,
                "retail_cents": _SKU_RETAIL_CENTS.get(sku, 0),
                "total_purchases": total,
                "delivered": delivered,
                "refunded": refunded,
                "disputed": disputed,
                "failed": failed,
                "gross_revenue_cents": gross_revenue_cents,
                "cost_cents": int(cost_cents or 0),
                "margin_cents": gross_revenue_cents - int(cost_cents or 0),
                "margin_pct": margin_pct,
                "refund_rate_pct": refund_rate,
                "dispute_rate_pct": dispute_rate,
            }
        out["windows"][window_key] = per_sku

    return out


# ---------------------------------------------------------------------------
# SMS Opt-In management (for live SMS testing)
# ---------------------------------------------------------------------------

class SmsOptInRequest(BaseModel):
    phone: str
    subscriber_id: Optional[int] = None
    keyword_used: Optional[str] = "YES"
    source: str = "manual"


@router.post("/sms-opt-in", dependencies=[Depends(get_current_admin)])
def create_sms_opt_in(req: SmsOptInRequest, db: Session = Depends(get_db)):
    """Create or update an SMS opt-in record. Used to seed consent before live SMS testing."""
    existing = db.query(SmsOptIn).filter(SmsOptIn.phone == req.phone).first()
    if existing:
        existing.subscriber_id = req.subscriber_id or existing.subscriber_id
        existing.keyword_used = req.keyword_used
        existing.opted_in_at = datetime.now(timezone.utc)
        db.commit()
        return {"ok": True, "action": "updated", "phone": req.phone}

    record = SmsOptIn(
        phone=req.phone,
        subscriber_id=req.subscriber_id,
        keyword_used=req.keyword_used,
        source=req.source,
        opt_in_message="Manual opt-in via admin API",
        opted_in_at=datetime.now(timezone.utc),
    )
    db.add(record)
    db.commit()
    return {"ok": True, "action": "created", "phone": req.phone}


@router.get("/sms-opt-in", dependencies=[Depends(get_current_admin)])
def list_sms_opt_ins(
    subscriber_id: Optional[int] = Query(None),
    phone: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """List SMS opt-in records. Filter by subscriber_id or phone for pre-test verification."""
    q = db.query(SmsOptIn)
    if subscriber_id:
        q = q.filter(SmsOptIn.subscriber_id == subscriber_id)
    if phone:
        q = q.filter(SmsOptIn.phone == phone)
    rows = q.order_by(SmsOptIn.opted_in_at.desc()).limit(50).all()
    return [
        {
            "id": r.id,
            "phone": r.phone,
            "subscriber_id": r.subscriber_id,
            "keyword_used": r.keyword_used,
            "source": r.source,
            "opted_in_at": r.opted_in_at.isoformat() if r.opted_in_at else None,
        }
        for r in rows
    ]


@router.delete("/sms-opt-in/{phone}", dependencies=[Depends(get_current_admin)])
def delete_sms_opt_in(phone: str, db: Session = Depends(get_db)):
    """Remove an SMS opt-in record (simulate opt-out for testing compliance gate)."""
    record = db.query(SmsOptIn).filter(SmsOptIn.phone == phone).first()
    if not record:
        raise HTTPException(status_code=404, detail="No opt-in record for that phone")
    db.delete(record)
    db.commit()
    return {"ok": True, "deleted": phone}


# ---------------------------------------------------------------------------
# Gate Monitoring endpoints
# ---------------------------------------------------------------------------

@router.get("/gate-metrics", dependencies=[Depends(get_current_admin)])
def gate_metrics(
    days: int = Query(1, ge=1, le=90),
    graph_name: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """
    Aggregate agent_decisions for the last N days.
    Returns per-graph pass/fail/abort rates and top block reasons.
    Powered by gate_metrics_aggregator.
    """
    from src.tasks.gate_metrics_aggregator import compute_block_reasons, compute_gate_metrics

    metrics = compute_gate_metrics(db, days=days)
    if graph_name:
        metrics = [g for g in metrics if g["graph_name"] == graph_name]

    block_reasons = compute_block_reasons(db, days=max(days, 7))
    return {
        "days": days,
        "graphs": metrics,
        "top_block_reasons": block_reasons,
    }


@router.get("/roas", dependencies=[Depends(get_current_admin)])
def roas(
    campaign_id: Optional[str] = Query(None),
    utm_campaign: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None, description="ISO date/datetime, inclusive lower bound"),
    end_date: Optional[str] = Query(None, description="ISO date/datetime, exclusive upper bound"),
    ad_spend: Optional[float] = Query(None, ge=0, description="Manual ad spend (v1) for ROAS calc"),
    db: Session = Depends(get_db),
):
    """Return revenue grouped by ad campaign (Meta closed-loop, S2).

    Sums `conversion_attribution_events.revenue_amount` joined to subscribers'
    campaign attribution. ROAS = total_revenue / ad_spend; null when ad_spend is
    missing or zero. Revenue reflects post-S2 data only — historical conversion
    rows predate revenue_amount capture.
    """
    rows = db.execute(
        text("""
            WITH rev AS (
                SELECT
                    COALESCE(s.campaign_id, s.utm_campaign, 'unattributed') AS campaign_key,
                    s.campaign_id,
                    s.utm_campaign,
                    cae.revenue_amount
                FROM conversion_attribution_events cae
                JOIN subscribers s ON s.id = cae.subscriber_id
                WHERE cae.revenue_amount IS NOT NULL
                  AND (:campaign_id  IS NULL OR s.campaign_id  = :campaign_id)
                  AND (:utm_campaign IS NULL OR s.utm_campaign = :utm_campaign)
                  AND (:start_date   IS NULL OR cae.occurred_at >= CAST(:start_date AS timestamptz))
                  AND (:end_date     IS NULL OR cae.occurred_at <  CAST(:end_date   AS timestamptz))
            )
            SELECT
                campaign_key,
                MIN(campaign_id)  AS campaign_id,
                MIN(utm_campaign) AS utm_campaign,
                SUM(revenue_amount) AS total_revenue,
                COUNT(*)            AS purchase_count
            FROM rev
            GROUP BY campaign_key
            ORDER BY total_revenue DESC
        """),
        {
            "campaign_id": campaign_id,
            "utm_campaign": utm_campaign,
            "start_date": start_date,
            "end_date": end_date,
        },
    ).mappings().all()

    spend = float(ad_spend) if ad_spend else 0.0
    results = []
    for row in rows:
        revenue = float(row["total_revenue"] or 0)
        results.append({
            "campaign_id": row["campaign_id"],
            "utm_campaign": row["utm_campaign"],
            "total_revenue": revenue,
            "ad_spend": spend,
            "roas": round(revenue / spend, 4) if spend else None,
            "purchase_count": int(row["purchase_count"]),
        })

    logger.info(
        "roas_query_completed groups=%d campaign_id=%s utm_campaign=%s ad_spend=%s",
        len(results), campaign_id, utm_campaign, spend,
    )
    return results


# Manual-entry channels for marketing_spend. "meta" is the single combined
# channel for Facebook + Instagram ad spend — the compiler's channel-key
# normalizes any utm_source of facebook/instagram/fb/ig/meta into this one
# bucket (revenue_metrics._CHANNEL_KEY_SQL), mirroring how meta_capi_service.py
# already treats both placements as one Meta integration. Quora
# (quora_topics.cumulative_spend) and affiliate (affiliate_payout_ledger)
# already track real cost and are auto-pulled by the CAC/payback compiler;
# they are deliberately excluded here to avoid double-entry.
MANUAL_SPEND_CHANNELS = frozenset({"meta", "google", "dbpr_email"})


class MarketingSpendCreateRequest(BaseModel):
    channel: str
    campaign_key: Optional[str] = None
    period_start: date
    period_end: date
    amount: float = Field(gt=0, description="Spend in dollars")
    currency: str = "usd"
    notes: Optional[str] = None

    @model_validator(mode="after")
    def validate_channel_and_period(self) -> "MarketingSpendCreateRequest":
        if self.channel not in MANUAL_SPEND_CHANNELS:
            raise ValueError(
                f"channel must be one of {sorted(MANUAL_SPEND_CHANNELS)} — "
                "Quora and affiliate spend are auto-sourced, not entered here"
            )
        if self.period_end < self.period_start:
            raise ValueError("period_end must be >= period_start")
        return self


@router.post("/marketing-spend", status_code=201, dependencies=[Depends(get_current_admin)])
def create_marketing_spend(body: MarketingSpendCreateRequest, db: Session = Depends(get_db)):
    """Record manually-entered ad spend for a channel/period (Block 4).

    Upserts on (channel, campaign_key, period_start, period_end) — re-submitting
    the same period updates the amount rather than duplicating the row.
    """
    from src.core.models import MarketingSpend

    existing = db.execute(
        text("""
            SELECT id FROM marketing_spend
            WHERE channel = :channel
              AND campaign_key IS NOT DISTINCT FROM :campaign_key
              AND period_start = :period_start
              AND period_end   = :period_end
        """),
        {
            "channel": body.channel,
            "campaign_key": body.campaign_key,
            "period_start": body.period_start,
            "period_end": body.period_end,
        },
    ).scalar_one_or_none()

    amount_cents = round(body.amount * 100)
    if existing:
        db.execute(
            text("""
                UPDATE marketing_spend
                SET amount_cents = :amount_cents, currency = :currency,
                    notes = :notes, updated_at = now()
                WHERE id = :id
            """),
            {"amount_cents": amount_cents, "currency": body.currency, "notes": body.notes, "id": existing},
        )
        db.commit()
        logger.info("marketing_spend_updated id=%s channel=%s", existing, body.channel)
        return {"id": existing, "updated": True}

    row = MarketingSpend(
        channel=body.channel,
        campaign_key=body.campaign_key,
        period_start=body.period_start,
        period_end=body.period_end,
        amount_cents=amount_cents,
        currency=body.currency,
        notes=body.notes,
    )
    db.add(row)
    db.commit()
    logger.info("marketing_spend_created id=%s channel=%s amount_cents=%d", row.id, body.channel, amount_cents)
    return {"id": row.id, "updated": False}


@router.get("/marketing-spend", dependencies=[Depends(get_current_admin)])
def list_marketing_spend(
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    db: Session = Depends(get_db),
):
    """List manually-entered spend rows overlapping the given window (or all, if omitted)."""
    rows = db.execute(
        text("""
            SELECT id, channel, campaign_key, period_start, period_end,
                   amount_cents, currency, notes, updated_at
            FROM marketing_spend
            WHERE (:from_date IS NULL OR period_end   >= CAST(:from_date AS date))
              AND (:to_date   IS NULL OR period_start <= CAST(:to_date   AS date))
            ORDER BY period_start DESC
        """),
        {"from_date": from_date, "to_date": to_date},
    ).mappings().all()
    return [
        {
            "id": r["id"],
            "channel": r["channel"],
            "campaign_key": r["campaign_key"],
            "period_start": r["period_start"].isoformat(),
            "period_end": r["period_end"].isoformat(),
            "amount": round(r["amount_cents"] / 100, 2),
            "currency": r["currency"],
            "notes": r["notes"],
            "updated_at": r["updated_at"].isoformat(),
        }
        for r in rows
    ]


@router.get("/cac-payback", dependencies=[Depends(get_current_admin)])
def cac_payback(
    from_date: str = Query(..., alias="from", description="ISO date, inclusive"),
    to_date: str = Query(..., alias="to", description="ISO date, exclusive"),
    db: Session = Depends(get_db),
):
    """Per-channel CAC / payback rollup (Block 4 #23).

    Thin wrapper over revenue_metrics.compute_channel_metrics — the single
    source of truth also used by the daily_dashboard PDF section, so the
    on-demand view and the scheduled report can never disagree.
    """
    from datetime import datetime as _dt
    from src.services.revenue_metrics import compute_channel_metrics

    frm = _dt.fromisoformat(from_date)
    to = _dt.fromisoformat(to_date)
    return compute_channel_metrics(db, frm, to)


@router.get("/kill-switch-status", dependencies=[Depends(get_current_admin)])
def kill_switch_status_overview():
    """
    Return current kill-switch colour + cached observed metric for every
    configured feature. Requires kill_switch_metric_ingest cron to have run.
    """
    from config.lifecycle_guardrails import KILL_SWITCH
    from src.services.kill_switch_service import get_cached_metric, get_kill_switch_status as kill_switch_status

    results = []
    for feature, cfg in KILL_SWITCH.items():
        observed = get_cached_metric(feature)
        status = kill_switch_status(feature, observed)
        results.append({
            "feature": feature,
            "observed_value": observed,
            "color": status.get("color"),
            "action": status.get("action") or cfg.get("action"),
            "threshold_yellow": cfg.get("yellow"),
            "threshold_red": cfg.get("red"),
            "cached": observed is not None,
        })

    return {"features": results}


@router.get("/freemium-status", dependencies=[Depends(get_current_admin)])
def freemium_funnel_status():
    """
    T-B3-01 launch verification: effective state of every freemium-funnel leg
    behind the FREEMIUM_FUNNEL_ENABLED master toggle. A leg is ON iff the
    master is ON; the abandonment leg additionally reports its raw
    first_payment_rate kill-switch color and the cart-recovery sweep flag.
    """
    from config.settings import get_settings
    from src.services.kill_switch_service import get_cached_metric, get_kill_switch_status

    settings = get_settings()
    master = settings.freemium_funnel_enabled

    try:
        ks = get_kill_switch_status("first_payment_rate", get_cached_metric("first_payment_rate"))
        ks_color = ks.get("color", "unknown")
    except Exception:
        logger.warning("freemium-status: kill-switch read failed", exc_info=True)
        ks_color = "unknown"

    simple = "ON" if master else "OFF"
    return {
        "master": master,
        "legs": {
            "free_signup": {"effective": simple, "gates": {"master": master}},
            "blurred_teaser": {"effective": simple, "gates": {"master": master}},
            "monetization_wall": {"effective": simple, "gates": {"master": master}},
            "flash_scarcity": {"effective": simple, "gates": {"master": master}},
            "abandonment": {
                "effective": simple,
                "gates": {
                    "master": master,
                    "first_payment_rate": ks_color,
                    "checkout_recovery_enabled": settings.checkout_recovery_enabled,
                },
            },
        },
    }


@router.get("/decision-audit/{decision_id}", dependencies=[Depends(get_current_admin)])
def decision_audit(decision_id: str, db: Session = Depends(get_db)):
    """
    Return full decision trace for a single agent_decisions row.
    Includes hierarchy_path, kill_switch_color, tokens, cost, and full summary JSONB.
    """
    from src.core.models import AgentDecision

    row = db.execute(
        select(AgentDecision).where(AgentDecision.decision_id == decision_id)
    ).scalar_one_or_none()

    if not row:
        raise HTTPException(status_code=404, detail="Decision not found")

    return {
        "decision_id": row.decision_id,
        "graph_name": row.graph_name,
        "subscriber_id": row.subscriber_id,
        "terminal_status": row.terminal_status,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "completed_at": row.completed_at.isoformat() if row.completed_at else None,
        "tokens_used": row.tokens_used,
        "cost_usd": float(row.cost_usd) if row.cost_usd is not None else None,
        "summary": row.summary,
    }


# ---------------------------------------------------------------------------
# Slack county-launch interactive endpoint
# ---------------------------------------------------------------------------

import hashlib
import hmac
import time
from urllib.parse import parse_qs

from fastapi import Request


def _verify_slack_signature(headers: dict, body: bytes) -> bool:
    """Verify Slack request signature (HMAC-SHA256). Rejects replays > 5 min old.

    Tries the FA Max signing secret first, then falls back to the shared secret,
    so button clicks from both Slack apps are accepted at this single endpoint.
    """
    ts = headers.get("x-slack-request-timestamp", "")
    try:
        if abs(time.time() - int(ts)) > 300:
            return False
    except (TypeError, ValueError):
        return False
    sig_base = f"v0:{ts}:{body.decode('utf-8')}"
    received = headers.get("x-slack-signature", "")
    candidates = [settings.fa_max_slack_signing_secret, settings.slack_signing_secret]
    for secret in candidates:
        if not secret:
            continue
        expected = "v0=" + hmac.new(
            secret.get_secret_value().encode(),
            sig_base.encode(),
            hashlib.sha256,
        ).hexdigest()
        if hmac.compare_digest(expected, received):
            return True
    return False


def _slack_ephemeral(text: str) -> dict:
    return {"response_type": "ephemeral", "text": text}


def _reject_if_wrong_command_channel(channel_id: str) -> Optional[dict]:
    """Shared gate for both FA Max pipeline slash commands, per the
    client's decision that the Command Center channel is where Josh
    manages submissions, status updates, and questions — all of it, one
    place. Unset setting means the channel hasn't been configured yet and
    fails OPEN (never lock Josh out of his own commands before the
    channel ID exists) — see plan Task 21's design note.

    Takes a plain channel_id string rather than a raw Slack form dict so
    both the HTTP Request-URL route (parse_qs, list-wrapped values) and
    the Socket Mode slash-command listener (already-scalar payload dict)
    can call it after normalizing to the same shape.
    """
    required_channel = get_settings().fa_max_slack_cc_channel
    if not required_channel:
        return None
    if channel_id == required_channel:
        return None
    return _slack_ephemeral(
        "Please use this command in the Command Center channel (#fa-max-command-center), not here."
    )


def _handle_borrower_search_suggestion(payload: dict, db: Session) -> dict:
    """block_suggestion handler for the log-submission modal's borrower
    external_select (Task 18). Slack's options[].text.text field has a 75
    character limit, hence the truncation.
    """
    from src.services.fa_max_person_search import search_fa_max_persons

    query = payload.get("value", "")
    matches = search_fa_max_persons(db, query)
    options = []
    for m in matches:
        detail_parts = [p for p in (m.get("email"), m.get("phone"), m.get("last_stage")) if p]
        detail = " · ".join(detail_parts) if detail_parts else "no contact on file"
        label = f"{m['full_name'] or 'Unnamed'} ({detail})"[:75]
        options.append({
            "text": {"type": "plain_text", "text": label},
            "value": m["person_id"],
        })
    return {"options": options}


def _parse_slack_interactive_payload(raw: bytes) -> dict:
    """Shared by every Block Kit button endpoint below (not /slack/kill,
    which is a slash command with a differently-shaped body)."""
    try:
        payload_str = parse_qs(raw.decode("utf-8")).get("payload", ["{}"])[0]
        return json.loads(payload_str)
    except Exception:
        raise HTTPException(status_code=400, detail="Malformed payload")


@router.post("/slack/interact")
async def slack_interact(request: Request, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """
    Single Interactivity Request URL for every Slack Block Kit button in
    this app. Slack allows exactly one Interactivity Request URL per app —
    county-launch, relay-decision, and win-story previously each tried to
    register their own, which meant at most one of the three was ever
    actually reachable from Slack. This endpoint is the one Request URL
    Slack's Interactivity setting should point at; it dispatches on the
    clicked button's action_id.
    Auth: Slack HMAC-SHA256 signature (no JWT — Slack signature IS the auth).
    """
    raw = await request.body()
    if not _verify_slack_signature(dict(request.headers), raw):
        raise HTTPException(status_code=401, detail="Invalid Slack signature")

    payload = _parse_slack_interactive_payload(raw)

    # view_submission (WP-T2-2 Revise modal) arrives at this same
    # Interactivity Request URL, not as a "block_actions" payload with an
    # `actions` list — it must be checked before indexing into `actions`.
    if payload.get("type") == "view_submission":
        callback_id = payload.get("view", {}).get("callback_id")
        if callback_id == "fa_max_revise_submit":
            return _handle_relay_revise_submission(payload)
        if callback_id == "fa_max_log_submission_submit":
            return _handle_log_submission_view_submit(payload, db)

    # block_suggestion (external_select live search) also arrives at this
    # same Interactivity Request URL, with neither an "actions" list nor a
    # "view"/callback_id shape — checked here for the same reason
    # view_submission is checked before indexing into `actions` above.
    if payload.get("type") == "block_suggestion" and payload.get("action_id") == "borrower_search":
        return _handle_borrower_search_suggestion(payload, db)

    actions = payload.get("actions", [])
    action_id = actions[0].get("action_id") if actions else None

    _CORA_EXACT = frozenset({"approve_all", "reject_batch", "ratify_standing_order", "decline_standing_order"})
    _CORA_PREFIXES = ("view_", "archive_standing_order_", "keep_standing_order_")

    if action_id == "county_launch_decision":
        return _handle_county_launch_interact(payload, db)
    if action_id in ("approve", "reject"):
        return _handle_relay_decision(payload)
    if action_id == "fa_max_skip":
        return _handle_relay_skip(payload)
    if action_id == "fa_max_snooze":
        return _handle_relay_snooze(payload)
    if action_id == "fa_max_revise":
        return _handle_relay_revise_open(payload)
    if action_id == "log_submission_new_borrower":
        return _handle_log_submission_new_borrower_click(payload)
    if action_id in ("approve_win_story", "dismiss_win_story"):
        return _handle_win_story_interact(payload, db)
    # Builder entity-link actions (EXCEPTIONS lane — WP-T2-8)
    if action_id and action_id.startswith("confirm_entity_link_"):
        return _handle_confirm_entity_link(payload, db)
    if action_id and action_id.startswith("reject_entity_link_"):
        return _handle_reject_entity_link(payload, db)
    if action_id and action_id.startswith("view_entity_link_"):
        return _handle_view_entity_link(payload, db)
    # Builder opportunity actions (RELATIONSHIPS lane — WP-T2-8)
    if action_id and action_id.startswith("add_builder_to_diallist_"):
        return _handle_add_builder_to_diallist(payload, db)
    if action_id and action_id.startswith("snooze_builder_"):
        return _handle_snooze_builder(payload, db)
    if action_id and action_id.startswith("dismiss_builder_"):
        return _handle_dismiss_builder(payload, db)
    if action_id in _CORA_EXACT or (
        action_id and any(action_id.startswith(p) for p in _CORA_PREFIXES)
    ) or (action_id and action_id.startswith("reject_")):
        return _handle_cora_batch_interact(payload, db, background_tasks)

    return _slack_ephemeral(f"Unrecognized action: {action_id}")


@router.post("/slack/events")
async def slack_events(request: Request):
    """Slack Events API endpoint for Relay card thread actions.

    The same HMAC verification used for button interactivity applies here.
    Only exact ``approve`` and ``reject`` replies are commands; normal
    conversation in a queue-card thread is deliberately left untouched.
    """
    raw = await request.body()
    if not _verify_slack_signature(dict(request.headers), raw):
        raise HTTPException(status_code=401, detail="Invalid Slack signature")
    try:
        payload = json.loads(raw)
    except Exception:
        raise HTTPException(status_code=400, detail="Malformed payload")
    if payload.get("type") == "url_verification":
        return {"challenge": payload.get("challenge", "")}
    if payload.get("type") == "event_callback":
        _handle_relay_thread_action(payload)
    return {"ok": True}


def _handle_relay_thread_action(payload: dict) -> None:
    """Turn an authorized exact command in a Relay card thread into a
    regular durable Relay decision.

    A typed reply has no revision snapshot. After any revision, typed
    approval is refused; the approver must use the refreshed button, whose
    revision count is checked atomically with the decision. This remains
    safe if Slack failed to refresh the card.
    """
    event = payload.get("event") or {}
    if event.get("type") != "message" or event.get("subtype") or event.get("bot_id"):
        return
    command = str(event.get("text") or "").strip().casefold()
    if command not in {"approve", "reject"}:
        return
    thread_ts = event.get("thread_ts")
    user_id = event.get("user")
    if not thread_ts or not user_id:
        return
    from src.services.relay import queue as relay_queue
    item = relay_queue.get_item_by_slack_message_ts(str(thread_ts))
    if item is None or not _relay_approver_authorized(str(user_id), item.venture_key):
        return
    # A typed reply carries no revision token. For a revised FA Max draft,
    # require the refreshed button's atomic revision check instead.
    if command == "approve" and item.venture_key == "fa_max_lending" and item.revision_count:
        _post_relay_thread_note(item, "Review the revised card and use its Approve button.")
        return
    # Reuse the normal decision path, including its pending-row CAS, state
    # transition transaction, interaction audit, and Slack-card update.
    _handle_relay_decision({
        "user": {"id": str(user_id)},
        "actions": [{"value": json.dumps({"item_id": item.id, "action": command,
                                           "revision_count_at_post": item.revision_count})}],
    })


@router.post("/slack/county-launch/interact")
async def slack_county_launch_interact(request: Request, db: Session = Depends(get_db)):
    """Deprecated individual URL — kept as an alias until the Slack app's
    Interactivity Request URL is cut over to /slack/interact."""
    raw = await request.body()
    if not _verify_slack_signature(dict(request.headers), raw):
        raise HTTPException(status_code=401, detail="Invalid Slack signature")
    return _handle_county_launch_interact(_parse_slack_interactive_payload(raw), db)


def _handle_county_launch_interact(payload: dict, db: Session) -> dict:
    """Approve/Skip a county-launch candidate."""
    user_id = payload.get("user", {}).get("id", "")
    approvers = settings.county_launch_approvers
    # Fail CLOSED: an empty/unset COUNTY_LAUNCH_APPROVERS must mean nobody is
    # authorized, not everybody — this endpoint hadn't received the fix
    # already applied to _relay_approver_authorized (PR #179 finding #3).
    if not approvers or user_id not in approvers:
        return _slack_ephemeral("Not authorized to approve county launches.")

    actions = payload.get("actions", [])
    if not actions:
        return _slack_ephemeral("No action found in payload.")

    try:
        action_data = json.loads(actions[0].get("value", "{}"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid action value")

    candidate_id = action_data.get("candidate_id")
    action = action_data.get("action")
    if not candidate_id or action not in ("approve", "skip"):
        raise HTTPException(status_code=400, detail="Invalid action data")

    # SELECT FOR UPDATE — idempotency guard against double-tap races
    candidate = db.execute(
        select(ExpansionCandidate)
        .where(ExpansionCandidate.id == candidate_id)
        .with_for_update()
    ).scalar_one_or_none()

    if not candidate:
        return _slack_ephemeral(f"Candidate {candidate_id} not found.")

    if candidate.status != "queued":
        msg = f"Already {candidate.status}"
        if candidate.approved_by_slack_user:
            msg += f" by <@{candidate.approved_by_slack_user}>"
        return _slack_ephemeral(msg)

    now = datetime.now(timezone.utc)
    audit_actor = f"slack:{user_id}"

    if action == "approve":
        candidate.status = "approved"
        candidate.approved_at = now
        candidate.approved_by_slack_user = user_id
        event_type = "approved"
        reply_text = f":white_check_mark: Launch approved by <@{user_id}>."
    else:
        candidate.status = "skipped"
        event_type = "rejected"
        reply_text = f":no_entry: Launch skipped by <@{user_id}>."

    audit_row = CountyLaunchAudit(
        county_id=candidate.county_id,
        event_type=event_type,
        actor=audit_actor,
        detail={"candidate_id": candidate_id, "action": action},
    )
    db.add(audit_row)
    db.commit()

    # Strip buttons from original message
    _update_slack_message(candidate, reply_text)

    return {"ok": True}


def _update_slack_message(candidate: "ExpansionCandidate", reply_text: str) -> None:
    """Replace button block with result text in the original Slack message."""
    token = settings.slack_bot_token
    channel = settings.county_launch_slack_channel
    if not token or not channel or not candidate.last_slack_message_ts:
        return
    try:
        from slack_sdk import WebClient
        client = WebClient(token=token.get_secret_value())
        client.chat_update(
            channel=channel,
            ts=candidate.last_slack_message_ts,
            text=reply_text,
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": reply_text},
                }
            ],
        )
    except Exception as exc:
        logger.error("[SlackInteract] chat.update failed: %s", exc)


# ===========================================================================
# RELAY — APPROVAL QUEUE DECISION + KILL COMMAND (RELAY-v2.2 sub-task R1)
# ===========================================================================

def _relay_approver_authorized(user_id: str, venture_key: Optional[str] = None) -> bool:
    """Fail CLOSED: an empty/unset RELAY_APPROVERS means NOBODY is
    authorized, not everybody (PR #179 review finding #3). The previous
    per-endpoint checks (`if approvers and user_id not in approvers`)
    short-circuited to a no-op when `approvers` was the default empty
    list, silently accepting any Slack workspace member as an approver
    (a valid Slack signature only proves the request came from Slack for
    this app -- it says nothing about which workspace member sent it).
    Shared by both /slack/relay-decision and /slack/kill so the fix lives
    in one place rather than two easily-desynced copies.

    `venture_key` widens the list for one venture (CLONE-v2.2 / CL3):
    RELAY_APPROVERS is the FLEET operator list and authorizes every venture,
    while a venture's own ventures.relay_approvers adds approvers for just
    that venture. Union of the two, so venture A's venture-specific approvers
    still cannot decide venture B's items. Omitted for the fleet-wide
    /slack/kill command, which has no item and therefore no venture.

    The fleet list is checked FIRST, deliberately: it reads this module's own
    `settings` binding, needs no DB, and cannot be skewed by
    get_venture_config()'s 5-minute cache. Both empty still means NOBODY is
    authorized. (A venture-specific approver removal takes up to that cache
    TTL to take effect — remove them from the fleet list too if it must be
    immediate.)
    """
    if not user_id:
        return False

    fleet_approvers = settings.relay_approvers or ()
    if user_id in fleet_approvers:
        return True

    if venture_key is None:
        return False

    from src.utils.venture_config import get_venture_config

    venture_approvers = get_venture_config(venture_key).relay_approvers or ()
    return user_id in venture_approvers


def _update_relay_slack_message(
    item, reply_text: str,
) -> None:
    """Replace the Approve/Reject buttons with the decision outcome, in
    place. Mirrors _update_slack_message's county-launch pattern.

    The channel must be the venture's own (CLONE-v2.2 / CL3) — a ts from one
    channel cannot be edited in another, so using a single global channel here
    would fail every edit for every venture but the first.
    """
    from src.services.relay.slack_post import _resolve_bot_token, _resolve_channel

    token = _resolve_bot_token(item, settings)
    channel = _resolve_channel(item, settings)
    if not token or not channel or not item.slack_message_ts:
        return
    try:
        from slack_sdk import WebClient
        client = WebClient(token=token.get_secret_value())
        client.chat_update(
            channel=channel,
            ts=item.slack_message_ts,
            text=reply_text,
            blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": reply_text}}],
        )
    except Exception as exc:
        logger.error("[RelayInteract] chat.update failed: %s", exc)


@router.post("/slack/relay-decision")
async def slack_relay_decision(request: Request):
    """Deprecated individual URL — kept as an alias until the Slack app's
    Interactivity Request URL is cut over to /slack/interact."""
    raw = await request.body()
    if not _verify_slack_signature(dict(request.headers), raw):
        raise HTTPException(status_code=401, detail="Invalid Slack signature")
    return _handle_relay_decision(_parse_slack_interactive_payload(raw))


def _handle_relay_decision(payload: dict) -> dict:
    """Approve/Reject a pending Relay approval-queue item (build spec
    §1.1.13 tap surface).

    For FA Max items (venture_key='fa_max_lending'): if the approved item's
    payload contains a 'fa_max_transition' key, calls state_engine.transition()
    to advance the FA Max person/opportunity state as part of the same approval.
    Approval and its required state transition commit atomically. A failed
    transition leaves the queue item pending and returns a refusal reason.
    """
    import logging as _logging

    from src.services.relay import queue as relay_queue

    _log = _logging.getLogger(__name__)
    _FA_MAX_VENTURE = "fa_max_lending"

    user_id = payload.get("user", {}).get("id", "")

    actions = payload.get("actions", [])
    if not actions:
        return _slack_ephemeral("No action found in payload.")

    try:
        action_data = json.loads(actions[0].get("value", "{}"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid action value")

    item_id = action_data.get("item_id")
    action = action_data.get("action")
    if not item_id or action not in ("approve", "reject"):
        raise HTTPException(status_code=400, detail="Invalid action data")

    # Authorization is scoped to the item's own venture (CLONE-v2.2 / CL3), so
    # the row has to be read before the check. An unknown id falls back to
    # venture #1's approver list rather than skipping the check.
    existing = relay_queue.get_item(item_id)
    venture_key = existing.venture_key if existing is not None else DEFAULT_VENTURE_KEY
    if not _relay_approver_authorized(user_id, venture_key):
        return _slack_ephemeral("Not authorized to approve Relay sends.")
    if existing is None:
        return _slack_ephemeral(f"Item #{item_id} not found.")

    # Stale-card guard (WP-T2-2 item 9): a Revise submitted after this card
    # was posted bumps revision_count. approve_value/reject_value carry the
    # revision_count that was current AT POST TIME
    # (src.services.relay.slack_post.post_for_approval) — if the row's
    # current revision_count has moved since, this Approve click is acting
    # on stale (pre-revision) content and must be refused rather than
    # silently sending the old draft.
    posted_revision_count = None
    if action == "approve":
        posted_revision_count = action_data.get("revision_count_at_post")
        if existing.venture_key == _FA_MAX_VENTURE and posted_revision_count is None:
            return _slack_ephemeral("Approval card is missing a revision token; open the current card.")
        if posted_revision_count is not None and existing.revision_count != posted_revision_count:
            return _slack_ephemeral(
                f"Item #{item_id} was revised (now revision #{existing.revision_count}) after this "
                "card was posted — check the revision note in this thread before approving."
            )
        # This early check is a fast, friendly error message — the actual
        # guarantee against a Revise landing between this check and the
        # decision commit is expected_revision_count on record_decision()
        # below, which folds the same comparison into the atomic UPDATE
        # itself (WP-T2-2 review fix — see that function's docstring).

    has_fa_transition = (
        action == "approve"
        and existing.venture_key == _FA_MAX_VENTURE
        and isinstance(existing.payload, dict)
        and existing.payload.get("fa_max_transition")
    )
    if has_fa_transition:
        # Lock the queue row, perform the state transition, and persist approval
        # in one transaction. Any failure rolls both operations back, leaving
        # the card pending and therefore impossible for Relay to dispatch.
        spec = existing.payload["fa_max_transition"]
        try:
            from src.services.state_engine import (
                ensure_entity_registry,
                get_opportunity_state,
                get_person_state,
                transition,
                TransitionOutcome,
            )
            from src.core.database import get_db_context as _get_db

            entity_type = spec.get("entity_type", "person")
            # spec["entity_uuid"] is the entity's own native ID
            # (opportunity_id/person_id) -- get_opportunity_state/
            # get_person_state below need exactly that. transition()
            # needs a real fa_max_entity_registry entity_uuid instead
            # (resolved just before that call, WP-T2-6 review fix).
            entity_uuid = spec["entity_uuid"]

            with _get_db() as _db:
                item = relay_queue.get_item_for_update(item_id, session=_db)
                if item is None or item.status != "pending":
                    return _slack_ephemeral(
                        f"Item #{item_id} was already decided (not still pending)."
                    )
                spec = item.payload["fa_max_transition"]
                # Load current state now, at approval time, rather than trusting
                # spec["from_state"] — that value was written into the payload
                # when the item was enqueued, which can be hours or days before
                # Josh actually presses Approve, and the entity may have moved
                # via a different path (another agent, another Slack lane) in
                # the meantime. transition() is a CAS write keyed on from_state:
                # a stale value here silently reports already_advanced instead
                # of applying the transition Josh actually approved.
                if entity_type == "opportunity":
                    current = get_opportunity_state(session=_db, opportunity_id=entity_uuid)
                    current_from_state = current.get("current_stage") if current else None
                else:
                    current = get_person_state(session=_db, person_id=entity_uuid)
                    current_from_state = current.get("lifecycle_state") if current else None

                if current is None:
                    _log.error(
                        "FA Max state transition on relay approval: entity not found "
                        "item=%d entity_type=%s entity_uuid=%s",
                        item.id, entity_type, entity_uuid,
                    )
                    raise ValueError("transition entity not found")

                if current_from_state != spec.get("from_state"):
                    _log.warning(
                        "FA Max state transition on relay approval: stale from_state "
                        "in payload (expected %r, actual %r) — proceeding with actual "
                        "current state item=%d entity=%s",
                        spec.get("from_state"), current_from_state, item.id, entity_uuid,
                    )

                # person_id must be stamped on every event row regardless of
                # entity_type (person or opportunity) — it is the borrower-
                # history partition key get_person_history() reads. Both
                # get_person_state() and get_opportunity_state() return
                # "person_id" in their dict for exactly this reason. Omitting
                # it here would silently make this transition invisible to
                # the borrower's history — no error, just a missing row.
                person_id_for_event = current.get("person_id")

                registry_entity_uuid = ensure_entity_registry(
                    session=_db, entity_type=entity_type, native_id=entity_uuid,
                )
                result = transition(
                    entity_type=entity_type,
                    entity_uuid=registry_entity_uuid,
                    from_state=current_from_state,
                    to_state=spec["to_state"],
                    actor=f"slack_approver:{user_id}",
                    source_component="src.api.admin_router",
                    idempotency_key=spec.get("idempotency_key"),
                    state_version=current.get("state_version"),
                    decision_id=spec.get("decision_id"),
                    person_id=person_id_for_event,
                    session=_db,
                )
                if result.outcome not in (
                    TransitionOutcome.succeeded, TransitionOutcome.idempotent_skip,
                ):
                    raise ValueError(f"state transition refused: {result.outcome.value}")
                item = relay_queue.record_decision(
                    item_id, approved=True, decided_by=user_id, session=_db,
                    expected_revision_count=posted_revision_count,
                )
                if item is None:
                    raise ValueError("queue item is no longer pending (or was revised after this card was posted)")
        except Exception as exc:
            _log.error(
                "FA Max state transition failed on relay approval item=%d: %s",
                item_id, exc,
            )
            return _slack_ephemeral(
                f"Approval held: required state transition failed for item #{item_id}."
            )
    else:
        item = relay_queue.record_decision(
            item_id, approved=(action == "approve"), decided_by=user_id,
            expected_revision_count=posted_revision_count,
        )
        if item is None:
            return _slack_ephemeral(
                f"Item #{item_id} was already decided, or was revised after this "
                "card was posted (not still pending at the expected revision)."
            )

    reply_text = (
        f":white_check_mark: Approved by <@{user_id}>."
        if action == "approve"
        else f":no_entry: Rejected by <@{user_id}>."
    )
    if item.lane:
        reply_text += f"  Lane: `{item.lane}`"
    if item.slack_message_ts:
        _update_relay_slack_message(item, reply_text)

    return {"ok": True}


def _relay_action_item_id(payload: dict) -> tuple[Optional[int], dict]:
    """Shared parse: pull item_id + the raw action_data dict off a
    block_actions payload's first action value. Same shape as
    _handle_relay_decision's own parsing, factored out so Skip/Snooze/
    Revise-open don't each re-derive it slightly differently."""
    actions = payload.get("actions", [])
    if not actions:
        return None, {}
    try:
        action_data = json.loads(actions[0].get("value", "{}"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid action value")
    return action_data.get("item_id"), action_data


def _handle_relay_skip(payload: dict) -> dict:
    """Slack Skip button (WP-T2-2 item 9).

    Calls the existing relay.queue.mark_skipped() exactly as built — it is
    NOT rebuilt here. mark_skipped() accepts a row in status 'approved' OR
    'pending' (widened during WP-T2-2 review specifically so this button
    works — see mark_skipped()'s own docstring for why 'pending' is safe to
    add there without reopening the RELAY-v2.2 double-send bug its guard
    exists for). A False return here means the row had already left
    'pending'/'approved' by the time this click landed (already decided,
    dispatched, or reclaimed) — a genuine stale-click case, not a routine
    outcome.
    """
    from src.services.relay import queue as relay_queue

    user_id = payload.get("user", {}).get("id", "")
    item_id, _ = _relay_action_item_id(payload)
    if not item_id:
        raise HTTPException(status_code=400, detail="Invalid action data")

    existing = relay_queue.get_item(item_id)
    venture_key = existing.venture_key if existing is not None else DEFAULT_VENTURE_KEY
    if not _relay_approver_authorized(user_id, venture_key):
        return _slack_ephemeral("Not authorized to decide Relay sends.")
    if existing is None:
        return _slack_ephemeral(f"Item #{item_id} not found.")

    ok = relay_queue.mark_skipped(item_id, f"slack_skip:{user_id}")
    if not ok:
        return _slack_ephemeral(
            f"Item #{item_id} could not be skipped — it may already be decided, "
            "dispatched, or reclaimed by a concurrent action."
        )
    item = relay_queue.get_item(item_id)
    if item and item.slack_message_ts:
        _update_relay_slack_message(item, f":fast_forward: Skipped by <@{user_id}>.")
    return {"ok": True}


def _handle_relay_snooze(payload: dict) -> dict:
    """Slack Snooze button (WP-T2-2 item 9) — defers a pending card via the
    existing relay.queue.snooze_item() helper."""
    from src.services.relay import queue as relay_queue

    user_id = payload.get("user", {}).get("id", "")
    item_id, _ = _relay_action_item_id(payload)
    if not item_id:
        raise HTTPException(status_code=400, detail="Invalid action data")

    existing = relay_queue.get_item(item_id)
    venture_key = existing.venture_key if existing is not None else DEFAULT_VENTURE_KEY
    if not _relay_approver_authorized(user_id, venture_key):
        return _slack_ephemeral("Not authorized to decide Relay sends.")
    if existing is None:
        return _slack_ephemeral(f"Item #{item_id} not found.")

    ok = relay_queue.snooze_item(item_id)
    if not ok:
        return _slack_ephemeral(f"Item #{item_id} could not be snoozed (not pending).")
    item = relay_queue.get_item(item_id)
    if item and item.slack_message_ts:
        _update_relay_slack_message(item, f":clock3: Snoozed by <@{user_id}> — will re-surface later.")
    return {"ok": True}


def _handle_relay_revise_open(payload: dict) -> dict:
    """Slack Revise button (WP-T2-2 item 9) — opens the revise modal
    (src.services.relay.slack_post.open_revise_modal). No existing
    free-text-capture Slack primitive covered this, so a modal +
    view_submission is the new mechanism (see that function's docstring)."""
    from src.services.relay import queue as relay_queue
    from src.services.relay.slack_post import open_revise_modal

    user_id = payload.get("user", {}).get("id", "")
    item_id, _ = _relay_action_item_id(payload)
    if not item_id:
        raise HTTPException(status_code=400, detail="Invalid action data")

    existing = relay_queue.get_item(item_id)
    venture_key = existing.venture_key if existing is not None else DEFAULT_VENTURE_KEY
    if not _relay_approver_authorized(user_id, venture_key):
        return _slack_ephemeral("Not authorized to decide Relay sends.")
    if existing is None or existing.status != "pending":
        return _slack_ephemeral(f"Item #{item_id} is not open for revision.")

    open_revise_modal(payload.get("trigger_id", ""), existing)
    return {}


def _is_material_edit(old_text: str, new_text: str) -> bool:
    """Normalized-token-diff used to compute material_edit for a Slack
    revision. record_revision() (src.services.relay.queue) only PERSISTS
    whatever material_edit value it is given — its body is a plain UPDATE,
    it does not compute one — so this is the computation, done once here
    at the single caller rather than inside that shared helper.

    A change is "material" when more than 15% of the union of the two
    texts' lowercased word tokens differ (symmetric difference / union).
    Threshold chosen to catch a rewritten sentence or changed number while
    ignoring whitespace/punctuation-only edits.
    """
    import re as _re

    def _tokens(text: str) -> set:
        return set(_re.findall(r"\w+", (text or "").lower()))

    old_tokens = _tokens(old_text)
    new_tokens = _tokens(new_text)
    union = old_tokens | new_tokens
    if not union:
        return False
    diff = old_tokens.symmetric_difference(new_tokens)
    return (len(diff) / len(union)) > 0.15


def _post_relay_thread_note(item, text: str) -> None:
    """Post a threaded reply under a Relay card without touching its
    buttons — unlike _update_relay_slack_message (which replaces the whole
    message and is reserved for a terminal decision), a revision must leave
    Approve/Reject/Skip/Snooze/Revise live on the original card."""
    from src.services.relay.slack_post import _resolve_bot_token, _resolve_channel

    token = _resolve_bot_token(item, settings)
    channel = _resolve_channel(item, settings)
    if not token or not channel or not item.slack_message_ts:
        return
    try:
        from slack_sdk import WebClient
        WebClient(token=token.get_secret_value()).chat_postMessage(
            channel=channel, thread_ts=item.slack_message_ts, text=text,
        )
    except Exception as exc:
        logger.error("[RelayInteract] thread note post failed for item %d: %s", item.id, exc)


def _handle_relay_revise_submission(payload: dict) -> dict:
    """Slack Revise modal submission (WP-T2-2 item 9).

    Computes material_edit (see _is_material_edit) and calls the existing
    relay.queue.record_revision() helper with it. Posts the new content as
    a thread reply under the original card rather than replacing the card,
    since the item is still pending a decision.
    """
    from src.services.relay import queue as relay_queue

    user_id = payload.get("user", {}).get("id", "")
    view = payload.get("view", {})
    try:
        metadata = json.loads(view.get("private_metadata", "{}"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid view metadata")
    item_id = metadata.get("item_id")
    if not item_id:
        raise HTTPException(status_code=400, detail="Invalid view metadata")

    existing = relay_queue.get_item(item_id)
    venture_key = existing.venture_key if existing is not None else DEFAULT_VENTURE_KEY
    if not _relay_approver_authorized(user_id, venture_key):
        return {"response_action": "errors", "errors": {"revised_content_block": "Not authorized."}}
    if existing is None or existing.status != "pending":
        return {"response_action": "errors", "errors": {"revised_content_block": "Item is no longer pending."}}

    try:
        new_content = (
            view["state"]["values"]["revised_content_block"]["revised_content"]["value"] or ""
        )
    except Exception:
        return {"response_action": "errors", "errors": {"revised_content_block": "Missing revised content."}}

    # Baseline is ALWAYS the original draft, never the previous revision
    # (WP-T2-2 review fix): comparing each edit only to its immediate
    # predecessor lets a sequence of individually-small revisions add up to
    # a large overall rewrite without ever crossing the material-edit
    # threshold. material_edit is also sticky (OR'd with its current value)
    # so a later small, non-material tweak can never un-flag an item a
    # prior revision already made material -- this flag feeds the Tier B
    # graduation edit-rate gate, where under-counting edits is the unsafe
    # direction.
    baseline = existing.original_draft or ""
    material = bool(existing.material_edit) or _is_material_edit(baseline, new_content)

    item = relay_queue.record_revision(
        item_id, final_content=new_content, revised_by=f"slack:{user_id}", material_edit=material,
    )
    if item is None:
        return {"response_action": "errors", "errors": {"revised_content_block": "Item is no longer pending."}}

    _post_relay_thread_note(
        item,
        f":pencil2: Revised by <@{user_id}> (revision #{item.revision_count}"
        f"{', material change' if material else ''}):\n{new_content[:2900]}",
    )
    # WP-T2-2 review fix: the original card's Approve button was posted with
    # revision_count_at_post baked in from BEFORE this revision, so without
    # refreshing it, the stale-card guard in _handle_relay_decision would
    # refuse that button FOREVER after even one revision -- there would be
    # no working Approve path left for this item via Slack. Rebuilding the
    # card in place gives it fresh buttons whose baked-in revision_count
    # matches the row this revision just produced.
    from src.services.relay.slack_post import refresh_card_after_revision

    if not refresh_card_after_revision(item):
        return {"response_action": "errors", "errors": {
            "revised_content_block": "Revision saved, but Slack could not refresh the approval card. Reopen Revise and submit again before approval."
        }}
    return {"response_action": "clear"}


def _handle_log_submission_new_borrower_click(payload: dict) -> dict:
    """"Not on this list — new borrower" button inside the log-submission
    modal — swaps the view in place via views.update, preserving whatever
    Josh already entered (e.g. a partial search) in private_metadata."""
    from src.services.relay.slack_post import open_log_submission_new_entry_view

    view = payload.get("view", {})
    open_log_submission_new_entry_view(
        view.get("id", ""),
        view.get("hash", ""),
        json.loads(view.get("private_metadata") or "{}"),
    )
    return {}


def _log_submission_field(values: dict, block_id: str, action_id: str) -> str:
    block = values.get(block_id, {}).get(action_id, {})
    # `selected_option` arrives as an explicit JSON null (not a missing
    # key) for an optional select with nothing chosen, so the default
    # from .get() is never reached — coalesce the null itself.
    return (block.get("value") or (block.get("selected_option") or {}).get("value") or "").strip()


def _log_submission_pre_validate(payload: dict) -> Optional[dict]:
    """Fast, DB-free validation for the log-submission modal submit --
    both of _handle_log_submission_view_submit's only two error-returning
    checks, factored out so the Socket Mode listener
    (src/services/relay/socket_listener.py) can run them BEFORE acking.
    The DB work in _handle_log_submission_view_submit below can take
    several seconds against this dev DB's network latency -- past
    Slack's ~3s Socket Mode ack window -- so that listener acks
    immediately once these two (in-memory only) checks pass, then does
    the actual DB work afterward. There is no response_action channel
    left post-ack, so these are the only validation this modal can ever
    show inline once Socket Mode is the delivery mechanism.
    """
    view = payload["view"]
    metadata = json.loads(view.get("private_metadata") or "{}")
    values = view["state"]["values"]

    if metadata.get("mode") == "new_borrower":
        if not _log_submission_field(values, "new_full_name_block", "new_full_name"):
            return {
                "response_action": "errors",
                "errors": {"new_full_name_block": "Borrower name is required."},
            }
    else:
        if not _log_submission_field(values, "borrower_search_block", "borrower_search"):
            return {
                "response_action": "errors",
                "errors": {"borrower_search_block": "Select a borrower or choose \"new borrower\"."},
            }
    return None


def _handle_log_submission_view_submit(payload: dict, db: Session) -> dict:
    """Slack `/fa-max-log-submission` modal submission (WP-T2-6 addendum,
    Task 19). This is the moment Josh tells the system he already submitted
    a deal to Backflip -- it creates/links the borrower, creates a new
    opportunity, jumps it straight to 'submitted' via the admin-override
    transition path (the deal already happened outside our normal funnel),
    and records backflip_ref if given.

    person_id for the "existing borrower" path is read from
    view.state.values, not private_metadata: the modal's borrower_search
    external_select has no dispatch_action, so Slack only reports the
    selection at submission time, inside `values` -- exactly like every
    other select block in this modal (e.g. opportunity_type_block).
    """
    from src.services import fa_max_file_state, state_engine
    from src.services.phone_utils import normalize as normalize_phone

    pre_validation_error = _log_submission_pre_validate(payload)
    if pre_validation_error is not None:
        return pre_validation_error

    view = payload["view"]
    metadata = json.loads(view.get("private_metadata") or "{}")
    values = view["state"]["values"]

    def _field(block_id: str, action_id: str) -> str:
        return _log_submission_field(values, block_id, action_id)

    if metadata.get("mode") == "new_borrower":
        full_name = _field("new_full_name_block", "new_full_name")
        email = _field("new_email_block", "new_email") or None
        phone = normalize_phone(_field("new_phone_block", "new_phone") or None)
        person_row = db.execute(
            text(
                "INSERT INTO fa_max_persons (source, full_name, email, phone) "
                "VALUES ('manual_submission_modal', :name, :email, :phone) "
                "RETURNING person_id"
            ),
            {"name": full_name, "email": email, "phone": phone},
        ).fetchone()
        db.commit()
        person_id = str(person_row.person_id)
    else:
        person_id = _field("borrower_search_block", "borrower_search")
        full_name = db.execute(
            text("SELECT full_name FROM fa_max_persons WHERE person_id = :pid ::uuid"),
            {"pid": person_id},
        ).scalar() or "borrower"

    opportunity_type = _field("opportunity_type_block", "opportunity_type")
    opportunity_id = state_engine.create_fa_max_opportunity(
        session=db, person_id=person_id, opportunity_type=opportunity_type,
        source="manual_submission_modal",
    )

    user_id = payload.get("user", {}).get("id", "unknown")
    # transition() requires a real fa_max_entity_registry entity_uuid, not
    # the opportunity's own native ID (WP-T2-6 review fix -- confirmed
    # live: every existing call site in this codebase passed the native ID
    # directly, which transition()'s registry lookup never matched since
    # nothing had registered it, so the transition silently no-op'd).
    entity_uuid = state_engine.ensure_entity_registry(
        session=db, entity_type="opportunity", native_id=opportunity_id,
    )
    state_engine.transition(
        session=db, entity_type="opportunity", entity_uuid=entity_uuid,
        from_state="new", to_state="submitted",
        actor="user:josh", source_component="src.api.admin_router",
        idempotency_key=f"log_submission:{opportunity_id}:submitted",
        validate_allowed_next=False,
        context={"reason": "manual log of an already-completed Backflip submission via Slack modal",
                 "recorded_by_slack_user": user_id},
    )

    fa_max_file_state.ensure_file_state(db, opportunity_id=opportunity_id, person_id=person_id)

    # Deliberately NOT fa_max_file_state.record_terms(): that function also
    # transitions submitted -> term_sheet whenever current_stage is already
    # 'submitted' (it's designed for a genuine terms-received event). This
    # block just records a reference Josh already knows at submission time
    # -- no underwriting has happened yet, so the stage must not move.
    backflip_ref = _field("backflip_ref_block", "backflip_ref") or None
    # new_property_address_block lives in _deal_detail_blocks() -- present
    # on both views, since a repeat existing borrower's new loan can be
    # for a different property than any of their prior ones.
    property_address = _field("new_property_address_block", "new_property_address") or None
    # loan_amount_block was read from Slack and then silently discarded --
    # never written anywhere (WP-T2-6 review fix). Dollars-to-cents
    # conversion matches record_terms()'s own established convention.
    # Josh may type digits with commas/a "$" prefix; anything else
    # unparseable is dropped rather than guessed at or crashing the
    # submission over a formatting slip.
    loan_amount_cents = None
    loan_amount_raw = _field("loan_amount_block", "loan_amount")
    if loan_amount_raw:
        try:
            loan_amount_cents = round(float(loan_amount_raw.replace(",", "").replace("$", "")) * 100)
        except ValueError:
            logger.warning(
                "log-submission: unparseable loan_amount %r for opportunity_id=%s -- dropped",
                loan_amount_raw, opportunity_id,
            )
    if backflip_ref or property_address or loan_amount_cents is not None:
        db.execute(
            text(
                "UPDATE fa_max_opportunities SET "
                "backflip_ref = COALESCE(:backflip_ref, backflip_ref), "
                "property_address = COALESCE(:property_address, property_address), "
                "loan_amount_cents = COALESCE(:loan_amount_cents, loan_amount_cents), "
                "updated_at = NOW() WHERE opportunity_id = :opportunity_id ::uuid"
            ),
            {
                "backflip_ref": backflip_ref, "property_address": property_address,
                "loan_amount_cents": loan_amount_cents, "opportunity_id": opportunity_id,
            },
        )
        db.commit()

    from src.services.relay.slack_post import post_log_submission_confirmation

    post_log_submission_confirmation(metadata.get("channel_id", ""), full_name, backflip_ref)
    return {}


# ===========================================================================
# FA MAX AGENT TASK DISPATCH (WP-T2-2 review fix)
#
# Manual, admin-JWT-gated producer for fa_max_work_queue(queue_name=
# 'fa_max_agent') -- src.agents.fa_max.worker.FaMaxWorker has no other
# production caller as of WP-T2-2 (see that module's docstring and
# docs/PLATFORM-OPERATIONS-GUIDE.md's FA Max agent worker section). This
# endpoint does NOT decide *when* Cora should act -- it is Josh (or another
# admin) explicitly choosing to dispatch a bounded task, mirroring the
# existing admin-upload precedent (POST /api/admin/upload/tax-delinquency)
# for "a human action is today's real production trigger." An automatic
# event-driven producer (which event, which graph node) is a separate,
# not-yet-built work package's decision.
# ===========================================================================

class FaMaxAgentTaskRequest(BaseModel):
    person_id: str
    agent_name: str
    steps: List[Dict[str, Any]] = Field(default_factory=list)
    task_description: Optional[str] = None
    context: Dict[str, Any] = Field(default_factory=dict)
    idempotency_key: Optional[str] = None


class FaMaxOpportunityFromSendRequest(BaseModel):
    relay_item_id: int
    opportunity_type: str


class FaMaxOpportunityAdvanceRequest(BaseModel):
    to_state: str = Field(min_length=1)
    expected_version: int = Field(ge=0)
    idempotency_key: str = Field(min_length=1)


@router.post("/fa-max/opportunities/from-send")
def create_fa_max_opportunity_from_send(
    body: FaMaxOpportunityFromSendRequest,
    _admin: dict = Depends(get_current_admin),
):
    """Record an opportunity from the specific sent Tier C interaction."""
    from src.services.state_engine import create_opportunity_from_relay_send

    if body.relay_item_id <= 0 or body.opportunity_type not in {
        "acquisition", "rehab", "construction", "extension", "refinance",
        "dscr_takeout", "repeat",
    }:
        raise HTTPException(status_code=400, detail="Invalid Relay item or opportunity type")
    with get_db_context() as session:
        try:
            opportunity_id = create_opportunity_from_relay_send(
                session=session, relay_item_id=body.relay_item_id,
                opportunity_type=body.opportunity_type,
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"opportunity_id": opportunity_id}


@router.post("/fa-max/opportunities/{opportunity_id}/advance")
def advance_fa_max_opportunity(
    opportunity_id: str, body: FaMaxOpportunityAdvanceRequest,
    _admin: dict = Depends(get_current_admin),
):
    """Advance one opportunity through configured stages with CAS and audit."""
    from src.services.state_engine import (
        ensure_entity_registry, get_opportunity_state, transition, TransitionOutcome,
    )

    if body.to_state == "funded":
        raise HTTPException(status_code=400, detail="Use the funded endpoint")
    with get_db_context() as session:
        current = get_opportunity_state(session=session, opportunity_id=opportunity_id)
        if not current:
            raise HTTPException(status_code=404, detail="Opportunity not found")
        if current["state_version"] != body.expected_version:
            raise HTTPException(status_code=409, detail="Opportunity version changed")
        # transition() requires a real fa_max_entity_registry entity_uuid,
        # not the opportunity's own native ID (WP-T2-6 review fix).
        entity_uuid = ensure_entity_registry(
            session=session, entity_type="opportunity", native_id=opportunity_id,
        )
        result = transition(
            session=session, entity_type="opportunity", entity_uuid=entity_uuid,
            from_state=current["current_stage"], to_state=body.to_state,
            actor="admin:fa_max_opportunity", source_component="src.api.admin_router",
            idempotency_key=body.idempotency_key, state_version=body.expected_version,
        )
        if result.outcome not in (TransitionOutcome.succeeded, TransitionOutcome.idempotent_skip):
            raise HTTPException(status_code=409, detail=f"Transition refused: {result.outcome.value}")
    return {"opportunity_id": opportunity_id, "stage": result.current_state}


@router.post("/fa-max/opportunities/{opportunity_id}/funded")
def mark_fa_max_opportunity_funded(
    opportunity_id: str,
    _admin: dict = Depends(get_current_admin),
):
    """Record a verified closing -> funded event in the FA Max ledger."""
    from src.services.state_engine import mark_opportunity_funded, TransitionOutcome

    with get_db_context() as session:
        result = mark_opportunity_funded(
            session=session, opportunity_id=opportunity_id,
            actor="admin:fa_max_funding", idempotency_key=f"funded:{opportunity_id}",
        )
        if result.outcome not in (TransitionOutcome.succeeded, TransitionOutcome.idempotent_skip):
            raise HTTPException(status_code=409, detail=f"Funding transition refused: {result.outcome.value}")
    return {"opportunity_id": opportunity_id, "outcome": "funded"}


@router.post("/fa-max/agent-tasks")
def create_fa_max_agent_task(
    body: FaMaxAgentTaskRequest,
    _admin: dict = Depends(get_current_admin),
):
    """Enqueue one bounded tool-call task for the FA Max agent worker.

    Validates every step's tool name against FA_MAX_TOOL_REGISTRY before
    enqueueing -- a typo'd tool name should fail this request with a 400,
    not surface as an 'unknown_tool' error deep in the worker's audit log
    after the item was already claimed.
    """
    from src.agents.fa_max.tool_registry import FA_MAX_TOOL_REGISTRY, select_task_tools
    from src.agents.fa_max.worker import FA_MAX_QUEUE_NAME
    from src.services.state_engine import enqueue_work_item

    if bool(body.steps) == bool(body.task_description):
        raise HTTPException(status_code=400, detail="Provide either steps or task_description")
    if body.task_description:
        try:
            selected = select_task_tools(body.task_description, body.context)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if selected[0]["tool"] == "send" and body.context.get("agent_name") != body.agent_name:
            raise HTTPException(status_code=400, detail="send agent_name must match task agent_name")
    unknown_tools = sorted({
        step.get("tool") for step in body.steps if step.get("tool") not in FA_MAX_TOOL_REGISTRY
    })
    if unknown_tools:
        raise HTTPException(status_code=400, detail=f"Unknown tool name(s): {unknown_tools}")

    with get_db_context() as session:
        work_item_id = enqueue_work_item(
            session=session,
            queue_name=FA_MAX_QUEUE_NAME,
            payload={"agent_name": body.agent_name, "steps": body.steps,
                     "task_description": body.task_description, "context": body.context},
            idempotency_key=body.idempotency_key,
            person_id=body.person_id,
        )
        session.commit()

    if work_item_id is None:
        return {"ok": True, "work_item_id": None, "note": "idempotency_key already queued — no duplicate created"}
    return {"ok": True, "work_item_id": work_item_id}


# ===========================================================================
# THROUGH-v2.2 — BATCH-APPROVAL SLACK INTERACTION (T1)
# ===========================================================================

def _through_approver_authorized(user_id: str) -> bool:
    """Fail CLOSED, same reasoning as _relay_approver_authorized: an empty/
    unset CORA_THROUGHPUT_APPROVERS means nobody is authorized, not
    everybody."""
    approvers = settings.cora_throughput_approvers
    return bool(approvers) and user_id in approvers


def _bg_batch_decision(batch_id: str, action: str, user_id: str, draft_id: Optional[str]) -> None:
    """Background task: run the actual batch decision + Slack update after the endpoint has ACKed Slack."""
    from src.services.cora_throughput import batch_slack as through_slack
    from src.services.cora_throughput.decisions import record_batch_decision

    with get_db_context() as bg_db:
        result = record_batch_decision(bg_db, batch_id, action, decided_by=user_id, draft_id=draft_id)
        bg_db.commit()
        if not result.get("ok"):
            logger.warning("[Through] bg batch decision %s/%s failed: %s", batch_id[:8], action, result.get("reason"))
            return

        slack_message_ts = bg_db.execute(
            text("SELECT slack_message_ts FROM cora_draft_batches WHERE batch_id = :batch_id"),
            {"batch_id": batch_id},
        ).scalar()

        if action == "reject_item":
            active = bg_db.execute(
                text(
                    "SELECT d.draft_id, d.opportunity_thread_id, d.cell_id, d.recommended_channel, "
                    "d.subject, d.body, d.contact_email "
                    "FROM cora_batch_items bi JOIN outbound_drafts d ON d.draft_id = bi.draft_id "
                    "WHERE bi.batch_id = :batch_id AND bi.decision = 'included' ORDER BY bi.id ASC"
                ),
                {"batch_id": batch_id},
            ).mappings().all()
            rejected_ids = [
                r[0] for r in bg_db.execute(
                    text(
                        "SELECT draft_id FROM cora_batch_items "
                        "WHERE batch_id = :batch_id AND decision = 'exception_rejected'"
                    ),
                    {"batch_id": batch_id},
                ).all()
            ]
            if slack_message_ts:
                through_slack.refresh_batch_card(slack_message_ts, batch_id, [dict(d) for d in active], rejected_ids)
        elif action == "reject_batch":
            reply_text = (
                f":no_entry: Batch rejected by <@{user_id}> — "
                f"{result['rejected_count']} draft(s) rejected."
            )
            if slack_message_ts:
                through_slack.update_batch_slack_message(slack_message_ts, reply_text)
        else:
            reply_text = (
                f":white_check_mark: Batch approved by <@{user_id}> — "
                f"{result['approved_count']} sent to Relay, {result['rejected_count']} exception-rejected."
            )
            if slack_message_ts:
                through_slack.update_batch_slack_message(slack_message_ts, reply_text)


def _handle_cora_batch_interact(payload: dict, db: Session, background_tasks: BackgroundTasks) -> dict:
    from src.services.cora_throughput import batch_slack as through_slack
    from src.services.cora_throughput.decisions import record_standing_order_decision

    user_id = payload.get("user", {}).get("id", "")
    if not _through_approver_authorized(user_id):
        return _slack_ephemeral("Not authorized to approve Cora batches.")

    actions = payload.get("actions", [])
    if not actions:
        return _slack_ephemeral("No action found in payload.")

    try:
        action_data = json.loads(actions[0].get("value", "{}"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid action value")

    action = action_data.get("action")

    if action == "keep_standing_order":
        return {"ok": True}

    if action in ("ratify_standing_order", "decline_standing_order", "archive_standing_order"):
        standing_order_id = action_data.get("standing_order_id")
        if not standing_order_id:
            raise HTTPException(status_code=400, detail="Invalid action data")
        result = record_standing_order_decision(db, standing_order_id, action, decided_by=user_id)
        db.commit()
        if not result.get("ok"):
            return _slack_ephemeral(f"Standing order #{standing_order_id}: {result.get('reason', 'could not be decided')}.")
        reply_text = {
            "ratify_standing_order": f":white_check_mark: Standing order ratified by <@{user_id}>.",
            "decline_standing_order": f":no_entry: Standing order declined by <@{user_id}>.",
            "archive_standing_order": f":wastebasket: Standing order archived by <@{user_id}>.",
        }[action]
        if result.get("slack_message_ts"):
            through_slack.update_batch_slack_message(result["slack_message_ts"], reply_text)
        return {"ok": True}

    if action == "view_draft":
        draft_id = action_data.get("draft_id")
        if not draft_id:
            raise HTTPException(status_code=400, detail="Invalid action data")
        row = db.execute(
            text("SELECT draft_id, subject, body, contact_email, opportunity_thread_id FROM outbound_drafts WHERE draft_id = :id"),
            {"id": draft_id},
        ).mappings().first()
        if not row:
            return _slack_ephemeral(f"Draft {draft_id[:8]} not found.")
        through_slack.open_draft_modal(payload.get("trigger_id", ""), dict(row))
        return {"ok": True}

    batch_id = action_data.get("batch_id")
    draft_id = action_data.get("draft_id")
    if not batch_id or action not in ("approve_all", "reject_batch", "reject_item"):
        raise HTTPException(status_code=400, detail="Invalid action data")

    # Quick idempotency check — give instant feedback on stale/decided batches.
    batch_status = db.execute(
        text("SELECT status FROM cora_draft_batches WHERE batch_id = :batch_id"),
        {"batch_id": batch_id},
    ).scalar()
    if batch_status is None:
        return _slack_ephemeral(f"Batch {batch_id[:8]}: not found.")
    if batch_status != "pending":
        return _slack_ephemeral(f"Batch {batch_id[:8]}: already {batch_status}.")

    # ACK Slack within the 3-second window; relay enqueue + Slack update run in background.
    background_tasks.add_task(_bg_batch_decision, batch_id, action, user_id, draft_id)
    return {"ok": True}


@router.post("/slack/cora-batch/interact")
async def slack_cora_batch_interact(request: Request, db: Session = Depends(get_db)):
    """
    Receives Slack interactive component payloads for THROUGH-v2.2's batch
    review ("Approve Batch" / per-item "Reject") and standing-order
    proposals ("Ratify" / "Decline") — two different action families on the
    same endpoint, distinguished by whether the button's value carries a
    batch_id or a standing_order_id.
    Auth: Slack HMAC-SHA256 signature (no JWT — Slack signature IS the auth).
    """
    from src.services.cora_throughput import batch_slack as through_slack
    from src.services.cora_throughput.decisions import record_batch_decision, record_standing_order_decision

    raw = await request.body()
    if not _verify_slack_signature(dict(request.headers), raw):
        raise HTTPException(status_code=401, detail="Invalid Slack signature")

    try:
        payload_str = parse_qs(raw.decode("utf-8")).get("payload", ["{}"])[0]
        payload = json.loads(payload_str)
    except Exception:
        raise HTTPException(status_code=400, detail="Malformed payload")

    user_id = payload.get("user", {}).get("id", "")
    if not _through_approver_authorized(user_id):
        return _slack_ephemeral("Not authorized to approve Cora batches.")

    actions = payload.get("actions", [])
    if not actions:
        return _slack_ephemeral("No action found in payload.")

    try:
        action_data = json.loads(actions[0].get("value", "{}"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid action value")

    action = action_data.get("action")

    if action == "keep_standing_order":
        # Monthly-digest "Keep" button — inaction means keep. Ack only.
        return {"ok": True}

    if action in ("ratify_standing_order", "decline_standing_order", "archive_standing_order"):
        standing_order_id = action_data.get("standing_order_id")
        if not standing_order_id:
            raise HTTPException(status_code=400, detail="Invalid action data")
        result = record_standing_order_decision(db, standing_order_id, action, decided_by=user_id)
        db.commit()
        if not result.get("ok"):
            return _slack_ephemeral(f"Standing order #{standing_order_id}: {result.get('reason', 'could not be decided')}.")
        reply_text = {
            "ratify_standing_order": f":white_check_mark: Standing order ratified by <@{user_id}>.",
            "decline_standing_order": f":no_entry: Standing order declined by <@{user_id}>.",
            "archive_standing_order": f":wastebasket: Standing order archived by <@{user_id}>.",
        }[action]
        if result.get("slack_message_ts"):
            through_slack.update_batch_slack_message(result["slack_message_ts"], reply_text)
        return {"ok": True}

    batch_id = action_data.get("batch_id")
    draft_id = action_data.get("draft_id")
    if not batch_id or action not in ("approve_all", "reject_item"):
        raise HTTPException(status_code=400, detail="Invalid action data")

    result = record_batch_decision(db, batch_id, action, decided_by=user_id, draft_id=draft_id)
    db.commit()

    if not result.get("ok"):
        return _slack_ephemeral(f"Batch {batch_id[:8]}: {result.get('reason', 'could not be decided')}.")

    slack_message_ts_row = db.execute(
        text("SELECT slack_message_ts FROM cora_draft_batches WHERE batch_id = :batch_id"),
        {"batch_id": batch_id},
    ).first()
    slack_message_ts = slack_message_ts_row[0] if slack_message_ts_row else None

    if action == "approve_all":
        reply_text = (
            f":white_check_mark: Batch approved by <@{user_id}> — "
            f"{result['approved_count']} sent to Relay, {result['rejected_count']} exception-rejected."
        )
    else:
        reply_text = f":no_entry: Draft `{draft_id[:8]}` exception-rejected by <@{user_id}> — rest of the batch still open."

    if slack_message_ts:
        through_slack.update_batch_slack_message(slack_message_ts, reply_text)

    return {"ok": True}


# ===========================================================================
# THROUGH-v2.2 — QUEUE VISIBILITY (read-only — all decisions happen in Slack)
# ===========================================================================

@router.get("/cora-batches")
def get_cora_batches(
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Read-only visibility into THROUGH-v2.2's batch queue and standing orders
    — approve/reject/ratify/decline only ever happen via the Slack tap
    (POST /admin/slack/cora-batch/interact). This endpoint has no mutation
    path, deliberately: the founder-facing "one tap" flow stays in Slack.
    """
    batches = db.execute(
        text(
            """
            SELECT b.batch_id, b.status, b.created_at, b.decided_by, b.decided_at,
                   (SELECT count(*) FROM cora_batch_items WHERE cora_batch_items.batch_id = b.batch_id) AS item_count,
                   (SELECT count(*) FROM cora_batch_items
                    WHERE cora_batch_items.batch_id = b.batch_id AND cora_batch_items.decision = 'exception_rejected') AS rejected_count
            FROM cora_draft_batches b
            ORDER BY b.created_at DESC
            LIMIT 20
            """
        )
    ).mappings().all()

    standing_orders = db.execute(
        text(
            "SELECT id, cell_id, rule_text, active, created_by, created_at "
            "FROM cora_standing_orders ORDER BY created_at DESC LIMIT 20"
        )
    ).mappings().all()

    return {
        "batches": [dict(r) for r in batches],
        "standing_orders": [dict(r) for r in standing_orders],
    }


# ===========================================================================
# THROUGH-v2.2 — CLOSING COCKPIT (T3)
# ===========================================================================

@router.get("/closing-cockpit/{opportunity_thread_id}")
def get_closing_cockpit(
    opportunity_thread_id: str,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    One-screen call-time reference: prospect identity, latest pre-call brief
    (objections/recommended offer/pricing/links — already built by Cora's
    pre_call subgraph, no new brief logic here), and best-effort call
    history. The SynthflowCall/CloserCall join is phone-based, not an FK —
    none exists linking either table back to opportunity_thread_id — so a
    miss here means "no match found," never an error.
    """
    from src.agents.cora import store
    from src.agents.cora.tools.read_tools import get_buyer_entity_by_opportunity_thread_id, get_contact_channel
    from src.services.phone_utils import normalize as normalize_phone

    buyer_entity = get_buyer_entity_by_opportunity_thread_id(db, opportunity_thread_id)
    if buyer_entity is None:
        raise HTTPException(status_code=404, detail="Opportunity thread not found")

    briefs = sorted(
        store.read_pre_call_briefs(opportunity_thread_id=opportunity_thread_id),
        key=lambda b: b.get("created_at") or "",
    )
    latest_brief = briefs[-1] if briefs else None

    contact = get_contact_channel(db, buyer_entity["id"])
    normalized_phone = normalize_phone(contact.get("phone"))

    synthflow_calls: List[Dict[str, Any]] = []
    closer_calls: List[Dict[str, Any]] = []
    if normalized_phone:
        synthflow_rows = db.execute(
            text(
                "SELECT id, outcome, vertical, call_date, duration_seconds, recording_url "
                "FROM synthflow_calls WHERE prospect_phone = :phone ORDER BY call_date DESC LIMIT 10"
            ),
            {"phone": normalized_phone},
        ).mappings().all()
        synthflow_calls = [dict(r) for r in synthflow_rows]

        closer_rows = db.execute(
            text(
                "SELECT id, closer_name, direction, started_at, ended_at, call_outcome, "
                "sentiment, objections, objection_resolved "
                "FROM closer_calls WHERE dialed_e164 = :phone ORDER BY started_at DESC LIMIT 10"
            ),
            {"phone": normalized_phone},
        ).mappings().all()
        closer_calls = [dict(r) for r in closer_rows]

    return {
        "opportunity_thread_id": opportunity_thread_id,
        "buyer_entity": buyer_entity,
        "contact": {"email": contact.get("email"), "phone": normalized_phone},
        "pre_call_brief": latest_brief.get("content") if latest_brief else None,
        "call_history_match": "phone" if normalized_phone else "none",
        "synthflow_calls": synthflow_calls,
        "closer_calls": closer_calls,
    }


_RELAY_KILL_TARGETS = ("ALL", "RELAY", "VERA", "HUNTER", "CORA")


def _kill_feature_for_target(target: str) -> str:
    return "global" if target == "ALL" else f"{target.lower()}_global"


@router.post("/slack/kill")
async def slack_kill_command(request: Request):
    """
    Slack slash command: '/relay-kill ALL' or '/relay-kill RELAY' (also
    accepts VERA/HUNTER/CORA, same <agent>_global convention). Sets the shared
    Redis kill-switch override that src.services.kill_switch_service reads
    fleet-wide — build spec §9.1: "the kill command instantly."

    Optional second token 'FOREVER' (e.g. '/relay-kill CORA FOREVER') sets
    the override with no TTL instead of the default auto-expiring one — it
    stays red until /slack/resume (the '/relay-resume' command) explicitly
    clears it. Without FOREVER, behavior is unchanged: auto-clears after
    KILL_OVERRIDE_TTL_SECONDS.

    Slash-command bodies are plain form-encoded (NOT wrapped in a "payload"
    field like interactive-component callbacks) — parsed directly here.
    """
    from src.core.redis_client import rset
    from src.services.relay.config import KILL_OVERRIDE_TTL_SECONDS

    raw = await request.body()
    if not _verify_slack_signature(dict(request.headers), raw):
        raise HTTPException(status_code=401, detail="Invalid Slack signature")

    form = parse_qs(raw.decode("utf-8"))

    # Slack slash commands carry the invoking user as a top-level 'user_id'
    # field (unlike interactive-component payloads, where it's nested under
    # payload.user.id) — reuses relay_approvers, the same allowlist that
    # gates /slack/relay-decision, since killing the fleet is at least as
    # consequential as approving one send.
    user_id = form.get("user_id", [""])[0]
    if not _relay_approver_authorized(user_id):
        return _slack_ephemeral("Not authorized to issue kill commands.")

    tokens = form.get("text", [""])[0].strip().upper().split()
    target = tokens[0] if tokens else ""
    forever = len(tokens) == 2 and tokens[1] == "FOREVER"
    if target not in _RELAY_KILL_TARGETS or not (len(tokens) == 1 or forever):
        return _slack_ephemeral(
            "Usage: /relay-kill ALL | RELAY | VERA | HUNTER | CORA [FOREVER]"
        )

    feature = _kill_feature_for_target(target)
    if not rset(
        f"kill_switch_override:{feature}", "red",
        ttl_seconds=None if forever else KILL_OVERRIDE_TTL_SECONDS,
    ):
        return _slack_ephemeral(
            f"⚠️ Failed to set kill switch for {target} — Redis unavailable. "
            "STOP did NOT take effect — retry immediately."
        )
    if forever:
        reply = f"\U0001F6D1 STOP {target} — kill switch RED until /relay-resume {target} is run."
    else:
        reply = (
            f"\U0001F6D1 STOP {target} — kill switch RED for "
            f"{KILL_OVERRIDE_TTL_SECONDS // 60} min. Auto-clears on expiry."
        )
    return _slack_ephemeral(reply)


@router.post("/slack/resume")
async def slack_resume_command(request: Request):
    """
    Slack slash command: '/relay-resume ALL' or '/relay-resume CORA' (also
    RELAY/VERA/HUNTER). Clears a manual kill-switch override set by
    /relay-kill — the only way to bring back a target killed with FOREVER,
    and also usable to end an in-progress auto-expiring kill early.
    """
    from src.core.redis_client import rdelete

    raw = await request.body()
    if not _verify_slack_signature(dict(request.headers), raw):
        raise HTTPException(status_code=401, detail="Invalid Slack signature")

    form = parse_qs(raw.decode("utf-8"))
    user_id = form.get("user_id", [""])[0]
    if not _relay_approver_authorized(user_id):
        return _slack_ephemeral("Not authorized to issue resume commands.")

    target = form.get("text", [""])[0].strip().upper()
    if target not in _RELAY_KILL_TARGETS:
        return _slack_ephemeral("Usage: /relay-resume ALL | RELAY | VERA | HUNTER | CORA")

    feature = _kill_feature_for_target(target)
    if not rdelete(f"kill_switch_override:{feature}"):
        return _slack_ephemeral(
            f"⚠️ Failed to clear kill switch for {target} — Redis unavailable. "
            "Override may still be active. Try again or clear it directly."
        )
    return _slack_ephemeral(f"✅ RESUME {target} — kill switch override cleared.")


def _fa_max_file_update_command(form: dict, db: Session) -> dict:
    """
    Pure logic for the Slack slash command (WP-T2-6):
        /fa-max-file-update <backflip_ref> <stage>
        /fa-max-file-update <backflip_ref> doc:<document name>
        /fa-max-file-update <backflip_ref> received:<document name>

    Stage tokens match config.fa_max_stage_monitoring.BACKFLIP_STAGE_KEYS
    exactly (snake_case: under_review, conditional_approval, docs_requested,
    cleared_to_close, funded, declined). A doc: prefix records a document
    request instead of a stage change; a received: prefix closes one out,
    stopping its chase timers before they escalate.

    Same authorization gate as /relay-kill — relay_approvers, since manually
    moving a file's stage/document state is at least as consequential.

    Takes an already-scalar form dict (not Slack's raw list-wrapped
    parse_qs shape) so both the HTTP Request-URL route below and the
    Socket Mode slash-command listener (src/services/relay/socket_listener.py)
    can share it. This app has no Interactivity/slash-command Request URL
    option once Socket Mode is enabled, so the HTTP route is reachable
    only on a dev/test app running with Socket Mode off — the Socket Mode
    listener is what actually serves this command in production.
    """
    from config.fa_max_stage_monitoring import BACKFLIP_STAGE_KEYS
    from src.agents.reply_concierge.backflip_stage_ingest import resolve_opportunity_by_backflip_ref
    from src.services import fa_max_file_state

    channel_rejection = _reject_if_wrong_command_channel(form.get("channel_id", ""))
    if channel_rejection is not None:
        return channel_rejection
    user_id = form.get("user_id", "")
    if not _relay_approver_authorized(user_id, "fa_max_lending"):
        return _slack_ephemeral("🚫 Not authorized to update FA Max file state.")

    tokens = (form.get("text") or "").strip().split(maxsplit=1)
    if len(tokens) != 2:
        return _slack_ephemeral(
            "Usage: /fa-max-file-update <backflip_ref> <stage> | "
            "/fa-max-file-update <backflip_ref> doc:<document name> | "
            "/fa-max-file-update <backflip_ref> received:<document name>"
        )
    backflip_ref, action = tokens[0], tokens[1].strip()

    resolved = resolve_opportunity_by_backflip_ref(db, backflip_ref)
    if resolved is None:
        return _slack_ephemeral(f"🔍 No opportunity found for `{backflip_ref}`.")

    if action.lower().startswith("received:"):
        document_name = action[len("received:"):].strip()
        if not document_name:
            return _slack_ephemeral(
                "Usage: /fa-max-file-update <backflip_ref> received:<document name>"
            )
        closed = fa_max_file_state.record_document_received(
            db, opportunity_id=resolved["opportunity_id"], document_name=document_name,
        )
        if not closed:
            return _slack_ephemeral(
                f"⚠️ No outstanding request named *{document_name}* for `{backflip_ref}`."
            )
        return _slack_ephemeral(
            f"✅ Marked *{document_name}* received for `{backflip_ref}` — chase stopped."
        )

    if action.lower().startswith("doc:"):
        document_name = action[len("doc:"):].strip()
        if not document_name:
            return _slack_ephemeral("Usage: /fa-max-file-update <backflip_ref> doc:<document name>")
        fa_max_file_state.ensure_file_state(
            db, opportunity_id=resolved["opportunity_id"], person_id=resolved["person_id"],
        )
        fa_max_file_state.record_document_request(
            db, opportunity_id=resolved["opportunity_id"], person_id=resolved["person_id"],
            document_name=document_name, source="manual",
            idempotency_key=f"docreq:{resolved['opportunity_id']}:{document_name}",
        )
        from src.agents.reply_concierge import stage_monitor

        file_state = fa_max_file_state.get_file_state(db, opportunity_id=resolved["opportunity_id"])
        stage_monitor.send_first_chase_touch(
            db, opportunity_id=resolved["opportunity_id"], person_id=resolved["person_id"],
            document_name=document_name,
            contact_email=(file_state or {}).get("contact_email"),
        )
        return _slack_ephemeral(f"📄 Recorded document request *{document_name}* for `{backflip_ref}`.")

    stage = action.lower()
    if stage not in BACKFLIP_STAGE_KEYS:
        return _slack_ephemeral(
            f"Usage: /fa-max-file-update {backflip_ref} <stage> — one of: "
            + ", ".join(sorted(BACKFLIP_STAGE_KEYS))
        )
    fa_max_file_state.ensure_file_state(
        db, opportunity_id=resolved["opportunity_id"], person_id=resolved["person_id"],
    )
    fa_max_file_state.update_backflip_stage(
        db, opportunity_id=resolved["opportunity_id"], to_stage=stage,
        actor=f"manual:{user_id}", source="manual",
    )
    return _slack_ephemeral(f"✅ `{backflip_ref}` updated to stage: *{stage}*.")


def _fa_max_log_submission_command(form: dict) -> dict:
    """
    Pure logic for the Slack slash command '/fa-max-log-submission' (no
    arguments — opens a modal). Addendum to WP-T2-6: records the moment
    Josh submits a deal to Backflip, which nothing else in this codebase
    does today (backflip_ref was previously only ever created downstream,
    when terms arrive).

    Same authorization gate as /fa-max-file-update — relay_approvers. See
    _fa_max_file_update_command's docstring for why this takes an
    already-scalar form dict and is shared with the Socket Mode listener.
    """
    channel_rejection = _reject_if_wrong_command_channel(form.get("channel_id", ""))
    if channel_rejection is not None:
        return channel_rejection
    user_id = form.get("user_id", "")
    if not _relay_approver_authorized(user_id, "fa_max_lending"):
        return _slack_ephemeral("Not authorized to log a Backflip submission.")

    trigger_id = form.get("trigger_id", "")
    if not open_log_submission_modal(trigger_id, form.get("channel_id", "")):
        return _slack_ephemeral("Couldn't open the form — try again in a moment.")
    return {"response_type": "ephemeral"}


def _fa_max_backflip_files_command(form: dict, db: Session) -> dict:
    """
    Pure logic for the Slack slash command '/fa-max-backflip-files' (no
    arguments — lists open files with a Backflip reference on record).

    Exists because /fa-max-file-update requires Josh to already know the
    exact backflip_ref string, which he won't always remember -- this
    lets him look it up instead of guessing. Deliberately a plain list
    rather than a name-based fallback lookup on /fa-max-file-update
    itself: the same borrower name can have multiple opportunities in
    different stages, so resolving by name alone is ambiguous in exactly
    the case this command exists to help with. Listing every ref sidesteps
    that -- Josh picks the exact one himself, no disambiguation needed.

    Same authorization gate as the other two FA Max commands.
    """
    channel_rejection = _reject_if_wrong_command_channel(form.get("channel_id", ""))
    if channel_rejection is not None:
        return channel_rejection
    user_id = form.get("user_id", "")
    if not _relay_approver_authorized(user_id, "fa_max_lending"):
        return _slack_ephemeral("Not authorized to list FA Max files.")

    rows = db.execute(
        text("""
            SELECT p.full_name, o.backflip_ref, o.current_stage
            FROM fa_max_opportunities o
            JOIN fa_max_persons p ON p.person_id = o.person_id
            WHERE o.backflip_ref IS NOT NULL AND o.outcome = 'open'
            ORDER BY o.updated_at DESC
            LIMIT 25
        """)
    ).fetchall()
    if not rows:
        return _slack_ephemeral("No open files with a Backflip reference on record.")

    names = [r.full_name or "Unnamed" for r in rows]
    name_width = max(len("Borrower"), *(len(n) for n in names))
    ref_width = max(len("Ref"), *(len(r.backflip_ref) for r in rows))

    table_lines = [f"{'Borrower':<{name_width}}  {'Ref':<{ref_width}}  Stage"]
    for name, r in zip(names, rows):
        table_lines.append(f"{name:<{name_width}}  {r.backflip_ref:<{ref_width}}  {r.current_stage}")
    table_body = "\n".join(table_lines)

    message = (
        f"*Open Backflip Files* ({len(rows)})\n"
        f"```{table_body}```\n"
        f"Copy a ref above into `/fa-max-file-update <ref> ...`"
    )
    return _slack_ephemeral(message)


def _parse_slack_form(raw: bytes) -> dict:
    """Slack's slash-command HTTP body is form-encoded and parse_qs
    list-wraps every value; both slash-command routes just want the
    first (only) value per key, matching the already-scalar shape Socket
    Mode delivers the same payload in."""
    return {k: v[0] for k, v in parse_qs(raw.decode("utf-8")).items()}


@router.post("/slack/fa-max-file-update")
async def slack_fa_max_file_update_command(request: Request, db: Session = Depends(get_db)):
    """
    Slack slash command (WP-T2-6):
        /fa-max-file-update <backflip_ref> <stage>
        /fa-max-file-update <backflip_ref> doc:<document name>
        /fa-max-file-update <backflip_ref> received:<document name>
    """
    raw = await request.body()
    if not _verify_slack_signature(dict(request.headers), raw):
        raise HTTPException(status_code=401, detail="Invalid Slack signature")

    return _fa_max_file_update_command(_parse_slack_form(raw), db)


@router.post("/slack/fa-max-log-submission")
async def slack_log_submission_command(request: Request):
    """Slack slash command: '/fa-max-log-submission' (no arguments — opens a modal)."""
    raw = await request.body()
    if not _verify_slack_signature(dict(request.headers), raw):
        raise HTTPException(status_code=401, detail="Invalid Slack signature")

    return _fa_max_log_submission_command(_parse_slack_form(raw))


@router.post("/slack/fa-max-backflip-files")
async def slack_fa_max_backflip_files_command(request: Request, db: Session = Depends(get_db)):
    """Slack slash command: '/fa-max-backflip-files' (no arguments — lists open files with a Backflip reference)."""
    raw = await request.body()
    if not _verify_slack_signature(dict(request.headers), raw):
        raise HTTPException(status_code=401, detail="Invalid Slack signature")

    return _fa_max_backflip_files_command(_parse_slack_form(raw), db)


# ===========================================================================
# WIN-STORY APPROVAL
# ===========================================================================


@router.post("/win-stories/{asset_id}/approve")
def approve_win_story(
    asset_id: int,
    db: Session = Depends(get_db),
    admin: dict = Depends(get_current_admin),
):
    """
    Approve a staged win-story for public display.
    Sets is_public=True and records the approver's email.
    """
    row = db.execute(
        text("SELECT id, is_public FROM win_story_assets WHERE id = :id FOR UPDATE"),
        {"id": asset_id},
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Win-story not found")
    if row.is_public:
        return {"ok": True, "detail": "already_public"}
    db.execute(
        text("""
            UPDATE win_story_assets
               SET is_public = true, approved_by = :by
             WHERE id = :id
        """),
        {"by": admin.get("sub", "admin"), "id": asset_id},
    )
    db.commit()
    logger.info("[WinStory] approved asset_id=%d by=%s", asset_id, admin.get("sub"))
    return {"ok": True}


@router.post("/slack/win-story/interact")
async def slack_win_story_interact(request: Request, db: Session = Depends(get_db)):
    """Deprecated individual URL — kept as an alias until the Slack app's
    Interactivity Request URL is cut over to /slack/interact."""
    raw = await request.body()
    if not _verify_slack_signature(dict(request.headers), raw):
        raise HTTPException(status_code=401, detail="Invalid Slack signature")
    return _handle_win_story_interact(_parse_slack_interactive_payload(raw), db)


def _handle_win_story_interact(payload: dict, db: Session) -> dict:
    """Approve/Dismiss a staged win-story asset."""
    actions = payload.get("actions", [])
    if not actions:
        return _slack_ephemeral("No action in payload.")

    try:
        action_data = json.loads(actions[0].get("value", "{}"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid action value")

    asset_id = action_data.get("asset_id")
    action = action_data.get("action")
    user_id = payload.get("user", {}).get("id", "unknown")

    if not asset_id or action not in ("approve", "dismiss"):
        raise HTTPException(status_code=400, detail="Invalid action data")

    row = db.execute(
        text("SELECT id, is_public, proof_text FROM win_story_assets WHERE id = :id FOR UPDATE"),
        {"id": asset_id},
    ).fetchone()

    if not row:
        return _slack_ephemeral(f"Win-story {asset_id} not found.")
    if row.is_public:
        return _slack_ephemeral("Already approved.")

    if action == "approve":
        db.execute(
            text("""
                UPDATE win_story_assets
                   SET is_public = true, approved_by = :by
                 WHERE id = :id
            """),
            {"by": f"slack:{user_id}", "id": asset_id},
        )
        db.commit()
        reply = f":white_check_mark: Win-story approved by <@{user_id}>: _{row.proof_text}_"
        logger.info("[WinStory] slack-approved asset_id=%d by=%s", asset_id, user_id)
    else:
        reply = f":no_entry: Win-story dismissed by <@{user_id}>."
        logger.info("[WinStory] slack-dismissed asset_id=%d by=%s", asset_id, user_id)

    _update_win_story_slack_message(asset_id, payload, reply)
    return {"ok": True}


def _handle_confirm_entity_link(payload: dict, db: Session) -> dict:
    """EXCEPTIONS lane — operator confirms a low-confidence buyer_entity_link."""
    actions = payload.get("actions", [])
    if not actions:
        return _slack_ephemeral("No action in payload.")
    value = actions[0].get("value", "")
    parts = value.split(":")
    if len(parts) != 3:
        return _slack_ephemeral("Invalid payload format.")
    src_table, src_id_str, entity_id_str = parts
    try:
        src_id, entity_id = int(src_id_str), int(entity_id_str)
    except ValueError:
        return _slack_ephemeral("Invalid IDs in payload.")
    user_id = payload.get("user", {}).get("id", "unknown")
    updated = db.execute(
        text("""
            UPDATE buyer_entity_links
               SET match_confidence = 100, match_method = 'manual'
             WHERE source_table = :tbl AND source_id = :sid AND buyer_entity_id = :eid
        """),
        {"tbl": src_table, "sid": src_id, "eid": entity_id},
    ).rowcount
    db.commit()
    if not updated:
        return _slack_ephemeral(f"Link not found ({src_table}#{src_id} -> entity #{entity_id}).")
    logger.info("[EntityLink] confirmed src=%s id=%d entity=%d by=%s", src_table, src_id, entity_id, user_id)
    return _slack_ephemeral(f":white_check_mark: Link confirmed. Entity #{entity_id} now trusted.")


def _handle_reject_entity_link(payload: dict, db: Session) -> dict:
    """EXCEPTIONS lane — operator rejects a low-confidence buyer_entity_link."""
    actions = payload.get("actions", [])
    if not actions:
        return _slack_ephemeral("No action in payload.")
    value = actions[0].get("value", "")
    parts = value.split(":")
    if len(parts) != 3:
        return _slack_ephemeral("Invalid payload format.")
    src_table, src_id_str, entity_id_str = parts
    try:
        src_id, entity_id = int(src_id_str), int(entity_id_str)
    except ValueError:
        return _slack_ephemeral("Invalid IDs in payload.")
    user_id = payload.get("user", {}).get("id", "unknown")
    updated = db.execute(
        text("""
            DELETE FROM buyer_entity_links
             WHERE source_table = :tbl AND source_id = :sid AND buyer_entity_id = :eid
        """),
        {"tbl": src_table, "sid": src_id, "eid": entity_id},
    ).rowcount
    if not updated:
        db.rollback()
        return _slack_ephemeral(f"Link not found ({src_table}#{src_id} -> entity #{entity_id}).")
    # Durable rejection: without this, the next nightly sweep sees the permit as
    # unresolved, recreates the identical singleton link, and re-alerts. The
    # permit extractor anti-joins on this row so the same pair is never proposed
    # again. Idempotent on (kind, left_ref, right_ref).
    db.execute(
        text("""
            INSERT INTO buyer_entity_match_exception
                (kind, left_ref, right_ref, explanation, status, resolved_by, resolved_at)
            VALUES ('rejected_permit_link', :left_ref, :right_ref,
                    :explanation, 'rejected', :by, now())
            ON CONFLICT (kind, left_ref, right_ref) DO NOTHING
        """),
        {
            "left_ref": f"{src_table}#{src_id}",
            "right_ref": f"buyer_entities#{entity_id}",
            "explanation": f"Operator rejected {src_table}#{src_id} -> entity #{entity_id}",
            "by": f"slack:{user_id}",
        },
    )
    db.commit()
    logger.info("[EntityLink] rejected src=%s id=%d entity=%d by=%s", src_table, src_id, entity_id, user_id)
    return _slack_ephemeral(":no_entry: Link rejected. This match will not be proposed again.")


def _handle_view_entity_link(payload: dict, db: Session) -> dict:
    """EXCEPTIONS lane — show current entity details for review."""
    actions = payload.get("actions", [])
    if not actions:
        return _slack_ephemeral("No action in payload.")
    value = actions[0].get("value", "")
    parts = value.split(":")
    if len(parts) != 3:
        return _slack_ephemeral("Invalid payload format.")
    _, src_id_str, entity_id_str = parts
    try:
        entity_id = int(entity_id_str)
    except ValueError:
        return _slack_ephemeral("Invalid entity ID.")
    row = db.execute(
        text("SELECT canonical_name, entity_type, confidence_score FROM buyer_entities WHERE id = :id"),
        {"id": entity_id},
    ).fetchone()
    if not row:
        return _slack_ephemeral(f"Entity #{entity_id} not found.")
    return _slack_ephemeral(
        f"*Entity #{entity_id}*\nName: {row.canonical_name}\nType: {row.entity_type}\nConfidence: {row.confidence_score}%"
    )


def _handle_add_builder_to_diallist(payload: dict, db: Session) -> dict:
    """RELATIONSHIPS lane — queue builder entity for dial list consideration."""
    actions = payload.get("actions", [])
    if not actions:
        return _slack_ephemeral("No action in payload.")
    try:
        entity_id = int(actions[0].get("value", ""))
    except ValueError:
        return _slack_ephemeral("Invalid entity ID.")
    user_id = payload.get("user", {}).get("id", "unknown")
    db.execute(
        text("""
            INSERT INTO builder_dial_queue (buyer_entity_id, queued_by, queued_at)
            VALUES (:eid, :by, NOW())
            ON CONFLICT (buyer_entity_id) DO UPDATE SET queued_by = EXCLUDED.queued_by, queued_at = NOW()
        """),
        {"eid": entity_id, "by": f"slack:{user_id}"},
    )
    db.commit()
    logger.info("[BuilderRelationships] entity=%d added to dial queue by=%s", entity_id, user_id)
    return _slack_ephemeral(f":phone: Builder #{entity_id} added to dial list queue.")


def _handle_snooze_builder(payload: dict, db: Session) -> dict:
    """RELATIONSHIPS lane — snooze builder alert for 7 days."""
    actions = payload.get("actions", [])
    if not actions:
        return _slack_ephemeral("No action in payload.")
    try:
        entity_id = int(actions[0].get("value", ""))
    except ValueError:
        return _slack_ephemeral("Invalid entity ID.")
    user_id = payload.get("user", {}).get("id", "unknown")
    db.execute(
        text("""
            INSERT INTO builder_dial_queue (buyer_entity_id, queued_by, queued_at, snoozed_until)
            VALUES (:eid, :by, NOW(), NOW() + INTERVAL '7 days')
            ON CONFLICT (buyer_entity_id) DO UPDATE
               SET snoozed_until = NOW() + INTERVAL '7 days', queued_by = EXCLUDED.queued_by
        """),
        {"eid": entity_id, "by": f"slack:{user_id}"},
    )
    db.commit()
    logger.info("[BuilderRelationships] entity=%d snoozed 7d by=%s", entity_id, user_id)
    return _slack_ephemeral(f":zzz: Builder #{entity_id} snoozed for 7 days.")


def _handle_dismiss_builder(payload: dict, db: Session) -> dict:
    """RELATIONSHIPS lane — mark builder as not a fit (permanent dismiss)."""
    actions = payload.get("actions", [])
    if not actions:
        return _slack_ephemeral("No action in payload.")
    try:
        entity_id = int(actions[0].get("value", ""))
    except ValueError:
        return _slack_ephemeral("Invalid entity ID.")
    user_id = payload.get("user", {}).get("id", "unknown")
    db.execute(
        text("""
            INSERT INTO builder_dial_queue (buyer_entity_id, queued_by, queued_at, dismissed)
            VALUES (:eid, :by, NOW(), TRUE)
            ON CONFLICT (buyer_entity_id) DO UPDATE SET dismissed = TRUE, queued_by = EXCLUDED.queued_by
        """),
        {"eid": entity_id, "by": f"slack:{user_id}"},
    )
    db.commit()
    logger.info("[BuilderRelationships] entity=%d dismissed by=%s", entity_id, user_id)
    return _slack_ephemeral(f":no_entry: Builder #{entity_id} marked as not a fit.")


def _update_win_story_slack_message(asset_id: int, payload: dict, reply_text: str) -> None:
    """Replace the Approve/Dismiss buttons with the outcome text."""
    from config.settings import get_settings
    s = get_settings()
    token = s.slack_bot_token
    channel = (payload.get("channel") or {}).get("id")
    message_ts = (payload.get("message") or {}).get("ts")
    if not token or not channel or not message_ts:
        return
    try:
        from slack_sdk import WebClient
        WebClient(token=token.get_secret_value()).chat_update(
            channel=channel,
            ts=message_ts,
            text=reply_text,
            blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": reply_text}}],
        )
    except Exception as exc:
        logger.error("[WinStory] chat.update failed for asset_id=%d: %s", asset_id, exc)


# ===========================================================================
# LIFECYCLE PROMPT EDITOR
# ===========================================================================
# GET  /api/admin/prompts              — list graphs + their files
# GET  /api/admin/prompts/{graph}/{file} — read a yaml file's content
# PUT  /api/admin/prompts/{graph}/{file} — overwrite a yaml file's content

_PROMPTS_ROOT = Path(__file__).resolve().parents[2] / "src" / "agents" / "prompts"
_ALLOWED_EXTS = {".yaml", ".yml"}


def _resolve_prompt_path(graph: str, filename: str) -> Path:
    if "/" in graph or "\\" in graph or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid graph or filename")
    path = (_PROMPTS_ROOT / graph / filename).resolve()
    if not str(path).startswith(str(_PROMPTS_ROOT)):
        raise HTTPException(status_code=400, detail="Invalid path")
    if path.suffix not in _ALLOWED_EXTS:
        raise HTTPException(status_code=400, detail="Only .yaml files are editable")
    return path


@router.get("/prompts", dependencies=[Depends(get_current_admin)])
def list_prompt_graphs():
    if not _PROMPTS_ROOT.is_dir():
        return {"graphs": []}
    graphs = []
    for graph_dir in sorted(_PROMPTS_ROOT.iterdir()):
        if not graph_dir.is_dir():
            continue
        files = sorted(
            f.name for f in graph_dir.iterdir()
            if f.is_file() and f.suffix in _ALLOWED_EXTS
        )
        if files:
            graphs.append({"graph": graph_dir.name, "files": files})
    return {"graphs": graphs}


@router.get("/prompts/{graph}/{filename}", dependencies=[Depends(get_current_admin)])
def get_prompt_file(graph: str, filename: str):
    path = _resolve_prompt_path(graph, filename)
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return {"graph": graph, "filename": filename, "content": path.read_text(encoding="utf-8")}


class PromptUpdateBody(BaseModel):
    content: str


@router.put("/prompts/{graph}/{filename}", dependencies=[Depends(get_current_admin)])
def update_prompt_file(graph: str, filename: str, body: PromptUpdateBody):
    path = _resolve_prompt_path(graph, filename)
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    path.write_text(body.content, encoding="utf-8")
    logger.info("[prompts] updated %s/%s", graph, filename)
    return {"graph": graph, "filename": filename, "saved": True}


# ===========================================================================
# BROKER STATE MACHINE — ADMIN ROUTES (Layer 3C)
# ===========================================================================

class _ReassignBrokerRequest(BaseModel):
    broker_id: str


@router.post("/lanes/{lane_id}/reassign-broker")
def reassign_broker(
    lane_id: str,
    body: _ReassignBrokerRequest,
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Admin override — forcibly assign a broker to any open lane."""
    from src.services.broker_state_machine import (
        LaneNotFound as _LaneNotFound,
        BrokerNotFound as _BrokerNotFound,
        BrokerInactive as _BrokerInactive,
        LaneNotOpen as _LaneNotOpen,
        reassign_lane as _bsm_reassign,
    )
    try:
        tid = _bsm_reassign(
            db,
            lane_id,
            body.broker_id,
            actor=_admin.get("sub", "admin"),
        )
    except _LaneNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except _BrokerNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except _BrokerInactive as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except _LaneNotOpen as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    return {
        "lane_id": lane_id,
        "assigned_broker_id": body.broker_id,
        "broker_state": "assigned",
        "transition_id": tid,
        "status": "reassigned",
    }


# ---------------------------------------------------------------------------
# Broker management
# ---------------------------------------------------------------------------

class _CreateBrokerRequest(BaseModel):
    email: str
    name: str


class _PatchBrokerRequest(BaseModel):
    is_active: bool


@router.post("/brokers", dependencies=[Depends(get_current_admin)])
def create_broker_route(body: _CreateBrokerRequest, db: Session = Depends(get_db)):
    """Create a new broker account and send an invite email."""
    from src.services.broker_admin import BrokerAlreadyExists, create_broker as _create_broker
    try:
        broker = _create_broker(db, email=body.email, name=body.name)
    except BrokerAlreadyExists as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    db.commit()
    try:
        from src.services.email import send_email
        from config.settings import get_settings as _get_settings
        _s = _get_settings()
        reset_url = f"{_s.app_base_url}/broker/reset-password/{broker.reset_token}"
        login_url = f"{_s.app_base_url}/broker/login"
        send_email(
            to=broker.email,
            subject="You've been invited to the Loan Lane broker portal",
            body_text=(
                f"Hi {broker.name},\n\n"
                f"An admin has created a broker account for you on the Loan Lane portal.\n\n"
                f"Click the link below to set your password (expires in 30 days):\n\n"
                f"  {reset_url}\n\n"
                f"Once your password is set, log in at:\n  {login_url}\n\n"
                "If you did not expect this invitation, you can safely ignore this email."
            ),
            body_html=f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:Inter,Arial,sans-serif;color:#e2e8f0;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0f172a;padding:40px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0"
             style="background:#1e293b;border:1px solid rgba(255,255,255,0.08);border-radius:16px;overflow:hidden;max-width:560px;width:100%;">
        <tr>
          <td style="padding:28px 40px;border-bottom:1px solid rgba(255,255,255,0.08);">
            <p style="margin:0;font-size:22px;font-weight:800;color:#ffffff;">
              Forced <span style="color:#fbbf24;">Action</span>
              <span style="margin-left:8px;font-size:13px;font-weight:600;color:#94a3b8;">Loan Lane</span>
            </p>
          </td>
        </tr>
        <tr>
          <td style="padding:32px 40px;">
            <h1 style="margin:0 0 8px;font-size:24px;font-weight:800;color:#ffffff;">
              Welcome to the broker portal, {broker.name}.
            </h1>
            <p style="margin:0 0 24px;color:#94a3b8;font-size:15px;">
              An admin has created a broker account for you. Click below to set your password and get started.
            </p>
            <table cellpadding="0" cellspacing="0" style="margin-bottom:20px;">
              <tr>
                <td style="background:#fbbf24;border-radius:8px;">
                  <a href="{reset_url}"
                     style="display:inline-block;padding:14px 28px;color:#0f172a;font-size:15px;font-weight:700;text-decoration:none;">
                    Set Your Password &rarr;
                  </a>
                </td>
              </tr>
            </table>
            <p style="margin:0 0 24px;font-size:13px;color:#64748b;">
              This link expires in <strong style="color:#94a3b8;">30 days</strong>.
              After setting your password you can always log in at
              <a href="{login_url}" style="color:#fbbf24;text-decoration:none;">{login_url}</a>
            </p>
            <p style="margin:0;font-size:13px;color:#64748b;">
              Not expecting this? You can safely ignore this email.
            </p>
          </td>
        </tr>
        <tr>
          <td style="padding:20px 40px;border-top:1px solid rgba(255,255,255,0.08);font-size:12px;color:#475569;text-align:center;">
            Forced Action &mdash; Loan Lane Broker Portal
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>""",
        )
    except Exception:
        logger.warning("[Admin] mailchimp invite email failed for broker=%s", broker.email)
    return {
        "broker_id": str(broker.broker_id),
        "email": broker.email,
        "name": broker.name,
        "is_active": broker.is_active,
        "reset_token": broker.reset_token,
    }


@router.get("/brokers", dependencies=[Depends(get_current_admin)])
def list_brokers_route(
    include_inactive: bool = False,
    db: Session = Depends(get_db),
):
    """List all brokers with open and assigned lane counts."""
    where_clause = "" if include_inactive else "WHERE b.is_active = true"
    rows = db.execute(
        text(f"""
            SELECT
                b.broker_id, b.email, b.name, b.role, b.is_active, b.created_at,
                COUNT(l.lane_id) FILTER (WHERE l.outcome = 'open') AS open_lanes,
                COUNT(l.lane_id) FILTER (
                    WHERE l.assigned_broker_id IS NOT NULL AND l.outcome = 'open'
                ) AS assigned_lanes
            FROM brokers b
            LEFT JOIN lanes l ON l.assigned_broker_id = b.broker_id
            {where_clause}
            GROUP BY b.broker_id, b.email, b.name, b.role, b.is_active, b.created_at
            ORDER BY b.created_at
        """)
    ).fetchall()
    brokers = [
        {
            "broker_id": str(r.broker_id),
            "email": r.email,
            "name": r.name,
            "role": r.role or "broker",
            "is_active": r.is_active,
            "open_lanes": int(r.open_lanes or 0),
            "assigned_lanes": int(r.assigned_lanes or 0),
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        for r in rows
    ]
    return {"brokers": brokers, "total": len(brokers)}


@router.patch("/brokers/{broker_id}", dependencies=[Depends(get_current_admin)])
def patch_broker_route(
    broker_id: str,
    body: _PatchBrokerRequest,
    db: Session = Depends(get_db),
):
    """Activate or deactivate a broker account."""
    from src.services.broker_admin import set_broker_active
    try:
        broker = set_broker_active(db, broker_id, body.is_active)
    except Exception as exc:
        if "not found" in str(exc).lower():
            raise HTTPException(status_code=404, detail=str(exc))
        raise
    return {
        "broker_id": str(broker.broker_id),
        "email": broker.email,
        "name": broker.name,
        "is_active": broker.is_active,
    }


@router.get("/brokers/{broker_id}/lanes", dependencies=[Depends(get_current_admin)])
def get_broker_lanes_route(
    broker_id: str,
    include_closed: bool = False,
    db: Session = Depends(get_db),
):
    """List lanes assigned to a specific broker."""
    where_extra = "" if include_closed else "AND l.outcome = 'open'"
    rows = db.execute(
        text(f"""
            SELECT l.lane_id, l.prospect_id, l.lane_type, l.current_stage,
                   l.outcome, l.claimed_at, l.last_activity_at
            FROM lanes l
            WHERE l.assigned_broker_id = CAST(:bid AS uuid)
            {where_extra}
            ORDER BY l.last_activity_at DESC NULLS LAST
        """),
        {"bid": broker_id},
    ).fetchall()
    lanes = [
        {
            "lane_id": str(r.lane_id),
            "prospect_id": str(r.prospect_id),
            "lane_type": r.lane_type,
            "current_stage": r.current_stage,
            "outcome": r.outcome,
            "claimed_at": r.claimed_at.isoformat() if r.claimed_at else None,
            "last_activity_at": r.last_activity_at.isoformat() if r.last_activity_at else None,
        }
        for r in rows
    ]
    return {"broker_id": broker_id, "lanes": lanes, "count": len(lanes)}


# ---------------------------------------------------------------------------
# Lender curation
# ---------------------------------------------------------------------------

class _CreateLenderRequest(BaseModel):
    name: str


class _PatchLenderRequest(BaseModel):
    name: Optional[str] = None
    is_cleared: Optional[bool] = None
    is_active: Optional[bool] = None


@router.get("/lenders", dependencies=[Depends(get_current_admin)])
def list_lenders_admin(
    include_inactive: bool = False,
    db: Session = Depends(get_db),
):
    """List lenders for admin curation."""
    where = "" if include_inactive else "WHERE is_active = true"
    rows = db.execute(
        text(
            f"SELECT lender_id, name, is_cleared, is_active, created_at "
            f"FROM lenders {where} ORDER BY name"
        )
    ).fetchall()
    return {
        "lenders": [
            {
                "lender_id": str(r.lender_id),
                "name": r.name,
                "is_cleared": r.is_cleared,
                "is_active": r.is_active,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ]
    }


@router.post("/lenders", dependencies=[Depends(get_current_admin)])
def create_lender_admin(body: _CreateLenderRequest, db: Session = Depends(get_db)):
    """Create a new lender."""
    row = db.execute(
        text(
            "INSERT INTO lenders (name, is_cleared, is_active) "
            "VALUES (:name, false, true) "
            "RETURNING lender_id, name, is_cleared, is_active"
        ),
        {"name": body.name.strip()},
    ).fetchone()
    db.commit()
    return {
        "lender_id": str(row.lender_id),
        "name": row.name,
        "is_cleared": row.is_cleared,
        "is_active": row.is_active,
    }


@router.patch("/lenders/{lender_id}", dependencies=[Depends(get_current_admin)])
def patch_lender_admin(
    lender_id: str,
    body: _PatchLenderRequest,
    db: Session = Depends(get_db),
):
    """Toggle is_cleared / is_active or rename a lender."""
    updates = body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update.")
    set_clauses = ", ".join(f"{k} = :{k}" for k in updates)
    params = {**updates, "lid": lender_id}
    row = db.execute(
        text(
            f"UPDATE lenders SET {set_clauses}, updated_at = NOW() "
            f"WHERE lender_id = CAST(:lid AS uuid) "
            f"RETURNING lender_id, name, is_cleared, is_active"
        ),
        params,
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Lender not found.")
    db.commit()
    return {
        "lender_id": str(row.lender_id),
        "name": row.name,
        "is_cleared": row.is_cleared,
        "is_active": row.is_active,
    }


# ===========================================================================
# DEV TOOLS GATE
# ===========================================================================

@router.get("/dev/ping")
def dev_ping(_admin: dict = Depends(get_current_admin)):
    """
    Returns 200 if DEV_TOOLS_ENABLED=true, 403 otherwise.
    The /dev frontend route calls this on mount — if it gets a 403 it shows
    a locked screen instead of rendering the dev tools.
    """
    if not settings.dev_tools_enabled:
        raise HTTPException(status_code=403, detail="Dev tools are disabled in this environment")
    return {"enabled": True}


# ===========================================================================
# COUNTY MANAGEMENT
# ===========================================================================
# GET  /api/admin/counties                           — list all counties
# POST /api/admin/counties                           — create county
# GET  /api/admin/counties/{county_id}               — get single county
# PATCH /api/admin/counties/{county_id}              — update county fields
# DELETE /api/admin/counties/{county_id}             — soft-delete (is_active=False)
# GET  /api/admin/counties/{county_id}/sources       — list sources for county
# POST /api/admin/counties/{county_id}/sources       — add source
# PUT  /api/admin/counties/{county_id}/sources/{id}  — update source
# DELETE /api/admin/counties/{county_id}/sources/{id} — soft-delete source
# ===========================================================================


class CountyCreateRequest(BaseModel):
    county_id: str
    display_name: str
    # Which venture owns this county (CLONE-v2.2 / CL3). Defaults to venture
    # #1 so existing callers are unaffected.
    venture_key: str = DEFAULT_VENTURE_KEY
    fips: Optional[str] = None
    nws_zone: Optional[str] = None
    parcel_id_format: str = "folio"
    bankruptcy_division: Optional[str] = None
    zip_prefixes: list[str] = []
    city_filer_keywords: list[str] = []
    code_lien_type_map: dict = {}
    landing_featured_testimonials: Optional[list[dict]] = None
    founding_price_deadline_at: Optional[datetime] = None


class CountyUpdateRequest(BaseModel):
    display_name: Optional[str] = None
    venture_key: Optional[str] = None
    fips: Optional[str] = None
    nws_zone: Optional[str] = None
    parcel_id_format: Optional[str] = None
    bankruptcy_division: Optional[str] = None
    zip_prefixes: Optional[list[str]] = None
    city_filer_keywords: Optional[list[str]] = None
    code_lien_type_map: Optional[dict] = None
    is_active: Optional[bool] = None
    landing_featured_testimonials: Optional[list[dict]] = None
    founding_price_deadline_at: Optional[datetime] = None


ScrapeMode = Literal[
    "ai_only", "playwright_only", "playwright_then_ai",
    "nodriver_only", "nodriver_then_ai",
    "static_download", "api",
]


class CountySourceCreateRequest(BaseModel):
    signal_type: str
    source_name: Optional[str] = None
    url: str
    description: Optional[str] = None
    navigation_hint: Optional[str] = None
    output_format: Optional[str] = None
    date_range_available: bool = True
    frequency: str = "daily"
    special_flags: dict = {}
    scrape_mode: ScrapeMode = "ai_only"
    # playwright_code is intentionally NOT settable from this endpoint —
    # callers go through /sources/{id}/playwright-code/{save,generate,validate}
    # so the AST + LLM safety pipeline runs first.

    @model_validator(mode='after')
    def validate_static_download(self) -> 'CountySourceCreateRequest':
        if self.scrape_mode == 'static_download':
            if not self.url:
                raise ValueError("url is required for static_download mode")
            if '{date}' not in self.url:
                raise ValueError("url must contain {date} placeholder for static_download mode")
        return self


class CountySourceUpdateRequest(BaseModel):
    source_name: Optional[str] = None
    url: Optional[str] = None
    description: Optional[str] = None
    navigation_hint: Optional[str] = None
    output_format: Optional[str] = None
    date_range_available: Optional[bool] = None
    frequency: Optional[str] = None
    is_active: Optional[bool] = None
    special_flags: Optional[dict] = None
    scrape_mode: Optional[ScrapeMode] = None


def _require_venture(venture_key: str, db: Session) -> None:
    """422 if venture_key names no active venture (CLONE-v2.2 / CL3).

    counties.venture_key is a real foreign key, so without this check a typo'd
    key surfaces as an IntegrityError and a 500 instead of a message the
    admin can act on.
    """
    exists = db.execute(
        text("SELECT 1 FROM ventures WHERE venture_key = :key AND is_active = true"),
        {"key": venture_key},
    ).first()
    if not exists:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown or inactive venture '{venture_key}'",
        )


def _county_to_dict(county: County) -> dict:
    return {
        "county_id":           county.county_id,
        "display_name":        county.display_name,
        "venture_key":         county.venture_key,
        "fips":                county.fips,
        "nws_zone":            county.nws_zone,
        "parcel_id_format":    county.parcel_id_format,
        "bankruptcy_division": county.bankruptcy_division,
        "zip_prefixes":        county.zip_prefixes or [],
        "city_filer_keywords": county.city_filer_keywords or [],
        "code_lien_type_map":  county.code_lien_type_map or {},
        "landing_featured_testimonials": county.landing_featured_testimonials or [],
        "founding_price_deadline_at": county.founding_price_deadline_at.isoformat() if county.founding_price_deadline_at else None,
        "is_active":           county.is_active,
        "created_at":          county.created_at.isoformat() if county.created_at else None,
        "updated_at":          county.updated_at.isoformat() if county.updated_at else None,
    }


def _source_to_dict(src: CountySource) -> dict:
    return {
        "id":                       src.id,
        "county_id":                src.county_id,
        "signal_type":              src.signal_type,
        "source_name":              src.source_name,
        "url":                      src.url,
        "description":              src.description,
        "navigation_hint":          src.navigation_hint,
        "output_format":            src.output_format,
        "date_range_available":     src.date_range_available,
        "frequency":                src.frequency,
        "is_active":                src.is_active,
        "special_flags":            src.special_flags or {},
        "scrape_mode":              src.scrape_mode,
        "playwright_code":          src.playwright_code,
        "playwright_code_version":  src.playwright_code_version,
        "playwright_code_approved": src.playwright_code_approved,
        "created_at":               src.created_at.isoformat() if src.created_at else None,
        "updated_at":               src.updated_at.isoformat() if src.updated_at else None,
    }


@router.get("/counties")
def list_counties(
    include_inactive: bool = Query(False),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    q = db.query(County)
    if not include_inactive:
        q = q.filter(County.is_active == True)
    counties = q.order_by(County.county_id).all()
    return [_county_to_dict(c) for c in counties]


@router.post("/counties", status_code=201)
def create_county(
    body: CountyCreateRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    if db.query(County).filter_by(county_id=body.county_id).first():
        raise HTTPException(status_code=409, detail=f"County '{body.county_id}' already exists")
    _require_venture(body.venture_key, db)

    county = County(
        county_id=body.county_id,
        display_name=body.display_name,
        venture_key=body.venture_key,
        fips=body.fips,
        nws_zone=body.nws_zone,
        parcel_id_format=body.parcel_id_format,
        bankruptcy_division=body.bankruptcy_division,
        zip_prefixes=body.zip_prefixes,
        city_filer_keywords=body.city_filer_keywords,
        code_lien_type_map=body.code_lien_type_map,
        landing_featured_testimonials=body.landing_featured_testimonials,
        founding_price_deadline_at=body.founding_price_deadline_at,
        is_active=True,
    )
    db.add(county)
    db.commit()
    db.refresh(county)
    logger.info("[Admin] County created: %s by %s", body.county_id, _admin.get("sub"))
    invalidate_cache(body.county_id)
    return _county_to_dict(county)


@router.get("/counties/{county_id}")
def get_county(
    county_id: str,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    county = db.query(County).filter_by(county_id=county_id).first()
    if not county:
        raise HTTPException(status_code=404, detail=f"County '{county_id}' not found")
    return _county_to_dict(county)


@router.patch("/counties/{county_id}")
def update_county(
    county_id: str,
    body: CountyUpdateRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    county = db.query(County).filter_by(county_id=county_id).first()
    if not county:
        raise HTTPException(status_code=404, detail=f"County '{county_id}' not found")

    updates = body.model_dump(exclude_unset=True)
    if "venture_key" in updates:
        _require_venture(updates["venture_key"], db)
    for field, value in updates.items():
        setattr(county, field, value)

    db.commit()
    db.refresh(county)
    logger.info("[Admin] County updated: %s fields=%s by %s", county_id, list(updates), _admin.get("sub"))
    invalidate_cache(county_id)
    return _county_to_dict(county)


@router.delete("/counties/{county_id}", status_code=204)
def deactivate_county(
    county_id: str,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    county = db.query(County).filter_by(county_id=county_id).first()
    if not county:
        raise HTTPException(status_code=404, detail=f"County '{county_id}' not found")
    county.is_active = False
    db.commit()
    logger.info("[Admin] County deactivated: %s by %s", county_id, _admin.get("sub"))
    invalidate_cache(county_id)


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

@router.get("/counties/{county_id}/sources")
def list_sources(
    county_id: str,
    include_inactive: bool = Query(False),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    if not db.query(County).filter_by(county_id=county_id).first():
        raise HTTPException(status_code=404, detail=f"County '{county_id}' not found")

    q = db.query(CountySource).filter_by(county_id=county_id)
    if not include_inactive:
        q = q.filter(CountySource.is_active == True)
    sources = q.order_by(CountySource.signal_type).all()
    return [_source_to_dict(s) for s in sources]


@router.post("/counties/{county_id}/sources", status_code=201)
def add_source(
    county_id: str,
    body: CountySourceCreateRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    if not db.query(County).filter_by(county_id=county_id).first():
        raise HTTPException(status_code=404, detail=f"County '{county_id}' not found")

    existing = db.query(CountySource).filter_by(
        county_id=county_id, signal_type=body.signal_type
    ).first()
    if existing and existing.is_active:
        raise HTTPException(
            status_code=409,
            detail=f"Active source for signal_type '{body.signal_type}' already exists in '{county_id}'",
        )

    src = CountySource(
        county_id=county_id,
        signal_type=body.signal_type,
        source_name=body.source_name,
        url=body.url,
        description=body.description,
        navigation_hint=body.navigation_hint,
        output_format=body.output_format,
        date_range_available=body.date_range_available,
        frequency=body.frequency,
        special_flags=body.special_flags,
        scrape_mode=body.scrape_mode,
        is_active=True,
    )
    db.add(src)
    db.commit()
    db.refresh(src)
    logger.info(
        "[Admin] Source added: county=%s signal=%s id=%s by %s",
        county_id, body.signal_type, src.id, _admin.get("sub"),
    )
    invalidate_cache(county_id)
    return _source_to_dict(src)


@router.put("/counties/{county_id}/sources/{source_id}")
def update_source(
    county_id: str,
    source_id: int,
    body: CountySourceUpdateRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    src = db.query(CountySource).filter_by(id=source_id, county_id=county_id).first()
    if not src:
        raise HTTPException(status_code=404, detail=f"Source {source_id} not found in '{county_id}'")

    # Validate static_download: effective mode after this update + effective URL after this update
    effective_mode = body.scrape_mode if body.scrape_mode is not None else src.scrape_mode
    effective_url = body.url if body.url is not None else (src.url or "")
    if effective_mode == 'static_download':
        if not effective_url:
            raise HTTPException(status_code=422, detail="url is required for static_download mode")
        if '{date}' not in effective_url:
            raise HTTPException(status_code=422, detail="url must contain {date} placeholder for static_download mode")

    updates = body.model_dump(exclude_none=True)
    for field, value in updates.items():
        setattr(src, field, value)

    db.commit()
    db.refresh(src)
    logger.info(
        "[Admin] Source updated: id=%s county=%s fields=%s by %s",
        source_id, county_id, list(updates), _admin.get("sub"),
    )
    invalidate_cache(county_id)
    return _source_to_dict(src)


@router.delete("/counties/{county_id}/sources/{source_id}", status_code=204)
def deactivate_source(
    county_id: str,
    source_id: int,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    src = db.query(CountySource).filter_by(id=source_id, county_id=county_id).first()
    if not src:
        raise HTTPException(status_code=404, detail=f"Source {source_id} not found in '{county_id}'")
    src.is_active = False
    db.commit()
    logger.info("[Admin] Source deactivated: id=%s county=%s by %s", source_id, county_id, _admin.get("sub"))
    invalidate_cache(county_id)


# ===========================================================================
# PLAYWRIGHT CODE LIFECYCLE
# ===========================================================================
# Mounted under /api/admin/counties/{cid}/sources/{sid}/playwright-code.
# Every route enforces ownership of the source by the county_id in the path.
# The on-disk module that does all the heavy lifting (AST validation, LLM
# generation, history-table append) is src/utils/action_sequence.py — these
# routes are thin HTTP wrappers around its helpers.
# ===========================================================================


class PlaywrightCodeSaveRequest(BaseModel):
    code: str
    approved: bool = False  # admin-authored code: True; LLM output: False
    prompt_version: Optional[str] = None


class PlaywrightCodeValidateRequest(BaseModel):
    code: str


def _require_source(county_id: str, source_id: int, db: Session) -> CountySource:
    src = db.query(CountySource).filter_by(id=source_id, county_id=county_id).first()
    if not src:
        raise HTTPException(
            status_code=404,
            detail=f"Source {source_id} not found in county '{county_id}'",
        )
    return src


@router.post("/counties/{county_id}/sources/{source_id}/playwright-code/generate")
def generate_playwright_code_route(
    county_id: str,
    source_id: int,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Ask the LLM to generate a run_scrape function for this source. Returns
    the code WITHOUT saving it — caller decides whether to validate, edit, or
    persist. Generation is read-only; the source row is not modified.
    """
    src = _require_source(county_id, source_id, db)
    from src.utils.action_sequence import (
        generate_playwright_code,
        PlaywrightCodeError,
    )
    source_dict = _source_to_dict(src) | (src.special_flags or {})
    try:
        code = generate_playwright_code(source_dict, signal_type=src.signal_type)
    except PlaywrightCodeError as exc:
        raise HTTPException(status_code=502, detail=f"Code generation failed: {exc}")
    return {"code": code, "approved": False}


@router.post("/counties/{county_id}/sources/{source_id}/playwright-code/validate")
def validate_playwright_code_route(
    county_id: str,
    source_id: int,
    body: PlaywrightCodeValidateRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Run the AST safety check + structural validation against caller-supplied
    code. Returns {valid, errors} without persisting. Use this from the admin
    UI before saving paste-your-own code.
    """
    _require_source(county_id, source_id, db)
    from src.utils.action_sequence import (
        validate_playwright_code,
        PlaywrightCodeError,
    )
    try:
        validate_playwright_code(body.code)
    except PlaywrightCodeError as exc:
        return {"valid": False, "errors": [str(exc)]}
    return {"valid": True, "errors": []}


@router.post("/counties/{county_id}/sources/{source_id}/playwright-code", status_code=201)
def save_playwright_code_route(
    county_id: str,
    source_id: int,
    body: PlaywrightCodeSaveRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Persist code to county_sources.playwright_code + append a history row.

    Re-runs the AST validator server-side so a client that bypasses /validate
    can't sneak unsafe code in. `approved=True` marks it admin-authored and
    skips the unapproved-code-warning at scrape time.
    """
    _require_source(county_id, source_id, db)
    from src.utils.action_sequence import (
        validate_playwright_code,
        persist_playwright_code,
        PlaywrightCodeError,
    )
    try:
        validate_playwright_code(body.code)
    except PlaywrightCodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid code: {exc}")

    persist_playwright_code(
        county_id, source_id, body.code,
        prompt_version=body.prompt_version,
        is_approved=body.approved,
    )
    logger.info(
        "[Admin] playwright_code saved: source_id=%s approved=%s by %s",
        source_id, body.approved, _admin.get("sub"),
    )
    return {"is_approved": body.approved}


@router.post("/counties/{county_id}/sources/{source_id}/playwright-code/approve")
def approve_playwright_code_route(
    county_id: str,
    source_id: int,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """Flip playwright_code_approved=True; appends an 'approved_by:<sub>' history row."""
    _require_source(county_id, source_id, db)
    from src.utils.action_sequence import approve_playwright_code
    approve_playwright_code(county_id, source_id, approved_by=_admin.get("sub") or "admin")
    return {"is_approved": True}


@router.delete("/counties/{county_id}/sources/{source_id}/playwright-code", status_code=204)
def clear_playwright_code_route(
    county_id: str,
    source_id: int,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """Wipe the cached code (engine will regenerate on next run)."""
    _require_source(county_id, source_id, db)
    from src.utils.action_sequence import clear_playwright_code
    clear_playwright_code(county_id, source_id)
    logger.info("[Admin] playwright_code cleared: source_id=%s by %s",
                source_id, _admin.get("sub"))


# ===========================================================================
# COLUMN MAPPING APPROVAL WORKFLOW
# ===========================================================================
# GET   /api/admin/mappings/pending          — list pending (LLM-proposed, awaiting review)
# GET   /api/admin/mappings/approved         — list all active approved mappings
# GET   /api/admin/mappings/rejected         — list rejected mappings (queued for re-map)
# GET   /api/admin/mappings/{id}             — single mapping detail
# POST  /api/admin/mappings/{id}/approve     — approve a pending mapping
# POST  /api/admin/mappings/{id}/reject      — reject mapping (with feedback for LLM)
# PATCH /api/admin/mappings/{id}             — edit column assignments on any mapping
# POST  /api/admin/mappings/preview-columns  — upload CSV/XLSX → get columns + sample rows
# POST  /api/admin/mappings/manual           — save human-created mapping as approved
# ===========================================================================


def _mapping_to_dict(m: CountyColumnMapping) -> dict:
    return {
        "id":              m.id,
        "source_id":       m.source_id,
        "source_columns":  m.source_columns,
        "mapping":         m.mapping,
        "is_approved":     m.is_approved,
        "mapped_by":       m.mapped_by,
        "approved_by":     m.approved_by,
        "approved_at":     m.approved_at.isoformat() if m.approved_at else None,
        "sample_rows":     m.sample_rows,
        "reject_feedback": m.reject_feedback,
        # Transformation fields — applied in order after the column rename.
        "post_processors": m.post_processors or [],
        "value_maps":      m.value_maps or {},
        "row_routing":     m.row_routing,
        "created_at":      m.created_at.isoformat() if m.created_at else None,
        "updated_at":      m.updated_at.isoformat() if m.updated_at else None,
    }


@router.get("/mappings/pending")
def list_pending_mappings(
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Return all column mappings awaiting approval, with their source info.
    The LLM column mapper saves mappings as is_approved=False when it encounters
    a new source or changed columns.
    """
    rows = (
        db.query(CountyColumnMapping, CountySource)
        .join(CountySource, CountySource.id == CountyColumnMapping.source_id)
        .filter(
            CountyColumnMapping.is_approved == False,
            CountyColumnMapping.reject_feedback == None,
        )
        .order_by(CountyColumnMapping.created_at.desc())
        .all()
    )
    return [
        {
            **_mapping_to_dict(mapping),
            "source": _source_to_dict(source),
        }
        for mapping, source in rows
    ]


@router.get("/mappings/rejected")
def list_rejected_mappings(
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Return all rejected column mappings (is_approved=False, reject_feedback set).
    These are queued for LLM re-map on the next scrape run.
    """
    rows = (
        db.query(CountyColumnMapping, CountySource)
        .join(CountySource, CountySource.id == CountyColumnMapping.source_id)
        .filter(
            CountyColumnMapping.is_approved == False,
            CountyColumnMapping.reject_feedback != None,
        )
        .order_by(CountyColumnMapping.created_at.desc())
        .all()
    )
    return [
        {
            **_mapping_to_dict(mapping),
            "source": _source_to_dict(source),
        }
        for mapping, source in rows
    ]


@router.get("/mappings/approved")
def list_approved_mappings(
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Return all active approved mappings, one per source (most recently approved wins).
    Used by the admin UI to browse and edit existing mappings.
    """
    from sqlalchemy import func as sqlfunc

    # Subquery: latest approved_at per source_id
    latest_subq = (
        db.query(
            CountyColumnMapping.source_id,
            sqlfunc.max(CountyColumnMapping.approved_at).label("latest_approved_at"),
        )
        .filter(CountyColumnMapping.is_approved == True)
        .group_by(CountyColumnMapping.source_id)
        .subquery()
    )

    rows = (
        db.query(CountyColumnMapping, CountySource)
        .join(CountySource, CountySource.id == CountyColumnMapping.source_id)
        .join(
            latest_subq,
            (CountyColumnMapping.source_id == latest_subq.c.source_id)
            & (CountyColumnMapping.approved_at == latest_subq.c.latest_approved_at),
        )
        .order_by(CountySource.county_id, CountySource.signal_type)
        .all()
    )
    return [
        {
            **_mapping_to_dict(mapping),
            "source": _source_to_dict(source),
        }
        for mapping, source in rows
    ]


@router.get("/mappings/{mapping_id}")
def get_mapping(
    mapping_id: int,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    mapping = db.query(CountyColumnMapping).filter_by(id=mapping_id).first()
    if not mapping:
        raise HTTPException(status_code=404, detail=f"Mapping {mapping_id} not found")
    source = db.query(CountySource).filter_by(id=mapping.source_id).first()
    return {**_mapping_to_dict(mapping), "source": _source_to_dict(source) if source else None}


class MappingUpdateRequest(BaseModel):
    # {source_col: new_canonical} — merged into the existing rename dict
    column_updates: Optional[dict] = None
    # Optional transformation-layer replacements. None = leave field unchanged.
    # Empty list / dict = clear the field.
    post_processors: Optional[list] = None
    value_maps: Optional[dict] = None
    row_routing: Optional[dict] = None


@router.patch("/mappings/{mapping_id}")
def update_mapping(
    mapping_id: int,
    body: MappingUpdateRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Edit column assignments and/or transformation rules on an existing mapping.

    Any field omitted from the body is left unchanged. column_updates is merged
    into the existing rename dict; post_processors / value_maps / row_routing
    are full replacements (since they're admin-curated structured payloads).

    Approval state: marks the mapping is_approved=True, clears any prior reject
    feedback, and stamps approved_by/at to reflect the edit.
    """
    from datetime import datetime, timezone

    mapping = db.query(CountyColumnMapping).filter_by(id=mapping_id).first()
    if not mapping:
        raise HTTPException(status_code=404, detail=f"Mapping {mapping_id} not found")

    if body.column_updates is not None:
        merged = dict(mapping.mapping)
        merged.update(body.column_updates)
        mapping.mapping = merged
    if body.post_processors is not None:
        mapping.post_processors = body.post_processors
    if body.value_maps is not None:
        mapping.value_maps = body.value_maps
    if body.row_routing is not None:
        mapping.row_routing = body.row_routing

    mapping.is_approved = True
    mapping.reject_feedback = None  # clear any prior rejection if editing back to approved
    mapping.approved_by = _admin.get("sub")
    mapping.approved_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(mapping)

    logger.info(
        "[Admin] Mapping updated: id=%s source_id=%s by %s cols=%s pp=%s vmap=%s routing=%s",
        mapping_id, mapping.source_id, _admin.get("sub"),
        list((body.column_updates or {}).keys()),
        body.post_processors is not None,
        body.value_maps is not None,
        body.row_routing is not None,
    )
    return _mapping_to_dict(mapping)


class MappingApproveRequest(BaseModel):
    mapping_overrides: Optional[dict] = None  # optional admin corrections before approving


class MappingRejectRequest(BaseModel):
    feedback: str  # plain-English reason — returned to LLM on re-map


@router.post("/mappings/{mapping_id}/approve")
def approve_mapping(
    mapping_id: int,
    body: MappingApproveRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Approve a pending column mapping. Optional mapping_overrides let the admin
    correct individual column assignments before approving — the overrides are
    merged into the LLM-proposed mapping before saving.
    """
    from datetime import datetime, timezone

    mapping = db.query(CountyColumnMapping).filter_by(id=mapping_id).first()
    if not mapping:
        raise HTTPException(status_code=404, detail=f"Mapping {mapping_id} not found")
    if mapping.is_approved:
        raise HTTPException(status_code=409, detail="Mapping is already approved")

    if body.mapping_overrides:
        merged = dict(mapping.mapping)
        merged.update(body.mapping_overrides)
        mapping.mapping = merged

    mapping.is_approved = True
    mapping.approved_by = _admin.get("sub")
    mapping.approved_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(mapping)

    logger.info(
        "[Admin] Mapping approved: id=%s source_id=%s by %s overrides=%s",
        mapping_id, mapping.source_id, _admin.get("sub"), bool(body.mapping_overrides),
    )
    return _mapping_to_dict(mapping)


@router.post("/mappings/{mapping_id}/reject")
def reject_mapping(
    mapping_id: int,
    body: MappingRejectRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Reject a pending mapping. The feedback string is stored on the mapping row
    so the LLM column mapper can incorporate it on the next re-map attempt.
    The rejected mapping is left in the table (is_approved=False) — the mapper
    will create a new pending mapping on the next scrape run.
    """
    mapping = db.query(CountyColumnMapping).filter_by(id=mapping_id).first()
    if not mapping:
        raise HTTPException(status_code=404, detail=f"Mapping {mapping_id} not found")

    mapping.reject_feedback = body.feedback
    db.commit()

    logger.info(
        "[Admin] Mapping rejected: id=%s source_id=%s by %s feedback=%r",
        mapping_id, mapping.source_id, _admin.get("sub"), body.feedback[:100],
    )
    return {"status": "rejected", "mapping_id": mapping_id, "feedback": body.feedback}


# ---------------------------------------------------------------------------
# Column Mapping — admin-created (human) mappings
# ---------------------------------------------------------------------------

@router.post("/mappings/preview-columns")
async def preview_columns(
    file: UploadFile,
    _admin: dict = Depends(get_current_admin),
):
    """
    Accept a CSV or XLSX file upload and return its column names plus the first
    5 rows as sample data.  Used by the admin UI to populate the manual mapping form.
    """
    import io
    import pandas as pd

    contents = await file.read()
    filename = (file.filename or "").lower()

    try:
        if filename.endswith(".xlsx") or filename.endswith(".xls"):
            df = pd.read_excel(io.BytesIO(contents), dtype=str, nrows=5)
        else:
            df = pd.read_csv(io.BytesIO(contents), dtype=str, nrows=5)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Could not parse file: {e}")

    df.columns = df.columns.str.strip()
    sample_rows = df.fillna("").astype(str).to_dict("records")

    return {
        "columns": list(df.columns),
        "sample_rows": sample_rows,
    }


class ManualMappingRequest(BaseModel):
    source_id: int
    mapping: dict          # {source_col: canonical_col}
    sample_rows: Optional[list] = None  # optional — populated from preview-columns step
    # Optional transformations alongside the rename. All admin-curated.
    post_processors: Optional[list] = None
    value_maps: Optional[dict] = None
    row_routing: Optional[dict] = None


@router.post("/mappings/manual")
def create_manual_mapping(
    body: ManualMappingRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Save a human-created column mapping as immediately approved.
    Supersedes any existing approved mapping for the same source.

    Optional post_processors / value_maps / row_routing fields carry
    transformations beyond the simple rename — used to express the BookPage
    split, DocType normalization, and DocType→bucket routing for sources
    that fan out into multiple downstream signals (e.g. clerk ORI exports).
    """
    from datetime import datetime, timezone

    source = db.query(CountySource).filter_by(id=body.source_id).first()
    if not source:
        raise HTTPException(status_code=404, detail=f"Source {body.source_id} not found")

    now = datetime.now(timezone.utc)
    row = CountyColumnMapping(
        source_id=body.source_id,
        source_columns=sorted(body.mapping.keys()),
        mapping=body.mapping,
        is_approved=True,
        mapped_by="human",
        approved_by=_admin.get("sub"),
        approved_at=now,
        sample_rows=body.sample_rows,
        post_processors=body.post_processors,
        value_maps=body.value_maps,
        row_routing=body.row_routing,
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    logger.info(
        "[Admin] Manual mapping saved: id=%s source_id=%s by %s pp=%s vmap=%s routing=%s",
        row.id, row.source_id, _admin.get("sub"),
        body.post_processors is not None,
        body.value_maps is not None,
        body.row_routing is not None,
    )
    return _mapping_to_dict(row)


# ===========================================================================
# VENDOR COST PAUSE — Phase 5 admin operations
# ===========================================================================
# GET  /api/admin/vendor-cost/pauses          — list active + recent pauses
# POST /api/admin/vendor-cost/pauses/{id}/resume — manually resume a pause
# GET  /api/admin/vendor-cost/pauses/{id}/skipped — inspect skipped actions
# ===========================================================================


class VendorCostResumeRequest(BaseModel):
    reason: str = Field(..., min_length=5)


def _pause_to_dict(p) -> dict:
    return {
        "id": p.id,
        "vendor": p.vendor,
        "pause_target": p.pause_target,
        "status": p.status,
        "reason": p.reason,
        "today_cost_usd": float(p.today_cost_usd) if p.today_cost_usd is not None else None,
        "threshold_usd": float(p.threshold_usd) if p.threshold_usd is not None else None,
        "anomaly_score": float(p.anomaly_score) if p.anomaly_score is not None else None,
        "sample_n": p.sample_n,
        "paused_at": p.paused_at.isoformat() if p.paused_at else None,
        "auto_resume_at": p.auto_resume_at.isoformat() if p.auto_resume_at else None,
        "resumed_at": p.resumed_at.isoformat() if p.resumed_at else None,
        "resumed_by": p.resumed_by,
        "created_by": p.created_by,
        "metadata": p.metadata_json or {},
    }


@router.get("/vendor-cost/pauses", dependencies=[Depends(get_current_admin)])
def list_vendor_cost_pauses(
    status: Optional[str] = Query(None, description="Filter: active | auto_resumed | manually_resumed | all"),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """List active and recent vendor cost pauses."""
    from src.core.models import VendorCostPause as _VCPause
    from sqlalchemy import select as _sel

    q = _sel(_VCPause).order_by(_VCPause.paused_at.desc()).limit(limit)
    if status and status != "all":
        q = q.where(_VCPause.status == status)
    elif not status:
        # default: active only
        q = q.where(_VCPause.status == "active")

    pauses = db.execute(q).scalars().all()
    return [_pause_to_dict(p) for p in pauses]


@router.post("/vendor-cost/pauses/{pause_id}/resume", dependencies=[Depends(get_current_admin)])
def resume_vendor_cost_pause(
    pause_id: int,
    body: VendorCostResumeRequest,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Manually resume an active vendor cost pause.
    Requires a reason (min 5 chars). The daily monitor may re-pause
    the next day if the anomaly condition still holds.
    """
    from src.core.models import VendorCostPause as _VCPause
    from src.services.vendor_cost_pause_service import manual_resume

    pause = db.execute(
        select(_VCPause).where(_VCPause.id == pause_id)
    ).scalar_one_or_none()

    if not pause:
        raise HTTPException(status_code=404, detail=f"Pause {pause_id} not found")
    if pause.status != "active":
        raise HTTPException(status_code=409, detail=f"Pause is already {pause.status}")

    resumed_by = f"admin:{_admin.get('sub', 'unknown')}"
    manual_resume(db, pause, resumed_by=resumed_by, reason=body.reason)
    db.commit()

    logger.info(
        "[Admin] VendorCostPause %s manually resumed by %s: %s",
        pause_id, resumed_by, body.reason,
    )
    return _pause_to_dict(pause)


@router.get("/vendor-cost/pauses/{pause_id}/skipped", dependencies=[Depends(get_current_admin)])
def list_skipped_actions(
    pause_id: int,
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """Inspect recent API calls blocked by a specific vendor cost pause."""
    from src.core.models import ApiUsageLog, VendorCostPause as _VCPause

    pause = db.execute(
        select(_VCPause).where(_VCPause.id == pause_id)
    ).scalar_one_or_none()
    if not pause:
        raise HTTPException(status_code=404, detail=f"Pause {pause_id} not found")

    rows = db.execute(
        select(ApiUsageLog)
        .where(
            ApiUsageLog.service == pause.vendor,
            ApiUsageLog.pause_target == pause.pause_target,
            ApiUsageLog.blocked_by_pause == True,
            ApiUsageLog.created_at >= pause.paused_at,
        )
        .order_by(ApiUsageLog.created_at.desc())
        .limit(limit)
    ).scalars().all()

    return {
        "pause_id": pause_id,
        "vendor": pause.vendor,
        "pause_target": pause.pause_target,
        "total_skipped": len(rows),
        "rows": [
            {
                "id": r.id,
                "graph_name": r.graph_name,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "block_reason": r.block_reason,
            }
            for r in rows
        ],
    }


# ── Sunbiz owner detail ─────────────────────────────────────────────────────
# Surfaces the fa031 piercing fields + latest sunbiz_snapshots row + portfolio
# (sibling properties owned by the same name). Admin-only; the subscriber feed
# only carries a portfolio_size count, not the full graph.

@router.get("/owners/{owner_id}/sunbiz")
def get_owner_sunbiz_detail(
    owner_id: int,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    from src.core.models import SunbizSnapshot as _SunbizSnapshotRow
    from src.services.owner_lookup import (
        portfolio_size,
        properties_by_normalized_owner,
    )

    owner = db.execute(select(Owner).where(Owner.id == owner_id)).scalar_one_or_none()
    if not owner:
        raise HTTPException(status_code=404, detail={"error": "owner_not_found"})

    latest_snapshot = None
    if owner.sunbiz_doc_number:
        snap = db.execute(
            select(_SunbizSnapshotRow)
            .where(_SunbizSnapshotRow.sunbiz_doc_number == owner.sunbiz_doc_number)
            .order_by(_SunbizSnapshotRow.scraped_at.desc())
            .limit(1)
        ).scalar_one_or_none()
        if snap:
            latest_snapshot = {
                "scraped_at": snap.scraped_at.isoformat() if snap.scraped_at else None,
                "status": snap.status,
                "parser_version": snap.parser_version,
                "raw_jsonb": snap.raw_jsonb,
            }

    portfolio_property_ids = properties_by_normalized_owner(
        db, owner.owner_name or "", county_id=None, limit=200,
    )

    return {
        "owner_id": owner.id,
        "property_id": owner.property_id,
        "owner_name": owner.owner_name,
        "owner_type": owner.owner_type,
        "sunbiz_doc_number": owner.sunbiz_doc_number,
        "sunbiz_status": owner.sunbiz_status,
        "sunbiz_enriched_at": (
            owner.sunbiz_enriched_at.isoformat() if owner.sunbiz_enriched_at else None
        ),
        "entity_status": owner.entity_status,
        "formation_date": owner.formation_date.isoformat() if owner.formation_date else None,
        "principal_address": owner.principal_address,
        "registered_agent_name": owner.registered_agent_name,
        "registered_agent_address": owner.registered_agent_address,
        "registered_agent_email": owner.registered_agent_email,
        "managing_members": owner.managing_members or [],
        "portfolio_size": portfolio_size(db, owner.owner_name),
        "portfolio_normalized_size": len(portfolio_property_ids),
        "portfolio_property_ids": portfolio_property_ids,
        "latest_snapshot": latest_snapshot,
    }


# ─────────────────────────────────────────────────────────────────────────
# fa036 — Lifecycle playbook lifecycle + autonomy summary endpoints
# ─────────────────────────────────────────────────────────────────────────

class _PlaybookActionBody(BaseModel):
    actor: str = Field(..., min_length=1, max_length=80,
                       description="Operator handle attributed to this action")
    reason: Optional[str] = Field(default=None, max_length=4000,
                                  description="Optional free-text reason (rejection only)")


@router.post("/lifecycle-playbook/{playbook_id}/adopt")
def adopt_lifecycle_playbook(
    playbook_id: int,
    body: _PlaybookActionBody,
    _admin: dict = Depends(get_current_admin),
):
    """Adopt a Lifecycle-authored recommendation. Transitions
    `recommended` → `adopted`. Idempotent: re-adopting an already-adopted
    playbook does nothing and returns the existing state.
    """
    from src.services.playbook_writer import transition_status

    with get_db_context() as db:
        ok = transition_status(
            db, playbook_id,
            to_status="adopted", actor=body.actor,
        )
        if not ok:
            row = db.execute(text(
                "SELECT status FROM lifecycle_playbook WHERE id = :id"
            ), {"id": playbook_id}).first()
            if row is None:
                raise HTTPException(status_code=404, detail={
                    "error": "not_found",
                    "message": f"lifecycle_playbook id={playbook_id} not found",
                })
            return {
                "ok": True, "id": playbook_id, "status": row.status,
                "note": "no transition — playbook was not in 'recommended' state",
            }
        return {"ok": True, "id": playbook_id, "status": "adopted"}


@router.post("/lifecycle-playbook/{playbook_id}/reject")
def reject_lifecycle_playbook(
    playbook_id: int,
    body: _PlaybookActionBody,
    _admin: dict = Depends(get_current_admin),
):
    """Reject a Lifecycle-authored recommendation. `recommended` → `rejected`.
    `body.reason` is optional but recommended for the audit log.
    """
    from src.services.playbook_writer import transition_status

    with get_db_context() as db:
        ok = transition_status(
            db, playbook_id,
            to_status="rejected", actor=body.actor, reason=body.reason,
        )
        if not ok:
            row = db.execute(text(
                "SELECT status FROM lifecycle_playbook WHERE id = :id"
            ), {"id": playbook_id}).first()
            if row is None:
                raise HTTPException(status_code=404, detail={
                    "error": "not_found",
                    "message": f"lifecycle_playbook id={playbook_id} not found",
                })
            return {
                "ok": True, "id": playbook_id, "status": row.status,
                "note": "no transition — playbook was not in 'recommended' state",
            }
        return {"ok": True, "id": playbook_id, "status": "rejected"}


# ─────────────────────────────────────────────────────────────────────────
# T-LEARN-03 — win/loss reason-code tap (human-supplied loss codes)
# ─────────────────────────────────────────────────────────────────────────

class _OpportunityOutcomeBody(BaseModel):
    opportunity_thread_id: str = Field(..., min_length=1, max_length=20,
                                       description="OPP-YYYY-##### thread id")
    reason_code: str = Field(..., description="One of the eight loss codes")


@router.post("/opportunity-outcome")
def record_opportunity_loss(
    body: _OpportunityOutcomeBody,
    admin: dict = Depends(get_current_admin),
):
    """Record a human-supplied terminal LOSS on an Agent Lane opportunity.

    Wins are auto-coded from payment.received; this tap is how Josh supplies
    the loss reason. Idempotent — a thread already terminal is not overwritten.
    """
    from src.services.opportunity_outcome import LOSS_REASON_CODES, record_loss

    if body.reason_code not in LOSS_REASON_CODES:
        raise HTTPException(status_code=422, detail={
            "error": "invalid_reason_code",
            "message": f"reason_code must be one of {list(LOSS_REASON_CODES)}",
        })

    admin_identity = admin.get("sub") or "admin"

    with get_db_context() as db:
        inserted = record_loss(
            db, body.opportunity_thread_id,
            reason_code=body.reason_code,
            coded_by=f"admin:{admin_identity}",
        )
        db.commit()

    return {
        "ok": True,
        "opportunity_thread_id": body.opportunity_thread_id,
        "outcome": "lost",
        "reason_code": body.reason_code,
        "inserted": inserted,
        "note": None if inserted else "thread already had a terminal outcome",
    }


@router.post("/lifecycle-playbook/{playbook_id}/retire")
def retire_lifecycle_playbook(
    playbook_id: int,
    body: _PlaybookActionBody,
    _admin: dict = Depends(get_current_admin),
):
    """Retire a previously-adopted playbook. `adopted` → `retired`. The
    Metric 5 ("net new playbooks") aggregation subtracts retirements in
    the window.
    """
    from src.services.playbook_writer import transition_status

    with get_db_context() as db:
        ok = transition_status(
            db, playbook_id,
            to_status="retired", actor=body.actor,
        )
        if not ok:
            row = db.execute(text(
                "SELECT status FROM lifecycle_playbook WHERE id = :id"
            ), {"id": playbook_id}).first()
            if row is None:
                raise HTTPException(status_code=404, detail={
                    "error": "not_found",
                    "message": f"lifecycle_playbook id={playbook_id} not found",
                })
            return {
                "ok": True, "id": playbook_id, "status": row.status,
                "note": "no transition — playbook was not in 'adopted' state",
            }
        return {"ok": True, "id": playbook_id, "status": "retired"}


@router.get("/lifecycle-autonomy")
def get_lifecycle_autonomy_summary(
    weeks: int = Query(default=8, ge=1, le=52,
                       description="Number of weekly snapshots to return"),
    _admin: dict = Depends(get_current_admin),
):
    """Return the latest weekly Lifecycle autonomy scorecard plus prior weeks
    for trend inspection. Driven by `learning_cards` rows with
    `card_type='autonomy_summary'`, written by
    `src/tasks/lifecycle_autonomy_report.py` Monday 08:45 UTC.
    """
    with get_db_context() as db:
        rows = db.execute(text("""
            SELECT card_date, summary_text, data_json
            FROM learning_cards
            WHERE card_type = 'autonomy_summary'
            ORDER BY card_date DESC
            LIMIT :weeks
        """), {"weeks": weeks}).fetchall()

        history = [
            {
                "card_date":     r.card_date.isoformat() if r.card_date else None,
                "summary_text":  r.summary_text,
                "metrics":       r.data_json,
            }
            for r in rows
        ]

    return {
        "ok":      True,
        "latest":  history[0] if history else None,
        "history": history,
    }


# ─────────────────────────────────────────────────────────────────────────
# fa036 — Lifecycle Playbook list (read endpoint — adopt/reject/retire above)
# ─────────────────────────────────────────────────────────────────────────


@router.get("/lifecycle-playbooks")
def list_lifecycle_playbooks(
    status: str = Query(default="recommended"),
    limit: int = Query(default=50, ge=1, le=200),
    _admin: dict = Depends(get_current_admin),
):
    """List lifecycle_playbook rows filtered by status. Used by admin Playbook
    Recommendations UI to surface items that need adopt/reject action."""
    from src.core.models import LifecyclePlaybook

    with get_db_context() as db:
        rows = db.execute(
            select(LifecyclePlaybook)
            .where(LifecyclePlaybook.status == status)
            .order_by(LifecyclePlaybook.authored_at.desc())
            .limit(limit)
        ).scalars().all()

    return {
        "ok": True,
        "status_filter": status,
        "playbooks": [
            {
                "id":               r.id,
                "name":             r.name,
                "description":      r.description,
                "authored_by":      r.authored_by,
                "authored_at":      r.authored_at.isoformat() if r.authored_at else None,
                "source_type":      r.source_type,
                "status":           r.status,
                "rejection_reason": r.rejection_reason,
                "created_at":       r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ],
    }


# ─────────────────────────────────────────────────────────────────────────
# fa037 — Revenue Signal Score per-subscriber admin view
# ─────────────────────────────────────────────────────────────────────────


@router.get("/subscribers/{subscriber_id}/revenue-signal")
def get_subscriber_revenue_signal(
    subscriber_id: int,
    history_limit: int = Query(default=20, ge=1, le=200,
                               description="Number of audit rows to return"),
    _admin: dict = Depends(get_current_admin),
):
    """Return the live Revenue Signal Score for a subscriber plus the
    last N audit rows from `revenue_signal_score_events`.

    Returns 404 when the subscriber id doesn't exist at all. Returns a
    safe-default body (`score=0, band='low', history=[]`) when the
    subscriber exists but has no UserSegment row yet (brand-new sign-up
    that hasn't fired a significant action).

    Driven by `src.services.revenue_signal.get_revenue_signal_score`
    (read-only — no recompute, no audit row written by this endpoint).
    """
    from src.core.models import Subscriber
    from src.services.revenue_signal import get_revenue_signal_score

    with get_db_context() as db:
        sub = db.get(Subscriber, subscriber_id)
        if sub is None:
            raise HTTPException(status_code=404, detail={
                "error": "not_found",
                "message": f"subscriber id={subscriber_id} not found",
            })

        snapshot = get_revenue_signal_score(subscriber_id, db)

        history_rows = db.execute(text("""
            SELECT action_type, old_score, new_score, delta, band,
                   metadata, created_at
            FROM revenue_signal_score_events
            WHERE subscriber_id = :sid
            ORDER BY created_at DESC
            LIMIT :limit
        """), {"sid": subscriber_id, "limit": history_limit}).fetchall()

        history = [
            {
                "action_type": r.action_type,
                "old_score":   r.old_score,
                "new_score":   r.new_score,
                "delta":       r.delta,
                "band":        r.band,
                "metadata":    r.metadata,
                "created_at":  r.created_at.isoformat() if r.created_at else None,
            }
            for r in history_rows
        ]

    return {
        "ok":                          True,
        "subscriber_id":               subscriber_id,
        "score":                       snapshot["score"],
        "band":                        snapshot["band"],
        "breakdown":                   snapshot["breakdown"],
        "reasons":                     snapshot["reasons"],
        "revenue_signal_updated_at":   snapshot["updated_at"],
        "last_significant_action_at":  snapshot["last_significant_action_at"],
        "last_action":                 snapshot["last_action"],
        "history":                     history,
    }


# ---------------------------------------------------------------------------
# Storm Packs — NWS alerts + bundle purchases
# ---------------------------------------------------------------------------

@router.get("/storm-packs")
def get_storm_packs(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    _admin: dict = Depends(get_current_admin),
):
    from src.core.models import BundlePurchase, NWSAlert

    with get_db_context() as db:
        total_alerts = db.execute(
            select(func.count()).select_from(NWSAlert)
        ).scalar() or 0

        alert_rows = db.execute(
            select(NWSAlert).order_by(NWSAlert.id.desc()).limit(limit).offset(offset)
        ).scalars().all()

        purchase_rows = db.execute(
            select(BundlePurchase).order_by(BundlePurchase.id.desc()).limit(limit).offset(offset)
        ).scalars().all()

    return {
        "ok": True,
        "total_alerts": total_alerts,
        "alerts": [
            {
                "id":                  a.id,
                "event":               a.event,
                "severity":            a.severity,
                "affected_zips":       a.affected_zips or [],
                "subscriber_count":    a.subscriber_count,
                "storm_pack_triggered": bool(a.storm_pack_triggered),
                "created_at":          a.processed_at.isoformat() if a.processed_at else None,
                "expires":             a.expires.isoformat() if a.expires else None,
            }
            for a in alert_rows
        ],
        "bundle_purchases": [
            {
                "id":           bp.id,
                "bundle_type":  bp.bundle_type,
                "status":       bp.status,
                "subscriber_id": bp.subscriber_id,
                "zip_code":     bp.zip_code,
                "lead_count":   len(bp.lead_ids) if bp.lead_ids else 0,
                "expires_at":   bp.expires_at.isoformat() if bp.expires_at else None,
                "created_at":   bp.purchased_at.isoformat() if bp.purchased_at else None,
            }
            for bp in purchase_rows
        ],
    }


# ---------------------------------------------------------------------------
# GET  /api/admin/lifecycle-messages/review-switch   — read current switch state
# POST /api/admin/lifecycle-messages/review-switch   — turn human review on/off
# ---------------------------------------------------------------------------

class _ReviewSwitchBody(BaseModel):
    enabled: bool


@router.get("/lifecycle-messages/review-switch")
def get_lifecycle_review_switch(
    _admin: dict = Depends(get_current_admin),
):
    """
    Return whether human review of outbound Lifecycle messages is currently ON.

    When ON, Lifecycle's outbound marketing SMS are held in the pending-review
    queue for manual approve/cancel. When OFF (the default) they send
    immediately.
    """
    from src.services.lifecycle_review_switch import is_review_enabled
    return {"ok": True, "enabled": is_review_enabled()}


@router.post("/lifecycle-messages/review-switch")
def set_lifecycle_review_switch(
    body: _ReviewSwitchBody,
    _admin: dict = Depends(get_current_admin),
):
    """
    Turn human review of outbound Lifecycle messages on or off at runtime.

    Takes effect immediately for all subsequent sends — no redeploy. Turning
    it OFF does not auto-send messages already sitting in the queue; clear
    those with approve/cancel.
    """
    from src.services.lifecycle_review_switch import set_review_enabled
    try:
        enabled = set_review_enabled(body.enabled, actor=_admin.get("sub"))
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    return {"ok": True, "enabled": enabled}


@router.get("/lifecycle-messages/pending")
def list_pending_lifecycle_messages(
    limit: int = Query(default=100, ge=1, le=500),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Return Lifecycle SMS messages waiting for manual review.

    Lifecycle writes outbound-message audit rows to message_outcomes. Messages held
    for review have send_status='pending_review' and requires_review=true; the
    composed SMS body is stored in context_snapshot['body'] by the Lifecycle write
    tool.
    Used by the admin UI to surface messages that may need manual review.

    Index recommendation (run once):
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mo_pending_review
        ON message_outcomes (created_at DESC, id DESC)
        WHERE send_status = 'pending_review'
          AND requires_review = true
          AND message_type = 'sms'
          AND cancelled_at IS NULL;
    """
    try:
        # Postgres builds the full response shape — json_build_object per row,
        # json_agg assembles the array. TO_CHAR emits ISO-8601 strings so no
        # Python datetime serialization is needed. ->> extracts phone/body as
        # plain text. The outer COALESCE turns the null json_agg returns on an
        # empty result set into an empty array.
        messages = db.execute(
            text("""
                SELECT COALESCE(
                    json_agg(msg ORDER BY msg_created_at DESC, msg_id DESC),
                    '[]'::json
                )
                FROM (
                    SELECT
                        json_build_object(
                            'id',                        mo.id,
                            'message_outcome_id',        mo.id,
                            'subscriber_id',             mo.subscriber_id,
                            'subscriber_email',          s.email,
                            'phone',                     COALESCE(mo.context_snapshot, '{}'::jsonb) ->> 'phone',
                            'body',                      COALESCE(mo.context_snapshot, '{}'::jsonb) ->> 'body',
                            'campaign',                  mo.template_id,
                            'variant_id',                mo.variant_id,
                            'message_type',              mo.message_type,
                            'channel',                   mo.channel,
                            'send_status',               mo.send_status,
                            'requires_review',           mo.requires_review,
                            'review_reason',             mo.review_reason,
                            'scheduled_send_at',         TO_CHAR(mo.scheduled_send_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
                            'created_at',                TO_CHAR(mo.created_at        AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
                            'decision_id',               mo.decision_id,
                            'trade_vertical',            mo.trade_vertical,
                            'county_id',                 mo.county_id,
                            'behavioral_segment',        mo.behavioral_segment,
                            'revenue_signal_score',      mo.revenue_signal_score,
                            'revenue_signal_score_band', mo.revenue_signal_score_band,
                            'last_action_recency_band',  mo.last_action_recency_band,
                            'prompt_version',            mo.prompt_version,
                            'context_snapshot',          COALESCE(mo.context_snapshot, '{}'::jsonb)
                        )                          AS msg,
                        mo.created_at              AS msg_created_at,
                        mo.id                      AS msg_id
                    FROM message_outcomes mo
                    LEFT JOIN subscribers s ON s.id = mo.subscriber_id
                    WHERE mo.message_type   = 'sms'
                      AND mo.requires_review = true
                      AND mo.send_status    = 'pending_review'
                      AND mo.cancelled_at  IS NULL
                    LIMIT :limit
                ) sub
            """),
            {"limit": limit},
        ).scalar()
    except Exception as exc:
        logger.error("[lifecycle-messages/pending] query failed: %s", exc)
        raise HTTPException(status_code=503, detail="Failed to fetch pending messages")

    return {
        "ok": True,
        "total": len(messages),
        "messages": messages,
    }


# ---------------------------------------------------------------------------
# POST /api/admin/lifecycle-messages/{id}/approve
# POST /api/admin/lifecycle-messages/{id}/cancel
# ---------------------------------------------------------------------------

class _MessageReviewBody(BaseModel):
    reason: Optional[str] = Field(default=None, max_length=255)


@router.post("/lifecycle-messages/{message_id}/approve")
def approve_lifecycle_message(
    message_id: int,
    body: _MessageReviewBody,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Approve a pending-review Lifecycle SMS message — and send it immediately.

    Marks the row approved (approved_by / approved_at), then dispatches the
    held body to the recipient through the compliance-gated outbound path.
    On a successful send the row lands at send_status='sent'; if the send is
    suppressed or fails it lands at 'failed'. Either way it leaves the queue.
    Returns 404 if the message does not exist, 409 if it is not pending_review.
    """
    try:
        row = db.execute(
            text("""
                SELECT
                    id,
                    send_status,
                    subscriber_id,
                    template_id,
                    variant_id,
                    decision_id,
                    COALESCE(context_snapshot, '{}'::jsonb) ->> 'phone' AS phone,
                    COALESCE(context_snapshot, '{}'::jsonb) ->> 'body'  AS body
                FROM message_outcomes
                WHERE id = :id
            """),
            {"id": message_id},
        ).fetchone()
    except Exception as exc:
        logger.error("[lifecycle-messages/approve] fetch failed id=%s: %s", message_id, exc)
        raise HTTPException(status_code=503, detail="Database error")

    if row is None:
        raise HTTPException(status_code=404, detail=f"Message {message_id} not found")
    if row.send_status != "pending_review":
        raise HTTPException(
            status_code=409,
            detail=f"Message {message_id} is '{row.send_status}', not pending_review",
        )
    if not row.phone or not row.body:
        raise HTTPException(
            status_code=409,
            detail=f"Message {message_id} is missing a stored phone or body — cannot send.",
        )

    # Stamp the approval first so the audit trail records who released it,
    # then dispatch through the compliance gate (opt-out / quiet-hours / caps).
    from src.services import sms_compliance

    try:
        db.execute(
            text("""
                UPDATE message_outcomes
                SET approved_by = :actor,
                    approved_at = NOW()
                WHERE id = :id
            """),
            {"id": message_id, "actor": _admin.get("sub")},
        )

        sent = sms_compliance.send_sms(
            to=row.phone,
            body=row.body,
            db=db,
            message_type="marketing",
            subscriber_id=row.subscriber_id,
            task_type=row.template_id,
            campaign=row.template_id,
            variant_id=row.variant_id,
            decision_id=row.decision_id,
        )

        final_status = "sent" if sent else "failed"
        db.execute(
            text("""
                UPDATE message_outcomes
                SET send_status = :status,
                    sent_at     = CASE WHEN :status = 'sent' THEN NOW() ELSE sent_at END
                WHERE id = :id
            """),
            {"id": message_id, "status": final_status},
        )
        db.commit()
    except Exception as exc:
        logger.error("[lifecycle-messages/approve] send/update failed id=%s: %s", message_id, exc)
        raise HTTPException(status_code=503, detail="Failed to approve and send message")

    logger.info(
        "[lifecycle-messages/approve] id=%s approved by %s → %s",
        message_id, _admin.get("sub"), final_status,
    )
    return {"ok": True, "id": message_id, "send_status": final_status}


@router.post("/lifecycle-messages/{message_id}/cancel")
def cancel_lifecycle_message(
    message_id: int,
    body: _MessageReviewBody,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Cancel a pending-review Lifecycle SMS message.
    Sets send_status='cancelled', cancelled_by, cancelled_at, and optionally cancel_reason.
    Returns 404 if the message does not exist, 409 if it is not pending_review.
    """
    try:
        row = db.execute(
            text("""
                SELECT id, send_status
                FROM message_outcomes
                WHERE id = :id
            """),
            {"id": message_id},
        ).fetchone()
    except Exception as exc:
        logger.error("[lifecycle-messages/cancel] fetch failed id=%s: %s", message_id, exc)
        raise HTTPException(status_code=503, detail="Database error")

    if row is None:
        raise HTTPException(status_code=404, detail=f"Message {message_id} not found")
    if row.send_status != "pending_review":
        raise HTTPException(
            status_code=409,
            detail=f"Message {message_id} is '{row.send_status}', not pending_review",
        )

    try:
        db.execute(
            text("""
                UPDATE message_outcomes
                SET send_status   = 'cancelled',
                    cancelled_by  = :actor,
                    cancelled_at  = NOW(),
                    cancel_reason = :reason
                WHERE id = :id
            """),
            {
                "id":     message_id,
                "actor":  _admin.get("sub"),
                "reason": body.reason,
            },
        )
        db.commit()
    except Exception as exc:
        logger.error("[lifecycle-messages/cancel] update failed id=%s: %s", message_id, exc)
        raise HTTPException(status_code=503, detail="Failed to cancel message")

    logger.info(
        "[lifecycle-messages/cancel] id=%s cancelled by %s reason=%r",
        message_id, _admin.get("sub"), body.reason,
    )


# ── DFY-Lite order dashboard ──────────────────────────────────────────────────

@router.get("/dfy-lite/orders")
def admin_dfy_lite_orders(
    status: Optional[str] = Query(default=None),
    subscriber_id: Optional[int] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=100),
    db: Session = Depends(get_db),
    _admin: dict = Depends(get_current_admin),
) -> dict:
    """Paginated list of all DFY-Lite orders for the admin dashboard."""
    from src.services.dfy_lite_service import list_orders_admin
    return list_orders_admin(db, status=status, subscriber_id=subscriber_id, page=page, page_size=page_size)
    return {"ok": True, "id": message_id, "send_status": "cancelled"}


# ---------------------------------------------------------------------------
# S5: Revenue Leak Log
# ---------------------------------------------------------------------------

@router.get("/revenue-leak", dependencies=[Depends(get_current_admin)])
def get_revenue_leak(
    county_id: Optional[str] = Query(default=None),
    limit: int = Query(default=30, ge=1, le=90),
    db: Session = Depends(get_db),
):
    """
    Returns recent revenue_leak_log rows (nightly per-county aggregates).
    Each row shows how many Gold+ leads went undelivered >48h and their
    estimated dollar value, broken down by vertical.
    """
    try:
        rows = db.execute(
            text("""
                SELECT log_date,
                       county_id,
                       total_leads_leaked,
                       estimated_dollar_value,
                       vertical_breakdown
                FROM revenue_leak_log
                WHERE (:county IS NULL OR county_id = :county)
                ORDER BY log_date DESC, county_id
                LIMIT :limit
            """),
            {"county": county_id, "limit": limit},
        ).fetchall()
    except Exception as exc:
        logger.error("[revenue-leak] query failed: %s", exc)
        raise HTTPException(status_code=503, detail="Database error")

    return [
        {
            "log_date": str(r.log_date),
            "county_id": r.county_id,
            "total_leads_leaked": r.total_leads_leaked,
            "estimated_dollar_value": str(r.estimated_dollar_value),
            "vertical_breakdown": r.vertical_breakdown or {},
        }
        for r in rows
    ]


# ── Quora answer review queue ─────────────────────────────────────────────────

@router.get("/quora/queue", dependencies=[Depends(get_current_admin)])
def get_quora_queue(
    page:      int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db = Depends(get_db),
) -> dict:
    """
    Paginated list of drafted Quora answers awaiting admin review.
    Ordered by priority_score DESC so highest-value questions surface first.
    """
    offset = (page - 1) * page_size
    try:
        rows = db.execute(
            text("""
                SELECT id, qid, title, url, intent_lane, priority_score,
                       risk_level, answer_draft, post_attempts, error_log,
                       last_classified_at
                FROM quora_questions
                WHERE answer_status = 'drafted'
                ORDER BY priority_score DESC NULLS LAST
                LIMIT :limit OFFSET :offset
            """),
            {"limit": page_size, "offset": offset},
        ).fetchall()

        total = db.execute(
            text("SELECT COUNT(*) FROM quora_questions WHERE answer_status = 'drafted'")
        ).scalar()
    except Exception as exc:
        logger.error("[quora-queue] query failed: %s", exc)
        raise HTTPException(status_code=503, detail="Database error")

    return {
        "items": [
            {
                "id":                 r.id,
                "qid":                r.qid,
                "title":              r.title,
                "url":                r.url,
                "intent_lane":        r.intent_lane,
                "priority_score":     r.priority_score,
                "risk_level":         r.risk_level,
                "answer_draft":       r.answer_draft or {},
                "post_attempts":      r.post_attempts,
                "error_log":          r.error_log,
                "last_classified_at": r.last_classified_at.isoformat() if r.last_classified_at else None,
            }
            for r in rows
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/quora/posted", dependencies=[Depends(get_current_admin)])
def get_quora_posted(
    page:      int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=100),
    db = Depends(get_db),
) -> dict:
    """Paginated list of successfully posted Quora answers, newest first."""
    offset = (page - 1) * page_size
    try:
        rows = db.execute(
            text("""
                SELECT id, qid, title, url, intent_lane, priority_score,
                       matched_keyword, quora_answer_id, published_at
                FROM quora_questions
                WHERE answer_status = 'published'
                ORDER BY published_at DESC NULLS LAST
                LIMIT :limit OFFSET :offset
            """),
            {"limit": page_size, "offset": offset},
        ).fetchall()
        total = db.execute(
            text("SELECT COUNT(*) FROM quora_questions WHERE answer_status = 'published'")
        ).scalar() or 0
    except Exception as exc:
        logger.error("[quora-posted] query failed: %s", exc)
        raise HTTPException(status_code=503, detail="Database error")

    return {
        "items": [
            {
                "id":               r.id,
                "qid":              r.qid,
                "title":            r.title,
                "url":              r.url,
                "intent_lane":      r.intent_lane,
                "priority_score":   r.priority_score,
                "matched_keyword":  r.matched_keyword,
                "quora_answer_id":  r.quora_answer_id,
                "published_at":     r.published_at.isoformat() if r.published_at else None,
            }
            for r in rows
        ],
        "total": total,
    }


@router.patch("/quora/{question_id}/draft", dependencies=[Depends(get_current_admin)])
def update_quora_draft(
    question_id: int,
    body: dict,
    db = Depends(get_db),
) -> dict:
    """
    Update the answer_markdown inside answer_draft before posting.
    Only edits the markdown field — preserves utm_links, safety_notes, etc.
    """
    new_markdown = body.get("answer_markdown", "").strip()
    if not new_markdown:
        raise HTTPException(status_code=400, detail="answer_markdown is required")

    try:
        result = db.execute(
            text("""
                UPDATE quora_questions
                SET answer_draft = jsonb_set(
                    COALESCE(answer_draft, '{}'::jsonb),
                    '{answer_markdown}',
                    to_jsonb(:markdown::text)
                )
                WHERE id = :id AND answer_status = 'drafted'
                RETURNING id
            """),
            {"id": question_id, "markdown": new_markdown},
        ).fetchone()
    except Exception as exc:
        logger.error("[quora-draft] update failed id=%s: %s", question_id, exc)
        raise HTTPException(status_code=503, detail="Database error")

    if not result:
        raise HTTPException(status_code=404, detail="Question not found or not in drafted status")

    return {"ok": True, "id": question_id}


@router.post("/quora/{question_id}/post", dependencies=[Depends(get_current_admin)])
async def post_quora_answer(
    question_id: int,
    db = Depends(get_db),
) -> dict:
    """
    Trigger Playwright to post the drafted answer to Quora.
    Runs synchronously — caller waits for confirmation.
    On success: answer_status → published.
    On failure: increments post_attempts, records error_log.
    After 3 failures: answer_status → failed.
    """
    from datetime import datetime, timezone as _tz
    from src.scrappers.quora.quora_poster import post_answer_to_quora, _MAX_POST_ATTEMPTS

    try:
        row = db.execute(
            text("""
                SELECT id, qid, url, answer_draft, post_attempts, matched_keyword
                FROM quora_questions
                WHERE id = :id AND answer_status IN ('drafted', 'failed')
            """),
            {"id": question_id},
        ).fetchone()
    except Exception as exc:
        logger.error("[quora-post] DB read failed id=%s: %s", question_id, exc)
        raise HTTPException(status_code=503, detail="Database error")

    if not row:
        raise HTTPException(status_code=404, detail="Question not found or not in a postable status")

    if row.post_attempts >= _MAX_POST_ATTEMPTS:
        raise HTTPException(status_code=409, detail="Maximum post attempts reached")

    answer_markdown = (row.answer_draft or {}).get("answer_markdown", "")
    if not answer_markdown:
        raise HTTPException(status_code=422, detail="No answer_markdown in draft — edit the draft first")

    # Deterministically append a non-promotional resource footer.
    # The AI-generated answer contains no platform mention; the footer is
    # appended here so every post has exactly one consistent resource link.
    # Must match autonomous_tuning_worker's lookup exactly (docs/adr/0021) — both
    # sides go through campaign_slug() so conversions attribute back correctly.
    utm_slug = campaign_slug(row.matched_keyword) if row.matched_keyword else "quora_organic"
    qid_str  = str(row.qid) if row.qid else str(question_id)
    footer = (
        f"\n\nFor more information on distressed property resources in Florida, "
        f"visit [Forced Action](https://forcedaction.com/?utm_source=quora"
        f"&utm_medium=organic_answer&utm_campaign={utm_slug}&utm_content=qid_{qid_str})."
    )
    answer_markdown = answer_markdown.rstrip() + footer

    try:
        import asyncio
        loop = asyncio.get_event_loop()
        answer_id = await loop.run_in_executor(
            None,
            lambda: post_answer_to_quora(
                qid=row.qid,
                question_url=row.url,
                answer_markdown=answer_markdown,
            ),
        )
        now = datetime.now(tz=_tz.utc)
        db.execute(
            text("""
                UPDATE quora_questions
                SET answer_status   = 'published',
                    quora_answer_id = :answer_id,
                    posted_at       = :now,
                    error_log       = NULL
                WHERE id = :id
            """),
            {"answer_id": answer_id, "now": now, "id": question_id},
        )
        logger.info("[quora-post] published  id=%s  qid=%s  answer_id=%s",
                    question_id, row.qid, answer_id)
        return {"ok": True, "id": question_id, "quora_answer_id": answer_id}

    except Exception as exc:
        error_msg = str(exc)[:1000]
        new_attempts = row.post_attempts + 1
        new_status = "failed" if new_attempts >= _MAX_POST_ATTEMPTS else "drafted"
        try:
            db.execute(
                text("""
                    UPDATE quora_questions
                    SET post_attempts = :attempts,
                        error_log     = :error,
                        answer_status = :status
                    WHERE id = :id
                """),
                {"attempts": new_attempts, "error": error_msg,
                 "status": new_status, "id": question_id},
            )
            db.commit()  # persist failure state before the exception bubbles up
        except Exception as db_exc:
            logger.error("[quora-post] failed to record error id=%s: %s", question_id, db_exc)

        logger.error("[quora-post] post failed  id=%s  attempt=%d  error=%s",
                     question_id, new_attempts, error_msg)
        raise HTTPException(status_code=502, detail="Quora post failed — see error_log for details")


# ── Quora topic management ────────────────────────────────────────────────────

def _clamp_cooldown(db) -> None:
    """After a topic is deactivated, reduce cooldown_days to stay within the valid range."""
    try:
        quora_clamp_cooldown(db)
        db.commit()
    except Exception:
        db.rollback()


@router.get("/quora/topics", dependencies=[Depends(get_current_admin)])
def get_quora_topics(db=Depends(get_db)) -> dict:
    """List all topics (active + inactive) plus current cooldown settings."""
    try:
        rows = db.execute(text("""
            SELECT id, keyword, is_active, last_run_at, created_at
            FROM quora_topics
            ORDER BY created_at ASC
        """)).fetchall()
        settings_row = db.execute(text(
            "SELECT cooldown_days FROM quora_settings WHERE id = 1"
        )).fetchone()
    except Exception as exc:
        logger.error("[quora-topics] query failed: %s", exc)
        raise HTTPException(status_code=503, detail="Database error")

    cooldown_days = settings_row.cooldown_days if settings_row else 1
    active_count  = sum(1 for r in rows if r.is_active)
    max_cooldown  = max(0, active_count - 1)

    return {
        "items": [
            {
                "id":          r.id,
                "keyword":     r.keyword,
                "is_active":   r.is_active,
                "last_run_at": r.last_run_at.isoformat() if r.last_run_at else None,
                "created_at":  r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ],
        "cooldown_days": cooldown_days,
        "active_count":  active_count,
        "max_cooldown":  max_cooldown,
    }


class _TopicCreate(BaseModel):
    keyword: str = Field(..., min_length=1, max_length=200)


@router.post("/quora/topics", dependencies=[Depends(get_current_admin)])
def create_quora_topic(body: _TopicCreate, db=Depends(get_db)) -> dict:
    """Add a keyword to the topic pool. Re-activates it if it was previously deactivated."""
    keyword = body.keyword.strip()
    if not keyword:
        raise HTTPException(status_code=400, detail="keyword must not be blank")
    try:
        row = db.execute(text("""
            INSERT INTO quora_topics (keyword)
            VALUES (:keyword)
            ON CONFLICT (keyword) DO UPDATE SET is_active = true
            RETURNING id, keyword, is_active, last_run_at, created_at
        """), {"keyword": keyword}).fetchone()
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("[quora-topics] insert failed: %s", exc)
        raise HTTPException(status_code=503, detail="Database error")

    return {
        "id":          row.id,
        "keyword":     row.keyword,
        "is_active":   row.is_active,
        "last_run_at": row.last_run_at.isoformat() if row.last_run_at else None,
        "created_at":  row.created_at.isoformat() if row.created_at else None,
    }


@router.delete("/quora/topics/{topic_id}", dependencies=[Depends(get_current_admin)])
def delete_quora_topic(topic_id: int, db=Depends(get_db)) -> dict:
    """Soft-deactivate a topic (run history is preserved)."""
    try:
        result = db.execute(text(
            "UPDATE quora_topics SET is_active = false WHERE id = :id"
        ), {"id": topic_id})
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("[quora-topics] deactivate failed: %s", exc)
        raise HTTPException(status_code=503, detail="Database error")

    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Topic not found")

    _clamp_cooldown(db)
    return {"ok": True}


class _SettingsUpdate(BaseModel):
    cooldown_days: int = Field(..., ge=0)


@router.put("/quora/settings", dependencies=[Depends(get_current_admin)])
def update_quora_settings(body: _SettingsUpdate, db=Depends(get_db)) -> dict:
    """
    Update cooldown_days. Rejected if it would make a daily run impossible.
    Rule: cooldown_days must be <= active_topic_count - 1.
    """
    active_count = db.execute(text(
        "SELECT COUNT(*) FROM quora_topics WHERE is_active = true"
    )).scalar() or 0
    max_cooldown = max(0, active_count - 1)

    if body.cooldown_days > max_cooldown:
        raise HTTPException(
            status_code=422,
            detail=(
                f"cooldown_days cannot exceed {max_cooldown} "
                f"with {active_count} active topic(s). "
                f"Add more topics or lower the cooldown."
            ),
        )

    try:
        db.execute(text("""
            INSERT INTO quora_settings (id, cooldown_days) VALUES (1, :days)
            ON CONFLICT (id) DO UPDATE SET cooldown_days = :days
        """), {"days": body.cooldown_days})
        db.commit()
    except Exception as exc:
        db.rollback()
        logger.error("[quora-settings] update failed: %s", exc)
        raise HTTPException(status_code=503, detail="Database error")

    return {"ok": True, "cooldown_days": body.cooldown_days, "max_cooldown": max_cooldown}


# ---------------------------------------------------------------------------
# Quora remote auth — WebSocket browser stream
# ---------------------------------------------------------------------------

@router.websocket("/quora/auth/ws")
async def quora_auth_ws(websocket: WebSocket, token: str = Query(...)):
    """
    Remote Quora authentication via a streamed headless browser.

    The client sends the admin JWT as a query parameter (WebSocket cannot send
    Authorization headers). On connect: launches Playwright, navigates to
    quora.com/login, streams JPEG frames at ~1fps, and forwards click/keyboard
    actions. Session is captured automatically when login is detected.
    """
    try:
        verify_token(token)
    except HTTPException:
        await websocket.close(code=1008)   # Policy violation — bad token
        return

    await websocket.accept()
    try:
        from src.scrappers.quora.quora_auth_ws import run_auth_session
        await run_auth_session(websocket)
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.error("[quora-auth-ws] unhandled error: %s", exc)
        try:
            await websocket.send_text(
                '{"type":"error","msg":"Internal server error — check server logs."}'
            )
        except Exception:
            pass
