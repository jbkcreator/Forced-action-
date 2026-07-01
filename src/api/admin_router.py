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
import json
import logging
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Literal

import pandas as pd
import stripe
from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, Query, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import and_, case, distinct, func, or_, select, text
from sqlalchemy.orm import Session

from config.settings import settings
from src.api.deps import get_db
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
from src.utils.county_config import invalidate_cache
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
    return verify_token(credentials.credentials)


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
    for fname in ["finetuner_roofing_agent.json", "finetuner_remediation_agent.json"]:
        p = _CONFIG_DIR / fname
        if not p.exists():
            continue
        data = json.loads(p.read_text(encoding="utf-8"))
        cfg = data.get("configuration", {})
        meta = data.get("source_metadata", {})
        vertical = "roofing" if "roofing" in fname else "remediation"
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
    for fname in ["synthflow_roofing_agent.yaml", "synthflow_remediation_agent.yaml"]:
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


@router.get("/kill-switch-status", dependencies=[Depends(get_current_admin)])
def kill_switch_status_overview():
    """
    Return current kill-switch colour + cached observed metric for every
    configured feature. Requires kill_switch_metric_ingest cron to have run.
    """
    from config.cora_guardrails import KILL_SWITCH
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
    """Verify Slack request signature (HMAC-SHA256). Rejects replays > 5 min old."""
    secret = settings.slack_signing_secret
    if not secret:
        return False
    ts = headers.get("x-slack-request-timestamp", "")
    try:
        if abs(time.time() - int(ts)) > 300:
            return False
    except (TypeError, ValueError):
        return False
    sig_base = f"v0:{ts}:{body.decode('utf-8')}"
    expected = "v0=" + hmac.new(
        secret.get_secret_value().encode(),
        sig_base.encode(),
        hashlib.sha256,
    ).hexdigest()
    received = headers.get("x-slack-signature", "")
    return hmac.compare_digest(expected, received)


def _slack_ephemeral(text: str) -> dict:
    return {"response_type": "ephemeral", "text": text}


@router.post("/slack/county-launch/interact")
async def slack_county_launch_interact(request: Request, db: Session = Depends(get_db)):
    """
    Receives Slack interactive component payloads for county-launch approval buttons.
    Auth: Slack HMAC-SHA256 signature (no JWT — Slack signature IS the auth).
    """
    raw = await request.body()
    if not _verify_slack_signature(dict(request.headers), raw):
        raise HTTPException(status_code=401, detail="Invalid Slack signature")

    try:
        payload_str = parse_qs(raw.decode("utf-8")).get("payload", ["{}"])[0]
        payload = json.loads(payload_str)
    except Exception:
        raise HTTPException(status_code=400, detail="Malformed payload")

    user_id = payload.get("user", {}).get("id", "")
    approvers = settings.county_launch_approvers
    if approvers and user_id not in approvers:
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
        sa_text("SELECT id, is_public FROM win_story_assets WHERE id = :id FOR UPDATE"),
        {"id": asset_id},
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Win-story not found")
    if row.is_public:
        return {"ok": True, "detail": "already_public"}
    db.execute(
        sa_text("""
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
    """
    Receives Slack interactive payloads for win-story Approve/Dismiss buttons.
    Auth: Slack HMAC-SHA256 signature.
    """
    raw = await request.body()
    if not _verify_slack_signature(dict(request.headers), raw):
        raise HTTPException(status_code=401, detail="Invalid Slack signature")

    try:
        payload_str = parse_qs(raw.decode("utf-8")).get("payload", ["{}"])[0]
        payload = json.loads(payload_str)
    except Exception:
        raise HTTPException(status_code=400, detail="Malformed payload")

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
        sa_text("SELECT id, is_public, proof_text FROM win_story_assets WHERE id = :id FOR UPDATE"),
        {"id": asset_id},
    ).fetchone()

    if not row:
        return _slack_ephemeral(f"Win-story {asset_id} not found.")
    if row.is_public:
        return _slack_ephemeral("Already approved.")

    if action == "approve":
        db.execute(
            sa_text("""
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
# CORA PROMPT EDITOR
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
    fips: Optional[str] = None
    nws_zone: Optional[str] = None
    parcel_id_format: str = "folio"
    bankruptcy_division: Optional[str] = None
    city_filer_keywords: list[str] = []
    code_lien_type_map: dict = {}


class CountyUpdateRequest(BaseModel):
    display_name: Optional[str] = None
    fips: Optional[str] = None
    nws_zone: Optional[str] = None
    parcel_id_format: Optional[str] = None
    bankruptcy_division: Optional[str] = None
    city_filer_keywords: Optional[list[str]] = None
    code_lien_type_map: Optional[dict] = None
    is_active: Optional[bool] = None


ScrapeMode = Literal["ai_only", "playwright_only", "playwright_then_ai", "static_download", "api"]


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


def _county_to_dict(county: County) -> dict:
    return {
        "county_id":           county.county_id,
        "display_name":        county.display_name,
        "fips":                county.fips,
        "nws_zone":            county.nws_zone,
        "parcel_id_format":    county.parcel_id_format,
        "bankruptcy_division": county.bankruptcy_division,
        "city_filer_keywords": county.city_filer_keywords or [],
        "code_lien_type_map":  county.code_lien_type_map or {},
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

    county = County(
        county_id=body.county_id,
        display_name=body.display_name,
        fips=body.fips,
        nws_zone=body.nws_zone,
        parcel_id_format=body.parcel_id_format,
        bankruptcy_division=body.bankruptcy_division,
        city_filer_keywords=body.city_filer_keywords,
        code_lien_type_map=body.code_lien_type_map,
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

    updates = body.model_dump(exclude_none=True)
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
# fa036 — Cora playbook lifecycle + autonomy summary endpoints
# ─────────────────────────────────────────────────────────────────────────

class _PlaybookActionBody(BaseModel):
    actor: str = Field(..., min_length=1, max_length=80,
                       description="Operator handle attributed to this action")
    reason: Optional[str] = Field(default=None, max_length=4000,
                                  description="Optional free-text reason (rejection only)")


@router.post("/cora-playbook/{playbook_id}/adopt")
def adopt_cora_playbook(
    playbook_id: int,
    body: _PlaybookActionBody,
    _admin: dict = Depends(get_current_admin),
):
    """Adopt a Cora-authored recommendation. Transitions
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
                "SELECT status FROM cora_playbook WHERE id = :id"
            ), {"id": playbook_id}).first()
            if row is None:
                raise HTTPException(status_code=404, detail={
                    "error": "not_found",
                    "message": f"cora_playbook id={playbook_id} not found",
                })
            return {
                "ok": True, "id": playbook_id, "status": row.status,
                "note": "no transition — playbook was not in 'recommended' state",
            }
        return {"ok": True, "id": playbook_id, "status": "adopted"}


@router.post("/cora-playbook/{playbook_id}/reject")
def reject_cora_playbook(
    playbook_id: int,
    body: _PlaybookActionBody,
    _admin: dict = Depends(get_current_admin),
):
    """Reject a Cora-authored recommendation. `recommended` → `rejected`.
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
                "SELECT status FROM cora_playbook WHERE id = :id"
            ), {"id": playbook_id}).first()
            if row is None:
                raise HTTPException(status_code=404, detail={
                    "error": "not_found",
                    "message": f"cora_playbook id={playbook_id} not found",
                })
            return {
                "ok": True, "id": playbook_id, "status": row.status,
                "note": "no transition — playbook was not in 'recommended' state",
            }
        return {"ok": True, "id": playbook_id, "status": "rejected"}


@router.post("/cora-playbook/{playbook_id}/retire")
def retire_cora_playbook(
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
                "SELECT status FROM cora_playbook WHERE id = :id"
            ), {"id": playbook_id}).first()
            if row is None:
                raise HTTPException(status_code=404, detail={
                    "error": "not_found",
                    "message": f"cora_playbook id={playbook_id} not found",
                })
            return {
                "ok": True, "id": playbook_id, "status": row.status,
                "note": "no transition — playbook was not in 'adopted' state",
            }
        return {"ok": True, "id": playbook_id, "status": "retired"}


@router.get("/cora-autonomy")
def get_cora_autonomy_summary(
    weeks: int = Query(default=8, ge=1, le=52,
                       description="Number of weekly snapshots to return"),
    _admin: dict = Depends(get_current_admin),
):
    """Return the latest weekly Cora autonomy scorecard plus prior weeks
    for trend inspection. Driven by `learning_cards` rows with
    `card_type='autonomy_summary'`, written by
    `src/tasks/cora_autonomy_report.py` Monday 08:45 UTC.
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
# fa036 — Cora Playbook list (read endpoint — adopt/reject/retire above)
# ─────────────────────────────────────────────────────────────────────────


@router.get("/cora-playbooks")
def list_cora_playbooks(
    status: str = Query(default="recommended"),
    limit: int = Query(default=50, ge=1, le=200),
    _admin: dict = Depends(get_current_admin),
):
    """List cora_playbook rows filtered by status. Used by admin Playbook
    Recommendations UI to surface items that need adopt/reject action."""
    from src.core.models import CoraPlaybook

    with get_db_context() as db:
        rows = db.execute(
            select(CoraPlaybook)
            .where(CoraPlaybook.status == status)
            .order_by(CoraPlaybook.authored_at.desc())
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
# GET  /api/admin/cora-messages/review-switch   — read current switch state
# POST /api/admin/cora-messages/review-switch   — turn human review on/off
# ---------------------------------------------------------------------------

class _ReviewSwitchBody(BaseModel):
    enabled: bool


@router.get("/cora-messages/review-switch")
def get_cora_review_switch(
    _admin: dict = Depends(get_current_admin),
):
    """
    Return whether human review of outbound Cora messages is currently ON.

    When ON, Cora's outbound marketing SMS are held in the pending-review
    queue for manual approve/cancel. When OFF (the default) they send
    immediately.
    """
    from src.services.cora_review_switch import is_review_enabled
    return {"ok": True, "enabled": is_review_enabled()}


@router.post("/cora-messages/review-switch")
def set_cora_review_switch(
    body: _ReviewSwitchBody,
    _admin: dict = Depends(get_current_admin),
):
    """
    Turn human review of outbound Cora messages on or off at runtime.

    Takes effect immediately for all subsequent sends — no redeploy. Turning
    it OFF does not auto-send messages already sitting in the queue; clear
    those with approve/cancel.
    """
    from src.services.cora_review_switch import set_review_enabled
    try:
        enabled = set_review_enabled(body.enabled, actor=_admin.get("sub"))
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    return {"ok": True, "enabled": enabled}


@router.get("/cora-messages/pending")
def list_pending_cora_messages(
    limit: int = Query(default=100, ge=1, le=500),
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Return Cora SMS messages waiting for manual review.

    Cora writes outbound-message audit rows to message_outcomes. Messages held
    for review have send_status='pending_review' and requires_review=true; the
    composed SMS body is stored in context_snapshot['body'] by the Cora write
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
        logger.error("[cora-messages/pending] query failed: %s", exc)
        raise HTTPException(status_code=503, detail="Failed to fetch pending messages")

    return {
        "ok": True,
        "total": len(messages),
        "messages": messages,
    }


# ---------------------------------------------------------------------------
# POST /api/admin/cora-messages/{id}/approve
# POST /api/admin/cora-messages/{id}/cancel
# ---------------------------------------------------------------------------

class _MessageReviewBody(BaseModel):
    reason: Optional[str] = Field(default=None, max_length=255)


@router.post("/cora-messages/{message_id}/approve")
def approve_cora_message(
    message_id: int,
    body: _MessageReviewBody,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Approve a pending-review Cora SMS message — and send it immediately.

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
        logger.error("[cora-messages/approve] fetch failed id=%s: %s", message_id, exc)
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
        logger.error("[cora-messages/approve] send/update failed id=%s: %s", message_id, exc)
        raise HTTPException(status_code=503, detail="Failed to approve and send message")

    logger.info(
        "[cora-messages/approve] id=%s approved by %s → %s",
        message_id, _admin.get("sub"), final_status,
    )
    return {"ok": True, "id": message_id, "send_status": final_status}


@router.post("/cora-messages/{message_id}/cancel")
def cancel_cora_message(
    message_id: int,
    body: _MessageReviewBody,
    _admin: dict = Depends(get_current_admin),
    db: Session = Depends(get_db),
):
    """
    Cancel a pending-review Cora SMS message.
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
        logger.error("[cora-messages/cancel] fetch failed id=%s: %s", message_id, exc)
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
        logger.error("[cora-messages/cancel] update failed id=%s: %s", message_id, exc)
        raise HTTPException(status_code=503, detail="Failed to cancel message")

    logger.info(
        "[cora-messages/cancel] id=%s cancelled by %s reason=%r",
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
