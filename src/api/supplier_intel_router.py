"""
Supplier Intelligence Foundation — Admin + Supplier API (fa067).

Admin endpoints  (/api/admin/supplier-intel/...)   — admin JWT
Supplier endpoints (/api/supplier/...)              — access_token header

Admin: create accounts, manage subscriptions, trigger reports, download exports.
Supplier: view account status, browse reports, download exports.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, EmailStr, Field, field_validator
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.supplier_intel_config import SUPPLIER_INTEL_TIERS, VALID_TIERS
from src.api.admin_router import get_current_admin
from src.api.deps import get_db as _get_db
from src.services.supplier_intel.report_engine import generate_report
from src.services.supplier_intel.subscription import get_current_supplier

logger = logging.getLogger(__name__)

router = APIRouter(tags=["supplier-intel"])


# ── Request models ─────────────────────────────────────────────────────────────

class CreateAccountRequest(BaseModel):
    company_name: str
    contact_name: Optional[str] = None
    contact_email: str
    counties: Optional[list[str]] = None
    verticals: Optional[list[str]] = None
    plan_tier: Optional[str] = None   # foundation | standard | premium; None = no subscription yet

    @field_validator("contact_email")
    @classmethod
    def _validate_email(cls, v):
        v = v.strip().lower()
        if "@" not in v:
            raise ValueError("invalid email")
        return v

    @field_validator("plan_tier")
    @classmethod
    def _validate_plan_tier(cls, v):
        if v is not None and v not in ("foundation", "standard", "premium"):
            raise ValueError("plan_tier must be foundation, standard, or premium")
        return v


class CreateCheckoutRequest(BaseModel):
    plan_tier: str   # foundation | standard | premium
    with_trial: bool = True

    @field_validator("plan_tier")
    @classmethod
    def _validate_plan_tier(cls, v):
        if v not in ("foundation", "standard", "premium"):
            raise ValueError("plan_tier must be foundation, standard, or premium")
        return v


class UpdateAccountRequest(BaseModel):
    company_name: Optional[str] = None
    contact_name: Optional[str] = None
    contact_email: Optional[str] = None
    counties: Optional[list[str]] = None
    verticals: Optional[list[str]] = None
    status: Optional[str] = None


class GenerateReportRequest(BaseModel):
    county_id: str
    plan_tier: Optional[str] = None


# ── Admin: account CRUD ────────────────────────────────────────────────────────

@router.get("/api/admin/supplier-intel/accounts")
def list_accounts(
    status: Optional[str] = Query(None),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
    db: Session = Depends(_get_db),
    _admin: dict = Depends(get_current_admin),
):
    rows = db.execute(sa_text("""
        SELECT sa.id, sa.company_name, sa.contact_email, sa.contact_name,
               sa.status, sa.counties, sa.verticals, sa.access_token,
               sa.stripe_customer_id, sa.created_at,
               ss.plan_tier, ss.status AS sub_status, ss.trial_ends_at,
               COUNT(*) OVER() AS _total
        FROM supplier_accounts sa
        LEFT JOIN supplier_subscriptions ss ON ss.account_id = sa.id
        WHERE (:status IS NULL OR sa.status = :status)
        ORDER BY sa.created_at DESC
        LIMIT :limit OFFSET :offset
    """), {"status": status, "limit": limit, "offset": offset}).mappings().fetchall()

    total = int(rows[0]["_total"]) if rows else 0
    return {
        "total": total, "limit": limit, "offset": offset,
        "accounts": [dict(r) for r in rows],
    }


@router.post("/api/admin/supplier-intel/accounts", status_code=201)
def create_account(
    body: CreateAccountRequest,
    db: Session = Depends(_get_db),
    _admin: dict = Depends(get_current_admin),
):
    # Check duplicate email
    existing = db.execute(sa_text("""
        SELECT id FROM supplier_accounts WHERE contact_email = :email LIMIT 1
    """), {"email": body.contact_email}).first()
    if existing:
        raise HTTPException(status_code=409, detail=f"Account with email '{body.contact_email}' already exists")

    access_token = str(uuid.uuid4())
    result = db.execute(sa_text("""
        INSERT INTO supplier_accounts
            (company_name, contact_name, contact_email, status,
             counties, verticals, access_token, created_at, updated_at)
        VALUES
            (:company, :contact_name, :email, 'active',
             CAST(:counties AS jsonb), CAST(:verticals AS jsonb),
             :token, NOW(), NOW())
        RETURNING id
    """), {
        "company": body.company_name,
        "contact_name": body.contact_name,
        "email": body.contact_email,
        "counties": json.dumps(body.counties or []),
        "verticals": json.dumps(body.verticals or []),
        "token": access_token,
    }).first()
    db.flush()

    logger.info("[supplier-api] created account id=%s email=%s", result.id, body.contact_email)
    return {"id": result.id, "access_token": access_token, "status": "active"}


@router.post("/api/admin/supplier-intel/accounts/{account_id}/checkout")
def create_supplier_checkout(
    account_id: int,
    body: CreateCheckoutRequest,
    db: Session = Depends(_get_db),
    _admin: dict = Depends(get_current_admin),
):
    """Generate a Stripe checkout URL for a supplier account.

    The plan_tier is resolved to a Stripe price ID on the backend using the
    pre-configured STRIPE_*_PRICE_SUPPLIER_INTEL_* env vars.
    Raw price IDs are never accepted from the frontend.
    """
    from config.settings import get_settings
    from src.services.supplier_intel.subscription import create_checkout

    acc = db.execute(sa_text("""
        SELECT id, contact_email, company_name FROM supplier_accounts
        WHERE id = :id AND status = 'active' LIMIT 1
    """), {"id": account_id}).first()
    if not acc:
        raise HTTPException(status_code=404, detail="Account not found or inactive")

    settings = get_settings()
    base = settings.app_base_url.rstrip("/")
    try:
        result = create_checkout(
            account_id=account_id,
            plan_tier=body.plan_tier,
            success_url=f"{base}/admin/supplier-intel?checkout=success&account={account_id}",
            cancel_url=f"{base}/admin/supplier-intel?checkout=cancel&account={account_id}",
            customer_email=acc.contact_email,
            with_trial=body.with_trial,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    logger.info("[supplier-api] checkout created account=%s tier=%s", account_id, body.plan_tier)
    return {
        "account_id": account_id,
        "plan_tier": body.plan_tier,
        "checkout_url": result["url"],
        "session_id": result["session_id"],
    }


@router.get("/api/admin/supplier-intel/accounts/{account_id}")
def get_account(
    account_id: int,
    db: Session = Depends(_get_db),
    _admin: dict = Depends(get_current_admin),
):
    row = db.execute(sa_text("""
        SELECT sa.*, ss.plan_tier, ss.status AS sub_status,
               ss.trial_ends_at, ss.canceled_at, ss.price_cents
        FROM supplier_accounts sa
        LEFT JOIN supplier_subscriptions ss ON ss.account_id = sa.id
        WHERE sa.id = :id LIMIT 1
    """), {"id": account_id}).first()
    if not row:
        raise HTTPException(status_code=404, detail="Account not found")

    reports = db.execute(sa_text("""
        SELECT id, county_id, status, generated_at, created_at
        FROM supplier_reports WHERE account_id = :id
        ORDER BY created_at DESC LIMIT 10
    """), {"id": account_id}).mappings().fetchall()

    result = dict(row._mapping)
    result["reports"] = [dict(r) for r in reports]
    return result


@router.patch("/api/admin/supplier-intel/accounts/{account_id}")
def update_account(
    account_id: int,
    body: UpdateAccountRequest,
    db: Session = Depends(_get_db),
    _admin: dict = Depends(get_current_admin),
):
    sets, params = ["updated_at = NOW()"], {"id": account_id}
    for field, value in body.model_dump(exclude_none=True).items():
        if field in ("counties", "verticals"):
            sets.append(f"{field} = CAST(:{field} AS jsonb)")
            params[field] = json.dumps(value)
        else:
            sets.append(f"{field} = :{field}")
            params[field] = value
    if len(sets) == 1:
        raise HTTPException(status_code=400, detail="No fields to update")
    db.execute(sa_text(f"UPDATE supplier_accounts SET {', '.join(sets)} WHERE id = :id"), params)
    db.flush()
    return {"ok": True, "updated": body.model_dump(exclude_none=True)}


# ── Admin: reports ─────────────────────────────────────────────────────────────

@router.post("/api/admin/supplier-intel/accounts/{account_id}/reports/generate")
def trigger_report(
    account_id: int,
    body: GenerateReportRequest,
    db: Session = Depends(_get_db),
    _admin: dict = Depends(get_current_admin),
):
    acc = db.execute(sa_text("""
        SELECT id, company_name, counties, verticals FROM supplier_accounts
        WHERE id = :id AND status = 'active' LIMIT 1
    """), {"id": account_id}).first()
    if not acc:
        raise HTTPException(status_code=404, detail="Account not found or inactive")

    counties = acc.counties or [body.county_id]
    verticals = acc.verticals or []

    # Insert pending report row
    rpt = db.execute(sa_text("""
        INSERT INTO supplier_reports
            (account_id, county_id, status, created_at)
        VALUES (:aid, :county, 'pending', NOW())
        RETURNING id
    """), {"aid": account_id, "county": body.county_id}).first()
    report_id = rpt.id
    db.flush()

    try:
        data = generate_report(account_id, body.county_id, counties, verticals, db)
        db.execute(sa_text("""
            UPDATE supplier_reports
            SET status = 'generated',
                sections_json = CAST(:sections AS jsonb),
                data_readiness_snapshot = CAST(:readiness AS jsonb),
                generated_at = NOW(),
                report_period_start = :pstart,
                report_period_end = :pend
            WHERE id = :id
        """), {
            "sections": json.dumps(data["sections"]),
            "readiness": json.dumps(data["data_readiness_snapshot"]),
            "pstart": data["period_start"],
            "pend": data["period_end"],
            "id": report_id,
        })
        db.flush()
        return {"report_id": report_id, "status": "generated"}
    except Exception as exc:
        db.execute(sa_text("""
            UPDATE supplier_reports SET status='failed', error_message=:err WHERE id=:id
        """), {"err": str(exc)[:500], "id": report_id})
        db.flush()
        raise HTTPException(status_code=500, detail=f"Report generation failed: {exc}")


@router.get("/api/admin/supplier-intel/accounts/{account_id}/reports/{report_id}/export")
def admin_export_report(
    account_id: int,
    report_id: int,
    format: str = Query("pdf", pattern="^(pdf|csv)$"),
    db: Session = Depends(_get_db),
    _admin: dict = Depends(get_current_admin),
):
    return _export_report(account_id, report_id, format, db)


# ── Supplier-facing endpoints ─────────────────────────────────────────────────

@router.get("/api/supplier/status")
def supplier_status(
    x_access_token: str = Header(..., alias="X-Access-Token"),
    db: Session = Depends(_get_db),
):
    row = get_current_supplier(x_access_token, db)
    return {
        "company_name": row.company_name,
        "status": row.status,
        "sub_status": row.sub_status,
        "plan_tier": row.plan_tier,
        "trial_ends_at": row.trial_ends_at.isoformat() if row.trial_ends_at else None,
        "counties": row.counties or [],
        "verticals": row.verticals or [],
    }


@router.get("/api/supplier/reports/latest")
def supplier_latest_report(
    x_access_token: str = Header(..., alias="X-Access-Token"),
    db: Session = Depends(_get_db),
):
    row = get_current_supplier(x_access_token, db)
    report = db.execute(sa_text("""
        SELECT id, county_id, status, sections_json, data_readiness_snapshot,
               generated_at, report_period_start, report_period_end
        FROM supplier_reports
        WHERE account_id = :aid AND status = 'generated'
        ORDER BY generated_at DESC LIMIT 1
    """), {"aid": row.id}).first()
    if not report:
        raise HTTPException(status_code=404, detail="No reports generated yet")
    return dict(report._mapping)


@router.get("/api/supplier/reports")
def supplier_report_list(
    x_access_token: str = Header(..., alias="X-Access-Token"),
    db: Session = Depends(_get_db),
):
    row = get_current_supplier(x_access_token, db)
    reports = db.execute(sa_text("""
        SELECT id, county_id, status, generated_at, created_at
        FROM supplier_reports WHERE account_id = :aid
        ORDER BY created_at DESC LIMIT 20
    """), {"aid": row.id}).mappings().fetchall()
    return {"reports": [dict(r) for r in reports]}


@router.get("/api/supplier/reports/{report_id}/export")
def supplier_export_report(
    report_id: int,
    format: str = Query("pdf", pattern="^(pdf|csv)$"),
    # Accept token as header (API calls) OR query param (browser download links).
    # Browser <a href> clicks cannot set custom headers, so the query-param
    # fallback is required for direct downloads.
    x_access_token: Optional[str] = Header(default=None, alias="X-Access-Token"),
    token: Optional[str] = Query(default=None),
    db: Session = Depends(_get_db),
):
    resolved_token = x_access_token or token
    if not resolved_token:
        raise HTTPException(status_code=422, detail="Access token required (header X-Access-Token or query param ?token=)")
    row = get_current_supplier(resolved_token, db)
    return _export_report(row.id, report_id, format, db)


# ── Shared export helper ───────────────────────────────────────────────────────

def _export_report(account_id: int, report_id: int, format: str, db: Session):
    # Verify report belongs to account
    rpt = db.execute(sa_text("""
        SELECT sr.id, sr.sections_json, sr.county_id, sa.company_name,
               sa.id AS sa_id, sa.counties, sa.verticals
        FROM supplier_reports sr
        JOIN supplier_accounts sa ON sa.id = sr.account_id
        WHERE sr.id = :rid AND sr.account_id = :aid AND sr.status = 'generated'
        LIMIT 1
    """), {"rid": report_id, "aid": account_id}).first()
    if not rpt:
        raise HTTPException(status_code=404, detail="Report not found or not yet generated")

    # Check for cached export
    cached = db.execute(sa_text("""
        SELECT file_path FROM supplier_report_exports
        WHERE report_id = :rid AND format = :fmt
          AND file_path IS NOT NULL
        ORDER BY created_at DESC LIMIT 1
    """), {"rid": report_id, "fmt": format}).first()

    if cached and cached.file_path and Path(cached.file_path).exists():
        return FileResponse(cached.file_path,
                           media_type="application/pdf" if format == "pdf" else "text/csv",
                           filename=Path(cached.file_path).name)

    # Generate export
    from types import SimpleNamespace
    account = SimpleNamespace(
        id=rpt.sa_id,
        company_name=rpt.company_name,
        counties=rpt.counties,
        verticals=rpt.verticals,
    )
    report_data = {
        "sections": rpt.sections_json or {},
        "county_id": rpt.county_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "period_start": "", "period_end": "",
    }

    try:
        from src.services.supplier_intel.pdf_export import render_pdf, export_csv
        if format == "pdf":
            path = render_pdf(report_data, account)
        else:
            path = export_csv(report_data, account)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Export failed: {exc}")

    db.execute(sa_text("""
        INSERT INTO supplier_report_exports
            (report_id, format, file_path, exported_at, created_at)
        VALUES (:rid, :fmt, :path, NOW(), NOW())
    """), {"rid": report_id, "fmt": format, "path": str(path)})
    db.execute(sa_text("""
        UPDATE supplier_reports SET status = 'exported' WHERE id = :id
    """), {"id": report_id})
    db.flush()

    media_type = "application/pdf" if format == "pdf" else "text/csv"
    return FileResponse(str(path), media_type=media_type, filename=path.name)
