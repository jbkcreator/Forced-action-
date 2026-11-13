"""Cora P2.2 instant quote endpoint, served by ``lending-api`` under ``/api/lending/``.

POST /api/lending/instant-quote: deal facts + guarantor soft-pull FICOs in, an indicative quote out.
Off until ``LENDING_INSTANT_QUOTE_ENABLED`` (404 while off); callers send ``X-Api-Key``; rate limited.
The response never names a lender or an internal tier and is never more than "indicative, subject to underwriting".
"""
from __future__ import annotations

import hashlib
import hmac
import logging
from dataclasses import asdict
from decimal import Decimal
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from pydantic import BaseModel, Field, model_validator

from config.lending_instant_quote import (
    API_KEY_HEADER,
    RATE_LIMIT_PER_WINDOW,
    RATE_LIMIT_SCOPE,
    RATE_LIMIT_WINDOW_SECONDS,
)
from config.settings import get_settings
from src.lending.instant_quote import Guarantor, QuoteRequest, QuoteResult, quote
from src.lending.instant_quote.engine import BorrowerType, Purpose, QuoteStatus
from src.lending.instant_quote.store import QuoteRecord, QuoteStore, SqlQuoteStore
from src.services.rate_limit import enforce_or_429

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/lending", tags=["lending-instant-quote"])


class GuarantorIn(BaseModel):
    fico: int = Field(ge=300, le=850)
    completed_deals: int = Field(default=0, ge=0)
    exits: int = Field(default=0, ge=0)


class QuoteIn(BaseModel):
    state: str = Field(min_length=2, max_length=2)
    zip_code: str = Field(pattern=r"^\d{5}$")
    borrower_type: BorrowerType
    guarantors: list[GuarantorIn] = Field(min_length=1, max_length=10)
    purpose: Purpose
    purchase_price: Decimal = Field(gt=0)
    rehab: Decimal = Field(ge=0)
    arv: Decimal = Field(gt=0)
    existing_debt: Optional[Decimal] = Field(default=None, ge=0)

    @model_validator(mode="after")
    def _refinance_needs_debt(self) -> "QuoteIn":
        if self.purpose == "refinance" and self.existing_debt is None:
            raise ValueError("existing_debt is required for a refinance")
        return self


class CutOut(BaseModel):
    reason: str
    pct: Decimal


class ConditionOut(BaseModel):
    code: str
    amount: Optional[Decimal]


class QuoteOut(BaseModel):
    quote_id: str
    status: QuoteStatus
    reason: Optional[str]
    loan_amount: Optional[Decimal]
    rehab_funding: Optional[Decimal]
    purchase_advance: Optional[Decimal]
    effective_ltc: Optional[Decimal]
    cuts: list[CutOut]
    fee_surcharge_pct: Decimal
    conditions: list[ConditionOut]
    borrower_score: int
    unconfirmed: bool
    disclaimer: str


def _require_key(supplied: Optional[str]) -> str:
    """The caller's rate-limit bucket (a hash of its key), or 401."""
    expected = get_settings().lending_instant_quote_api_key
    secret = expected.get_secret_value() if expected is not None else ""
    if not secret or not supplied or not hmac.compare_digest(supplied.encode(), secret.encode()):
        raise HTTPException(status_code=401, detail="Invalid API key")
    return hashlib.sha256(supplied.encode()).hexdigest()[:16]


def _to_request(body: QuoteIn) -> QuoteRequest:
    return QuoteRequest(
        state=body.state.upper(),
        zip_code=body.zip_code,
        borrower_type=body.borrower_type,
        guarantors=tuple(Guarantor(g.fico, g.completed_deals, g.exits) for g in body.guarantors),
        purpose=body.purpose,
        purchase_price=body.purchase_price,
        rehab=body.rehab,
        arv=body.arv,
        existing_debt=body.existing_debt,
    )


def _to_response(quote_id: str, result: QuoteResult) -> QuoteOut:
    return QuoteOut(
        quote_id=quote_id,
        status=result.status,
        reason=result.reason,
        loan_amount=result.loan_amount,
        rehab_funding=result.rehab_funding,
        purchase_advance=result.purchase_advance,
        effective_ltc=result.effective_ltc,
        cuts=[CutOut(reason=c.reason, pct=c.pct) for c in result.cuts],
        fee_surcharge_pct=result.fee_surcharge_pct,
        conditions=[ConditionOut(code=c.code, amount=c.amount) for c in result.conditions],
        borrower_score=result.borrower_score,
        unconfirmed=result.unconfirmed,
        disclaimer=result.disclaimer,
    )


def get_quote_store() -> QuoteStore:
    return SqlQuoteStore()


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def _record(body: QuoteIn, result: QuoteResult, key_bucket: str) -> QuoteRecord:
    return QuoteRecord(
        status=result.status,
        reason=result.reason,
        request=_json_safe(body.model_dump()),
        result=_json_safe(asdict(result)),
        rules_fired=list(result.rules_fired),
        unconfirmed=result.unconfirmed,
        api_key_label=key_bucket,
    )


@router.post("/instant-quote", response_model=QuoteOut)
def instant_quote(
    body: QuoteIn,
    request: Request,
    api_key: Optional[str] = Header(default=None, alias=API_KEY_HEADER),
    store: QuoteStore = Depends(get_quote_store),
) -> QuoteOut:
    if not get_settings().lending_instant_quote_enabled:
        raise HTTPException(status_code=404, detail="Not found")
    key_bucket = _require_key(api_key)
    enforce_or_429(request, scope=f"{RATE_LIMIT_SCOPE}:{key_bucket}", limit=RATE_LIMIT_PER_WINDOW,
                   window_seconds=RATE_LIMIT_WINDOW_SECONDS)
    result = quote(_to_request(body))
    try:
        quote_id = store.save(_record(body, result, key_bucket))
    except Exception as exc:
        logger.error("[instant-quote] audit write failed, quote withheld: %s", type(exc).__name__)
        raise HTTPException(status_code=500, detail="Quote unavailable") from exc
    logger.info("[instant-quote] quote=%s status=%s reason=%s unconfirmed=%s",
                quote_id, result.status, result.reason, result.unconfirmed)
    return _to_response(quote_id, result)
