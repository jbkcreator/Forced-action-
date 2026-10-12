"""Partner Door (P-1): what happens after a partner's file is saved, and partner status messages.

Runs after the submission has committed (a FastAPI background task), so the partner gets an
immediate answer and a failure here never loses the file. In order:

1. Lender box (T-05) scores the file; the result is stored on it (internal only).
2. The minute-5 pre-qual PDF (T-07) is generated and stored with the file when a lender fits and its
   loan limits are known. It is not sent: borrower contact for partner files is off until Josh confirms it.
3. ``route_file`` hands the file to the router. Until the overflow router (R-1) exists, the file is
   held with reason "awaiting router".
4. Josh gets a Slack note; the partner gets a confirmation email.

Nothing here texts, emails or creates a CRM contact for the borrower. Partner consent is not
borrower consent.
"""
from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import text

from config.lending_partners import AWAITING_ROUTER_REASON, CREDIT_BANDS, PRODUCT_LABELS, PRODUCTS
from src.lending.contracts import BorrowerProfile, LenderFitResult, LoanRequest
from src.lending.partners import Partner, StatusChange, partner_by_id, record_status_change
from src.lending.prequal import TEMPLATE, FitLimits, PrequalLead, build_context
from src.lending.prequal_fit import fit_limits_from_result

logger = logging.getLogger(__name__)

EmailSender = Callable[[str, str, str], Any]  # (to, subject, body) -> provider message id
SmsSender = Callable[[str, str], Any]  # (phone, body) -> provider message id
SlackPoster = Callable[[str], None]
PdfRenderer = Callable[[str, dict], bytes]  # (template, context) -> PDF bytes
FitEvaluator = Callable[[BorrowerProfile, LoanRequest], LenderFitResult]
FitLimitsLookup = Callable[[LenderFitResult], list[FitLimits]]  # fitting lenders -> their loan-amount limits

_STATUS_WORDING = {
    "routed": "has been sent to a lender for review",
    "declined": "was declined by a lender; we are looking at the next option",
    "approved": "was approved by a lender",
    "funded": "has funded. Thank you",
}


@dataclass(frozen=True)
class PipelineServices:
    """Outside services the pipeline uses; any of them may be None when not configured."""

    email: Optional[EmailSender]
    sms: Optional[SmsSender]
    slack: Optional[SlackPoster]
    render_pdf: Optional[PdfRenderer]
    evaluate_fit: FitEvaluator
    fit_limits: FitLimitsLookup
    upload_dir: str
    prequal_pct: int


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def live_services() -> PipelineServices:
    from config.settings import get_settings
    from src.lending.ghl_email import get_email_sender
    from src.lending.ghl_sms import get_sender
    from src.lending.lender_fit import evaluate_lender_fit
    from src.lending.pdf.render import NON_BINDING_PREQUAL_WATERMARK, render_pdf

    settings = get_settings()
    email_sender, sms_sender = get_email_sender(), get_sender()

    def slack(message: str) -> None:
        channel, token = settings.lending_partner_notify_channel, settings.lending_slack_bot_token
        if not (channel and token):
            logger.info("[partner-door] Slack note skipped: notify channel or bot token not set")
            return
        from slack_sdk import WebClient

        WebClient(token=token.get_secret_value()).chat_postMessage(channel=channel, text=message)

    return PipelineServices(
        email=(lambda to, subject, body: email_sender(to, subject, body)) if email_sender else None,
        sms=(lambda phone, body: sms_sender(phone, body)) if sms_sender else None,
        slack=slack,
        render_pdf=lambda template, context: render_pdf(template, context, watermark=NON_BINDING_PREQUAL_WATERMARK),
        evaluate_fit=evaluate_lender_fit,
        fit_limits=fit_limits_from_result,
        upload_dir=settings.lending_partner_upload_dir,
        prequal_pct=settings.lending_prequal_range_pct,
    )


def _money(value: Optional[int]) -> Optional[Decimal]:
    return Decimal(value) if value is not None else None


def _score(services: PipelineServices, file: dict[str, Any]) -> Optional[LenderFitResult]:
    loan_type = PRODUCTS.get(file["product"])
    if loan_type is None:
        return None
    borrower = BorrowerProfile(credit_band_min_fico=CREDIT_BANDS.get(file["credit_band"] or ""),
                               completed_projects=file["completed_projects"], state=file["property_state"])
    loan = LoanRequest(loan_type=loan_type, loan_amount=Decimal(file["loan_amount"]), state=file["property_state"],
                       purchase_price=_money(file["purchase_price"]), rehab_budget=_money(file["rehab_budget"]),
                       arv=_money(file["arv"]), property_address=file["property_address"])
    return services.evaluate_fit(borrower, loan)


def _prequal_pdf(services: PipelineServices, file: dict[str, Any], fit: LenderFitResult) -> Optional[str]:
    """Render the pre-qual letter and store it next to the file's uploads. None when no lender fits."""
    if services.render_pdf is None:
        return None
    loan_type = PRODUCTS[file["product"]]
    lead = PrequalLead(credit_band=file["credit_band"], loan_amount=int(file["loan_amount"]),
                       property_state=file["property_state"], loan_type=loan_type.value)
    context = build_context(lead, services.fit_limits(fit), services.prequal_pct)
    if context is None:
        return None
    folder = Path(services.upload_dir) / str(file["id"])
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / "prequal.pdf"
    path.write_bytes(services.render_pdf(TEMPLATE, context))
    return str(path)


def route_file(db, file_id: int, now: Optional[datetime] = None) -> None:
    """Hand the file to the router. Until the overflow router exists, hold it as "awaiting router"."""
    record_status_change(db, file_id, "held", AWAITING_ROUTER_REASON, now=now)


def _josh_note(file: dict[str, Any], partner: Partner, fit: Optional[LenderFitResult]) -> str:
    fitting = len(fit.fitting) if fit is not None else 0
    return (f"New partner file #{file['id']} from {partner.name}"
            f"{f' ({partner.company})' if partner.company else ''}: "
            f"{PRODUCT_LABELS.get(file['product'], file['product'])}, ${int(file['loan_amount']):,}, "
            f"{file['property_state']}, borrower {file['borrower_last_name']}. "
            f"Lenders fitting: {fitting}. Status: held, awaiting router.")


def _place(address: Optional[str], last_name: str) -> str:
    return address or f"the {last_name} file"


def _send_partner(services: PipelineServices, partner: Partner, subject: str, body: str) -> bool:
    """Email the partner; also text them when they agreed to texts. True when at least one went out."""
    sent = False
    if partner.email and services.email is not None:
        try:
            services.email(partner.email, subject, body)
            sent = True
        except Exception as exc:
            logger.error("[partner-door] partner email failed partner=%s (%s)", partner.id, type(exc).__name__)
    if partner.sms_consent and partner.phone and services.sms is not None:
        try:
            services.sms(partner.phone, f"{subject}. {body}")
            sent = True
        except Exception as exc:
            logger.error("[partner-door] partner text failed partner=%s (%s)", partner.id, type(exc).__name__)
    return sent


def process_new_file(file_id: int, services: Optional[PipelineServices] = None,
                     session_factory: Optional[Callable[[], Any]] = None) -> None:
    """Background entry point after a partner submission commits. Never raises."""
    try:
        if session_factory is None:
            from src.lending.db import lending_session as session_factory
        services = services or live_services()
        _process(file_id, services, session_factory)
    except Exception as exc:
        logger.error("[partner-door] processing file=%s crashed (%s)", file_id, type(exc).__name__)


def _process(file_id: int, services: PipelineServices, session_factory: Callable[[], Any]) -> None:
    with session_factory() as db:
        file = db.execute(text("SELECT * FROM lending.loan_files WHERE id = :id"), {"id": file_id}).mappings().first()
        if file is None:
            logger.error("[partner-door] file=%s not found", file_id)
            return
        file = dict(file)
        partner = partner_by_id(db, int(file["partner_id"]))

    fit: Optional[LenderFitResult] = None
    try:
        fit = _score(services, file)
    except Exception as exc:
        logger.error("[partner-door] lender fit failed file=%s (%s)", file_id, type(exc).__name__)
    pdf_path: Optional[str] = None
    if fit is not None:
        try:
            pdf_path = _prequal_pdf(services, file, fit)
        except Exception as exc:
            logger.error("[partner-door] pre-qual PDF failed file=%s (%s)", file_id, type(exc).__name__)

    now = _utcnow()
    with session_factory() as db:
        db.execute(
            text("UPDATE lending.loan_files SET lender_fit = :fit, prequal_pdf_path = :pdf, updated_at = :now "
                 "WHERE id = :id"),
            {"fit": fit.model_dump_json() if fit is not None else None, "pdf": pdf_path, "now": now, "id": file_id},
        )
        route_file(db, file_id, now=now)

    if partner is None:
        return
    with session_factory() as db:
        flags = db.execute(text("SELECT josh_notified_at, partner_confirmed_at FROM lending.partner_submissions "
                                "WHERE file_id = :id"), {"id": file_id}).mappings().first() or {}
    if not flags.get("josh_notified_at") and services.slack is not None:
        try:
            services.slack(_josh_note(file, partner, fit))
            _stamp(session_factory, file_id, "josh_notified_at")
        except Exception as exc:
            logger.error("[partner-door] Slack note failed file=%s (%s)", file_id, type(exc).__name__)
    if not flags.get("partner_confirmed_at"):
        place = _place(file["property_address"], file["borrower_last_name"])
        body = (f"Hi {partner.name.split()[0]}, we received your file for {place} "
                f"(reference #{file['id']}). You can follow its status from the same link you used to submit it.")
        if _send_partner(services, partner, f"We received your file for {place}", body):
            _stamp(session_factory, file_id, "partner_confirmed_at")


def _stamp(session_factory: Callable[[], Any], file_id: int, column: str) -> None:
    if column not in {"josh_notified_at", "partner_confirmed_at"}:
        raise ValueError(column)
    with session_factory() as db:
        db.execute(text(f"UPDATE lending.partner_submissions SET {column} = :now WHERE file_id = :id"),
                   {"now": _utcnow(), "id": file_id})


def notify_partner_status(change: StatusChange, services: Optional[PipelineServices] = None,
                          session_factory: Optional[Callable[[], Any]] = None) -> bool:
    """Tell the partner their file moved (routed / declined / approved / funded). Never raises."""
    try:
        if session_factory is None:
            from src.lending.db import lending_session as session_factory
        services = services or live_services()
        with session_factory() as db:
            partner = partner_by_id(db, change.partner_id)
        if partner is None:
            return False
        place = _place(change.property_address, change.borrower_last_name)
        wording = _STATUS_WORDING.get(change.status, f"is now {change.status}")
        return _send_partner(services, partner, f"Update on {place}",
                             f"Your file for {place} (reference #{change.file_id}) {wording}.")
    except Exception as exc:
        logger.error("[partner-door] status notice failed file=%s (%s)", change.file_id, type(exc).__name__)
        return False
