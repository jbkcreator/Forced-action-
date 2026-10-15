"""Partner Door (P-1): what happens after a partner's file is saved, and partner status messages.

Runs after the submission has committed (a FastAPI background task), so the partner gets an
immediate answer and a failure here never loses the file; ``src.tasks.lending_partner_sweep`` reruns
a file whose run never finished (still 'received'). In order:

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
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional, Sequence

from sqlalchemy import text

from config.lender_matrix import LenderRules
from config.lending_partners import (
    AWAITING_ROUTER_REASON,
    CREDIT_BAND_MAX_FICO,
    CREDIT_BANDS,
    PRODUCT_LABELS,
    PRODUCTS,
    SOURCE_PARTNER,
)
from src.lending.contracts import BorrowerProfile, LenderFitResult, LoanRequest
from src.lending.partners import Partner, StatusChange, partner_by_id, record_status_change
from src.lending.prequal import TEMPLATE, FitLimits, PrequalLead, build_context
from src.lending.prequal_fit import fit_limits_from_result

logger = logging.getLogger(__name__)

EmailSender = Callable[[str, str, str], Any]  # (to, subject, body) -> provider message id
SmsSender = Callable[[str, str], Any]  # (phone, body) -> provider message id
SlackPoster = Callable[[str], None]
PdfRenderer = Callable[[str, dict], bytes]  # (template, context) -> PDF bytes
Matrix = Sequence[LenderRules]
FitEvaluator = Callable[[BorrowerProfile, LoanRequest, Matrix], LenderFitResult]
FitLimitsLookup = Callable[[LenderFitResult, Matrix], list[FitLimits]]  # fitting lenders -> their loan-amount limits
ContactCheck = Callable[[str], bool]  # phone or email -> True when blocked / allowed (see field)
MatrixLoader = Callable[[], Matrix]  # the lender rules in force (config, or the DB table when LENDING_RULES_FROM_DB)

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
    load_matrix: MatrixLoader
    phone_opted_out: ContactCheck  # STOP / suppression list: never text this number
    email_opted_out: ContactCheck  # email opt-out: never email this address
    text_window_open: ContactCheck  # 8am-8pm in the recipient's timezone(s); closed = email instead
    upload_dir: str
    prequal_pct: int


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def live_services() -> PipelineServices:
    from config.settings import get_settings
    from src.lending.ghl_email import get_email_sender
    from src.lending.ghl_sms import get_sender
    from src.lending.lender_fit import evaluate_lender_fit
    from src.lending.compliance import text_window_open
    from src.lending.lender_rules_store import current_matrix
    from src.lending.pdf.render import NON_BINDING_PREQUAL_WATERMARK, render_pdf

    settings = get_settings()
    email_sender, sms_sender = get_email_sender(), get_sender()

    def slack(message: str) -> None:
        channel, token = settings.lending_dial_tasks_channel, settings.lending_slack_bot_token  # Josh's work channel; R-1 posts here too
        if not (channel and token):
            logger.info("[partner-door] Slack note skipped: LENDING_DIAL_TASKS_CHANNEL or bot token not set")
            return
        from slack_sdk import WebClient

        WebClient(token=token.get_secret_value()).chat_postMessage(channel=channel, text=message)

    return PipelineServices(
        email=email_sender,
        sms=sms_sender,
        slack=slack,
        render_pdf=lambda template, context: render_pdf(template, context, watermark=NON_BINDING_PREQUAL_WATERMARK),
        evaluate_fit=lambda borrower, loan, matrix: evaluate_lender_fit(borrower, loan, matrix=tuple(matrix)),
        fit_limits=fit_limits_from_result,
        load_matrix=current_matrix,
        phone_opted_out=_phone_opted_out,
        email_opted_out=_email_opted_out,
        text_window_open=lambda phone: text_window_open(phone),
        upload_dir=settings.lending_partner_upload_dir,
        prequal_pct=settings.lending_prequal_range_pct,
    )


def _phone_opted_out(phone: str) -> bool:
    from src.lending.compliance import _suppressed_phones
    from src.lending.db import lending_session

    with lending_session() as db:
        return bool(_suppressed_phones(db, [phone]))


def _email_opted_out(email: str) -> bool:
    from src.lending.db import lending_session
    from src.services.email_suppression import is_email_suppressed

    with lending_session() as db:
        return is_email_suppressed(db, email)


def _allowed(check: ContactCheck, value: str, partner_id: int, what: str) -> bool:
    """Run one opt-out check; a failing check blocks the send (fail closed)."""
    try:
        return not check(value)
    except Exception as exc:
        logger.error("[partner-door] %s opt-out check failed partner=%s (%s)", what, partner_id, type(exc).__name__)
        return False


def _money(value: Optional[int]) -> Optional[Decimal]:
    return Decimal(value) if value is not None else None


def _score(services: PipelineServices, file: dict[str, Any], matrix: Matrix) -> Optional[LenderFitResult]:
    loan_type = PRODUCTS.get(file["product"])
    if loan_type is None:
        return None
    band = file["credit_band"] or ""
    borrower = BorrowerProfile(credit_band_min_fico=CREDIT_BANDS.get(band), credit_band_max_fico=CREDIT_BAND_MAX_FICO.get(band),
                               completed_projects=file["completed_projects"], state=file["property_state"])
    loan = LoanRequest(loan_type=loan_type, loan_amount=Decimal(file["loan_amount"]), state=file["property_state"],
                       purchase_price=_money(file["purchase_price"]), rehab_budget=_money(file["rehab_budget"]),
                       arv=_money(file["arv"]), property_address=file["property_address"])
    return services.evaluate_fit(borrower, loan, matrix)


def _prequal_pdf(services: PipelineServices, file: dict[str, Any], fit: LenderFitResult,
                 matrix: Matrix) -> Optional[str]:
    """Render the pre-qual letter and store it next to the file's uploads. None when no lender fits.

    Same rule as T-07 (``prequal_fit.get_fit_evaluator``): while no lender in the matrix is verified,
    no letter is produced, so a letter never rests on unconfirmed lender values alone.
    """
    if services.render_pdf is None or not any(rules.verified for rules in matrix):
        return None
    loan_type = PRODUCTS[file["product"]]
    lead = PrequalLead(credit_band=file["credit_band"], loan_amount=int(file["loan_amount"]),
                       property_state=file["property_state"], loan_type=loan_type.value)
    context = build_context(lead, services.fit_limits(fit, matrix), services.prequal_pct)
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
    possible = len(fit.possible) if fit is not None else 0
    return (f"New partner file #{file['id']} from {partner.name}"
            f"{f' ({partner.company})' if partner.company else ''}: "
            f"{PRODUCT_LABELS.get(file['product'], file['product'])}, ${int(file['loan_amount']):,}, "
            f"{file['property_state']}, borrower {file['borrower_last_name']}. "
            f"Lenders fitting: {fitting}, possible (confirm on call): {possible}. Status: held, awaiting router.")


def _place(address: Optional[str], last_name: str) -> str:
    return address or f"the {last_name} file"


def _send_partner(services: PipelineServices, partner: Partner, subject: str, body: str) -> bool:
    """Text the partner when they agreed to texts, otherwise email them. A number that opted out (STOP),
    a closed text window (8am-8pm their time) or a failed text falls back to email; an opted-out email
    address is never emailed. True when a message went out."""
    can_text = (partner.sms_consent and bool(partner.phone) and services.sms is not None
                and _allowed(services.phone_opted_out, partner.phone, partner.id, "phone"))
    if can_text and not _window_open(services, partner):
        logger.info("[partner-door] text window closed partner=%s; emailing instead", partner.id)
        can_text = False
    if can_text:
        try:
            services.sms(partner.phone, f"{subject}. {body}")
            return True
        except Exception as exc:
            logger.error("[partner-door] partner text failed partner=%s (%s)", partner.id, type(exc).__name__)
    if (partner.email and services.email is not None
            and _allowed(services.email_opted_out, partner.email, partner.id, "email")):
        try:
            services.email(partner.email, subject, body)
            return True
        except Exception as exc:
            logger.error("[partner-door] partner email failed partner=%s (%s)", partner.id, type(exc).__name__)
    return False


def _window_open(services: PipelineServices, partner: Partner) -> bool:
    try:
        return services.text_window_open(partner.phone)
    except Exception as exc:
        logger.error("[partner-door] text window check failed partner=%s (%s)", partner.id, type(exc).__name__)
        return False


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
    matrix: Matrix = ()
    try:
        matrix = services.load_matrix()
        fit = _score(services, file, matrix)
    except Exception as exc:
        logger.error("[partner-door] lender fit failed file=%s (%s)", file_id, type(exc).__name__)
    pdf_path: Optional[str] = None
    if fit is not None:
        try:
            pdf_path = _prequal_pdf(services, file, fit, matrix)
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


def change_status(file_id: int, status: str, reason: Optional[str] = None, services: Optional[PipelineServices] = None,
                  session_factory: Optional[Callable[[], Any]] = None) -> Optional[StatusChange]:
    """Move a file to ``status``, commit, then tell the partner. The one call R-1 makes on every route,
    decline, approval and funding, so no status change can skip the partner notice."""
    if session_factory is None:
        from src.lending.db import lending_session as session_factory
    with session_factory() as db:
        change = record_status_change(db, file_id, status, reason)
    if change is not None:
        notify_partner_status(change, services=services, session_factory=session_factory)
    return change


def unprocessed_file_ids(db, older_than_minutes: int, limit: int, now: Optional[datetime] = None) -> list[int]:
    """Partner files still 'received' after ``older_than_minutes``: their background run never finished."""
    rows = db.execute(
        text("SELECT id FROM lending.loan_files WHERE source = :source AND status = 'received' "
             "AND created_at < :cutoff ORDER BY created_at LIMIT :limit"),
        {"source": SOURCE_PARTNER, "cutoff": (now or _utcnow()) - timedelta(minutes=older_than_minutes), "limit": limit},
    ).all()
    return [int(row[0]) for row in rows]
