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
SlackPoster = Callable[["SlackNote"], None]
PdfRenderer = Callable[[str, dict], bytes]  # (template, context) -> PDF bytes
Matrix = Sequence[LenderRules]
FitEvaluator = Callable[[BorrowerProfile, LoanRequest, Matrix], LenderFitResult]
FitLimitsLookup = Callable[[LenderFitResult, Matrix], list[FitLimits]]  # fitting lenders -> their loan-amount limits
ContactCheck = Callable[[str], bool]  # phone -> True when the text window is open
MatrixLoader = Callable[[], Matrix]
LetterSender = Callable[["Partner", dict, bytes], None]  # emails the pre-qual PDF to the partner  # the lender rules in force (config, or the DB table when LENDING_RULES_FROM_DB)

_STATUS_WORDING = {
    "routed": "has been sent to a lender for review",
    "declined": "was declined by a lender; we are looking at the next option",
    "approved": "was approved by a lender",
    "funded": "has funded. Thank you",
}


@dataclass(frozen=True)
class SlackNote:
    """``text`` is the notification / fallback line; ``blocks`` is the card Slack shows."""

    text: str
    blocks: list[dict[str, Any]]


@dataclass(frozen=True)
class OptOuts:
    phone: bool
    email: bool


OptOutLookup = Callable[[Optional[str], Optional[str]], OptOuts]


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
    opt_outs: OptOutLookup  # (phone, email) -> which of them opted out, read in one session
    text_window_open: ContactCheck  # 8am-8pm in the recipient's timezone(s); closed = email instead
    upload_dir: str
    prequal_pct: int
    send_letter: Optional[LetterSender] = None  # None = GHL account not configured


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

    from src.lending.partner_ghl import get_partner_letter_sink

    settings = get_settings()
    email_sender, sms_sender = get_email_sender(), get_sender()
    letters = get_partner_letter_sink()

    def slack(note: SlackNote) -> None:
        channel, token = settings.lending_partner_channel, settings.lending_slack_bot_token  # #ndl-partner-deals
        if not (channel and token):
            logger.info("[partner-door] Slack note skipped: LENDING_PARTNER_CHANNEL or bot token not set")
            return
        from slack_sdk import WebClient

        WebClient(token=token.get_secret_value()).chat_postMessage(channel=channel, text=note.text, blocks=note.blocks)

    return PipelineServices(
        email=email_sender,
        sms=sms_sender,
        slack=slack,
        render_pdf=lambda template, context: render_pdf(template, context, watermark=NON_BINDING_PREQUAL_WATERMARK),
        evaluate_fit=lambda borrower, loan, matrix: evaluate_lender_fit(borrower, loan, matrix=tuple(matrix)),
        fit_limits=fit_limits_from_result,
        load_matrix=current_matrix,
        opt_outs=_opt_outs,
        text_window_open=lambda phone: text_window_open(phone),
        upload_dir=settings.lending_partner_upload_dir,
        prequal_pct=settings.lending_prequal_range_pct,
        send_letter=(lambda partner, file, pdf: letters.send_letter(
            partner.email, partner.name, _place(file["property_address"], file["borrower_last_name"]), file["id"], pdf))
        if letters else None,
    )


def _opt_outs(phone: Optional[str], email: Optional[str]) -> OptOuts:
    from src.lending.compliance import is_phone_suppressed
    from src.lending.db import lending_session
    from src.services.email_suppression import is_email_suppressed

    with lending_session() as db:
        return OptOuts(phone=bool(phone) and is_phone_suppressed(db, phone),
                       email=bool(email) and is_email_suppressed(db, email))


def _read_opt_outs(services: PipelineServices, partner: Partner) -> Optional[OptOuts]:
    """Both opt-outs for the partner; None when the lookup fails, which blocks both channels (fail closed)."""
    try:
        return services.opt_outs(partner.phone, partner.email)
    except Exception as exc:
        logger.error("[partner-door] opt-out check failed partner=%s (%s)", partner.id, type(exc).__name__)
        return None


def _stored_fit(value: Any) -> Optional[LenderFitResult]:
    """``lender_fit`` as read back: Postgres jsonb arrives decoded (a dict), SQLite as JSON text."""
    if not value:
        return None
    return LenderFitResult.model_validate(value) if isinstance(value, dict) else LenderFitResult.model_validate_json(value)


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


def _mrkdwn(value: Any) -> str:
    """Escape the three characters Slack mrkdwn treats as markup."""
    return str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _josh_note(file: dict[str, Any], partner: Partner, fit: Optional[LenderFitResult]) -> SlackNote:
    fitting = len(fit.fitting) if fit is not None else 0
    possible = len(fit.possible) if fit is not None else 0
    who = partner.name + (f" · {partner.company}" if partner.company else "")
    loan = f"{PRODUCT_LABELS.get(file['product'], file['product'])} · ${int(file['loan_amount']):,}"
    lender_fit = f"{fitting} fit · {possible} possible" if fit is not None else "Not scored"
    text = f"New partner file #{file['id']} from {who}: {loan}, {file['property_state']}, borrower {file['borrower_last_name']}"
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn",
                                     "text": f"*New partner file  #{file['id']}*\n{_mrkdwn(who)}"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Loan*\n{_mrkdwn(loan)}"},
            {"type": "mrkdwn", "text": f"*State*\n{_mrkdwn(file['property_state'])}"},
            {"type": "mrkdwn", "text": f"*Borrower*\n{_mrkdwn(file['borrower_last_name'])}"},
            {"type": "mrkdwn", "text": f"*Lender fit*\n{lender_fit}"},
        ]},
        {"type": "context", "elements": [{"type": "mrkdwn",
                                          "text": "Held, awaiting router  ·  Borrower not contacted (partner consent only)"}]},
    ]
    return SlackNote(text=text, blocks=blocks)


def _place(address: Optional[str], last_name: str) -> str:
    return address or f"the {last_name} file"


@dataclass(frozen=True)
class Delivery:
    emailed: bool
    texted: bool
    complete: bool  # every channel the partner should get has gone out; nothing left to retry


def _deliver(services: PipelineServices, partner: Partner, subject: str, body: str, *,
             emailed: bool = False, texted: bool = False) -> Delivery:
    """Email the partner, and also text them when they agreed to texts and it is 8am-8pm their time. An opted-out
    address or number (STOP) is never used. ``emailed`` / ``texted`` say which channel already went out, so a retry
    sends only the missing one. Complete = the email went out; for a partner with no usable email, the text did."""
    opted_out = _read_opt_outs(services, partner)
    if opted_out is None:
        return Delivery(emailed, texted, complete=False)
    can_email = bool(partner.email) and services.email is not None and not opted_out.email
    can_text = bool(partner.sms_consent and partner.phone) and services.sms is not None and not opted_out.phone
    if can_email and not emailed:
        try:
            services.email(partner.email, subject, body)
            emailed = True
        except Exception as exc:
            logger.error("[partner-door] partner email failed partner=%s (%s)", partner.id, type(exc).__name__)
    if can_text and not texted and _window_open(services, partner):
        try:
            services.sms(partner.phone, f"{subject}. {body}")
            texted = True
        except Exception as exc:
            logger.error("[partner-door] partner text failed partner=%s (%s)", partner.id, type(exc).__name__)
    complete = emailed if can_email else (texted if can_text else True)
    return Delivery(emailed, texted, complete)


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

    file["prequal_pdf_path"] = pdf_path
    _after_routing(file, partner, fit, services, session_factory)


def _after_routing(file: dict[str, Any], partner: Optional[Partner], fit: Optional[LenderFitResult],
                   services: PipelineServices, session_factory: Callable[[], Any]) -> None:
    """Josh's Slack card, the partner's confirmation and the pre-qual letter to the partner.
    Each is done once (stamped on success); the sweep retries whatever is still unstamped."""
    if partner is None:
        return
    file_id = file["id"]
    with session_factory() as db:
        flags = db.execute(text("SELECT josh_notified_at, partner_confirmed_at, partner_emailed_at, partner_texted_at, "
                                "prequal_sent_at FROM lending.partner_submissions WHERE file_id = :id"),
                           {"id": file_id}).mappings().first() or {}
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
        sent = _deliver(services, partner, f"We received your file for {place}", body,
                        emailed=bool(flags.get("partner_emailed_at")), texted=bool(flags.get("partner_texted_at")))
        _record_confirmation(session_factory, file_id, sent)
    if not flags.get("prequal_sent_at") and file.get("prequal_pdf_path") and services.send_letter is not None:
        _send_letter(services, file, partner, session_factory)


def _record_confirmation(session_factory: Callable[[], Any], file_id: int, sent: Delivery) -> None:
    with session_factory() as db:
        db.execute(text("UPDATE lending.partner_submissions SET "
                        "partner_emailed_at = CASE WHEN :e THEN COALESCE(partner_emailed_at, :now) END, "
                        "partner_texted_at = CASE WHEN :t THEN COALESCE(partner_texted_at, :now) END, "
                        "partner_confirmed_at = CASE WHEN :c THEN :now END WHERE file_id = :id"),
                   {"e": sent.emailed, "t": sent.texted, "c": sent.complete, "now": _utcnow(), "id": file_id})


def _send_letter(services: PipelineServices, file: dict[str, Any], partner: Partner,
                 session_factory: Callable[[], Any]) -> None:
    """Email the pre-qual PDF to the partner, never the borrower. A send whose outcome is unknown is stamped
    anyway so the partner never gets the letter twice."""
    from src.lending.prequal_ghl import SendOutcomeUnknown

    opted_out = _read_opt_outs(services, partner)
    if not partner.email or opted_out is None or opted_out.email:
        return
    try:
        services.send_letter(partner, file, Path(file["prequal_pdf_path"]).read_bytes())
    except SendOutcomeUnknown as exc:
        logger.error("[partner-door] pre-qual letter outcome unknown file=%s (%s); not retried", file["id"], exc)
    except Exception as exc:
        logger.error("[partner-door] pre-qual letter failed file=%s (%s)", file["id"], type(exc).__name__)
        return
    _stamp(session_factory, file["id"], "prequal_sent_at")


def send_pending_notices(file_id: int, services: Optional[PipelineServices] = None,
                         session_factory: Optional[Callable[[], Any]] = None) -> None:
    """Retry the new-file notices for a file that was already scored and routed. Never raises."""
    try:
        if session_factory is None:
            from src.lending.db import lending_session as session_factory
        services = services or live_services()
        with session_factory() as db:
            file = db.execute(text("SELECT * FROM lending.loan_files WHERE id = :id"), {"id": file_id}).mappings().first()
            if file is None:
                return
            file = dict(file)
            partner = partner_by_id(db, int(file["partner_id"]))
        _after_routing(file, partner, _stored_fit(file["lender_fit"]), services, session_factory)
    except Exception as exc:
        logger.error("[partner-door] notice retry failed file=%s (%s)", file_id, type(exc).__name__)


def _stamp(session_factory: Callable[[], Any], file_id: int, column: str) -> None:
    if column not in {"josh_notified_at", "prequal_sent_at"}:
        raise ValueError(column)
    with session_factory() as db:
        db.execute(text(f"UPDATE lending.partner_submissions SET {column} = :now WHERE file_id = :id"),
                   {"now": _utcnow(), "id": file_id})


def notify_partner_status(change: StatusChange, services: Optional[PipelineServices] = None,
                          session_factory: Optional[Callable[[], Any]] = None) -> bool:
    """Tell the partner their file moved (routed / declined / approved / funded). ``record_status_change`` queued
    the notice on the file (``notice_status``); it is cleared only once every required channel went out, so the
    sweep retries a failed send without the status changing again. True when complete. Never raises."""
    try:
        if session_factory is None:
            from src.lending.db import lending_session as session_factory
        services = services or live_services()
        with session_factory() as db:
            partner = partner_by_id(db, change.partner_id)
            pending = db.execute(text("SELECT notice_status, notice_emailed_at, notice_texted_at FROM lending.loan_files "
                                      "WHERE id = :id"), {"id": change.file_id}).mappings().first()
        if partner is None or pending is None or pending["notice_status"] != change.status:
            return False
        place = _place(change.property_address, change.borrower_last_name)
        wording = _STATUS_WORDING.get(change.status, f"is now {change.status}")
        sent = _deliver(services, partner, f"Update on {place}",
                        f"Your file for {place} (reference #{change.file_id}) {wording}.",
                        emailed=bool(pending["notice_emailed_at"]), texted=bool(pending["notice_texted_at"]))
        with session_factory() as db:
            db.execute(text("UPDATE lending.loan_files SET "
                            "notice_emailed_at = CASE WHEN :e THEN COALESCE(notice_emailed_at, :now) END, "
                            "notice_texted_at = CASE WHEN :t THEN COALESCE(notice_texted_at, :now) END, "
                            "notice_status = CASE WHEN :c THEN NULL ELSE notice_status END "
                            "WHERE id = :id AND notice_status = :status"),
                       {"e": sent.emailed, "t": sent.texted, "c": sent.complete, "now": _utcnow(),
                        "id": change.file_id, "status": change.status})
        return sent.complete
    except Exception as exc:
        logger.error("[partner-door] status notice failed file=%s (%s)", change.file_id, type(exc).__name__)
        return False


def retry_status_notice(file_id: int, services: Optional[PipelineServices] = None,
                        session_factory: Optional[Callable[[], Any]] = None) -> bool:
    """Resend the queued status notice for a file (sweep). Never raises."""
    try:
        if session_factory is None:
            from src.lending.db import lending_session as session_factory
        with session_factory() as db:
            row = db.execute(text("SELECT partner_id, notice_status, property_address, borrower_last_name "
                                  "FROM lending.loan_files WHERE id = :id AND partner_id IS NOT NULL"),
                             {"id": file_id}).mappings().first()
        if row is None or not row["notice_status"]:
            return False
        change = StatusChange(file_id, int(row["partner_id"]), row["notice_status"], None, row["property_address"],
                              row["borrower_last_name"])
        return notify_partner_status(change, services=services, session_factory=session_factory)
    except Exception as exc:
        logger.error("[partner-door] status notice retry failed file=%s (%s)", file_id, type(exc).__name__)
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


def pending_status_notice_ids(db, older_than_minutes: int, max_age_hours: int, limit: int,
                              now: Optional[datetime] = None) -> list[int]:
    """Partner files whose last status notice has not fully gone out, retried until ``max_age_hours`` after the
    status change."""
    now = now or _utcnow()
    rows = db.execute(
        text("SELECT id FROM lending.loan_files WHERE source = :source AND notice_status IS NOT NULL "
             "AND status_at < :cutoff AND status_at > :oldest ORDER BY status_at LIMIT :limit"),
        {"source": SOURCE_PARTNER, "cutoff": now - timedelta(minutes=older_than_minutes),
         "oldest": now - timedelta(hours=max_age_hours), "limit": limit},
    ).all()
    return [int(row[0]) for row in rows]


def unprocessed_file_ids(db, older_than_minutes: int, limit: int, now: Optional[datetime] = None) -> list[int]:
    """Partner files still 'received' after ``older_than_minutes``: their background run never finished."""
    rows = db.execute(
        text("SELECT id FROM lending.loan_files WHERE source = :source AND status = 'received' "
             "AND created_at < :cutoff ORDER BY created_at LIMIT :limit"),
        {"source": SOURCE_PARTNER, "cutoff": (now or _utcnow()) - timedelta(minutes=older_than_minutes), "limit": limit},
    ).all()
    return [int(row[0]) for row in rows]


def unnotified_file_ids(db, older_than_minutes: int, max_age_hours: int, limit: int,
                        now: Optional[datetime] = None, *, letters: bool = False) -> list[int]:
    """Partner files already past 'received' with something still unsent: the Slack card, the partner
    confirmation, and (when configured) the pre-qual letter to a partner with an email.
    Retried until ``max_age_hours`` after the file arrived."""
    now = now or _utcnow()
    rows = db.execute(
        text("SELECT f.id FROM lending.loan_files f JOIN lending.partner_submissions s ON s.file_id = f.id "
             "JOIN lending.partners p ON p.id = f.partner_id "
             "WHERE f.source = :source AND f.status <> 'received' "
             "AND (s.josh_notified_at IS NULL OR s.partner_confirmed_at IS NULL "
             "     OR (:letters AND f.prequal_pdf_path IS NOT NULL AND s.prequal_sent_at IS NULL "
             "         AND p.email IS NOT NULL)) "
             "AND f.created_at < :cutoff AND f.created_at > :oldest ORDER BY f.created_at LIMIT :limit"),
        {"source": SOURCE_PARTNER, "cutoff": now - timedelta(minutes=older_than_minutes),
         "oldest": now - timedelta(hours=max_age_hours), "limit": limit, "letters": letters},
    ).all()
    return [int(row[0]) for row in rows]
