"""Lending Engine Wave 0 — compliance floor tables (schema ``lending``).

Own ``MetaData`` on purpose: keeps these tables out of ``Base.metadata`` so FA's
SQLite ``create_all`` fixtures never see a schema-qualified table. DDL is applied
by ``migrations/apply_lending_compliance.py`` (ADR 0024); see
lender-engine/dev2-compliance-floor/adr/0001-lending-schema-in-shared-fa-db.md.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any, Optional

from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    ForeignKey,
    DateTime,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
    SmallInteger,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from config.lending_text_back import SLOT_HOLDING_SQL

LENDING_SCHEMA = "lending"


class LendingBase(DeclarativeBase):
    metadata = MetaData(schema=LENDING_SCHEMA)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class LendingSuppression(LendingBase):
    """Permanent opt-outs, litigators, explicit exclusions (spec §3.2)."""

    __tablename__ = "suppression_list"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    phone: Mapped[Optional[str]] = mapped_column(String(20), unique=True)  # phone_utils.normalize
    email: Mapped[Optional[str]] = mapped_column(String(255), unique=True)  # lower-cased
    reason: Mapped[str] = mapped_column(String(30), nullable=False)  # SuppressionReason: OPT_OUT / LITIGATOR / WARM_NETWORK
    source_channel: Mapped[str] = mapped_column(String(30), nullable=False)  # sms / email / dialer / backfill:*
    source_ref: Mapped[Optional[str]] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))

    __table_args__ = (
        CheckConstraint("phone IS NOT NULL OR email IS NOT NULL", name="ck_lending_suppression_identifier"),
    )


class LendingContact(LendingBase):
    """One dialable phone. ``last_dnc_scrub`` mirrors dnc_phone_checks.checked_at (spec §4.2)."""

    __tablename__ = "contacts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    phone: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)
    phone_hash: Mapped[Optional[str]] = mapped_column(String(64), index=True)  # joins opt_out_events.phone_hash
    email: Mapped[Optional[str]] = mapped_column(String(255))
    last_dnc_scrub: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    line_type: Mapped[Optional[str]] = mapped_column(String(20))  # Tracerfy phone_type (mobile/landline)
    do_not_contact: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    nurture: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))  # phone blocked by DNC; email nurture only
    pinned_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))  # after-hours voicemail: call first next morning
    pinned_until: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))  # pin also ends at the first outbound attempt
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))


class LendingOptOutEvent(LendingBase):
    """One opt-out and its per-store propagation timestamps (60 s evidence, spec §3.2)."""

    __tablename__ = "opt_out_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    channel: Mapped[str] = mapped_column(String(20), nullable=False)  # sms / email / dialer
    source_ref: Mapped[Optional[str]] = mapped_column(String(100))  # call id / message id
    phone_hash: Mapped[Optional[str]] = mapped_column(String(64))  # never raw phone
    actor: Mapped[Optional[str]] = mapped_column(String(100))  # caller seat
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    suppression_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    sms_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    email_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    dialer_removed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    ghl_dnd_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))  # GHL do-not-disturb set
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending", server_default="pending")  # OptOutStatus
    fa_table: Mapped[Optional[str]] = mapped_column(String(20))  # sms_opt_outs / email_opt_outs (poller origin)
    fa_row_id: Mapped[Optional[int]] = mapped_column(Integer)

    __table_args__ = (
        Index("idx_lending_opt_out_events_received", "received_at"),
        Index("idx_lending_opt_out_events_status", "status"),
        Index("uq_lending_opt_out_events_fa_row", "fa_table", "fa_row_id", unique=True,
              postgresql_where=text("fa_row_id IS NOT NULL")),
    )


class LendingLoadExclusion(LendingBase):
    """Why a pool record was not loaded — proves A2 from stored data."""

    __tablename__ = "load_exclusions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False)
    phone_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    reason: Mapped[str] = mapped_column(String(40), nullable=False)  # ReasonCode
    detail: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))

    __table_args__ = (Index("idx_lending_load_exclusions_run", "run_id"),)


class LendingDncScrub(LendingBase):
    """Tracerfy results fetched by lending (spec §4.2). FA's dnc_phone_checks is
    read-only for lending (least privilege); both are consulted, freshest wins."""

    __tablename__ = "dnc_scrubs"

    phone: Mapped[str] = mapped_column(String(20), primary_key=True)  # phone_utils.normalize
    national_dnc: Mapped[bool] = mapped_column(Boolean, nullable=False)
    litigator: Mapped[bool] = mapped_column(Boolean, nullable=False)
    state_dnc: Mapped[bool] = mapped_column(Boolean, nullable=False)
    line_type: Mapped[Optional[str]] = mapped_column(String(20))
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    raw_result: Mapped[Optional[dict]] = mapped_column(JSONB)

    __table_args__ = (Index("idx_lending_dnc_scrubs_checked_at", "checked_at"),)


class LendingDialerHold(LendingBase):
    """A contact lending pulled from the dialer *temporarily* (calling window
    or attempt cap). The sweep restores only open holds, and never a suppressed
    number. Opt-out removals are permanent and never create a hold."""

    __tablename__ = "dialer_holds"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    phone: Mapped[str] = mapped_column(String(20), nullable=False)  # phone_utils.normalize
    reason: Mapped[str] = mapped_column(String(20), nullable=False)  # RemovalReason: call_window / attempt_cap
    held_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    released_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    release_reason: Mapped[Optional[str]] = mapped_column(String(20))  # restored / suppressed

    __table_args__ = (
        Index("uq_lending_dialer_holds_open", "phone", unique=True, postgresql_where=text("released_at IS NULL")),
    )


class LendingDialerLoadRecord(LendingBase):
    """One pool record loaded into the dialer, and what the caller sees for it.

    At most one active row per phone, so a call-time lookup by phone resolves
    to a single record. Earlier loads stay as inactive history; call events
    resolve by dialer_contact_id, or by the latest row loaded before the call.
    """

    __tablename__ = "dialer_load_records"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False)
    pool: Mapped[str] = mapped_column(String(30), nullable=False)
    source_record_ref: Mapped[str] = mapped_column(String(100), nullable=False)
    phone: Mapped[str] = mapped_column(String(20), nullable=False)  # phone_utils.normalize (E.164)
    phone_hash: Mapped[str] = mapped_column(String(64), nullable=False)  # joins load_exclusions / opt_out_events
    campaign_tag: Mapped[Optional[str]] = mapped_column(String(40))
    borrower_name: Mapped[Optional[str]] = mapped_column(Text)
    entity_name: Mapped[Optional[str]] = mapped_column(Text)
    property_address: Mapped[Optional[str]] = mapped_column(Text)
    estimated_loan_value: Mapped[Optional[float]] = mapped_column(Numeric(14, 2))
    recent_permit_details: Mapped[Optional[str]] = mapped_column(Text)
    dialer_contact_id: Mapped[Optional[str]] = mapped_column(String(64))
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text("true"))
    loaded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    deactivated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    deactivation_reason: Mapped[Optional[str]] = mapped_column(String(40))

    __table_args__ = (
        Index("uq_lending_dialer_load_records_active_phone", "phone", unique=True,
              postgresql_where=text("active")),
        Index("idx_lending_dialer_load_records_phone_loaded", "phone", "loaded_at"),
        Index("idx_lending_dialer_load_records_run", "run_id"),
        Index("idx_lending_dialer_load_records_contact", "dialer_contact_id",
              postgresql_where=text("dialer_contact_id IS NOT NULL")),
        CheckConstraint("active OR deactivated_at IS NOT NULL", name="ck_lending_dialer_load_deactivated_at"),
    )


class LendingDialerUnconfirmedCreate(LendingBase):
    """A dialer create that failed ambiguously (timeout / 5xx): the contact may exist with no
    load row, so an opt-out cannot be reported complete for this phone until someone checks the
    dialer and sets ``resolved_at``."""

    __tablename__ = "dialer_unconfirmed_creates"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False)
    phone: Mapped[str] = mapped_column(String(20), nullable=False)
    phone_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    source_record_ref: Mapped[str] = mapped_column(String(100), nullable=False)
    error_status: Mapped[Optional[int]] = mapped_column()  # HTTP status; NULL for a network error
    attempted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    resolution_note: Mapped[Optional[str]] = mapped_column(Text)

    __table_args__ = (
        Index("idx_lending_dialer_unconfirmed_creates_open", "phone", postgresql_where=text("resolved_at IS NULL")),
    )


class LendingCallDisposition(LendingBase):
    """One dialer call: attempt record, result code and delivery state (spec §4.4).

    Vendor-neutral on purpose: ``dialer_call_id`` / ``dialer_contact_id`` are whatever
    the dialer calls them. The disposition is validated in code (config/lending_dispositions),
    not by a CHECK, so an unknown code is stored raw in ``disposition_raw`` and never dropped.
    """

    __tablename__ = "call_dispositions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    dialer_call_id: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    direction: Mapped[Optional[str]] = mapped_column(String(20))
    phone: Mapped[Optional[str]] = mapped_column(String(20))
    caller_seat: Mapped[Optional[str]] = mapped_column(String(100))
    caller_name: Mapped[Optional[str]] = mapped_column(String(200))
    caller_id_number: Mapped[Optional[str]] = mapped_column(String(50))  # outbound DID shown to the borrower
    campaign_tag: Mapped[Optional[str]] = mapped_column(String(50))
    dialer_contact_id: Mapped[Optional[str]] = mapped_column(String(64))
    disposition: Mapped[Optional[str]] = mapped_column(String(50))  # a config DISPOSITIONS code, else NULL
    disposition_raw: Mapped[Optional[str]] = mapped_column(String(100))  # exactly what the dialer sent
    disposition_list_version: Mapped[Optional[str]] = mapped_column(String(20))
    unfunded_cause: Mapped[Optional[str]] = mapped_column(String(30))
    unfunded_cause_provisional: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    talk_duration_sec: Mapped[Optional[int]] = mapped_column()
    call_started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    call_ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    disposition_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    disposition_missing_alerted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    recording_disclosure_logged: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    recording_ref: Mapped[Optional[str]] = mapped_column(String(500))
    booking_blocked: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    sheet_synced_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    sheet_synced_disposition: Mapped[Optional[str]] = mapped_column(String(50))
    slack_posted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    slack_posted_disposition: Mapped[Optional[str]] = mapped_column(String(50))
    slack_ts: Mapped[Optional[str]] = mapped_column(String(50))
    opt_out_propagated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    raw_event: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    # Go Live call log (WP-GL-1)
    queue: Mapped[Optional[str]] = mapped_column(String(30))
    source_tag: Mapped[Optional[str]] = mapped_column(String(40))
    seat_group: Mapped[Optional[str]] = mapped_column(String(10))
    consent_checked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))  # last on-call "yes" read
    recording_status: Mapped[Optional[str]] = mapped_column(String(12))  # pending | readable | forbidden | missing
    recording_checked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    dialer_campaign_id: Mapped[Optional[str]] = mapped_column(String(64))  # BatchDialer campaign (scoreboard)

    __table_args__ = (
        Index("idx_lending_call_dispositions_phone_ended", "phone", "call_ended_at"),
        Index("idx_lending_call_dispositions_seat_ended", "caller_seat", "call_ended_at"),
        Index("idx_lending_call_dispositions_dialer_campaign_ended", "dialer_campaign_id", "call_ended_at"),
        Index(
            "idx_lending_call_dispositions_undelivered",
            "disposition_at",
            postgresql_where=text(
                "sheet_synced_disposition IS DISTINCT FROM disposition "
                "OR slack_posted_disposition IS DISTINCT FROM disposition"
            ),
        ),
    )


class LendingMissedCallEvent(LendingBase):
    """An unanswered call, queued for the missed-call text (one per contact per ET day).

    #319 only records the event; the sender that consumes ``status='pending'`` is a separate task.
    """

    __tablename__ = "missed_call_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    dialer_call_id: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    phone: Mapped[str] = mapped_column(String(20), nullable=False)
    event_date_et: Mapped[date] = mapped_column(Date, nullable=False)
    caller_id_number: Mapped[Optional[str]] = mapped_column(String(50))
    property_address: Mapped[Optional[str]] = mapped_column(String(300))
    reason: Mapped[Optional[str]] = mapped_column(String(300))
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="pending", server_default="pending")
    # pending / sending / sent / send_unknown / dry_run / failed / blocked / duplicate_day / skipped_*
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    decided_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    template_key: Mapped[Optional[str]] = mapped_column(String(20))
    provider_message_id: Mapped[Optional[str]] = mapped_column(String(100))

    __table_args__ = (
        Index("uq_lending_missed_call_phone_day", "phone", "event_date_et", unique=True, postgresql_where=text(SLOT_HOLDING_SQL)),
        Index("idx_lending_missed_call_events_pending", "created_at", postgresql_where=text("status = 'pending'")),
    )


class LendingTextConsent(LendingBase):
    """Evidence that a number agreed to automated texts (client Q27). Revoked rows never gate-pass."""

    __tablename__ = "text_consents"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    phone: Mapped[str] = mapped_column(String(20), nullable=False)  # phone_utils.normalize
    source: Mapped[str] = mapped_column(String(20), nullable=False)  # inbound_call / inbound_text / web_form / on_call_yes
    call_id: Mapped[Optional[str]] = mapped_column(String(100))
    captured_by: Mapped[Optional[str]] = mapped_column(String(200))  # caller name for on_call_yes
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    revoked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    __table_args__ = (Index("uq_lending_text_consents_phone_source", "phone", "source", unique=True),)


class LendingGhlStageEvent(LendingBase):
    """A GHL opportunity entering a pipeline stage (workflow webhook). One row per opportunity + stage:
    a redelivered webhook is a no-op, so the scoreboard counts each stage entry once."""

    __tablename__ = "ghl_stage_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    ghl_opportunity_id: Mapped[str] = mapped_column(String(100), nullable=False)
    pipeline_id: Mapped[Optional[str]] = mapped_column(String(100))
    stage_name: Mapped[str] = mapped_column(String(100), nullable=False)  # as GHL sent it
    stage_key: Mapped[str] = mapped_column(String(100), nullable=False)  # lower-cased, for matching config
    phone: Mapped[Optional[str]] = mapped_column(String(20))  # phone_utils.normalize
    booked_by: Mapped[Optional[str]] = mapped_column(String(200))  # fallback when no BOOKED call matches the phone
    event_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    raw_event: Mapped[dict] = mapped_column(JSONB, nullable=False)

    __table_args__ = (
        Index("uq_lending_ghl_stage_events_opp_stage", "ghl_opportunity_id", "stage_key", unique=True),
        Index("idx_lending_ghl_stage_events_stage_at", "stage_key", "event_at"),
    )


class LendingMissedCallText(LendingBase):
    """One decision per unanswered call for the missed-call text (WP-GL-9): sent or why not."""

    __tablename__ = "missed_call_texts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    dialer_call_id: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    phone: Mapped[str] = mapped_column(String(20), nullable=False)  # phone_utils.normalize
    event_date_et: Mapped[datetime] = mapped_column(Date, nullable=False)
    outcome: Mapped[str] = mapped_column(String(30), nullable=False)  # sent / dry_run / skipped_* (incl. skipped_sms_gate)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))

    __table_args__ = (
        Index("uq_lending_missed_call_texts_sent_day", "phone", "event_date_et", unique=True,
              postgresql_where=text("outcome = 'sent'")),
    )


class LendingWebLead(LendingBase):
    """One nextdeallending.com form submission (WP-GL-11), stored before any GHL call.

    ``sms_consent`` / ``deal_drop_optin`` hold exactly what the visitor ticked, with the label
    text, page URL, IP and timestamp that make it evidence. A later unticked submission never
    edits an earlier row; every submission is its own row. ``ghl_status`` is the delivery
    state: pending -> synced / contact_only (no new-lead stage configured) / failed.
    """

    __tablename__ = "web_leads"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    phone: Mapped[str] = mapped_column(String(20), nullable=False)  # phone_utils.normalize
    email: Mapped[Optional[str]] = mapped_column(String(255))  # lower-cased
    property_city: Mapped[Optional[str]] = mapped_column(String(80))
    deal_type: Mapped[Optional[str]] = mapped_column(String(60))
    completed_projects_3y: Mapped[Optional[str]] = mapped_column(String(30))
    sms_consent: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    deal_drop_optin: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    consent_text: Mapped[Optional[str]] = mapped_column(Text)  # the label the visitor saw, verbatim
    consent_text_matches: Mapped[Optional[bool]] = mapped_column(Boolean)  # equals config.lending_web.SMS_CONSENT_TEXT
    page_url: Mapped[Optional[str]] = mapped_column(String(300))
    ip_address: Mapped[Optional[str]] = mapped_column(String(45))
    user_agent: Mapped[Optional[str]] = mapped_column(String(300))
    suppressed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    suppression_reason: Mapped[Optional[str]] = mapped_column(String(30))  # suppression_list | do_not_contact; NULL when not suppressed
    ghl_status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending", server_default=text("'pending'"))
    ghl_contact_id: Mapped[Optional[str]] = mapped_column(String(64))
    ghl_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    ghl_last_error: Mapped[Optional[str]] = mapped_column(String(200))
    ghl_last_attempt_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    ghl_synced_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    ghl_alerted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))  # Slack warning sent: not in GHL after the alert wait

    __table_args__ = (
        CheckConstraint("ghl_status IN ('pending', 'synced', 'contact_only', 'failed')", name="ck_lending_web_leads_ghl_status"),
        Index("idx_lending_web_leads_delivery", "ghl_status", "received_at"),
        Index("idx_lending_web_leads_phone", "phone", "received_at"),
    )


class LendingSoftApprovalCard(LendingBase):
    """The Slack card posted for one finished call (T-08): one per call, claimed before posting."""

    __tablename__ = "soft_approval_cards"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    dialer_call_id: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    phone: Mapped[str] = mapped_column(String(20), nullable=False)  # phone_utils.normalize
    channel: Mapped[Optional[str]] = mapped_column(String(40))
    message_ts: Mapped[Optional[str]] = mapped_column(String(50))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))


class LendingSoftApproval(LendingBase):
    """The Call-One soft approval for one lead (T-08), keyed by normalized phone.

    Interim "deal record": the Person/Property/Deal tables arrive with T-11. One row per lead; a
    resubmission with changed figures updates it in place and appends the replaced values to ``history``.
    ``pdf`` is NULL unless ``status`` is ``generated``. ``lender_key`` is internal; the PDF never names it.
    """

    __tablename__ = "soft_approvals"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    phone: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)  # phone_utils.normalize
    dialer_call_id: Mapped[str] = mapped_column(String(100), nullable=False)  # call of the latest submission
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    reason: Mapped[Optional[str]] = mapped_column(String(60))
    lender_key: Mapped[Optional[str]] = mapped_column(String(60))
    facts: Mapped[dict] = mapped_column(JSONB, nullable=False)
    figures: Mapped[Optional[dict]] = mapped_column(JSONB)
    pdf: Mapped[Optional[bytes]] = mapped_column(LargeBinary)
    template_version: Mapped[Optional[str]] = mapped_column(String(20))
    submitted_by: Mapped[Optional[str]] = mapped_column(String(40))  # Slack user id
    history: Mapped[list] = mapped_column(JSONB, nullable=False, default=list, server_default=text("'[]'::jsonb"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))

    __table_args__ = (
        CheckConstraint(
            "status IN ('generated', 'no_fit', 'no_lead', 'out_of_scope', 'terms_unconfirmed', 'render_failed')",
            name="ck_lending_soft_approvals_status",
        ),
    )


class LendingLenderRuleValue(LendingBase):
    """One lender parameter and its status (T-05, Josh A1): one row per lender key and field.

    A row per field, not a column per field, so a field Josh changes his mind about is a new or edited
    row, never a migration. ``value`` is JSON (number, string, list or null); the loader in
    ``src/lending/lender_rules_store.py`` validates it. ``status`` is confirmed / placeholder /
    confirm_at_submission. ``lender_key`` and ``field`` are the names in ``config/lender_matrix.py``.
    """

    __tablename__ = "lender_rule_values"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    lender_key: Mapped[str] = mapped_column(String(60), nullable=False)
    field: Mapped[str] = mapped_column(String(60), nullable=False)
    value: Mapped[Any] = mapped_column(JSONB, nullable=False, server_default=text("'null'::jsonb"))
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="placeholder", server_default=text("'placeholder'"))
    note: Mapped[Optional[str]] = mapped_column(Text)
    updated_by: Mapped[Optional[str]] = mapped_column(String(60))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))

    __table_args__ = (
        CheckConstraint(
            "status IN ('confirmed', 'placeholder', 'confirm_at_submission')",
            name="ck_lending_lender_rule_values_status",
        ),
        Index("uq_lending_lender_rule_values_key_field", "lender_key", "field", unique=True),
    )


class LendingAgentHaltState(LendingBase):
    """Cora's kill switch: one row, read by every Cora process before it acts (packages/agent_core/halt.py)."""

    __tablename__ = "agent_halt_state"

    id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=False)
    halted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    reason: Mapped[str] = mapped_column(Text, nullable=False, default="", server_default=text("''"))
    set_by: Mapped[Optional[str]] = mapped_column(String(32))  # Slack user id
    set_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))

    __table_args__ = (CheckConstraint("id = 1", name="ck_agent_halt_state_single_row"),)


class LendingAgentMemory(LendingBase):
    """Cora's standing rules, compiled into <standing_rules> on every turn (packages/agent_core/memory.py).

    Columns exactly as the CORA-CORE specification gives them; rules are deactivated, never deleted.
    """

    __tablename__ = "agent_memory"

    memory_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    category: Mapped[str] = mapped_column(String(64), nullable=False)
    rule_text: Mapped[str] = mapped_column(Text, nullable=False)
    source_thread_ts: Mapped[Optional[str]] = mapped_column(String(64))  # Slack thread the rule was given in
    is_active: Mapped[Optional[bool]] = mapped_column(Boolean, server_default=text("true"))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP"))


class LendingPendingAction(LendingBase):
    """One external send Cora drafted, frozen until an approver clicks (packages/agent_core/pending_actions.py)."""

    __tablename__ = "pending_actions"

    action_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tool_name: Mapped[str] = mapped_column(String(100), nullable=False)
    channel: Mapped[str] = mapped_column(String(64), nullable=False)  # relay executor key, e.g. ghl_sms
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)  # exactly what is sent on approval
    summary: Mapped[str] = mapped_column(Text, nullable=False, default="", server_default=text("''"))
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending", server_default=text("'pending'"))
    requested_by: Mapped[Optional[str]] = mapped_column(String(32))
    source_channel: Mapped[Optional[str]] = mapped_column(String(32))
    source_thread_ts: Mapped[Optional[str]] = mapped_column(String(64))
    card_channel: Mapped[Optional[str]] = mapped_column(String(32))
    card_ts: Mapped[Optional[str]] = mapped_column(String(64))
    decided_by: Mapped[Optional[str]] = mapped_column(String(32))  # approved or rejected it
    decided_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    revised_by: Mapped[Optional[str]] = mapped_column(String(32))  # opened the last revision
    revision_note: Mapped[Optional[str]] = mapped_column(Text)
    revisions: Mapped[list] = mapped_column(JSONB, nullable=False, default=list, server_default=text("'[]'::jsonb"))  # replaced drafts, oldest first
    recipient_phone: Mapped[Optional[str]] = mapped_column(String(20))  # phone_utils.normalize; read by the send-time opt-out check
    recipient_email: Mapped[Optional[str]] = mapped_column(String(255))  # lower-cased
    contact_ref: Mapped[Optional[str]] = mapped_column(String(64))
    deal_ref: Mapped[Optional[str]] = mapped_column(String(64))
    idempotency_key: Mapped[Optional[str]] = mapped_column(String(128))  # e.g. the model's tool_use_id; a repeat returns the same row
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    executed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    provider_ref: Mapped[Optional[str]] = mapped_column(String(200))
    error: Mapped[Optional[str]] = mapped_column(Text)  # long digit runs masked
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'revising', 'approved', 'rejected', 'sending', 'sent', 'failed', 'blocked', 'expired')",
            name="ck_pending_actions_status",
        ),
        Index("ix_pending_actions_status", "status", "decided_at"),
        Index("uq_pending_actions_idempotency_key", "idempotency_key", unique=True),
        Index("ix_pending_actions_revised_by", "revised_by", postgresql_where=text("status = 'revising'")),
        Index("ix_pending_actions_expires_at", "expires_at", postgresql_where=text("status IN ('pending', 'revising')")),
    )


class LendingPrequalLetter(LendingBase):
    """One Minute-5 pre-qualification letter per lead (T-07), queued before any render or GHL call.

    The unique (lead_source, lead_ref) key is the once-per-lead guard: a retried or re-delivered
    lead never queues a second letter. The four core fields are kept so a retry can re-render.
    ``status``: pending -> sending (claimed) -> sent / skipped (no fitting lender) / failed (retried);
    a ``sending`` row whose outcome was never saved becomes ``uncertain`` (checked by hand, never resent).
    """

    __tablename__ = "prequal_letters"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    lead_source: Mapped[str] = mapped_column(String(30), nullable=False)  # e.g. lendingflow
    lead_ref: Mapped[str] = mapped_column(String(64), nullable=False)  # the source's own lead id
    ghl_contact_id: Mapped[str] = mapped_column(String(64), nullable=False)
    credit_band: Mapped[str] = mapped_column(String(40), nullable=False)
    loan_amount: Mapped[int] = mapped_column(BigInteger, nullable=False)
    property_state: Mapped[str] = mapped_column(String(20), nullable=False)
    loan_type: Mapped[str] = mapped_column(String(40), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending", server_default=text("'pending'"))
    skip_reason: Mapped[Optional[str]] = mapped_column(String(60))
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    last_attempt_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[Optional[str]] = mapped_column(String(200))
    sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint("status IN ('pending', 'sending', 'sent', 'skipped', 'failed', 'uncertain')",
                        name="ck_lending_prequal_letters_status"),
        Index("uq_lending_prequal_letters_lead", "lead_source", "lead_ref", unique=True),
        Index("idx_lending_prequal_letters_delivery", "status", "created_at"),
    )


class LendingLendingFlowLead(LendingBase):
    """One deduplicated LendingFlow lead (T-11): the Deal record, linked to its Person (``lending.contacts``).

    Two UNIQUE keys enforce dedupe in the database: ``vendor_lead_id`` and ``dedupe_hash``
    (sha256 of normalized phone + lower-cased email). ``ghl_status``: pending -> synced /
    contact_only / failed; ``skipped`` = suppressed number, never pushed. ``event_emitted_at``
    is set once the ``LendingFlowLeadCreated`` event was emitted; ``followup_done_at`` once the event and the
    pre-qual hand-off both succeeded (the sweep redoes a synced lead whose follow-ups never finished). ``raw_payload`` is kept for reparse.
    """

    __tablename__ = "lendingflow_leads"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    lead_uuid: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, unique=True, default=uuid.uuid4,
                                                 server_default=text("gen_random_uuid()"))
    vendor_lead_id: Mapped[str] = mapped_column(String(100), nullable=False)
    dedupe_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    contact_id: Mapped[Optional[int]] = mapped_column(ForeignKey("lending.contacts.id"))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    first_name: Mapped[Optional[str]] = mapped_column(String(80))
    last_name: Mapped[Optional[str]] = mapped_column(String(80))
    phone: Mapped[str] = mapped_column(String(20), nullable=False)  # phone_utils.normalize
    email: Mapped[Optional[str]] = mapped_column(String(255))  # lower-cased
    credit_band: Mapped[Optional[str]] = mapped_column(String(40))
    credit_band_min_fico: Mapped[Optional[int]] = mapped_column(Integer)
    loan_amount: Mapped[Optional[int]] = mapped_column(BigInteger)  # = loan_amount_min (event / pre-qual input)
    loan_amount_range: Mapped[Optional[str]] = mapped_column(String(40))  # raw band, e.g. "$500K - $1M"
    loan_amount_min: Mapped[Optional[int]] = mapped_column(BigInteger)
    loan_amount_max: Mapped[Optional[int]] = mapped_column(BigInteger)
    loan_type: Mapped[Optional[str]] = mapped_column(String(40))
    property_state: Mapped[Optional[str]] = mapped_column(String(20))  # USPS code
    property_address: Mapped[Optional[str]] = mapped_column(String(200))
    property_city: Mapped[Optional[str]] = mapped_column(String(80))
    property_zip: Mapped[Optional[str]] = mapped_column(String(10))
    lead_source_campaign: Mapped[Optional[str]] = mapped_column(String(100))  # LendingFlow "Source"
    submitted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))  # borrower submit time on LendingFlow
    consent_status: Mapped[str] = mapped_column(String(10), nullable=False, default="missing", server_default=text("'missing'"))
    suppressed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    suppression_reason: Mapped[Optional[str]] = mapped_column(String(30))
    ghl_status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending", server_default=text("'pending'"))
    ghl_contact_id: Mapped[Optional[str]] = mapped_column(String(64))
    ghl_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    ghl_last_error: Mapped[Optional[str]] = mapped_column(String(200))
    ghl_last_attempt_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    ghl_synced_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    ghl_alerted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    event_emitted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    followup_done_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))  # event emitted AND pre-qual handed off
    intro_sms_tagged_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))  # T-09: ready tag added (starts the GHL intro text); NULL = not yet, or held for the send window
    duplicate_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    last_duplicate_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    raw_payload: Mapped[Any] = mapped_column(JSONB, nullable=False)

    __table_args__ = (
        CheckConstraint("consent_status IN ('present', 'missing')", name="ck_lending_lendingflow_leads_consent"),
        CheckConstraint("ghl_status IN ('pending', 'synced', 'contact_only', 'failed', 'skipped')",
                        name="ck_lending_lendingflow_leads_ghl_status"),
        Index("uq_lending_lendingflow_leads_vendor", "vendor_lead_id", unique=True),
        Index("uq_lending_lendingflow_leads_dedupe", "dedupe_hash", unique=True),
        Index("idx_lending_lendingflow_leads_delivery", "ghl_status", "received_at"),
        Index("idx_lending_lendingflow_leads_phone", "phone", "received_at"),
    )


class LendingLeadConsentCertificate(LendingBase):
    """Append-only consent evidence for a LendingFlow lead (T-11). ``raw_certificate`` is verbatim.

    ``verified_at`` is the timestamp inside the certificate (``certificate_timestamp``) or, when it
    carries none, the receipt time (``receipt_only``). No third-party verification is claimed.
    """

    __tablename__ = "lead_consent_certificates"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    lead_id: Mapped[int] = mapped_column(ForeignKey("lending.lendingflow_leads.id"), nullable=False, index=True)
    lead_source: Mapped[str] = mapped_column(String(30), nullable=False, default="lendingflow", server_default=text("'lendingflow'"))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    raw_certificate: Mapped[str] = mapped_column(Text, nullable=False)
    certificate_id: Mapped[Optional[str]] = mapped_column(String(200))
    certificate_url: Mapped[Optional[str]] = mapped_column(String(500))
    verified_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    verification_method: Mapped[str] = mapped_column(String(30), nullable=False)
    client_ip: Mapped[Optional[str]] = mapped_column(String(45))
    source_url: Mapped[Optional[str]] = mapped_column(String(500))
    tcpa_disclosure_text: Mapped[Optional[str]] = mapped_column(Text)
    is_duplicate_delivery: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))

    __table_args__ = (
        CheckConstraint("verification_method IN ('certificate_timestamp', 'receipt_only')",
                        name="ck_lending_lead_consent_certificates_method"),
    )


class LendingLendingFlowEnrichment(LendingBase):
    """Background enrichment card + routing for one LendingFlow lead (T-12), one row per lead.

    ``captured_address`` / ``target_close_date`` come from the call or booking form and win over the
    address LendingFlow sent; ``routing_tag`` is FULL_MACHINE or NURTURE (a DB value only; it is not
    the ``lending.contacts.nurture`` DNC flag and moves no GHL stage). ``card`` and ``fit`` hold what
    was shown to Josh; ``post_signature`` guards the deal-thread post so an unchanged address is never
    posted twice. Re-running updates the row quietly (Josh B3).
    ``status``: pending -> running (claimed) -> ready / failed (retried with backoff, capped).
    """

    __tablename__ = "lendingflow_enrichment"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    lead_id: Mapped[int] = mapped_column(ForeignKey("lending.lendingflow_leads.id"), nullable=False, unique=True)
    status: Mapped[str] = mapped_column(String(10), nullable=False, default="pending", server_default=text("'pending'"))
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    last_attempt_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[Optional[str]] = mapped_column(String(200))
    captured_address: Mapped[Optional[str]] = mapped_column(String(200))
    target_close_date: Mapped[Optional[date]] = mapped_column(Date)
    facts_source: Mapped[Optional[str]] = mapped_column(String(30))  # who last supplied them: booking_form / slack_form
    property_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    match_confidence: Mapped[Optional[int]] = mapped_column(Integer)
    routing_tag: Mapped[Optional[str]] = mapped_column(String(15))
    routing_reason: Mapped[Optional[str]] = mapped_column(String(40))
    closer_priority: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    card: Mapped[Optional[dict]] = mapped_column(JSONB)
    fit: Mapped[Optional[dict]] = mapped_column(JSONB)
    thread_ts: Mapped[Optional[str]] = mapped_column(String(40))
    post_signature: Mapped[Optional[str]] = mapped_column(String(64))  # the card was delivered for this signature
    analysis_signature: Mapped[Optional[str]] = mapped_column(String(64))  # the Street View / comps reply was too
    posted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))

    __table_args__ = (
        CheckConstraint("status IN ('pending', 'running', 'ready', 'failed')", name="ck_lending_lendingflow_enrichment_status"),
        CheckConstraint("routing_tag IS NULL OR routing_tag IN ('FULL_MACHINE', 'NURTURE')",
                        name="ck_lending_lendingflow_enrichment_routing"),
        Index("idx_lending_lendingflow_enrichment_sweep", "status", "last_attempt_at"),
    )


# ── R-1 Overflow Router ───────────────────────────────────────────────────────

class LendingDeclineReasonCode(LendingBase):
    """Decline reason codes supplied by Josh — stored as data so the enum is editable.

    Seeded by ``migrations/apply_lending_lender_routing.py``.
    """

    __tablename__ = "decline_reason_codes"

    code: Mapped[str] = mapped_column(String(50), primary_key=True)
    label: Mapped[str] = mapped_column(String(200), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text("true"))


class LendingLenderRoutingRule(LendingBase):
    """One row per lender in the routing waterfall.

    ``is_primary``: at most one row has this flag.  The primary lender gets first look
    and carries the seat cap.  Moving the primary flag to a different lender is a table
    edit (no deploy).

    ``agreement_status``: only rows with status 'signed' are ever routed to.

    ``construction_min_amount``: minimum loan amount for the primary lender's
    ground-up product (rule 1 threshold, currently $500,000 per the addendum spec;
    editable without a deploy).

    ``takes_ground_up``: marks overflow lenders eligible for rule 3 (ground-up below
    the primary's construction minimum).

    OPEN (Q1 / task-analysis): ``monthly_cap`` counts actions in
    ``SEAT_CAP_COUNTED_ACTION`` (default 'funded'); Josh to confirm.
    """

    __tablename__ = "lender_routing_rules"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text("true"))
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    agreement_status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending", server_default=text("'pending'"))
    priority: Mapped[int] = mapped_column(Integer, nullable=False)
    monthly_cap: Mapped[Optional[int]] = mapped_column(Integer)
    construction_min_amount: Mapped[Optional[int]] = mapped_column(BigInteger)
    points_paid_to_broker: Mapped[Optional[float]] = mapped_column(Numeric(5, 4))
    takes_ground_up: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    # T-05 lender keys (config/lender_matrix.py) whose rule values define this lender's eligibility. NULL = the
    # lender's own lender_product_boxes are used (a lender T-05 does not define).
    rule_keys: Mapped[Optional[list[str]]] = mapped_column(ARRAY(String))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))

    __table_args__ = (
        CheckConstraint(
            "agreement_status IN ('signed','pending','none','terminated')",
            name="ck_lender_routing_rules_agreement",
        ),
    )


class LendingLenderProductBox(LendingBase):
    """Per-lender eligibility rules for one product group (e.g. flip, construction, DSCR).

    A lender may have multiple product-box rows.  The router evaluates each box for the
    file's loan type; a file that matches any of the lender's boxes is eligible (subject
    to the lender's agreement_status and active flag).
    """

    __tablename__ = "lender_product_boxes"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    lender_id: Mapped[int] = mapped_column(ForeignKey("lending.lender_routing_rules.id", ondelete="CASCADE"), nullable=False, index=True)
    loan_types: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=False)
    min_loan_amount: Mapped[Optional[int]] = mapped_column(BigInteger)
    max_loan_amount: Mapped[Optional[int]] = mapped_column(BigInteger)
    approved_states: Mapped[Optional[list[str]]] = mapped_column(ARRAY(String))  # NULL = all states
    credit_floor: Mapped[Optional[int]] = mapped_column(Integer)
    max_ltv: Mapped[Optional[float]] = mapped_column(Numeric(5, 4))
    max_ltc: Mapped[Optional[float]] = mapped_column(Numeric(5, 4))
    min_completed_projects: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    requires_ground_up_build: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))


class LendingFileRouting(LendingBase):
    """Current routing state for one file: a LendingFlow lead (``lead_id``) or a partner file (``loan_file_id``).

    Exactly one of the two keys is set. The file is 1:1 with that row.

    ``lenders_tried``: ordered list of lender IDs already offered the file (so
    the decline cascade can skip them on the next pass).
    ``hold_reason``: 'no_match' or 'exhausted'.
    """

    __tablename__ = "file_routing"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    lead_id: Mapped[Optional[int]] = mapped_column(ForeignKey("lending.lendingflow_leads.id"), unique=True, index=True)
    loan_file_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("lending.loan_files.id"), unique=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="unrouted", server_default=text("'unrouted'"))
    current_lender_id: Mapped[Optional[int]] = mapped_column(ForeignKey("lending.lender_routing_rules.id"))
    current_lender_name: Mapped[Optional[str]] = mapped_column(String(100))
    route_position: Mapped[Optional[int]] = mapped_column(Integer)
    lenders_tried: Mapped[list[int]] = mapped_column(ARRAY(Integer), nullable=False, default=list, server_default=text("'{}'"))
    hold_reason: Mapped[Optional[str]] = mapped_column(String(30))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))

    __table_args__ = (
        CheckConstraint("status IN ('unrouted','routed','held','exhausted')", name="ck_file_routing_status"),
        CheckConstraint("(lead_id IS NULL) <> (loan_file_id IS NULL)", name="ck_file_routing_one_file"),
        Index("idx_file_routing_sweep", "status", "lead_id"),
    )


class LendingFileRoutingEvent(LendingBase):
    """Append-only routing event log (DELETE and core-field UPDATE blocked by DB trigger).

    ``action``:
      routed   — file offered to a lender.
      declined — lender declined the file.
      approved — lender approved the file.
      funded   — loan funded (used for seat-cap counting — Q1).
      held     — file entered the hold queue (reason in hold_reason on file_routing).

    ``slack_ts`` / ``slack_posted_at``: only these two columns may be updated
    (the immutability trigger permits it for idempotent Slack retry).
    """

    __tablename__ = "file_routing_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    lead_id: Mapped[Optional[int]] = mapped_column(ForeignKey("lending.lendingflow_leads.id"), index=True)
    loan_file_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("lending.loan_files.id"), index=True)
    lender_id: Mapped[Optional[int]] = mapped_column(ForeignKey("lending.lender_routing_rules.id"))
    lender_name: Mapped[Optional[str]] = mapped_column(String(100))
    action: Mapped[str] = mapped_column(String(20), nullable=False)
    reason_code: Mapped[Optional[str]] = mapped_column(String(50))
    free_text: Mapped[Optional[str]] = mapped_column(Text)
    actor: Mapped[str] = mapped_column(String(100), nullable=False, default="system", server_default=text("'system'"))
    slack_ts: Mapped[Optional[str]] = mapped_column(String(40))
    slack_posted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))

    __table_args__ = (
        CheckConstraint("action IN ('routed','declined','approved','funded','held')", name="ck_file_routing_events_action"),
        CheckConstraint("(lead_id IS NULL) <> (loan_file_id IS NULL)", name="ck_fre_one_file"),
        Index("idx_fre_lead_created", "lead_id", "created_at"),
    )


class LendingUncalledAlarm(LendingBase):
    """T-10: one stopwatch per LendingFlow lead, copied from T-11's ``lending.lendingflow_leads``.

    ``lendingflow_lead_id`` is deliberately not a foreign key, so this table does not depend on T-11's
    migration order. ``fired_120_at`` / ``fired_300_at`` are claims committed before the send: an alarm fires
    at most once. ``*_status``: sent / retry (definitely not sent, will be retried) / sending (a retry is claimed) /
    send_unknown (may have been sent: never resent) / failed (retries used up) / superseded (no longer needed) /
    not_configured / skipped_late. ``alarm_*_kind``: no_call / not_reached. ``resolved_reason``: connected / escalated / preexisting (existed before T-10 was switched on).
    """

    __tablename__ = "uncalled_alarms"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    lendingflow_lead_id: Mapped[int] = mapped_column(Integer, nullable=False, unique=True)
    lead_uuid: Mapped[str] = mapped_column(String(36), nullable=False)
    phone: Mapped[str] = mapped_column(String(20), nullable=False)  # phone_utils.normalize (from T-11)
    arrived_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    fired_120_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    fired_300_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    alarm_120_kind: Mapped[Optional[str]] = mapped_column(String(12))
    alarm_300_kind: Mapped[Optional[str]] = mapped_column(String(12))
    sms_120_status: Mapped[Optional[str]] = mapped_column(String(20))
    sms_300_status: Mapped[Optional[str]] = mapped_column(String(20))
    slack_300_status: Mapped[Optional[str]] = mapped_column(String(20))
    sms_120_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    sms_300_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    slack_300_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    retry_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))  # next retry of any channel in 'retry'
    claimed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))  # when a retry send was claimed
    last_error: Mapped[Optional[str]] = mapped_column(String(200))  # exception class only, no PII
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    resolved_reason: Mapped[Optional[str]] = mapped_column(String(20))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))

    __table_args__ = (
        CheckConstraint("resolved_reason IN ('connected', 'escalated', 'preexisting')",
                        name="ck_lending_uncalled_alarms_reason"),
        Index("idx_lending_uncalled_alarms_open", "arrived_at", postgresql_where=text("resolved_at IS NULL")),
    )


class LendingWebhookFailure(LendingBase):
    """A GHL opt-out or consent webhook call that failed after it was received (for example while the database
    was unavailable). ``payload`` is the body GHL sent, replayed by ``src.lending.webhook_failures`` through the
    same handler until it succeeds; ``gave_up_at`` is set after MAX_RETRY_ATTEMPTS and ops is alerted once.
    ``endpoint``: ghl_opt_out / ghl_text_consent. ``last_error`` is an exception class only, no PII."""

    __tablename__ = "webhook_failures"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    endpoint: Mapped[str] = mapped_column(String(40), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    ghl_contact_id: Mapped[Optional[str]] = mapped_column(String(64))
    last_error: Mapped[Optional[str]] = mapped_column(String(200))
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default=text("1"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                                 server_default=text("now()"))
    last_attempt_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    gave_up_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        Index("idx_lending_webhook_failures_open", "created_at",
              postgresql_where=text("resolved_at IS NULL AND gave_up_at IS NULL")),
    )


class LendingEmailOptOutMessage(LendingBase):
    """One row per message the opt-out mailbox listener has examined, keyed like the opt-out itself (a hash of
    the Message-ID), so every sweep fetches only messages it has not settled: a burst of other mail can never push
    an unprocessed opt-out out of a sweep. ``outcome``: recorded (an opt-out, new or already known) /
    not_opt_out (not addressed to the opt-out address, or a bounce / auto-reply) / failed (retried until
    MAX_MESSAGE_ATTEMPTS) / gave_up (ops alerted) / unreadable (ops alerted). No addresses or message text."""

    __tablename__ = "email_opt_out_messages"

    message_key: Mapped[str] = mapped_column(String(40), primary_key=True)
    outcome: Mapped[str] = mapped_column(String(20), nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default=text("1"))
    opt_out_event_id: Mapped[Optional[int]] = mapped_column(Integer)
    last_error: Mapped[Optional[str]] = mapped_column(String(200))  # exception class only
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                                    server_default=text("now()"))
    last_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                                      server_default=text("now()"))

    __table_args__ = (
        CheckConstraint("outcome IN ('recorded', 'not_opt_out', 'failed', 'gave_up', 'unreadable')",
                        name="ck_lending_email_opt_out_messages_outcome"),
    )


class LendingLoanFile(LendingBase):
    """One borrower file in the lending pipeline, whatever its source (P-1 creates partner files).

    Columns are the ones any source can fill. The router (R-1) adds its routing columns and moves
    ``status``; source-specific evidence lives in its own table pointing here (``partner_submissions``).
    """

    __tablename__ = "loan_files"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(20), nullable=False)  # partner (LendingFlow joins later)
    partner_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("lending.partners.id"))
    borrower_first_name: Mapped[str] = mapped_column(String(80), nullable=False)
    borrower_last_name: Mapped[str] = mapped_column(String(80), nullable=False)
    borrower_phone: Mapped[Optional[str]] = mapped_column(String(20))  # phone_utils.normalize
    borrower_email: Mapped[Optional[str]] = mapped_column(String(255))  # lower-cased
    product: Mapped[str] = mapped_column(String(20), nullable=False)  # flip / ground_up / dscr / other
    loan_amount: Mapped[int] = mapped_column(BigInteger, nullable=False)
    property_address: Mapped[Optional[str]] = mapped_column(String(200))
    property_city: Mapped[Optional[str]] = mapped_column(String(80))
    property_state: Mapped[str] = mapped_column(String(2), nullable=False)  # USPS code
    property_zip: Mapped[Optional[str]] = mapped_column(String(10))
    purchase_price: Mapped[Optional[int]] = mapped_column(BigInteger)
    rehab_budget: Mapped[Optional[int]] = mapped_column(BigInteger)  # rehab or build budget
    arv: Mapped[Optional[int]] = mapped_column(BigInteger)  # ARV or completed value
    completed_projects: Mapped[Optional[int]] = mapped_column(Integer)
    credit_band: Mapped[Optional[str]] = mapped_column(String(20))
    timeline: Mapped[Optional[str]] = mapped_column(String(40))
    notes: Mapped[Optional[str]] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="received", server_default=text("'received'"))
    status_reason: Mapped[Optional[str]] = mapped_column(String(200))
    status_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    lender_fit: Mapped[Optional[dict]] = mapped_column(JSONB)  # evaluate_lender_fit result (internal only)
    prequal_pdf_path: Mapped[Optional[str]] = mapped_column(String(300))  # generated, never sent to the borrower
    funded_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    notice_status: Mapped[Optional[str]] = mapped_column(String(20))  # status whose partner notice is not fully sent
    notice_emailed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    notice_texted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))

    __table_args__ = (
        CheckConstraint("status IN ('received', 'held', 'routed', 'declined', 'approved', 'funded')",
                        name="ck_lending_loan_files_status"),
        Index("idx_lending_loan_files_partner", "partner_id", "created_at"),
        Index("idx_lending_loan_files_status", "status", "status_at"),
    )


class LendingPartner(LendingBase):
    """A wholesaler, broker or referral partner who may submit files through their own link (P-1).

    Only the SHA-256 of the link token is stored; the raw token is shown once when it is issued.
    Revoking sets ``token_revoked_at``; the record and the partner's files are kept.
    """

    __tablename__ = "partners"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    company: Mapped[Optional[str]] = mapped_column(String(160))
    partner_type: Mapped[str] = mapped_column(String(20), nullable=False)
    phone: Mapped[Optional[str]] = mapped_column(String(20))  # phone_utils.normalize
    email: Mapped[Optional[str]] = mapped_column(String(255))
    states: Mapped[Optional[str]] = mapped_column(String(200))  # comma-separated USPS codes
    agreement_status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending", server_default=text("'pending'"))
    referral_terms: Mapped[Optional[str]] = mapped_column(Text)
    sms_consent: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default=text("true"))
    created_by: Mapped[Optional[str]] = mapped_column(String(80))
    token_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    token_revoked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))

    __table_args__ = (
        CheckConstraint("partner_type IN ('wholesaler', 'broker', 'referral', 'other')", name="ck_lending_partners_type"),
        CheckConstraint("agreement_status IN ('pending', 'signed', 'terminated')", name="ck_lending_partners_agreement"),
    )


class LendingPartnerSubmission(LendingBase):
    """The partner's consent evidence for one submitted file. Partner consent is never borrower consent."""

    __tablename__ = "partner_submissions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    file_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("lending.loan_files.id"), nullable=False, unique=True)
    partner_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("lending.partners.id"), nullable=False)
    consent_checked: Mapped[bool] = mapped_column(Boolean, nullable=False)
    consent_text: Mapped[str] = mapped_column(Text, nullable=False)  # exactly what the form showed
    consented_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ip_address: Mapped[Optional[str]] = mapped_column(String(45))
    page_url: Mapped[Optional[str]] = mapped_column(String(300))  # the link token is removed before storing
    user_agent: Mapped[Optional[str]] = mapped_column(String(300))
    partner_confirmed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))  # every required channel sent
    partner_emailed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    partner_texted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    josh_notified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    prequal_sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))  # letter emailed to the partner
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))


class LendingPartnerFileUpload(LendingBase):
    """A document the partner attached (purchase contract, scope, photos). Stored on disk, path recorded here."""

    __tablename__ = "partner_file_uploads"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    file_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("lending.loan_files.id"), nullable=False)
    original_name: Mapped[str] = mapped_column(String(200), nullable=False)
    stored_path: Mapped[str] = mapped_column(String(300), nullable=False)
    content_type: Mapped[str] = mapped_column(String(80), nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))

    __table_args__ = (Index("idx_lending_partner_file_uploads_file", "file_id"),)
