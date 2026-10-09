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
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    ForeignKey,
    DateTime,
    Index,
    Integer,
    MetaData,
    Numeric,
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
    guards the once-per-lead ``LendingFlowLeadCreated`` event. ``raw_payload`` is kept for reparse.
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
