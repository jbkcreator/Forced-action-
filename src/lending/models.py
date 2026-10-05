"""Lending Engine Wave 0 — compliance floor tables (schema ``lending``).

Own ``MetaData`` on purpose: keeps these tables out of ``Base.metadata`` so FA's
SQLite ``create_all`` fixtures never see a schema-qualified table. DDL is applied
by ``migrations/apply_lending_compliance.py`` (ADR 0024); see
lender-engine/dev2-compliance-floor/adr/0001-lending-schema-in-shared-fa-db.md.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

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
    reason: Mapped[str] = mapped_column(String(30), nullable=False)  # SuppressionReason: OPT_OUT / LITIGATOR
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
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending", server_default="pending")  # pending / blocked / duplicate_day
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, server_default=text("now()"))

    __table_args__ = (
        Index("uq_lending_missed_call_phone_day", "phone", "event_date_et", unique=True, postgresql_where=text("status <> 'duplicate_day'")),
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
