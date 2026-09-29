"""Lending Engine Wave 0 — compliance floor tables (schema ``lending``).

Own ``MetaData`` on purpose: keeps these tables out of ``Base.metadata`` so FA's
SQLite ``create_all`` fixtures never see a schema-qualified table. DDL is applied
by ``migrations/apply_lending_compliance.py`` (ADR 0024); see
lender-engine/dev2-compliance-floor/adr/0001-lending-schema-in-shared-fa-db.md.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    MetaData,
    String,
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


class LendingCallDisposition(LendingBase):
    """One row per Aircall call: attempt record, result tag and delivery state (spec §4.4)."""

    __tablename__ = "call_dispositions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    aircall_call_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    direction: Mapped[Optional[str]] = mapped_column(String(10))  # 'outbound' / 'inbound' (Aircall value)
    phone: Mapped[Optional[str]] = mapped_column(String(20))  # borrower, E.164; NULL if unnormalizable
    caller_seat: Mapped[Optional[str]] = mapped_column(String(100))
    caller_name: Mapped[Optional[str]] = mapped_column(String(120))
    caller_line: Mapped[Optional[str]] = mapped_column(String(40))
    campaign_tag: Mapped[Optional[str]] = mapped_column(String(40))
    aircall_contact_id: Mapped[Optional[str]] = mapped_column(String(40))
    disposition: Mapped[Optional[str]] = mapped_column(String(30))  # NULL until a result tag is applied
    disposition_tag_raw: Mapped[Optional[str]] = mapped_column(String(80))
    multiple_dispositions: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    talk_duration_sec: Mapped[Optional[int]] = mapped_column(Integer)
    call_started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    call_ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))  # NULL until call.ended arrives
    disposition_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    recording_disclosure_logged: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    sheet_synced_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    sheet_synced_disposition: Mapped[Optional[str]] = mapped_column(String(30))
    slack_posted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    slack_posted_disposition: Mapped[Optional[str]] = mapped_column(String(30))
    slack_ts: Mapped[Optional[str]] = mapped_column(String(40))
    opt_out_propagated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    raw_event: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("now()"))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("now()"))

    __table_args__ = (
        CheckConstraint(
            "disposition IS NULL OR disposition IN "
            "('CONNECTED','LEFT_VOICEMAIL','BAD_NUMBER','DNC_REQUEST','QUALIFIED_APPOINTMENT')",
            name="ck_lending_call_dispositions_disposition",
        ),
        Index("idx_lending_call_dispositions_phone_ended", "phone", "call_ended_at"),
        Index("idx_lending_call_dispositions_seat_ended", "caller_seat", "call_ended_at"),
        Index(
            "idx_lending_call_dispositions_undelivered",
            "disposition_at",
            postgresql_where=text(
                "sheet_synced_disposition IS DISTINCT FROM disposition "
                "OR slack_posted_disposition IS DISTINCT FROM disposition"
            ),
        ),
    )
