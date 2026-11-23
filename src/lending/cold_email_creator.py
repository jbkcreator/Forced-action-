"""T1 cold-email creator: one ``send_email`` agent job per cold lead that has an email and has had no cold email.

Leads come from ``property_radar_traced_contacts`` (the same contacts the daily FA export reads: first email, lower
case) and pass the export's contact gates: a suppressed or do-not-contact number, a warm number (website or
LendingFlow lead) and a Georgia owner who is a natural person are left out, and so is an opted-out, suppressed or
do-not-contact email. A lead with no phone is left out (the GHL upsert is by phone).

An address is emailed once: it is skipped when a cold email to it is already in ``lending.pending_actions`` (any
status) or a job for it was already filed. The job row's ``drive_file_id`` is ``cold-email:<md5 of the email>``
(unique), so a re-run never files a second job for the same address. A phone is used once too (``file_name`` is
``cold-email-creator:<md5 of the phone>``): the GHL upsert is by phone, so a second email on the same number would
overwrite the first job's address. The job also carries ``recipient_email``, and the worker refuses a job whose GHL
contact no longer has that address.

A job the worker finished as "cold email rail not configured" does not count: the address and phone stay eligible and
the next run files them again in place. The task also refuses to run while the Instantly rail is not configured, and
only one run at a time holds the creator's advisory lock, so overlapping runs cannot exceed the daily cap.

The GHL contact is resolved by an upsert by phone (email set, cold do-not-disturb as the cold push sets it) plus the
tags ``NDL`` and ``ndl-cold``, which route the job to the Instantly rail. A number the dialer load already pushed to
GHL keeps its call-list tags and is still called (T0 cancelled). A new contact also gets ``ndl-do-not-call-now``, so
it never joins Josh's call lists. Contacts are created only for the jobs filed that day (at most the daily cap).

Jobs are written straight to ``lending.agent_jobs`` as ``held`` rows due now; the agent job worker then runs the
existing checks (lane, Backflip, opt-out, cold cap, hours), the approval card or graduation, and the Instantly send.
The CAN-SPAM footer and unsubscribe link are added by the Instantly sender (decision D22), never by the template.
"""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Any, Callable, Iterable, Optional, Protocol
from zoneinfo import ZoneInfo

from sqlalchemy import text

from config.lending_compliance import DEFAULT_TZ, GHL_DO_NOT_CALL_NOW_TAG, GHL_TAG_COLD_LEAD
from src.lending import fa_export as fx
from src.lending.agent_jobs.job import AgentJob
from src.lending.pr_maturity_bridge import first_phone

logger = logging.getLogger(__name__)

CREATED_BY = "cold_email_creator"
PHONE_KEY_PREFIX = "cold-email-creator:"
RAIL_NOT_CONFIGURED = "cold email rail not configured%"
RUN_LOCK_KEY = 7_405_311_901  # pg advisory lock: one creator run at a time
DRIVE_KEY_PREFIX = "cold-email:"
JOB_TTL = timedelta(days=3)
FETCH_PAGE = 500

TEMPLATE_VERSION = "NDL-COLD-EMAIL-v1"
SUBJECT = "Financing for your next investment property"
BODY = (
    "Hello,\n\n"
    "Next Deal Lending works with real estate investors on purchase, refinance, bridge and construction loans.\n\n"
    "If you have a property coming up, or a loan that is close to maturity, we can look at your options and give "
    "you a straight answer quickly.\n\n"
    "Reply to this email and we will reach out.\n\n"
    "Next Deal Lending"
)


@dataclass(frozen=True)
class ColdLead:
    radar_id: str
    email: str
    phone: str
    state: Optional[str]
    owner_name: Optional[str]
    ownership_type: Optional[str]
    in_ghl: bool


class ContactSink(Protocol):
    def push(self, record: dict[str, Any], tags: list[str]) -> str: ...  # the GHL contact id


@dataclass
class RunResult:
    filed: int = 0
    skipped_gate: int = 0
    failed: int = 0
    remaining_cap: int = 0
    halted: bool = False


class GhlColdEmailSink:
    """Upsert by phone with the email and the cold do-not-disturb only (no name fields, so a contact the dialer load
    already named keeps its name), then add-tags (an upsert would replace the tags)."""

    def __init__(self, account: Any) -> None:
        from src.lending.wholesaler_lists import GhlWholesalerSink

        self._ghl = GhlWholesalerSink(account)

    def push(self, record: dict[str, Any], tags: list[str]) -> str:
        from config.lending_compliance import COLD_LEAD_DND_CHANNELS, COLD_LEAD_DND_MESSAGE

        body = {"locationId": self._ghl._account.location_id, "phone": record["phone"], "email": record["email"],
                "dndSettings": {channel: {"status": "active", "message": COLD_LEAD_DND_MESSAGE}
                                for channel in COLD_LEAD_DND_CHANNELS}}
        data = self._ghl._call("contact upsert", "POST", "/contacts/upsert", json=body)
        contact_id = (data.get("contact") or {}).get("id")
        if not contact_id:
            raise RuntimeError("contact upsert: no contact id in response")
        self._ghl._call("add tags", "POST", f"/contacts/{contact_id}/tags", json={"tags": tags})
        return contact_id


def ghl_sink() -> Optional[GhlColdEmailSink]:
    from src.lending.ghl_account import lending_ghl_account

    account = lending_ghl_account()
    return GhlColdEmailSink(account) if account else None


def drive_key(email: str) -> str:
    return DRIVE_KEY_PREFIX + hashlib.md5(email.strip().lower().encode()).hexdigest()


def phone_key(phone: str) -> str:
    return PHONE_KEY_PREFIX + hashlib.md5(phone.encode()).hexdigest()


def ghl_tags(lead: ColdLead) -> list[str]:
    tags = [fx.GHL_TAG_NDL, GHL_TAG_COLD_LEAD]
    if not lead.in_ghl:
        tags.append(GHL_DO_NOT_CALL_NOW_TAG)
    return tags


def build_job(lead: ColdLead, contact_id: str, now: datetime) -> AgentJob:
    return AgentJob.model_validate({
        "job_id": str(uuid.uuid4()), "lane": "NDL", "created_by": CREATED_BY, "created_at": now,
        "type": "send_email", "ghl_contact_id": contact_id, "contact_label": "", "template_version": TEMPLATE_VERSION,
        "subject": SUBJECT, "body": BODY, "reason": f"T1 cold email (radar_id {lead.radar_id})",
        "recipient_email": lead.email,
        "requires_approval": False, "not_before": None, "expires_at": now + JOB_TTL,
    })


def gate_leads(leads: Iterable[ColdLead], phone_gates: dict[str, str], blocked: set[str]) -> Iterable[ColdLead]:
    """The export's contact gates; ``already_in_ghl`` is not a reason to skip (it only decides the call hold)."""
    for lead in leads:
        row = {"state": lead.state, "owner_name": lead.owner_name, "ownership_type": lead.ownership_type}
        if fx._is_ga_natural_person(row):
            continue
        if phone_gates.get(lead.phone) in (fx.GATE_SUPPRESSED, fx.GATE_WARM):
            continue
        if lead.email in blocked:
            continue
        yield lead


# Traced leads with an email and a phone whose address has no cold email and no filed job, oldest trace first; one
# row per address. Owner fields from the daily pull first, else the calling lists.
_CANDIDATES = text("""
    WITH rec AS (
        SELECT DISTINCT ON (radar_id) radar_id, state, owner_name, ownership_type
          FROM (SELECT radar_id, state, owner_name, ownership_type, 0 AS pref FROM property_radar_records
                UNION ALL
                SELECT radar_id, state, owner_name, ownership_type, 1 FROM lending.pr_list_records) r
         ORDER BY radar_id, pref
    ), t AS (
        SELECT DISTINCT ON (lower(emails[1])) radar_id, lower(emails[1]) AS email, phones, traced_at
          FROM property_radar_traced_contacts
         WHERE cardinality(emails) > 0 AND cardinality(phones) > 0
         ORDER BY lower(emails[1]), traced_at
    )
    SELECT t.radar_id, t.email, t.phones, rec.state, rec.owner_name, rec.ownership_type
      FROM t LEFT JOIN rec ON rec.radar_id = t.radar_id
     WHERE NOT EXISTS (SELECT 1 FROM lending.pending_actions p
                        WHERE p.channel = :channel AND lower(p.recipient_email) = t.email)
       AND NOT EXISTS (SELECT 1 FROM lending.agent_jobs j WHERE j.drive_file_id = :prefix || md5(t.email)
                          AND NOT (j.state = 'done' AND j.result_status = 'error'
                                   AND j.result->>'reason' LIKE :not_configured))
     ORDER BY t.traced_at, t.radar_id
     LIMIT :limit OFFSET :offset
""")

_IN_GHL = text("""
    SELECT DISTINCT phone FROM lending.dialer_load_records
     WHERE phone = ANY(CAST(:phones AS text[])) AND ghl_contact_id IS NOT NULL
""")

_PHONES_USED = text("""
    SELECT file_name FROM lending.agent_jobs
     WHERE file_name = ANY(CAST(:keys AS text[]))
       AND NOT (state = 'done' AND result_status = 'error' AND result->>'reason' LIKE :not_configured)
""")

_FILED_SINCE = text("""
    SELECT count(*) FROM lending.agent_jobs WHERE drive_file_id LIKE :prefix || '%' AND received_at >= :since
""")

_INSERT_JOB = text("""
    INSERT INTO lending.agent_jobs (job_id, drive_file_id, file_name, created_by, job_type, template_version,
                                    ghl_contact_id, job, state, release_at, received_at, updated_at)
    VALUES (:job_id, :drive_file_id, :file_name, :created_by, 'send_email', :template_version, :ghl_contact_id,
            CAST(:job AS jsonb), 'held', :now, :now, :now)
    ON CONFLICT (drive_file_id) DO UPDATE
       SET job_id = EXCLUDED.job_id, file_name = EXCLUDED.file_name, ghl_contact_id = EXCLUDED.ghl_contact_id,
           job = EXCLUDED.job, template_version = EXCLUDED.template_version, state = 'held', held_reason = NULL,
           rail = NULL, action_id = NULL, result_status = NULL, result = NULL, result_written_at = NULL,
           release_at = EXCLUDED.release_at, received_at = EXCLUDED.received_at, updated_at = EXCLUDED.updated_at
     WHERE agent_jobs.state = 'done' AND agent_jobs.result_status = 'error'
       AND agent_jobs.result->>'reason' LIKE :not_configured
""")


def _et_day_start(now: datetime) -> datetime:
    local = now.astimezone(ZoneInfo(DEFAULT_TZ))
    return datetime.combine(local.date(), time.min, tzinfo=local.tzinfo)


def remaining_cap(session, now: datetime, daily_cap: int) -> int:
    filed = session.execute(_FILED_SINCE, {"prefix": DRIVE_KEY_PREFIX, "since": _et_day_start(now)}).scalar() or 0
    return max(0, daily_cap - int(filed))


def _candidate_page(session, offset: int) -> list[dict[str, Any]]:
    from config.lending_agent_jobs import COLD_EMAIL_CHANNEL

    return [dict(r) for r in session.execute(_CANDIDATES, {
        "channel": COLD_EMAIL_CHANNEL, "prefix": DRIVE_KEY_PREFIX, "not_configured": RAIL_NOT_CONFIGURED,
        "limit": FETCH_PAGE, "offset": offset,
    }).mappings()]


def _to_leads(session, rows: list[dict[str, Any]]) -> list[ColdLead]:
    with_phone = [(r, first_phone(list(r["phones"] or []))) for r in rows]
    phones = sorted({p for _, p in with_phone if p})
    in_ghl = set(session.execute(_IN_GHL, {"phones": phones}).scalars()) if phones else set()
    used = set(session.execute(_PHONES_USED, {"keys": [phone_key(p) for p in phones],
                                              "not_configured": RAIL_NOT_CONFIGURED}).scalars()) if phones else set()
    return [ColdLead(radar_id=r["radar_id"], email=r["email"], phone=p, state=r["state"], owner_name=r["owner_name"],
                     ownership_type=r["ownership_type"], in_ghl=p in in_ghl)
            for r, p in with_phone if p and phone_key(p) not in used]


def eligible_leads(session, limit: int) -> list[ColdLead]:
    """Up to ``limit`` leads that pass every gate, read a page at a time (gates are batch reads per page)."""
    picked: list[ColdLead] = []
    phones_taken: set[str] = set()
    offset = 0
    while len(picked) < limit:
        rows = _candidate_page(session, offset)
        if not rows:
            break
        offset += len(rows)
        leads = _to_leads(session, rows)
        gate_rows = [{"phone": lead.phone, "email": lead.email} for lead in leads]
        phone_gates = fx.contact_gates(session, gate_rows)
        blocked = fx.blocked_emails(session, gate_rows)
        for lead in gate_leads(leads, phone_gates, blocked):
            if lead.phone in phones_taken:  # one email per number: the GHL upsert is by phone
                continue
            phones_taken.add(lead.phone)
            picked.append(lead)
            if len(picked) >= limit:
                break
    return picked


def file_job(session, lead: ColdLead, contact_id: str, now: datetime) -> bool:
    job = build_job(lead, contact_id, now)
    inserted = session.execute(_INSERT_JOB, {
        "job_id": str(job.job_id), "drive_file_id": drive_key(lead.email), "file_name": phone_key(lead.phone),
        "created_by": CREATED_BY, "template_version": TEMPLATE_VERSION, "ghl_contact_id": contact_id,
        "job": json.dumps(job.model_dump(mode="json")), "now": now, "not_configured": RAIL_NOT_CONFIGURED,
    }).rowcount
    return bool(inserted)


class RunInProgress(RuntimeError):
    """Another creator run holds the advisory lock."""


def run(session, *, sink: ContactSink, now: datetime, daily_cap: int, halted: Callable[[], bool]) -> RunResult:
    """File today's jobs, up to what is left of the daily cap, holding the creator's session advisory lock so two runs
    never read the same remaining cap. Commits after each filed job."""
    if not session.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": RUN_LOCK_KEY}).scalar():
        raise RunInProgress("another cold-email creator run is in progress")
    try:
        return _run_locked(session, sink=sink, now=now, daily_cap=daily_cap, halted=halted)
    finally:
        session.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": RUN_LOCK_KEY})


def _run_locked(session, *, sink: ContactSink, now: datetime, daily_cap: int, halted: Callable[[], bool]) -> RunResult:
    result = RunResult(remaining_cap=remaining_cap(session, now, daily_cap))
    if result.remaining_cap == 0:
        logger.info("[cold-email] daily cap %d already reached", daily_cap)
        return result
    for lead in eligible_leads(session, result.remaining_cap):
        if halted():
            result.halted = True
            logger.warning("[cold-email] emergency halt set; stopping")
            break
        record = {"phone": lead.phone, "email": lead.email}
        try:
            contact_id = sink.push(record, ghl_tags(lead))
        except Exception as exc:  # class only: GHL errors can embed contact values
            logger.warning("[cold-email] GHL upsert failed for radar_id=%s (%s)", lead.radar_id, type(exc).__name__)
            result.failed += 1
            continue
        if file_job(session, lead, contact_id, now):
            session.commit()
            result.filed += 1
            logger.info("[cold-email] job filed for radar_id=%s contact=%s", lead.radar_id, contact_id)
        else:
            session.rollback()
    return result
