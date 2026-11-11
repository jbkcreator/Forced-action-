"""Turn a checked agent job into a Cora draft (``EgressDraft``) with the same channel names and payload keys the
chat tools use (src/lending/cora_tools.py), so the existing executors send it unchanged.

Every payload also carries ``job_id``, ``ghl_contact_id`` and ``template_version`` (shown on the card, read back for
the receipt). The recipient comes from the GHL contact, never from the job file. A cold email's body is stored
without the CAN-SPAM footer: the Instantly sender adds it at send time, so a Revise can never edit or duplicate it
(decision D22). The summary is plain text: the approval card escapes Slack markup and masks contact details.
"""
from __future__ import annotations

from datetime import datetime, time
from zoneinfo import ZoneInfo

from config.lending_agent_jobs import COLD_EMAIL_CHANNEL, JOB_TYPES
from config.lending_compliance import DEFAULT_TZ
from packages.agent_core.tools import EgressDraft
from src.lending.agent_jobs.checks import Rail
from src.lending.agent_jobs.job import AgentJob
from src.lending.ghl_crm import GhlContact

DEFAULT_TASK_DUE = time(17, 0)  # 5 PM Eastern on the day the job was filed (decision C9)
_RAIL_LABEL = {"ghl": "GHL", "instantly": "Instantly (cold)"}


def _default_due(created_at: datetime) -> datetime:
    eastern = ZoneInfo(DEFAULT_TZ)
    return datetime.combine(created_at.astimezone(eastern).date(), DEFAULT_TASK_DUE, tzinfo=eastern)


def _fields(job: AgentJob, contact: GhlContact, channel: str) -> dict:
    if channel == "ghl_sms":
        return {"to_phone": contact.phone, "body": job.body}
    if channel == "ghl_email":
        return {"to_email": contact.email, "to_phone": contact.phone, "subject": job.subject, "body": job.body}
    if channel == COLD_EMAIL_CHANNEL:
        return {"to_email": contact.email, "subject": job.subject, "body": job.body}
    if channel == "ghl_stage":
        return {"stage": job.body.strip()}
    if channel == "ghl_tag":
        return {"tag": job.body.strip()}
    if channel == "ghl_note":
        return {"body": job.body.strip()}
    due = job.task_due_at or _default_due(job.created_at)
    return {"title": job.body.strip(), "due_at": due.isoformat(), "assignee": job.task_assignee}


def build_draft(job: AgentJob, contact: GhlContact, rail: Rail) -> EgressDraft:
    channel = COLD_EMAIL_CHANNEL if job.type == "send_email" and rail == "instantly" else JOB_TYPES[job.type]
    payload = {"job_id": str(job.job_id), "ghl_contact_id": job.ghl_contact_id,
               "template_version": job.template_version, **_fields(job, contact, channel)}
    last4 = f" (…{contact.phone[-4:]})" if contact.phone else ""
    summary = (f"🤖 {job.created_by} · {job.template_version} · via {_RAIL_LABEL[rail]}\n"
               f"{job.contact_label or job.ghl_contact_id}{last4}: {job.reason or job.type}")
    return EgressDraft(channel=channel, payload=payload, summary=summary, recipient_phone=contact.phone,
                       recipient_email=contact.email, contact_ref=f"ghl:{job.ghl_contact_id}")
