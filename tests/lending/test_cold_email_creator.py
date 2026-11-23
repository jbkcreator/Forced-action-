"""T1 cold-email creator: which leads get a job, the job itself, the GHL tags, and that the existing checks still
block an opted-out lead, a Backflip hit and a lead at the cold cap."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from src.lending import cold_email_creator as creator
from src.lending import fa_export as fx
from src.lending.agent_jobs.checks import check_job, rail_for
from src.lending.cora_tools import borrower_draft_problem
from src.lending.ghl_crm import GhlContact

NOW = datetime(2026, 11, 24, 15, 0, tzinfo=timezone.utc)  # 10:00 AM Eastern
PHONE = "+18135550142"
EMAIL = "owner@example.com"


def _lead(**overrides) -> creator.ColdLead:
    data = dict(radar_id="R1", email=EMAIL, phone=PHONE, state="FL", owner_name="ACME HOLDINGS LLC",
                ownership_type="Corporate", in_ghl=False)
    data.update(overrides)
    return creator.ColdLead(**data)


def _contact(**overrides) -> GhlContact:
    data = dict(contact_id="ghl-1", phone=PHONE, email=EMAIL, dnd_channels=frozenset({"Email", "SMS"}),
                tags=frozenset({"NDL", "ndl-cold"}))
    data.update(overrides)
    return GhlContact(**data)


def _open(_phone, **_kwargs) -> bool:
    return True


# -- pure: gates, job, tags ------------------------------------------------------------------------------------


def test_lead_with_email_passes_gates() -> None:
    assert list(creator.gate_leads([_lead()], {}, set())) == [_lead()]


def test_opted_out_email_is_skipped() -> None:
    assert list(creator.gate_leads([_lead()], {}, {EMAIL})) == []


@pytest.mark.parametrize("gate", [fx.GATE_SUPPRESSED, fx.GATE_WARM])
def test_suppressed_or_warm_number_is_skipped(gate) -> None:
    assert list(creator.gate_leads([_lead()], {PHONE: gate}, set())) == []


def test_number_already_in_ghl_is_not_skipped() -> None:
    assert list(creator.gate_leads([_lead()], {PHONE: fx.GATE_ALREADY_IN_GHL}, set())) == [_lead()]


def test_georgia_natural_person_is_skipped() -> None:
    lead = _lead(state="GA", owner_name="JOHN SMITH", ownership_type="Individual")
    assert list(creator.gate_leads([lead], {}, set())) == []


def test_georgia_llc_is_kept() -> None:
    lead = _lead(state="GA")
    assert list(creator.gate_leads([lead], {}, set())) == [lead]


def test_job_is_cold_ndl_email_without_numbers_or_personal_name() -> None:
    job = creator.build_job(_lead(), "ghl-1", NOW)
    assert (job.type, job.lane, job.ghl_contact_id) == ("send_email", "NDL", "ghl-1")
    assert job.subject and job.expires_at > NOW
    assert borrower_draft_problem({"subject": job.subject, "body": job.body}) is None
    assert job.body.rstrip().endswith("Next Deal Lending")
    assert "unsubscribe" not in job.body.lower()  # the Instantly sender adds footer + unsubscribe (D22)
    assert rail_for(job, _contact()) == "instantly"


def test_new_contact_gets_call_hold_existing_does_not() -> None:
    assert "ndl-do-not-call-now" in creator.ghl_tags(_lead(in_ghl=False))
    assert "ndl-do-not-call-now" not in creator.ghl_tags(_lead(in_ghl=True))
    assert {"NDL", "ndl-cold"} <= set(creator.ghl_tags(_lead()))


def test_drive_key_is_case_insensitive() -> None:
    assert creator.drive_key("Owner@Example.com ") == creator.drive_key(EMAIL)


# -- DB: candidates, dedupe, cap, and the existing checks on a filed job ---------------------------------------


@pytest.fixture
def db(lending_db):
    from packages.agent_core.schema import apply_to

    apply_to(lending_db.connection(), "lending")  # pending_actions
    lending_db.execute(text("DELETE FROM lending.agent_jobs WHERE drive_file_id LIKE 'cold-email:%'"))
    lending_db.execute(text("DELETE FROM property_radar_traced_contacts"))
    return lending_db


def _traced(db, radar_id: str, emails: list[str], phones: list[str], *, state: str = "FL") -> None:
    db.execute(text("INSERT INTO property_radar_traced_contacts (radar_id, phones, emails, traced_at) "
                    "VALUES (:r, :p, :e, :t)"), {"r": radar_id, "p": phones, "e": emails, "t": NOW - timedelta(days=5)})


def _cold_email_action(db, email: str, status: str = "sent") -> None:
    db.execute(text(
        "INSERT INTO lending.pending_actions (tool_name, channel, payload, status, recipient_email, executed_at, "
        "created_at, updated_at) VALUES ('agent_job:send_email', 'instantly_cold_email', :payload, :s, :e, :at, :at, :at)"),
        {"payload": json.dumps({}), "s": status, "e": email, "at": NOW - timedelta(days=1)})


class _Sink:
    def __init__(self) -> None:
        self.pushed: list[tuple[dict, list[str]]] = []

    def push(self, record, tags):
        self.pushed.append((record, tags))
        return f"ghl-{len(self.pushed)}"


def test_lead_without_email_gets_no_job(db) -> None:
    _traced(db, "R1", [], [PHONE])
    assert creator.eligible_leads(db, 10) == []


def test_lead_with_email_gets_one_job_and_rerun_files_none(db) -> None:
    _traced(db, "R1", ["Owner@Example.com"], [PHONE])
    sink = _Sink()
    first = creator.run(db, sink=sink, now=NOW, daily_cap=10, halted=lambda: False)
    second = creator.run(db, sink=sink, now=NOW, daily_cap=10, halted=lambda: False)
    assert (first.filed, second.filed) == (1, 0)
    assert sink.pushed[0][0] == {"phone": PHONE, "email": EMAIL}
    row = db.execute(text("SELECT state, job_type, job FROM lending.agent_jobs WHERE drive_file_id = :k"),
                     {"k": creator.drive_key(EMAIL)}).mappings().one()
    assert (row["state"], row["job_type"]) == ("held", "send_email")


def test_already_emailed_address_gets_no_job(db) -> None:
    _traced(db, "R1", [EMAIL], [PHONE])
    _cold_email_action(db, EMAIL, status="rejected")
    assert creator.eligible_leads(db, 10) == []


def test_daily_cap_is_respected(db) -> None:
    for i in range(3):
        _traced(db, f"R{i}", [f"o{i}@example.com"], [f"+1813555014{i}"])
    result = creator.run(db, sink=_Sink(), now=NOW, daily_cap=2, halted=lambda: False)
    assert result.filed == 2
    assert creator.remaining_cap(db, NOW, 2) == 0


def test_halt_stops_filing(db) -> None:
    _traced(db, "R1", [EMAIL], [PHONE])
    result = creator.run(db, sink=_Sink(), now=NOW, daily_cap=10, halted=lambda: True)
    assert (result.filed, result.halted) == (0, True)


def test_opted_out_lead_is_blocked_by_checks(db) -> None:
    db.execute(text("INSERT INTO email_opt_outs (email, source, opted_out_at) VALUES (:e, 'unsubscribe_link', now())"),
               {"e": EMAIL})
    job = creator.build_job(_lead(), "ghl-1", NOW)
    assert check_job(db, job, _contact(), NOW, text_window=_open).status == "blocked_consent"


def test_backflip_hit_is_blocked_by_checks(db) -> None:
    job = creator.build_job(_lead(), "ghl-1", NOW)
    verdict = check_job(db, job, _contact(email="x@backflip.com"), NOW, text_window=_open)
    assert verdict.status == "blocked_lane"


def test_lead_at_limit_is_blocked_by_checks(db) -> None:
    _cold_email_action(db, EMAIL)
    job = creator.build_job(_lead(), "ghl-1", NOW)
    assert check_job(db, job, _contact(), NOW, text_window=_open).status == "blocked_cap"
