"""
WP-7 self-serve pre-fill — acceptance harness.

Answers one question with one exit code: does a tracked click on a real
parcel produce a recognized pre-fill and a handoff with attribution intact,
per the client's own Done-When (amendment doc item 20, L262):

    "a borrower clicking a partner link sees their own property already
    recognized and completes handoff to Backflip in under three minutes."

WHAT IT DOES

Seeds one property and one tracked link bound to it directly (property_id set
at mint time — the admin-mint path, not the Slack command, which no longer
offers a property-bound kind since physical mail campaigns are out of scope),
then walks the real service functions exactly as the HTTP layer does:
resolve_slug -> record_click
-> assemble_prefill -> create_session -> submit_session -> FakeBackflipPort
.handoff(). Asserts the property was recognized without the borrower typing
anything, the pre-fill payload carries no banned fields, consent was recorded,
the suppression check ran, and the handoff produced attribution intact.

Timing note: this measures machine latency (service calls, no network, no
human), not a borrower's actual fill time — that can't be scripted. A
multi-second budget here is a floor check (the automated half of the flow
isn't itself the bottleneck), not a claim that the full three-minute
borrower experience was measured.

EVERYTHING RUNS IN ONE TRANSACTION THAT IS ALWAYS ROLLED BACK. Nothing is
committed, in either the pass or the fail path — same convention as
scripts/harness/venture_spinup_acceptance.py.

Usage:
    PYTHONPATH=. python scripts/harness/wp7_acceptance.py
    PYTHONPATH=. python scripts/harness/wp7_acceptance.py --verbose
"""
from __future__ import annotations

import argparse
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session as SASession

from config.settings import get_settings
from src.services.backflip_port import FakeBackflipPort, HandoffPayload
from src.services.prefill_assembly import assemble_prefill
from src.services.selfserve_sessions import (
    create_session,
    is_backflip_suppressed,
    submit_session,
)
from src.services.tracked_links import mint_link, record_click, resolve_slug

_BANNED_PAYLOAD_KEYS = {
    "property_id", "owner_id", "buyer_entity_id", "parcel_id", "arv",
    "estimated_income", "credit_score_tier",
}

# Floor budget for the scripted (non-human) portion of the flow — roughly a
# dozen sequential round trips against a real Postgres connection. See the
# module docstring's timing note. Not tuned to a fast local DB on purpose;
# it exists to catch a real regression (an N+1 loop, a hung external call),
# not to enforce a specific network's latency.
_MAX_MACHINE_SECONDS = 20.0


@dataclass
class Check:
    name: str
    outcome: str  # PASS | FAIL
    detail: str


class Report:
    def __init__(self) -> None:
        self.checks: list[Check] = []

    def record(self, name: str, ok: bool, detail: str = "") -> bool:
        self.checks.append(Check(name, "PASS" if ok else "FAIL", detail))
        return ok

    @property
    def failed(self) -> list[Check]:
        return [c for c in self.checks if c.outcome == "FAIL"]

    def render(self, verbose: bool = False) -> str:
        width = max((len(c.name) for c in self.checks), default=10)
        lines = []
        for c in self.checks:
            detail = c.detail if (verbose or c.outcome == "FAIL") else ""
            lines.append(f"  [{c.outcome}] {c.name.ljust(width)}  {detail}".rstrip())
        return "\n".join(lines)


class _HarnessAbort(Exception):
    """A prerequisite check failed badly enough that later checks would just
    raise on missing data — abort the checklist, still print what ran, still
    roll back."""


def run(verbose: bool = False) -> int:
    report = Report()
    engine = create_engine(str(get_settings().database_url))
    connection = engine.connect()
    trans = connection.begin()
    db = SASession(bind=connection)

    try:
        start = time.perf_counter()

        suffix = uuid.uuid4().hex[:8]
        prop_row = db.execute(
            text(
                "INSERT INTO properties (parcel_id, address, city, state, zip, county_id, "
                "beds, baths, sq_ft, year_built, created_at, updated_at) "
                "VALUES (:parcel, '42 Acceptance Ave', 'Tampa', 'FL', '33603', 'hillsborough', "
                "3, 2, 1600, 1998, now(), now()) RETURNING id"
            ),
            {"parcel": f"WP7-ACCEPT-{suffix}"},
        ).first()
        assert prop_row is not None
        property_id = prop_row.id
        db.execute(
            text("INSERT INTO owners (property_id, owner_name, mailing_address) VALUES (:pid, 'ACCEPT TESTER', '1 Test Way')"),
            {"pid": property_id},
        )
        db.execute(
            text("INSERT INTO financials (property_id, assessed_value_mkt) VALUES (:pid, 310000)"),
            {"pid": property_id},
        )

        link = mint_link(
            db, kind="source", label="acceptance harness link",
            created_by="wp7_acceptance", property_id=property_id,
        )
        db.flush()

        # --- click ---
        resolved = resolve_slug(db, link.slug)
        if not report.record("tracked link resolves", resolved is not None and resolved.property_id == property_id):
            raise _HarnessAbort("tracked link did not resolve — cannot continue")
        assert resolved is not None

        session_token = str(uuid.uuid4())
        record_click(db, tracked_link_id=resolved.id, session_token=session_token)

        # --- pre-fill assembly ---
        prefill = assemble_prefill(db, property_id).to_dict()
        report.record(
            "property recognized without borrower typing anything",
            prefill["fields"].get("address", {}).get("value") == "42 Acceptance Ave",
            detail=f"fields present: {sorted(prefill['fields'].keys())}",
        )
        report.record(
            "ARV never in the pre-fill payload (plan §7 Q3)",
            "arv" not in prefill["fields"],
        )

        session_row = create_session(db, prefill_snapshot=prefill, tracked_link_id=link.id, property_id=property_id)
        session_row.token = session_token
        db.flush()

        # --- borrower confirms + answers + consents ---
        contact = {"name": "Accept Tester", "email": f"accept-{suffix}@example.com", "phone": "8135551234"}
        confirmations = {"exit_strategy": "flip", "rehab_budget": "35000"}
        updated = submit_session(
            db, token=session_token, corrections=None, confirmations=confirmations,
            contact=contact, consent_channels=["email", "sms"],
        )
        report.record("session moved to confirmed", updated.status == "confirmed")

        consent_rows = db.execute(
            text("SELECT channel FROM fa_max_person_consent WHERE person_id = :pid"),
            {"pid": updated.person_id},
        ).fetchall()
        report.record(
            "consent recorded per channel",
            {r.channel for r in consent_rows} == {"email", "sms"},
        )

        # --- suppression check runs (WI-4) ---
        suppressed = is_backflip_suppressed(db, email=contact["email"], phone=contact["phone"])
        report.record("suppression check executes cleanly", suppressed is False, detail="no active Backflip touch seeded")

        # --- handoff (WI-6) ---
        port = FakeBackflipPort()
        result = port.handoff(HandoffPayload(
            session_token=session_token,
            prefill_fields={k: v.get("value") for k, v in prefill["fields"].items()},
            confirmations=confirmations,
            contact=contact,
        ))
        report.record("handoff produced a ref", bool(result.handoff_ref))
        report.record("attribution preserved in redirect", session_token in result.redirect_url)

        combined_payload_keys = set(prefill["fields"].keys()) | set(confirmations.keys()) | set(contact.keys())
        leaked = _BANNED_PAYLOAD_KEYS & combined_payload_keys
        report.record("no banned fields in the handoff payload", not leaked, detail=f"leaked={leaked}" if leaked else "")

        elapsed = time.perf_counter() - start
        report.record(
            "scripted flow completes within the machine-latency floor",
            elapsed < _MAX_MACHINE_SECONDS,
            detail=f"{elapsed:.2f}s (human fill time not measurable by a script — see docstring)",
        )

    except _HarnessAbort as exc:
        report.checks.append(Check("harness aborted early", "FAIL", str(exc)))
    finally:
        trans.rollback()
        connection.close()

    print(report.render(verbose=verbose))
    if report.failed:
        print(f"\nRESULT: FAIL — {len(report.failed)} check(s) failed\n")
        return 1
    print("\nRESULT: PASS — tracked click -> recognized pre-fill -> handoff with attribution intact\n")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    return run(verbose=args.verbose)


if __name__ == "__main__":
    sys.exit(main())
