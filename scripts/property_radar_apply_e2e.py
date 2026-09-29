"""End-to-end --apply test of the PropertyRadar pipeline, against the shared DB.

Feeds fake PropertyRadar API records — built from real Hillsborough / Pinellas
parcels, plus unloaded-county, long-term and broken records — through the real
runner stages: Dev 1 normalizer + budget/seen/watermark, Dev 2 staging + link,
Dev 3 handoff, Dev 4 ownership. Then checks every table, a second daily run,
a dry run, and the pilot rollup.

Everything runs in ONE transaction that is always rolled back: the session
joins it with savepoints, so the pipeline's own commits never reach the DB.
Nothing is bought (fake API client), nothing is sent.

Usage:
    PYTHONPATH=. python scripts/property_radar_apply_e2e.py [--per-county 8]
Exit code 0 = all pass.
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from config.settings import get_settings
from src.services import property_radar_port as port_mod
from src.services.campaign_pilot_rollup import pilot_rollup
from src.services.property_radar_port import FakePropertyRadarPort
from src.tasks import property_radar_runner as runner

logging.basicConfig(level=logging.WARNING, format="%(message)s")
logger = logging.getLogger("apply_e2e")
logger.setLevel(logging.INFO)

CAMPAIGN = "maturity_target_lender"
PREFIX = "TEST-E2E-"


@dataclass
class Result:
    name: str
    passed: bool
    detail: str = ""


def _real_parcels(session: Session, per_county: int) -> list[dict[str, Any]]:
    return [dict(r) for r in session.execute(text("""
        SELECT id, parcel_id, address, city, zip, county_id FROM (
            SELECT id, parcel_id, address, city, zip, county_id,
                   row_number() OVER (PARTITION BY county_id ORDER BY random()) AS rn
            FROM properties
            WHERE county_id IN ('hillsborough', 'pinellas') AND parcel_id NOT LIKE 'LOAD-%'
              AND normalized_address IS NOT NULL AND address ~ '^[0-9]+ [A-Z0-9]' AND zip IS NOT NULL
        ) s WHERE rn <= :n ORDER BY county_id, id
    """), {"n": per_county}).mappings()]


def _raw(radar_id: str, county: str, apn: str | None, address: str, city: str, zip_: str, **extra) -> dict:
    raw = {
        "RadarID": radar_id, "State": "FL", "County": county, "APN": apn,
        "Address": address, "City": city, "ZipFive": zip_, "PType": "SFR",
        "Owner": f"E2E OWNER {radar_id} LLC", "OwnershipType": "Corporate",
        "OwnerAddress": "100 E2E WAY", "OwnerCity": "BOSTON", "OwnerState": "MA", "OwnerZipFive": "02110",
        "FirstLenderOriginal": "KIAVI FNDG INC", "FirstDate": "2025-09-15", "FirstAmount": 300000,
        "FirstTermInYears": 1,
        "Persons": [{"PersonType": "Person", "isPrimaryContact": 1, "OwnershipRole": "Principal",
                     "FirstName": "JANE", "LastName": f"DOE{radar_id[-3:]}"}],
    }
    raw.update(extra)
    return raw


def build_fixture(session: Session, per_county: int) -> dict[str, Any]:
    parcels = _real_parcels(session, per_county)
    records, expected_link = [], {}
    for i, p in enumerate(parcels):
        rid = f"{PREFIX}{i:03d}"
        records.append(_raw(rid, p["county_id"].upper(), p["parcel_id"], p["address"], p["city"] or "", p["zip"]))
        expected_link[rid] = p["id"]
    unloaded = [f"{PREFIX}MD{i}" for i in range(2)]
    for rid in unloaded:
        records.append(_raw(rid, "MIAMI-DADE", f"{rid}-APN", "1 E2E BISCAYNE BLVD", "MIAMI", "33132"))
    long_term, no_apn = f"{PREFIX}LONG", f"{PREFIX}NOAPN"
    records.append(_raw(long_term, "HILLSBOROUGH", f"{long_term}-APN", "2 E2E ST", "TAMPA", "33602",
                        FirstTermInYears=30))
    records.append(_raw(no_apn, "HILLSBOROUGH", None, "3 E2E ST", "TAMPA", "33602"))

    valid = list(expected_link) + unloaded
    no_contact = valid[0]            # traced but nothing found -> skipped
    opted_out = valid[1]             # email on the opt-out list -> suppressed
    contacts = {rid: f"e2e-{rid.lower()}@example.com" for rid in valid if rid != no_contact}
    return {
        "records": records, "expected_link": expected_link, "unloaded": unloaded,
        "excluded": [long_term, no_apn], "valid": valid, "no_contact": no_contact,
        "opted_out": opted_out, "contacts": contacts,
    }


def write_trace_csv(contacts: dict[str, str]) -> Path:
    f = tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, newline="", encoding="utf-8")
    w = csv.DictWriter(f, fieldnames=["RadarID", "emails", "phones"])
    w.writeheader()
    for rid, email in contacts.items():
        w.writerow({"RadarID": rid, "emails": email, "phones": ""})
    f.close()
    return Path(f.name)


def q(session: Session, sql: str, **params) -> Any:
    return session.execute(text(sql), params).scalar()


def rows(session: Session, sql: str, **params) -> list[dict]:
    return [dict(r) for r in session.execute(text(sql), params).mappings()]


def run_checks(session: Session, fx: dict[str, Any], trace: Path) -> list[Result]:
    results: list[Result] = []
    like = PREFIX + "%"
    hub_before = tuple(session.execute(text(
        "SELECT (SELECT COUNT(*) FROM properties), (SELECT COUNT(*) FROM counties)")).one())

    # Preconditions inside the rolled-back transaction.
    session.execute(text("UPDATE fa_max_backflip_campaign_feed SET last_success_at = now() WHERE id = 1"))
    session.execute(text("INSERT INTO email_opt_outs (email) VALUES (:e)"), {"e": fx["contacts"][fx["opted_out"]]})

    fake = FakePropertyRadarPort(canned_records=fx["records"])
    port_mod.get_property_radar_port = lambda: fake  # runner resolves the port through this factory

    stages = runner.default_stages(trace)
    s1 = runner.run(session, stages, mode="backlog", state="FL", campaign=CAMPAIGN, apply=True)
    n_valid, n_total = len(fx["valid"]), len(fx["records"])

    # ── pull ──
    run1 = rows(session, "SELECT run_type, status, records_fetched, exports_consumed FROM property_radar_pull_runs "
                         "WHERE state='FL' AND campaign=:c ORDER BY id DESC LIMIT 1", c=CAMPAIGN)
    results.append(Result("pull: free count before buying", s1.would_fetch == n_total, f"count={s1.would_fetch}"))
    results.append(Result("pull: run recorded as done with counts",
                          bool(run1) and run1[0]["status"] == "done" and run1[0]["records_fetched"] == n_valid
                          and run1[0]["exports_consumed"] == n_total, str(run1[:1])))
    seen = q(session, "SELECT COUNT(*) FROM property_radar_seen_ids WHERE radar_id LIKE :p", p=like)
    results.append(Result("pull: bought records marked seen", seen == n_valid, f"{seen}/{n_valid}"))

    # ── stage ──
    staged = q(session, "SELECT COUNT(*) FROM property_radar_records WHERE radar_id LIKE :p", p=like)
    excluded_staged = q(session, "SELECT COUNT(*) FROM property_radar_records WHERE radar_id = ANY(:r)", r=fx["excluded"])
    results.append(Result("stage: every valid record staged once", staged == n_valid and s1.staged["inserted"] == n_valid,
                          f"rows={staged} summary={s1.staged}"))
    results.append(Result("stage: long-term + no-APN records excluded", excluded_staged == 0, f"{excluded_staged} staged"))
    sample = rows(session, "SELECT property_address, zip, lender_name, loan_recorded_date, principal_name, county_name "
                           "FROM property_radar_records WHERE radar_id = :r", r=fx["valid"][2])[0]
    results.append(Result("stage: contract fields populated (no silent blanks)",
                          all(sample[k] for k in ("property_address", "zip", "lender_name",
                                                  "loan_recorded_date", "principal_name", "county_name")), str(sample)))

    # ── link ──
    links = {r["radar_id"]: r["property_id"] for r in rows(
        session, "SELECT radar_id, property_id FROM property_radar_records WHERE radar_id LIKE :p", p=like)}
    correct = sum(links.get(rid) == pid for rid, pid in fx["expected_link"].items())
    wrong = [rid for rid, pid in fx["expected_link"].items() if links.get(rid) not in (None, pid)]
    results.append(Result("link: real parcels linked to the right property",
                          correct == len(fx["expected_link"]) and not wrong,
                          f"{correct}/{len(fx['expected_link'])} wrong={wrong}"))
    results.append(Result("link: unloaded county stays unlinked",
                          all(links.get(r) is None for r in fx["unloaded"]), ""))

    # ── handoff ──
    decisions = {r["radar_id"]: (r["outcome"], r["reason"]) for r in rows(
        session, "SELECT radar_id, outcome, reason FROM property_radar_handoff_decisions WHERE radar_id LIKE :p", p=like)}
    handed = [r for r, (o, _) in decisions.items() if o == "handed_off"]
    expected_handed = n_valid - 2
    results.append(Result("handoff: clean leads handed off", len(handed) == expected_handed,
                          f"{len(handed)}/{expected_handed} summary={s1.handoff}"))
    results.append(Result("handoff: no contact -> skipped",
                          decisions.get(fx["no_contact"]) == ("skipped", "no_contact_data"), str(decisions.get(fx["no_contact"]))))
    results.append(Result("handoff: opted-out email -> suppressed",
                          decisions.get(fx["opted_out"]) == ("suppressed", "email_opt_out"), str(decisions.get(fx["opted_out"]))))
    people = q(session, "SELECT COUNT(*) FROM fa_max_persons WHERE source_reference LIKE :p", p="property_radar:" + like)
    opps = q(session, "SELECT COUNT(*) FROM fa_max_opportunities WHERE source_reference LIKE :p", p="property_radar:" + like)
    results.append(Result("handoff: one person + one opportunity per handed-off lead",
                          people == opps == expected_handed, f"persons={people} opps={opps}"))
    assoc = q(session, "SELECT COUNT(*) FROM fa_max_property_associations a JOIN fa_max_persons p USING (person_id) "
                       "WHERE p.source_reference LIKE :p", p="property_radar:" + like)
    linked_handed = sum(1 for r in handed if links.get(r))
    results.append(Result("handoff: linked leads get a property association", assoc == linked_handed,
                          f"{assoc}/{linked_handed}"))

    # ── ownership + tag ──
    owned = rows(session, "SELECT radar_id, campaign, source, status, opportunity_id IS NOT NULL AS has_opp "
                          "FROM lead_campaign_assignments WHERE radar_id LIKE :p", p=like)
    results.append(Result("ownership: every handed-off lead owned + tagged",
                          len(owned) == expected_handed and all(
                              o["campaign"] == CAMPAIGN and o["source"] == "property_radar"
                              and o["status"] == "active" and o["has_opp"] for o in owned)
                          and {o["radar_id"] for o in owned} == set(handed), f"{len(owned)} rows"))
    results.append(Result("ownership: nobody suppressed/skipped is owned",
                          not ({o["radar_id"] for o in owned} & {fx["no_contact"], fx["opted_out"]}), ""))

    # ── second daily run: watermark + seen, nothing new ──
    s2 = runner.run(session, stages, mode="daily", state="FL", campaign=CAMPAIGN, apply=True)
    staged2 = q(session, "SELECT COUNT(*) FROM property_radar_records WHERE radar_id LIKE :p", p=like)
    people2 = q(session, "SELECT COUNT(*) FROM fa_max_persons WHERE source_reference LIKE :p", p="property_radar:" + like)
    runs = rows(session, "SELECT run_type, status FROM property_radar_pull_runs WHERE state='FL' AND campaign=:c "
                         "ORDER BY id DESC LIMIT 2", c=CAMPAIGN)
    results.append(Result("rerun: nothing restaged, no duplicate people",
                          s2.staged["inserted"] == 0 and staged2 == staged and people2 == people,
                          f"staged={s2.staged} persons {people}->{people2}"))
    results.append(Result("rerun: daily run recorded", runs and runs[0] == {"run_type": "daily", "status": "done"},
                          str(runs)))

    # ── dry run writes nothing ──
    counts = lambda: tuple(session.execute(text(  # noqa: E731
        "SELECT (SELECT COUNT(*) FROM property_radar_records), (SELECT COUNT(*) FROM property_radar_pull_runs), "
        "(SELECT COUNT(*) FROM property_radar_handoff_decisions), (SELECT COUNT(*) FROM lead_campaign_assignments), "
        "(SELECT COUNT(*) FROM fa_max_persons)")).one())
    before = counts()
    s3 = runner.run(session, stages, mode="daily", state="FL", campaign=CAMPAIGN, apply=False)
    results.append(Result("dry run: counts only, writes nothing", counts() == before and s3.would_fetch == n_total,
                          f"{before} -> {counts()}"))

    # ── rollup on the handed-off people ──
    person = q(session, "SELECT CAST(person_id AS text) FROM lead_campaign_assignments WHERE radar_id = :r", r=handed[0])
    session.execute(text(
        "INSERT INTO relay_approval_queue (idempotency_key, channel, recipient, payload, status, person_id, thread_id) "
        "VALUES ('e2e-relay', 'email', 'x@example.com', '{}'::jsonb, 'sent', CAST(:p AS uuid), 'E2E-OPP-1')"), {"p": person})
    session.execute(text("INSERT INTO fa_max_concierge_log (person_id, inbound_channel, classification) "
                         "VALUES (CAST(:p AS uuid), 'email', 'interested')"), {"p": person})
    session.execute(text(
        "INSERT INTO fa_max_bookings (booking_ref, calendar_id, attendee_email, topic, starts_at, ends_at, status, person_id) "
        "VALUES ('e2e-booking', 'e2e', 'x@example.com', 'e2e', now() + interval '1 day', "
        "now() + interval '1 day 30 minutes', 'confirmed', CAST(:p AS uuid))"), {"p": person})
    session.execute(text("INSERT INTO agent_lane_opportunity_outcomes (opportunity_thread_id, outcome, reason_code, coded_by) "
                         "VALUES ('E2E-OPP-1', 'lost', 'loan_paid_off', 'e2e')"))
    roll = {r.campaign: r for r in pilot_rollup(session)}.get(CAMPAIGN)
    results.append(Result("rollup: activity credited to the owning campaign",
                          roll is not None and (roll.reach, roll.reply, roll.booked, roll.paid_off) >= (1, 1, 1, 1),
                          str(roll)))

    hub_after = tuple(session.execute(text(
        "SELECT (SELECT COUNT(*) FROM properties), (SELECT COUNT(*) FROM counties)")).one())
    results.append(Result("hub: properties/counties untouched", hub_before == hub_after, f"{hub_before} -> {hub_after}"))
    return results


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--per-county", type=int, default=8)
    args = ap.parse_args()

    engine = create_engine(str(get_settings().database_url))
    results: list[Result] = []
    trace: Path | None = None
    with engine.connect() as conn:
        outer = conn.begin()
        session = Session(bind=conn, join_transaction_mode="create_savepoint")
        try:
            fx = build_fixture(session, args.per_county)
            trace = write_trace_csv(fx["contacts"])
            logger.info("fixture: %d API records (%d valid, %d excluded)",
                        len(fx["records"]), len(fx["valid"]), len(fx["excluded"]))
            results = run_checks(session, fx, trace)
        finally:
            session.close()
            outer.rollback()
            if trace:
                trace.unlink(missing_ok=True)

    with engine.connect() as conn:
        left = conn.execute(text(
            "SELECT (SELECT COUNT(*) FROM property_radar_records WHERE radar_id LIKE :p) + "
            "(SELECT COUNT(*) FROM lead_campaign_assignments WHERE radar_id LIKE :p) + "
            "(SELECT COUNT(*) FROM fa_max_persons WHERE source_reference LIKE :r)"),
            {"p": PREFIX + "%", "r": "property_radar:" + PREFIX + "%"}).scalar()
    results.append(Result("cleanup: rollback left no E2E rows", left == 0, f"{left} rows"))

    logger.info("")
    for r in results:
        logger.info("%s  %-55s %s", "PASS" if r.passed else "FAIL", r.name, r.detail[:160])
    failed = [r for r in results if not r.passed]
    logger.info("\n%d/%d passed", len(results) - len(failed), len(results))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
