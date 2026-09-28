"""End-to-end QA harness for PropertyRadar staging + linking.

Runs every scenario inside ONE transaction that is always rolled back, so it
is safe against the shared DB. Data sets:
  A  PropertyRadar pack samples (real field shapes, anonymized APNs)
  B  real FA parcels in loaded counties re-shaped as PropertyRadar records
  C  hand-built edge cases (change detection, gaps, bad records, scale)

Usage:
    PYTHONPATH=. python scripts/property_radar_qa_harness.py --samples-dir <team-pack>/samples
Exit code 0 = all pass.
"""
from __future__ import annotations

import argparse
import json
import logging
import random
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from config.property_radar import COUNTY_FIPS_TO_SLUG
from config.settings import get_settings
from src.services.property_radar.linking import link_unlinked
from src.services.property_radar.staging import upsert_records

logging.basicConfig(level=logging.WARNING, format="%(message)s")
logger = logging.getLogger("qa")
logger.setLevel(logging.INFO)

STATE_FIPS = "12"
UNLOADED_FIPS = "12025"  # Miami-Dade legacy FIPS; not a loaded FA county
SLUG_TO_FIPS = {slug: fips for fips, slug in COUNTY_FIPS_TO_SLUG.items()}
LINK_COUNTIES = ("hillsborough", "pinellas")


@dataclass
class Result:
    name: str
    passed: bool
    detail: str = ""


# ── data builders ────────────────────────────────────────────────────────────

def _principal(persons: list[dict[str, Any]] | None) -> str | None:
    for p in persons or []:
        if p.get("PersonType") == "Person" and (p.get("FirstName") or p.get("LastName")):
            return " ".join(x for x in (p.get("FirstName"), p.get("LastName")) if x)
    return None


def from_pack(raw: dict[str, Any], campaign: str) -> dict[str, Any]:
    county = (raw.get("County") or "").lower()
    return {
        "radar_id": f"TEST-QA-A-{raw['RadarID']}",
        "state_fips": STATE_FIPS,
        "county_fips": SLUG_TO_FIPS.get(county, UNLOADED_FIPS),
        "apn": raw["APN"],
        "state": raw.get("State"),
        "county_name": raw.get("County"),
        "property_address": raw.get("Address"),
        "city": raw.get("City"),
        "zip": raw.get("ZipFive"),
        "property_type": raw.get("PType"),
        "owner_name": raw.get("Owner"),
        "ownership_type": raw.get("OwnershipType"),
        "mailing_address": raw.get("OwnerAddress"),
        "mailing_city": raw.get("OwnerCity"),
        "mailing_state": raw.get("OwnerState"),
        "mailing_zip": raw.get("OwnerZipFive"),
        "principal_name": _principal(raw.get("Persons")),
        "lender_name": raw.get("FirstLenderOriginal"),
        "loan_amount": raw.get("FirstAmount"),
        "loan_recorded_date": raw.get("FirstDate"),
        "loan_term_years": str(raw["FirstTermInYears"]) if raw.get("FirstTermInYears") is not None else None,
        "est_maturity_date": None,
        "loan_doc_number": None,
        "campaign": campaign,
        "raw": raw,
    }


def load_pack(samples_dir: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    group = json.loads((samples_dir / "group1_campaign_samples.json").read_text(encoding="utf-8"))
    for campaign, recs in group["records"].items():
        out += [from_pack(r, campaign.lower().replace(" ", "_")) for r in recs]
    maturity = json.loads((samples_dir / "maturity_records_sample.json").read_text(encoding="utf-8"))
    out += [from_pack(r, "maturity_target_lender") for r in maturity["records"]]
    return out


def edge(n: str, **overrides: Any) -> dict[str, Any]:
    base = {
        "radar_id": f"TEST-QA-C-{n}", "state_fips": STATE_FIPS, "county_fips": UNLOADED_FIPS,
        "apn": f"TEST-QA-C-APN-{n}", "state": "FL", "county_name": "MIAMI-DADE",
        "property_address": f"{n} QA TEST ST", "city": "MIAMI", "zip": "33101",
        "owner_name": "QA HOLDINGS LLC", "lender_name": "KIAVI FUNDING INC",
        "loan_recorded_date": "2025-06-01", "loan_doc_number": f"DOC-{n}",
        "campaign": "maturity_target_lender", "raw": {},
    }
    base.update(overrides)
    return base


_SUFFIX_SWAPS = {"DR": "DRIVE", "ST": "STREET", "AVE": "AVENUE", "CT": "COURT", "LN": "LANE",
                 "RD": "ROAD", "BLVD": "BOULEVARD", "WAY": "WAY", "PL": "PLACE", "CIR": "CIRCLE"}


def _swap_suffix(address: str) -> str:
    """Spell out the street suffix and lower-case, as a different source would."""
    words = address.upper().split()
    words = [_SUFFIX_SWAPS.get(w, w) for w in words]
    return " ".join(words).title()


def build_real_links(session: Session, per_county: int) -> list[tuple[dict[str, Any], int, str]]:
    """Real FA parcels re-shaped as PR records. Returns (record, expected_property_id, variant)."""
    rows = session.execute(text("""
        SELECT id, parcel_id, address, city, zip, county_id FROM (
            SELECT id, parcel_id, address, city, zip, county_id,
                   row_number() OVER (PARTITION BY county_id ORDER BY random()) AS rn
            FROM properties
            WHERE county_id = ANY(:counties)
              AND parcel_id NOT LIKE 'LOAD-%'
              AND normalized_address IS NOT NULL
              AND address ~ '^[0-9]+ [A-Z0-9]' AND zip IS NOT NULL
        ) s WHERE rn <= :n
    """), {"counties": list(LINK_COUNTIES), "n": per_county}).mappings().all()

    variants = ("exact_apn", "exact_apn", "separator_apn", "address_only", "street_suffix")
    out = []
    for i, r in enumerate(rows):
        variant = variants[i % len(variants)]
        apn, address = r["parcel_id"], r["address"]
        if variant == "separator_apn":
            apn = "-".join(apn[i:i + 4] for i in range(0, len(apn), 4))
        elif variant in ("address_only", "street_suffix"):
            apn = f"TEST-QA-NOPARCEL-{r['id']}"
        if variant == "street_suffix":
            address = _swap_suffix(address)
        rec = {
            "radar_id": f"TEST-QA-B-{r['id']}", "state_fips": STATE_FIPS,
            "county_fips": SLUG_TO_FIPS[r["county_id"]], "apn": apn, "state": "FL",
            "county_name": r["county_id"].upper(), "property_address": address,
            "city": r["city"], "zip": r["zip"], "owner_name": "QA REAL PARCEL",
            "campaign": "maturity_target_lender", "raw": {},
        }
        out.append((rec, r["id"], variant))
    return out


# ── helpers ──────────────────────────────────────────────────────────────────

def row(session: Session, radar_id: str) -> dict[str, Any] | None:
    r = session.execute(
        text("SELECT * FROM property_radar_records WHERE radar_id = :r"), {"r": radar_id}
    ).mappings().first()
    return dict(r) if r else None


def count(session: Session, prefix: str = "TEST-QA-") -> int:
    return session.execute(
        text("SELECT COUNT(*) FROM property_radar_records WHERE radar_id LIKE :p"), {"p": prefix + "%"}
    ).scalar()


def hub_counts(session: Session) -> tuple[int, int]:
    return tuple(session.execute(
        text("SELECT (SELECT COUNT(*) FROM properties), (SELECT COUNT(*) FROM counties)")
    ).one())


def sequence(session: Session, n: str, steps: list[dict[str, Any]]) -> dict[str, Any]:
    for step in steps:
        upsert_records(session, [edge(n, **step)])
    return row(session, f"TEST-QA-C-{n}")


# ── scenarios ────────────────────────────────────────────────────────────────

def scenario_schema(session: Session) -> list[Result]:
    cons = {r[0] for r in session.execute(text(
        "SELECT conname FROM pg_constraint WHERE conrelid = 'property_radar_records'::regclass"))}
    idx = {r[0] for r in session.execute(text(
        "SELECT indexname FROM pg_indexes WHERE tablename = 'property_radar_records'"))}
    notnull = {r[0] for r in session.execute(text(
        "SELECT column_name FROM information_schema.columns WHERE table_name='property_radar_records' "
        "AND is_nullable='NO'"))}
    need_cons = {"uq_pr_state_county_apn", "uq_pr_radar_id", "ck_pr_status"}
    need_idx = {"ix_pr_campaign", "ix_pr_county_fips", "ix_pr_property_id"}
    need_nn = {"radar_id", "state_fips", "county_fips", "apn", "state", "county_name"}
    return [
        Result("schema: constraints", need_cons <= cons, str(sorted(need_cons - cons))),
        Result("schema: indexes", need_idx <= idx, str(sorted(need_idx - idx))),
        Result("schema: county NOT NULL", need_nn <= notnull, str(sorted(need_nn - notnull))),
    ]


def scenario_pack(session: Session, pack: list[dict[str, Any]]) -> list[Result]:
    unique_keys = {(r["state_fips"], r["county_fips"], r["apn"]) for r in pack}
    unique_rids = {r["radar_id"] for r in pack}
    ins, upd, skip = upsert_records(session, pack)
    first = count(session, "TEST-QA-A-")
    seen = session.execute(text(
        "SELECT min(first_seen_at), max(last_seen_at) FROM property_radar_records WHERE radar_id LIKE 'TEST-QA-A-%'")).one()
    session.execute(text("SELECT pg_sleep(0.01)"))
    ins2, upd2, skip2 = upsert_records(session, pack)
    second = count(session, "TEST-QA-A-")
    counties_set = session.execute(text(
        "SELECT COUNT(*) FROM property_radar_records WHERE radar_id LIKE 'TEST-QA-A-%' "
        "AND (county_name IS NULL OR county_fips IS NULL)")).scalar()
    principal = session.execute(text(
        "SELECT COUNT(principal_name) FROM property_radar_records WHERE radar_id LIKE 'TEST-QA-A-%'")).scalar()
    expected = min(len(unique_keys), len(unique_rids))
    return [
        Result("pack: load", first == expected,
               f"{len(pack)} samples -> ins={ins} upd={upd} skip={skip}, rows={first}, unique keys={len(unique_keys)}"),
        Result("pack: reload leaves row count unchanged", second == first and ins2 == 0,
               f"rows {first}->{second}, ins={ins2} upd={upd2}"),
        Result("pack: county set on every row", counties_set == 0, f"{counties_set} rows missing county"),
        Result("pack: principal_name captured", principal > 0, f"{principal}/{first} rows have principal"),
        Result("pack: first_seen_at not moved on reload", session.execute(text(
            "SELECT min(first_seen_at) FROM property_radar_records WHERE radar_id LIKE 'TEST-QA-A-%'")).scalar() == seen[0], ""),
    ]


def scenario_changes(session: Session) -> list[Result]:
    checks: list[tuple[str, list[dict[str, Any]], Callable[[dict[str, Any]], bool]]] = [
        ("owner A -> B = sold, prior kept",
         [{"owner_name": "ALPHA LLC"}, {"owner_name": "BETA LLC"}],
         lambda r: r["status"] == "sold" and r["prior"]["owner_name"] == "ALPHA LLC"),
        ("abbreviated owner = no change",
         [{"owner_name": "KIAVI FNDG INC"}, {"owner_name": "Kiavi Funding, Inc"}],
         lambda r: r["status"] == "active" and not r["change_flags"]),
        ("owner A -> blank -> A = no change",
         [{"owner_name": "ALPHA LLC"}, {"owner_name": None}, {"owner_name": "ALPHA LLC"}],
         lambda r: r["status"] == "active" and r["owner_name"] == "ALPHA LLC"),
        ("doc 1 -> doc 2 = refinanced",
         [{"loan_doc_number": "D1"}, {"loan_doc_number": "D2"}],
         lambda r: r["status"] == "refinanced" and r["prior"]["loan_doc_number"] == "D1"),
        ("no doc, lender+date change = refinanced",
         [{"loan_doc_number": None}, {"loan_doc_number": None, "lender_name": "LIMA ONE", "loan_recorded_date": "2026-02-01"}],
         lambda r: r["status"] == "refinanced"),
        ("no doc, lender abbreviation only = no change",
         [{"loan_doc_number": None, "lender_name": "KIAVI FNDG INC"}, {"loan_doc_number": None, "lender_name": "KIAVI FUNDING INC"}],
         lambda r: r["status"] == "active"),
        ("sold then 3 unchanged reloads = still flagged",
         [{"owner_name": "ALPHA LLC"}, {"owner_name": "BETA LLC"}, {"owner_name": "BETA LLC"},
          {"owner_name": "BETA LLC"}, {"owner_name": "BETA LLC"}],
         lambda r: r["status"] == "sold" and r["change_flags"] == ["owner_changed"] and r["prior"]["owner_name"] == "ALPHA LLC"),
        ("owner + loan change = sold, both flags",
         [{"owner_name": "ALPHA LLC", "loan_doc_number": "D1"}, {"owner_name": "BETA LLC", "loan_doc_number": "D2"}],
         lambda r: r["status"] == "sold" and set(r["change_flags"]) == {"owner_changed", "loan_changed"}),
        ("refinanced then owner change = sold",
         [{"loan_doc_number": "D1"}, {"loan_doc_number": "D2"}, {"loan_doc_number": "D2", "owner_name": "BETA LLC"}],
         lambda r: r["status"] == "sold"),
    ]
    results = []
    for i, (name, steps, ok) in enumerate(checks):
        r = sequence(session, f"CHG{i:02d}", steps)
        results.append(Result(f"change: {name}", bool(r) and ok(r),
                              f"status={r and r['status']} flags={r and r['change_flags']}"))
    return results


def scenario_bad_input(session: Session) -> list[Result]:
    same_apn = upsert_records(session, [
        edge("DUPAPN1", apn="TEST-QA-SHARED", county_fips="12057", county_name="HILLSBOROUGH"),
        edge("DUPAPN2", apn="TEST-QA-SHARED", county_fips="12103", county_name="PINELLAS"),
    ])
    missing = upsert_records(session, [edge("NOCOUNTY", county_name=None), edge("NOAPN", apn=""), edge("GOOD1")])
    in_batch = upsert_records(session, [edge("DUPKEY", owner_name="FIRST"), edge("DUPKEY", owner_name="SECOND")])
    upsert_records(session, [edge("RIDA")])
    collision = upsert_records(session, [edge("RIDA", apn="TEST-QA-OTHER-APN")])

    before = count(session)
    failed = False
    try:
        with session.begin_nested():
            upsert_records(session, [edge("OK1"), edge("TOOLONG", state="FLORIDA"), edge("OK2")])
    except Exception:
        failed = True
    after = count(session)
    return [
        Result("input: same APN, two counties = 2 rows", same_apn[0] == 2, str(same_apn)),
        Result("input: missing county/APN skipped, rest loads", missing == (1, 0, 2), str(missing)),
        Result("input: dup key in batch, last wins", in_batch == (1, 0, 1)
               and row(session, "TEST-QA-C-DUPKEY")["owner_name"] == "SECOND", str(in_batch)),
        Result("input: radar_id held by another key skipped", collision == (0, 0, 1), str(collision)),
        Result("input: bad value fails whole batch atomically", failed and before == after,
               f"raised={failed} rows {before}->{after}"),
    ]


def scenario_linking(session: Session, per_county: int) -> list[Result]:
    real = build_real_links(session, per_county)
    upsert_records(session, [edge("LLC1", county_fips="12057", county_name="HILLSBOROUGH",
                                  apn="TEST-QA-LLC-NOPE", property_address=None, owner_name="QA MANY PROPS LLC")])
    upsert_records(session, [rec for rec, _, _ in real])

    t0 = time.monotonic()
    counts = link_unlinked(session, batch_size=7)
    elapsed = time.monotonic() - t0

    by_variant: dict[str, list[bool]] = {}
    wrong: list[str] = []
    for rec, expected_id, variant in real:
        got = row(session, rec["radar_id"])["property_id"]
        by_variant.setdefault(variant, []).append(got == expected_id)
        if got is not None and got != expected_id:
            wrong.append(f"{rec['radar_id']} -> {got} (expected {expected_id})")
    correct = sum(sum(v) for v in by_variant.values())
    rate = correct / len(real) if real else 0.0
    variant_txt = ", ".join(f"{k}={sum(v)}/{len(v)}" for k, v in sorted(by_variant.items()))

    unloaded = session.execute(text(
        "SELECT COUNT(*) FROM property_radar_records WHERE radar_id LIKE 'TEST-QA-%' "
        "AND county_fips = :f AND property_id IS NOT NULL"), {"f": UNLOADED_FIPS}).scalar()
    fake_linked = session.execute(text(
        "SELECT COUNT(*) FROM property_radar_records WHERE radar_id LIKE 'TEST-QA-A-%' "
        "AND property_id IS NOT NULL")).scalar()
    rerun = link_unlinked(session)
    mismatched = session.execute(text("""
        SELECT COUNT(*) FROM property_radar_records r JOIN properties p ON p.id = r.property_id
        WHERE r.radar_id LIKE 'TEST-QA-%'
          AND p.county_id <> (CASE r.county_fips """ + " ".join(
        f"WHEN '{f}' THEN '{s}'" for f, s in COUNTY_FIPS_TO_SLUG.items()) + """ END)
    """)).scalar()

    for rec, expected_id, variant in real[:20]:
        got = row(session, rec["radar_id"])
        logger.info("  spot  %-10s %-14s APN=%-24s link=%s method=%s conf=%s",
                    rec["county_name"], variant, rec["apn"], got["property_id"] == expected_id,
                    got["match_method"], got["match_confidence"])

    return [
        Result("link: real parcels link to correct property (>=80%)", rate >= 0.8,
               f"{correct}/{len(real)} = {rate:.0%} ({variant_txt}) in {elapsed:.1f}s"),
        Result("link: zero wrong-property links", not wrong, "; ".join(wrong[:5])),
        Result("link: every link in the record's own county", mismatched == 0, f"{mismatched} cross-county"),
        Result("link: unloaded county never linked", unloaded == 0, f"{unloaded} linked"),
        Result("link: anonymized pack samples never link", fake_linked == 0, f"{fake_linked} false links"),
        Result("link: LLC with no parcel/address not linked",
               row(session, "TEST-QA-C-LLC1")["property_id"] is None, ""),
        Result("link: rerun reprocesses only still-unlinked", rerun["linked"] == 0, str(rerun)),
        Result("link: small pages covered every record", counts["linked"] + counts["no_match"]
               + counts["skipped_county_mismatch"] >= len(real) + 1, str(counts)),
    ]


def scenario_scale(session: Session, n: int) -> list[Result]:
    batch = [edge(f"S{i:05d}") for i in range(n)]
    t0 = time.monotonic()
    ins, _, _ = upsert_records(session, batch)
    t_insert = time.monotonic() - t0
    t0 = time.monotonic()
    _, upd, _ = upsert_records(session, batch)
    t_update = time.monotonic() - t0
    return [Result(f"scale: {n} records insert+update", ins == n and upd == n and t_insert < 30 and t_update < 30,
                   f"insert {t_insert:.1f}s, update {t_update:.1f}s")]


# ── runner ───────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--samples-dir", type=Path, required=True)
    ap.add_argument("--per-county", type=int, default=10)
    ap.add_argument("--scale", type=int, default=1000)
    args = ap.parse_args()

    random.seed(7)
    engine = create_engine(str(get_settings().database_url))
    results: list[Result] = []
    with engine.connect() as conn:
        trans = conn.begin()
        session = Session(bind=conn)
        try:
            hub_before = hub_counts(session)
            steps: list[tuple[str, Callable[[], list[Result]]]] = [
                ("schema", lambda: scenario_schema(session)),
                ("pack", lambda: scenario_pack(session, load_pack(args.samples_dir))),
                ("changes", lambda: scenario_changes(session)),
                ("bad input", lambda: scenario_bad_input(session)),
                ("linking", lambda: scenario_linking(session, args.per_county)),
                ("scale", lambda: scenario_scale(session, args.scale)),
            ]
            for label, step in steps:
                t0 = time.monotonic()
                results += step()
                logger.info("[%s done in %.1fs]", label, time.monotonic() - t0)
            results.append(Result("hub: properties/counties untouched", hub_counts(session) == hub_before,
                                  f"{hub_before} -> {hub_counts(session)}"))
        finally:
            session.close()
            trans.rollback()

    with engine.connect() as conn:
        leftover = conn.execute(text(
            "SELECT COUNT(*) FROM property_radar_records WHERE radar_id LIKE 'TEST-QA-%'")).scalar()
    results.append(Result("cleanup: rollback left no QA rows", leftover == 0, f"{leftover} rows"))

    logger.info("")
    for r in results:
        logger.info("%s  %-55s %s", "PASS" if r.passed else "FAIL", r.name, r.detail)
    failed = [r for r in results if not r.passed]
    logger.info("\n%d/%d passed", len(results) - len(failed), len(results))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
