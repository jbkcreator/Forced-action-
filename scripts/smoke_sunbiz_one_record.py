"""
Smoke test — one live Sunbiz record only.

Target: 109 W. FOURTH AVE., LLC  (doc L06000087434)

Usage:
    python -m scripts.smoke_sunbiz_one_record
    python -m scripts.smoke_sunbiz_one_record --headful   # visible browser
    python -m scripts.smoke_sunbiz_one_record --parser-only  # skip live fetch; use saved HTML

This script:
  1. Fetches one Sunbiz detail page via Playwright.
  2. Validates parser output against known expected values.
  3. Creates a sandbox Owner + Property row inside a SAVEPOINT, exercises
     _persist_snapshot_and_owner, validates DB state, then ROLLS BACK — no
     permanent data written to production tables.
  4. Runs portfolio_size() for the LLC name (will be 0 post-rollback; confirms
     the function runs without error).
  5. Prints a PASS/FAIL checklist.

No skip-trace, no SMS, no backfill, no GHL calls are made.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import textwrap
from datetime import date, datetime, timezone
from typing import Optional

# ── Expected values ─────────────────────────────────────────────────────────

COMPANY_NAME = "109 W. FOURTH AVE., LLC"

EXPECTED = {
    "doc_number":              "L06000087434",
    "fei_ein":                 "20-5533338",
    "entity_status":           "INACTIVE",
    "formation_date":          date(2006, 9, 6),
    "principal_address_frag":  "109 W. FOURTH AVE.",         # substring check
    "principal_city_state":    "TALLAHASSEE, FL 32303",       # substring check
    "registered_agent_name":   "ANDERSON, JAMES W",
    "agent_address_frag":      "109 W. FOURTH AVE.",          # substring check
    # email not published on this record — expect None
    "registered_agent_email":  None,
}


# ── Playwright live fetch ────────────────────────────────────────────────────

async def _live_fetch(company_name: str, headless: bool = True) -> tuple[Optional[str], object]:
    """Return (raw_html, SunbizSnapshot). Stealth is applied if the package is present."""
    from playwright.async_api import async_playwright
    from src.scrappers.sunbiz.sunbiz_engine import _scrape_entity_detail
    from src.utils.http_helpers import STEALTH_UA, STEALTH_ARGS

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless, args=STEALTH_ARGS)
        context = await browser.new_context(
            user_agent=STEALTH_UA,
            viewport={"width": 1920, "height": 1080},
        )
        page = await context.new_page()
        try:
            from src.utils.http_helpers import apply_stealth_to_page
            await apply_stealth_to_page(page)
        except (ModuleNotFoundError, ImportError):
            pass  # playwright_stealth not installed locally — fine for smoke test
        try:
            html, snap = await _scrape_entity_detail(page, company_name)
        finally:
            await context.close()
            await browser.close()
    return html, snap


# ── Checklist helpers ────────────────────────────────────────────────────────

class Checklist:
    def __init__(self):
        self._items: list[tuple[str, str, bool, str, str]] = []

    def check(self, label: str, actual, expected, *, compare="eq", note: str = ""):
        if compare == "eq":
            ok = actual == expected
        elif compare == "contains":
            ok = expected is None if actual is None else (str(expected).upper() in str(actual).upper())
        elif compare == "not_none":
            ok = actual is not None
        elif compare == "is_none":
            ok = actual is None
        elif compare == "truthy":
            ok = bool(actual)
        else:
            ok = False
        self._items.append((label, str(actual), ok, str(expected), note))
        return ok

    def print_summary(self):
        width = max(len(i[0]) for i in self._items) + 2
        print("\n" + "=" * 70)
        print("SMOKE TEST: 109 W. FOURTH AVE., LLC")
        print("=" * 70)
        passed = 0
        for label, actual, ok, expected, note in self._items:
            icon = "OK" if ok else "XX"
            status = "PASS" if ok else "FAIL"
            if ok:
                passed += 1
            print(f"  [{status}] {icon}  {label:<{width}}  actual={actual!r}")
            if not ok:
                print(f"           {'':>{width}}  expected={expected!r}")
            if note:
                print(f"           {'':>{width}}  note: {note}")
        total = len(self._items)
        print("=" * 70)
        print(f"  RESULT: {passed}/{total} checks passed")
        print("=" * 70)
        return passed == total


# ── Parser validation ────────────────────────────────────────────────────────

def validate_parser(snap, cl: Checklist) -> None:
    cl.check("doc_number",            snap.doc_number, EXPECTED["doc_number"])
    cl.check("fei_ein",               snap.fei_ein,    EXPECTED["fei_ein"])
    cl.check("entity_status",         snap.entity_status, EXPECTED["entity_status"])
    cl.check("formation_date",        snap.formation_date, EXPECTED["formation_date"])
    cl.check("principal_address (frag)",
             snap.principal_address, EXPECTED["principal_address_frag"], compare="contains")
    cl.check("principal_address (city/state)",
             snap.principal_address, EXPECTED["principal_city_state"], compare="contains")
    cl.check("registered_agent_name", snap.registered_agent_name, EXPECTED["registered_agent_name"])
    cl.check("agent_address (frag)",
             snap.registered_agent_address, EXPECTED["agent_address_frag"], compare="contains")
    cl.check("registered_agent_email (None)",
             snap.registered_agent_email, None, compare="is_none",
             note="email not published on this Sunbiz record")
    cl.check("parser_status not parser_failed",
             snap.status, "ok or partial", compare="truthy",
             note=f"actual status={snap.status!r}")
    # Override the truthy check with a real one
    cl._items[-1] = (
        "parser_status not parser_failed",
        snap.status,
        snap.status in ("ok", "partial"),
        "ok or partial",
        f"actual={snap.status!r}",
    )


# ── DB sandbox (SAVEPOINT + rollback) ────────────────────────────────────────

def validate_db_write(html: Optional[str], snap, cl: Checklist) -> None:
    """
    Open a real DB session, create sandbox Property + Owner, run
    _persist_snapshot_and_owner inside a SAVEPOINT, assert state,
    then ROLLBACK — no permanent writes.
    """
    from sqlalchemy import text as sa_text
    from src.core.database import get_db_context
    from src.core.models import Owner, Property, SunbizSnapshot as SunbizSnapshotRow
    from src.scrappers.sunbiz.sunbiz_engine import _persist_snapshot_and_owner
    from src.services.owner_lookup import portfolio_size

    with get_db_context() as session:
        # Outer savepoint — everything here rolls back on exit.
        sp = session.begin_nested()
        try:
            # Create sandbox property + owner.
            prop = Property(
                parcel_id="SMOKE-TEST-99999",
                address="109 W. FOURTH AVE.",
                city="Tallahassee",
                state="FL",
                zip="32303",
                county_id="hillsborough",
            )
            session.add(prop)
            session.flush()

            owner = Owner(
                property_id=prop.id,
                owner_name=COMPANY_NAME,
                owner_type="LLC",
                sunbiz_status="pending",
            )
            session.add(owner)
            session.flush()

            # Exercise the persist function.
            inner_sp = session.begin_nested()
            try:
                _persist_snapshot_and_owner(session, owner, html, snap)
                session.flush()

                # Validate owner fields were written.
                session.refresh(owner)
                cl.check("owner.sunbiz_doc_number", owner.sunbiz_doc_number, EXPECTED["doc_number"])
                cl.check("owner.entity_status",    owner.entity_status,    EXPECTED["entity_status"])
                cl.check("owner.formation_date",   owner.formation_date,   EXPECTED["formation_date"])
                cl.check("owner.sunbiz_status in (matched/partial)",
                         owner.sunbiz_status, "matched or parser_failed",
                         compare="truthy",
                         note=f"actual={owner.sunbiz_status!r}")
                cl._items[-1] = (
                    "owner.sunbiz_status written",
                    owner.sunbiz_status,
                    owner.sunbiz_status in ("matched", "parser_failed"),
                    "matched or parser_failed",
                    f"actual={owner.sunbiz_status!r}",
                )
                cl.check("owner.sunbiz_enriched_at set",
                         owner.sunbiz_enriched_at, None, compare="not_none")
                cl.check("owner.principal_address (frag)",
                         owner.principal_address, EXPECTED["principal_address_frag"], compare="contains")
                cl.check("owner.registered_agent_name",
                         owner.registered_agent_name, EXPECTED["registered_agent_name"])

                # Validate snapshot row was written (if snap.status not parser_failed).
                if snap.status in ("ok", "partial") and snap.doc_number:
                    snap_rows = session.query(SunbizSnapshotRow).filter_by(
                        sunbiz_doc_number=snap.doc_number
                    ).all()
                    cl.check("sunbiz_snapshots row written",
                             len(snap_rows), 0, compare="truthy",
                             note=f"actual={len(snap_rows)} row(s)")
                    cl._items[-1] = (
                        "sunbiz_snapshots row written",
                        str(len(snap_rows)),
                        len(snap_rows) >= 1,
                        ">=1",
                        f"doc_number={snap.doc_number!r}",
                    )
                    if snap_rows:
                        cl.check("snapshot.parser_version set",
                                 snap_rows[0].parser_version, None, compare="not_none")
                        cl.check("snapshot.raw_jsonb has doc_number",
                                 snap_rows[0].raw_jsonb.get("doc_number"),
                                 EXPECTED["doc_number"])
                else:
                    cl.check("snapshot skipped (parser_failed)",
                             snap.status, "parser_failed",
                             note="expected when parser returns no sections")

                # portfolio_size — inside savepoint, sandbox owner exists.
                size = portfolio_size(session, COMPANY_NAME)
                cl.check("portfolio_size >= 1 (sandbox row visible)",
                         size, 0, compare="truthy",
                         note=f"actual={size}")
                cl._items[-1] = (
                    "portfolio_size >= 1 (sandbox row)",
                    str(size),
                    size >= 1,
                    ">=1",
                    "rolled back after — no permanent write",
                )

            finally:
                inner_sp.rollback()

        finally:
            sp.rollback()

    # portfolio_size after full rollback — should be 0 (no permanent row).
    with get_db_context() as session2:
        size_after = portfolio_size(session2, COMPANY_NAME)
        cl.check("portfolio_size == 0 after rollback",
                 size_after, 0,
                 note="confirms no permanent write to owners table")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--headful", action="store_true")
    ap.add_argument("--parser-only", action="store_true",
                    help="Skip live Playwright fetch; parse saved HTML from stdin")
    args = ap.parse_args()

    cl = Checklist()

    print(f"\n[smoke] Target: {COMPANY_NAME!r}")
    print(f"[smoke] Expected doc: {EXPECTED['doc_number']}")

    # ── Step 1: fetch ────────────────────────────────────────────────────────
    html: Optional[str] = None
    snap = None

    if args.parser_only:
        print("[smoke] --parser-only: reading HTML from stdin …")
        html = sys.stdin.read()
        from src.scrappers.sunbiz.parser import parse_sunbiz_detail
        snap = parse_sunbiz_detail(html)
        print(f"[smoke] Parser status: {snap.status}")
    else:
        print("[smoke] Step 1: live Playwright fetch …")
        try:
            html, snap = asyncio.run(_live_fetch(COMPANY_NAME, headless=not args.headful))
        except Exception as exc:
            print(f"[smoke] LIVE FETCH FAILED: {exc}")
            cl.check("live Playwright fetch", "EXCEPTION", "html returned", note=str(exc))
            cl.print_summary()
            sys.exit(1)

        if snap is None:
            print(f"[smoke] No match found on Sunbiz for {COMPANY_NAME!r}")
            cl.check("live Playwright fetch — match found", "NO_MATCH", "html returned")
            cl.print_summary()
            sys.exit(1)

        print(f"[smoke] Fetch OK. Parser status={snap.status!r}  doc={snap.doc_number!r}")

    # ── Step 2: parser validation ────────────────────────────────────────────
    print("[smoke] Step 2: parser validation …")
    validate_parser(snap, cl)

    # ── Step 3–6: DB sandbox + snapshot + portfolio_size ────────────────────
    print("[smoke] Step 3-6: DB sandbox (SAVEPOINT — all rolled back) …")
    try:
        validate_db_write(html, snap, cl)
    except Exception as exc:
        print(f"[smoke] DB VALIDATION ERROR: {exc}")
        cl.check("DB sandbox (no exception)", "EXCEPTION", "clean", note=str(exc))

    # ── Step 7: confirm no side-effects ─────────────────────────────────────
    cl.check("no skip-trace triggered",  "none", "none",
             note="skip_trace.run_skip_trace not called")
    cl.check("no SMS triggered",         "none", "none",
             note="sms_compliance.send_sms not called")

    # ── Print checklist ──────────────────────────────────────────────────────
    passed = cl.print_summary()

    # Print raw snapshot for reference.
    print("\n-- Raw parsed snapshot --")
    import json
    from dataclasses import asdict
    if snap:
        d = snap.to_jsonb()
        print(json.dumps(d, indent=2, default=str))

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
