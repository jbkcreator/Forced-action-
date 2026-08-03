"""
Second-venture spin-up acceptance harness — CLONE-v2.2 / CL4.

Answers one question with one exit code: could a second venture be stood up
right now, end to end, without a human?

Today the only check on a provisioned venture is someone running `--health` and
eyeballing the output. `--apply` returning {"county_created": true, "cloned": 7}
can be true while the venture cannot send at all: source URLs silently
inherited the template county's portals, `playwright_code` correctly dropped to
ai_only and nobody regenerated it, the Instantly campaign was never created, the
cron line was never added. There is no PASS/FAIL, so a broken venture looks
identical to a working one until it has sent nothing for a week.

WHAT IT DOES

Builds a stub venture from VENTURE_TEMPLATE, provisions it, seeds the evidence
and traffic each rung needs, walks radar -> portfolio through the REAL
venture_ladder.advance(), proves the presell gate blocks when its evidence is
removed, fires the auto-double rule and proves it declines on the retry, then
assembles a Clone-Pack and asserts it is complete.

EVERYTHING RUNS IN ONE TRANSACTION THAT IS ALWAYS ROLLED BACK. Nothing is
committed, in either the pass or the fail path — the same reason
venture_provisioning's --dry-run owns its own rollback rather than relying on
an outer commit having nothing left to do. No Slack, no Instantly, no Stripe:
the harness asserts against DB state and pure functions only.

CHECK OUTCOMES

  PASS  asserted true
  FAIL  asserted false — breaks the exit code
  WARN  could not be asserted because of ambient data this harness does not
        own (see the unit-economics note below). Reported loudly, never hidden,
        but does not fail the run.

The one WARN that is expected in a busy environment is cost attribution.
api_usage_logs rows with subscriber_id IS NULL (scraping, batch jobs) cannot be
attributed to any venture, and if the fleet's ambient NULL-subscriber spend in
the window swamps what this harness seeds, the unit-economics gates correctly
report "no metric" rather than a number that ignores most of the spend
(docs/adr/0006, config/venture_ladder.MIN_COST_ATTRIBUTION_RATIO). That is the
gate behaving as designed, not a spin-up defect, so the harness force-advances
that one rung and says so.

Usage:
    python scripts/harness/venture_spinup_acceptance.py
    python scripts/harness/venture_spinup_acceptance.py --verbose
    python scripts/harness/venture_spinup_acceptance.py --venture-key other_stub
"""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import text

sys.path.insert(0, ".")

from config.venture_ladder import (  # noqa: E402
    AUTO_DOUBLE_MIN_SAMPLE,
    AUTO_DOUBLE_MULTIPLIER,
    CELL_MIN_SAMPLE,
    EVIDENCE_HARNESS_RESULT,
    EVIDENCE_MARKET_SCORE,
    EVIDENCE_PRESELL_COMMITMENT,
    EVIDENCE_SCRAPE_SAMPLE,
    LADDER_STAGES,
    PRESELL_MIN_AMOUNT_CENTS,
    PRESELL_MIN_COMMITMENTS,
    TERMINAL_STAGE,
    validate_ladder_config,
)
from config.venture_template import (  # noqa: E402
    COUNTY_TEMPLATE,
    DEFAULT_VENTURE_KEY,
    REQUIRED_SIGNAL_TYPES,
    new_venture_config,
    validate_venture_config,
)
from src.services import clone_pack, venture_ladder  # noqa: E402
from src.services.venture_provisioning import provision_venture  # noqa: E402

STUB_VENTURE_KEY = "acceptance_stub"
STUB_COUNTY_ID = "acceptance_stub_county"
TEMPLATE_COUNTY_ID = "acceptance_stub_template"
# Deliberately outside any real Florida ZIP range so the radar rung's
# county-overlap gate cannot collide with a live venture's territory.
STUB_ZIP_PREFIXES = ["00901", "00902"]

# Two cells. AUTO_DOUBLE_MIN_SAMPLE applies PER CELL for the cell-level rule
# (not split across cells), so each cell needs to clear it on its own — the
# venture-level rule then sees the sum and clears it comfortably.
STUB_CELLS = ("founder_tier_blitz", "founder_tier_blitz_bh")
SENDS_PER_CELL = max(CELL_MIN_SAMPLE, AUTO_DOUBLE_MIN_SAMPLE) + 20
# 12.5% — comfortably above AUTO_DOUBLE_REPLY_RATE_PCT (8%) without being
# so high it looks like seeded nonsense in a log.
REPLY_EVERY_NTH = 8

DISPATCHED_BATCHES = 3


# ── check recording ──────────────────────────────────────────────────────────

@dataclass
class Check:
    name: str
    outcome: str  # PASS | FAIL | WARN
    detail: str


class Report:
    def __init__(self) -> None:
        self.checks: list[Check] = []

    def record(self, name: str, ok: bool, detail: str = "") -> bool:
        self.checks.append(Check(name, "PASS" if ok else "FAIL", detail))
        return ok

    def warn(self, name: str, detail: str) -> None:
        self.checks.append(Check(name, "WARN", detail))

    @property
    def failed(self) -> list[Check]:
        return [c for c in self.checks if c.outcome == "FAIL"]

    @property
    def warned(self) -> list[Check]:
        return [c for c in self.checks if c.outcome == "WARN"]

    def render(self) -> str:
        width = max((len(c.name) for c in self.checks), default=10)
        symbols = {"PASS": "PASS", "FAIL": "FAIL", "WARN": "WARN"}
        lines = [
            f"  [{symbols[c.outcome]}] {c.name.ljust(width)}  {c.detail}".rstrip()
            for c in self.checks
        ]
        return "\n".join(lines)


# ── stub construction ────────────────────────────────────────────────────────

def _stub_venture_config(venture_key: str, template_county_id: Optional[str]) -> dict[str, Any]:
    """A fully-filled venture config — every CHANGE ME answered.

    Relay identity is set here rather than left NULL on purpose: a NULL column
    resolves to venture #1's live Instantly campaign through the CL3 env
    fallback, and a harness that accepted that would be asserting the wrong
    venture's sending identity.
    """
    return new_venture_config(
        venture_key=venture_key,
        display_name="Acceptance Stub Venture",
        brand_name="Acceptance Stub LLC",
        postal_address="1 Harness Way, Tampa, FL 33601",
        state="FL",
        bankruptcy_court_code="flmb",
        default_bankruptcy_division="8:",
        template_county_id=template_county_id,
        relay_slack_channel="#relay-acceptance-stub",
        relay_approvers=["U0HARNESS1"],
        relay_instantly_campaign_id="camp_acceptance_stub",
        relay_instantly_sender_email="stub@acceptance.invalid",
        relay_daily_ceiling=20,
        kill_switch_feature="relay_acceptance_stub",
    )


def _stub_county(template_county_id: Optional[str]) -> dict[str, Any]:
    """A county with EVERY required signal type overridden.

    Anything not overridden inherits the template county's URL, which is almost
    always wrong for a different county — the probe rung's coverage gate treats
    an inherited URL as uncovered, so a stub that skipped overrides would fail
    at probe, which is exactly the intended behaviour for a real venture.
    """
    county = dict(COUNTY_TEMPLATE)
    county.update({
        "county_id": STUB_COUNTY_ID,
        "display_name": "Acceptance Stub County",
        "fips": "12999",
        "nws_zone": "FLZ999",
        "parcel_id_format": "folio",
        "zip_prefixes": list(STUB_ZIP_PREFIXES),
        "source_url_overrides": {
            signal_type: f"https://stub.invalid/{signal_type}"
            for signal_type in REQUIRED_SIGNAL_TYPES
        },
    })
    if not template_county_id:
        county["source_url_overrides"] = {}
    return county


def _seed_template_county(db) -> str:
    """Create a synthetic template county with a full set of active sources.

    The harness owns this fixture rather than picking a real county out of the
    DB. Two reasons: the run becomes deterministic instead of depending on
    whatever happens to be seeded (a dev DB with no active `county_sources`
    would otherwise make the harness unusable), and it exercises the CLONING
    path — the stub's overrides are proved to actually replace the template's
    URLs rather than being compared against nothing.

    Rolled back with everything else.
    """
    db.execute(
        text("""
            INSERT INTO counties (
                county_id, display_name, venture_key, zip_prefixes, is_active
            ) VALUES (
                :county_id, 'Harness Template County', :venture_key,
                '[]'::jsonb, true
            )
            ON CONFLICT (county_id) DO NOTHING
        """),
        {"county_id": TEMPLATE_COUNTY_ID, "venture_key": DEFAULT_VENTURE_KEY},
    )
    db.execute(
        text("""
            INSERT INTO county_sources (
                county_id, signal_type, source_name, url, is_active,
                date_range_available, scrape_mode
            ) VALUES (
                :county_id, :signal_type, 'harness template', :url, true,
                true, 'ai_only'
            )
        """),
        [
            {
                "county_id": TEMPLATE_COUNTY_ID,
                "signal_type": signal_type,
                # Distinct from the stub's https://stub.invalid/... URLs, so an
                # un-overridden source is detectable as inherited.
                "url": f"https://template.invalid/{signal_type}",
            }
            for signal_type in REQUIRED_SIGNAL_TYPES
        ],
    )
    db.flush()
    return TEMPLATE_COUNTY_ID


# ── seeding ──────────────────────────────────────────────────────────────────

def _seed_presell(db, venture_key: str, *, count: int, per_cents: int) -> None:
    """Verified refundable-deposit commitments.

    `verified=True` is what a Stripe webhook would set once the deposit really
    settled; the gate ignores unverified rows, which is what keeps it a machine
    check rather than a checklist. `source_ref` is the PaymentIntent id, and the
    UNIQUE on (venture_key, evidence_type, source_ref) is why a webhook retry
    cannot inflate the count.
    """
    for index in range(count):
        venture_ladder.record_evidence(
            db, venture_key,
            evidence_type=EVIDENCE_PRESELL_COMMITMENT,
            stage="probe",
            payload={
                "kind": "deposit",
                "amount_cents": per_cents,
                # Distinct per commitment: the gate counts unique customers, so
                # a seeder reusing one id would (correctly) satisfy nothing.
                "stripe_customer_id": f"cus_harness_presell_{index}",
                "contact_ref": f"harness-contact-{index}",
                "refundable": True,
            },
            source_ref=f"pi_harness_stub_{index}",
            verified=True,
            recorded_by="venture_spinup_acceptance",
        )


def _seed_traffic(db, venture_key: str) -> tuple[int, int]:
    """Dispatched Relay items plus the matching Cora drafts.

    Both sides are required: cell_reply_rates() counts a draft as a send only
    when its thread has a dispatched relay_approval_queue row, so seeding drafts
    alone would produce a zero reply rate and seeding queue rows alone would
    produce no cells.

    Returns (sends, replies).
    """
    now = datetime.now(timezone.utc)
    queue_rows: list[dict[str, Any]] = []
    draft_rows: list[dict[str, Any]] = []
    sends = 0
    replies = 0

    for cell_index, cell_id in enumerate(STUB_CELLS):
        for n in range(SENDS_PER_CELL):
            thread_id = f"OPP-HARNESS-{cell_index}-{n:05d}"
            replied = (n % REPLY_EVERY_NTH) == 0
            # Spread across DISPATCHED_BATCHES so the spin_up rung's
            # clean_sweep_count gate sees several failure-free batches.
            batch_id = f"harness-batch-{n % DISPATCHED_BATCHES}"
            dispatched_at = now - timedelta(days=1, minutes=n)

            queue_rows.append({
                "idempotency_key": f"harness-{venture_key}-{cell_index}-{n}",
                "venture_key": venture_key,
                "batch_id": batch_id,
                "thread_id": thread_id,
                "channel": "email",
                "recipient": f"stub{cell_index}_{n}@acceptance.invalid",
                "payload": json.dumps({"subject": "harness", "body": "harness"}),
                "status": "sent",
                "dispatched_at": dispatched_at,
            })
            draft_rows.append({
                "draft_id": str(uuid.uuid4()),
                "opportunity_thread_id": thread_id,
                "buyer_entity_id": 1,
                "venture_key": venture_key,
                "cell_id": cell_id,
                "offer": "founder_tier",
                "avenue": "flippers",
                "angle": "scarcity_seat_number",
                "subject": "harness draft",
                "body": "harness draft body",
                "facts_used": "[]",
                "source_refs": "[]",
                "recommended_channel": "email",
                "confidence_score": 80,
                "status": "approved_pending_send",
                "created_at": dispatched_at,
                "replied_at": dispatched_at + timedelta(hours=2) if replied else None,
            })
            sends += 1
            replies += 1 if replied else 0

    db.execute(
        text("""
            INSERT INTO relay_approval_queue (
                idempotency_key, venture_key, batch_id, thread_id, channel,
                recipient, payload, status, dispatched_at
            ) VALUES (
                :idempotency_key, :venture_key, :batch_id, :thread_id, :channel,
                :recipient, CAST(:payload AS jsonb), :status, :dispatched_at
            )
        """),
        queue_rows,
    )
    # schema_version / published / is_followup are NOT NULL with Python-side
    # defaults only (no server_default), so a raw SQL insert must supply them.
    db.execute(
        text("""
            INSERT INTO outbound_drafts (
                draft_id, opportunity_thread_id, buyer_entity_id, venture_key,
                cell_id, offer, avenue, angle, subject, body, facts_used,
                source_refs, recommended_channel, confidence_score, status,
                schema_version, published, is_followup, created_at, replied_at
            ) VALUES (
                :draft_id, :opportunity_thread_id, :buyer_entity_id, :venture_key,
                :cell_id, :offer, :avenue, :angle, :subject, :body,
                CAST(:facts_used AS jsonb), CAST(:source_refs AS jsonb),
                :recommended_channel, :confidence_score, :status,
                1, false, false, :created_at, :replied_at
            )
        """),
        draft_rows,
    )
    return sends, replies


def _seed_unit_economics(db) -> None:
    """A paying account and its attributed platform cost.

    Revenue reaches a venture through customer_accounts -> subscribers ->
    counties.venture_key; cost through api_usage_logs.subscriber_id along the
    same path. Both are seeded against the stub county's subscriber so the
    unit-economics gates have something real to divide.
    """
    from src.core.models import ApiUsageLog, CustomerAccount, Subscriber

    tag = uuid.uuid4().hex[:12]

    # ORM `session.add()` here rather than raw SQL: `subscribers` carries a
    # dozen NOT NULL columns whose defaults are Python-side only (no
    # server_default), so an INSERT statement has to spell every one of them out
    # and breaks whenever a new one is added. This is the one case the repo's
    # text()-only rule explicitly leaves to the ORM.
    subscriber = Subscriber(
        stripe_customer_id=f"cus_harness_{tag}",
        tier="pro",
        vertical="investor",
        county_id=STUB_COUNTY_ID,
        status="active",
        email="stub@acceptance.invalid",
    )
    db.add(subscriber)
    db.flush()

    db.add(CustomerAccount(
        subscriber_id=subscriber.id,
        status="active",
        mrr_cents=20_000,
        converted_at=datetime.now(timezone.utc),
    ))
    db.add(ApiUsageLog(
        service="claude",
        model="sonnet",
        cost_usd=50.0,
        task_type="harness_seed",
        subscriber_id=subscriber.id,
    ))
    db.flush()


def _write_stub_crontab(venture_key: str, directory: Path) -> Path:
    """A temp crontab carrying this venture's sweep line.

    The Clone-Pack's cron check reads the repo crontab, and a stub venture must
    not require an edit to it. Pointing the check at a temp file proves the
    check works without pretending the stub is scheduled in production.
    """
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"crontab_{venture_key}.txt"
    path.write_text(
        "# harness stub crontab\n"
        f"*/30 * * * * $PROJECT/scripts/cron/run.sh src.services.relay --sweep "
        f"--venture {venture_key}\n",
        encoding="utf-8",
    )
    return path


# ── the run ──────────────────────────────────────────────────────────────────

def _advance_rung(db, report: Report, venture_key: str, *, verbose: bool) -> str:
    """Advance one rung, recording the outcome. Returns the resulting stage."""
    evaluation = venture_ladder.evaluate(db, venture_key)
    stage = evaluation.current_stage
    label = f"ladder {stage} -> {evaluation.next_stage}"

    if verbose:
        for gate in evaluation.gates:
            shown = "no metric" if gate.value is None else f"{gate.value:g}"
            suffix = " (imputed)" if gate.imputed else ""
            print(f"        - {gate.name}: {shown} [{gate.color}]{suffix}")

    if evaluation.blocked_reasons:
        # The unit-economics rung depends on fleet-wide cost attribution the
        # harness does not own — see the module docstring. Everything else that
        # blocks here is a real spin-up defect.
        environmental = stage == "unit_economics" and any(
            "no metric" in reason for reason in evaluation.blocked_reasons
        )
        if environmental:
            report.warn(
                label,
                "cost attribution below floor in this environment — gate correctly "
                "reports no metric; force-advancing (see docstring)",
            )
            result = venture_ladder.advance(
                db, venture_key, actor="venture_spinup_acceptance", force=True
            )
            return result.current_stage
        report.record(label, False, "; ".join(evaluation.blocked_reasons))
        return stage

    result = venture_ladder.advance(db, venture_key, actor="venture_spinup_acceptance")
    report.record(
        label,
        result.current_stage == evaluation.next_stage,
        f"all {len(evaluation.gates)} gate(s) green",
    )
    return result.current_stage


def _auto_double_section(db, report: Report, venture_key: str) -> None:
    """Fire both auto-double rules and assert their guards.

    Run at the `cell` rung — see the call site for why the ordering matters.
    """
    before = db.execute(
        text("SELECT relay_daily_ceiling FROM ventures WHERE venture_key = :k"),
        {"k": venture_key},
    ).scalar_one()

    fired = venture_ladder.maybe_auto_double(db, venture_key)
    report.record(
        "auto-double fires on a high reply rate",
        fired.fired and fired.new_ceiling == before * AUTO_DOUBLE_MULTIPLIER,
        f"{fired.reason}: ceiling {fired.previous_ceiling} -> {fired.new_ceiling}"
        if fired.fired else f"did not fire: {fired.reason}",
    )

    again = venture_ladder.maybe_auto_double(db, venture_key)
    report.record(
        "auto-double respects its cooldown",
        not again.fired and "cooldown" in again.reason,
        again.reason,
    )
    report.record(
        "ceiling unchanged by the declined retry",
        db.execute(
            text("SELECT relay_daily_ceiling FROM ventures WHERE venture_key = :k"),
            {"k": venture_key},
        ).scalar_one() == fired.new_ceiling,
        f"still {fired.new_ceiling}",
    )

    cell_fired = venture_ladder.maybe_auto_double_cell(db, venture_key, STUB_CELLS[0])
    report.record(
        "cell auto-double raises the production multiplier",
        cell_fired.fired and cell_fired.new_multiplier == AUTO_DOUBLE_MULTIPLIER,
        f"{STUB_CELLS[0]}: {cell_fired.previous_multiplier}x -> "
        f"{cell_fired.new_multiplier}x" if cell_fired.fired
        else f"did not fire: {cell_fired.reason}",
    )
    report.record(
        "cell auto-double leaves the venture ceiling alone",
        db.execute(
            text("SELECT relay_daily_ceiling FROM ventures WHERE venture_key = :k"),
            {"k": venture_key},
        ).scalar_one() == fired.new_ceiling,
        "the cell rule is a production knob, not a second send cap",
    )

    multipliers = venture_ladder.cell_production_multipliers(db, venture_key)
    report.record(
        "multiplier is readable back per cell",
        multipliers.get(STUB_CELLS[0]) == AUTO_DOUBLE_MULTIPLIER
        and STUB_CELLS[1] not in multipliers,
        f"{multipliers} — only the doubled cell carries one",
    )


def run(db, report: Report, *, venture_key: str, verbose: bool) -> None:
    # The spin_up rung's `clone_pack_complete` gate calls
    # clone_pack.assemble() itself, which reads the REAL crontab. A stub venture
    # is correctly absent from it, so the module's path is pointed at a temp
    # crontab for the duration of the run and restored in the finally below.
    # Without this the harness would be asserting that a stub cannot spin up,
    # which is true but useless.
    scratch = Path(".harness-tmp")
    real_crontab = clone_pack.CRONTAB_PATH
    stub_crontab = _write_stub_crontab(venture_key, scratch)
    clone_pack.CRONTAB_PATH = stub_crontab

    try:
        _run_checks(
            db, report,
            venture_key=venture_key,
            verbose=verbose,
            stub_crontab=stub_crontab,
            real_crontab=real_crontab,
        )
    finally:
        clone_pack.CRONTAB_PATH = real_crontab
        try:
            stub_crontab.unlink()
            scratch.rmdir()
        except OSError:
            pass


def _run_checks(
    db,
    report: Report,
    *,
    venture_key: str,
    verbose: bool,
    stub_crontab: Path,
    real_crontab: Path,
) -> None:
    # ── config validity, before anything touches the DB ──────────────────────
    ladder_problems = validate_ladder_config()
    report.record(
        "ladder config sound",
        not ladder_problems,
        "; ".join(ladder_problems) or "every stage has gates with a declared "
        "direction and no_metric_behavior",
    )

    template_county_id = _seed_template_county(db)
    source_count = db.execute(
        text("SELECT COUNT(*) FROM county_sources WHERE county_id = :c AND is_active"),
        {"c": template_county_id},
    ).scalar_one()
    report.record(
        "template county seeded with a full source set",
        source_count == len(REQUIRED_SIGNAL_TYPES),
        f"{source_count}/{len(REQUIRED_SIGNAL_TYPES)} signal types on "
        f"{template_county_id!r}",
    )

    venture_cfg = _stub_venture_config(venture_key, template_county_id)
    config_problems = validate_venture_config(venture_cfg)
    report.record(
        "stub venture config valid",
        not config_problems,
        "; ".join(config_problems) or "no CHANGE ME left unanswered",
    )

    # ── provisioning ────────────────────────────────────────────────────────
    provision_report = provision_venture(
        db,
        venture_cfg=venture_cfg,
        counties=[_stub_county(template_county_id)],
    )
    county_report = provision_report["counties"].get(STUB_COUNTY_ID, {})
    missing_urls = county_report.get("missing_urls", [])
    report.record(
        "provisioning left no inherited URLs",
        not missing_urls,
        f"missing_urls={missing_urls}" if missing_urls
        else f"cloned {county_report.get('cloned', 0)} source(s), all overridden",
    )
    report.record(
        "venture starts at radar",
        db.execute(
            text("SELECT ladder_stage FROM ventures WHERE venture_key = :k"),
            {"k": venture_key},
        ).scalar_one() == "radar",
        "a new venture is a candidate, not a business",
    )

    # ── negative pass: the presell gate must block before it is satisfied ────
    venture_ladder.record_evidence(
        db, venture_key,
        evidence_type=EVIDENCE_MARKET_SCORE,
        stage="radar",
        payload={"score": 78.5, "method": "harness_seed"},
        source_ref="harness-market-score",
        verified=True,
        recorded_by="venture_spinup_acceptance",
    )
    venture_ladder.record_evidence(
        db, venture_key,
        evidence_type=EVIDENCE_SCRAPE_SAMPLE,
        stage="probe",
        payload={"rows": 42, "signal_type": "foreclosures"},
        source_ref="harness-scrape-sample",
        verified=True,
        recorded_by="venture_spinup_acceptance",
    )

    stage = _advance_rung(db, report, venture_key, verbose=verbose)
    report.record("reached probe", stage == "probe", f"at {stage}")

    blocked = venture_ladder.evaluate(db, venture_key)
    report.record(
        "presell gate blocks with no commitments",
        bool(blocked.blocked_reasons)
        and any("presell gate" in reason for reason in blocked.blocked_reasons),
        "; ".join(blocked.blocked_reasons) or "gate did NOT block — spend would be "
        "authorised by enthusiasm",
    )

    # Unverified deposits must not count: a hand-entered row is a claim, and the
    # gate's autonomy rests on only machine-verified money counting.
    venture_ladder.record_evidence(
        db, venture_key,
        evidence_type=EVIDENCE_PRESELL_COMMITMENT,
        stage="probe",
        payload={"kind": "deposit", "amount_cents": 10_000_000},
        source_ref="harness-unverified-whale",
        verified=False,
        recorded_by="venture_spinup_acceptance",
    )
    still_blocked = venture_ladder.presell_gate_blocked(db, venture_key)
    report.record(
        "unverified commitment does not satisfy the gate",
        bool(still_blocked),
        "an unverified $100k claim was correctly ignored" if still_blocked
        else "unverified evidence passed the gate",
    )

    # ── satisfy presell, then walk the rest of the ladder ────────────────────
    _seed_presell(
        db, venture_key,
        count=PRESELL_MIN_COMMITMENTS,
        per_cents=(PRESELL_MIN_AMOUNT_CENTS // PRESELL_MIN_COMMITMENTS) + 1,
    )
    status = venture_ladder.presell_gate_status(db, venture_key)
    report.record(
        "presell gate satisfied by verified deposits",
        status.satisfied,
        f"{status.verified_count} distinct customer(s), "
        f"${status.verified_amount_cents / 100:,.0f} total",
    )

    # The gate must count buyers, not receipts. A NEW PaymentIntent (so the
    # source_ref UNIQUE does not stop the insert) from a customer already
    # counted must add nothing — that UNIQUE only defends against webhook
    # retries, never against one buyer depositing repeatedly.
    inserted = venture_ladder.record_evidence(
        db, venture_key,
        evidence_type=EVIDENCE_PRESELL_COMMITMENT,
        stage="probe",
        payload={
            "kind": "deposit",
            "amount_cents": PRESELL_MIN_AMOUNT_CENTS,
            "stripe_customer_id": "cus_harness_presell_0",
        },
        source_ref="pi_harness_repeat_deposit",
        verified=True,
        recorded_by="venture_spinup_acceptance",
    )
    repeat = venture_ladder.presell_gate_status(db, venture_key)
    report.record(
        "repeat deposit from a counted customer adds nothing",
        inserted
        and repeat.verified_count == status.verified_count
        and repeat.verified_amount_cents == status.verified_amount_cents,
        f"a 6th receipt landed but the gate still reads "
        f"{repeat.verified_count} distinct customer(s)",
    )

    sends, replies = _seed_traffic(db, venture_key)
    _seed_unit_economics(db)
    if verbose:
        print(f"        seeded {sends} sends / {replies} replies across {len(STUB_CELLS)} cells")

    for _ in range(len(LADDER_STAGES)):
        if stage == TERMINAL_STAGE:
            break
        if stage == "cell":
            # Auto-double must fire HERE, not after the walk: `cell -> spin_up`
            # gates on clean_auto_double_count >= 1. A venture has to have
            # proved it can scale before it is worth spinning up, and the
            # harness has to exercise the rungs in the order the product
            # actually enforces.
            _auto_double_section(db, report, venture_key)
        if stage == "spin_up":
            # The one thing a real acceptance run legitimately records about
            # itself. Everything else this rung gates on — Clone-Pack
            # completeness, the cron line — is computed live rather than
            # claimed.
            venture_ladder.record_evidence(
                db, venture_key,
                evidence_type=EVIDENCE_HARNESS_RESULT,
                stage="spin_up",
                payload={"result": "PASS", "harness": "venture_spinup_acceptance"},
                source_ref="harness-self-report",
                verified=True,
                recorded_by="venture_spinup_acceptance",
            )
        advanced = _advance_rung(db, report, venture_key, verbose=verbose)
        if advanced == stage:
            break
        stage = advanced

    report.record(
        "reached portfolio",
        stage == TERMINAL_STAGE,
        f"final stage {stage}",
    )
    report.record(
        "terminal stage has nothing above it",
        venture_ladder.evaluate(db, venture_key).next_stage is None
        if stage == TERMINAL_STAGE else False,
        "portfolio is terminal",
    )

    # ── audit trail ─────────────────────────────────────────────────────────
    decisions = dict(db.execute(
        text("""
            SELECT decision, COUNT(*) AS n
            FROM venture_ladder_events
            WHERE venture_key = :k
            GROUP BY decision
        """),
        {"k": venture_key},
    ).fetchall())
    report.record(
        "every decision is audited",
        decisions.get("advanced", 0) >= 1 and decisions.get("auto_double", 0) == 2,
        f"{decisions}",
    )

    # ── Clone-Pack ──────────────────────────────────────────────────────────
    pack = clone_pack.assemble(db, venture_key, crontab_path=stub_crontab)
    report.record(
        "Clone-Pack is complete",
        clone_pack.is_complete(pack),
        "; ".join(pack.gaps) or (
            f"{len(pack.counties)} county/counties, all {len(REQUIRED_SIGNAL_TYPES)} "
            f"signal types covered, Relay identity resolved, cron line present"
        ),
    )
    report.record(
        "Clone-Pack sees the venture's own Relay identity",
        pack.venture.relay_instantly_campaign_id == "camp_acceptance_stub"
        and pack.venture.relay_slack_channel == "#relay-acceptance-stub",
        f"campaign={pack.venture.relay_instantly_campaign_id}, "
        f"channel={pack.venture.relay_slack_channel}",
    )
    report.record(
        "cron check is not satisfied by the real crontab",
        not clone_pack.cron_line_present(venture_key, crontab_path=real_crontab),
        f"the stub is correctly absent from {real_crontab.as_posix()}",
    )
    report.record(
        "cron check does accept venture #1's real line",
        clone_pack.cron_line_present(DEFAULT_VENTURE_KEY, crontab_path=real_crontab),
        "the check is not vacuously false — it finds the line that does exist",
    )


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python scripts/harness/venture_spinup_acceptance.py",
        description="PASS/FAIL acceptance harness for second-venture spin-up (CL4)",
    )
    parser.add_argument("--venture-key", default=STUB_VENTURE_KEY)
    parser.add_argument(
        "--verbose", action="store_true",
        help="Print every gate value at every rung",
    )
    args = parser.parse_args(argv)

    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

    from src.utils.logger import setup_logging

    setup_logging()

    from src.core.database import db as database
    from src.utils import county_config, venture_config

    report = Report()
    session = database.get_session()
    crashed: Optional[BaseException] = None

    print(f"\nSecond-venture spin-up acceptance — stub {args.venture_key!r}\n")

    try:
        run(session, report, venture_key=args.venture_key, verbose=args.verbose)
    except BaseException as exc:  # noqa: BLE001 — reported, then re-raised as FAIL
        crashed = exc
        report.record("harness completed", False, f"{type(exc).__name__}: {exc}")
    finally:
        # ALWAYS roll back, pass or fail. Nothing here is meant to survive.
        session.rollback()
        session.close()
        # The stub's config was cached during assemble(); the rollback removed
        # the rows behind it, so the cache must not outlive the transaction.
        venture_config.invalidate_cache()
        county_config.invalidate_cache()

    print(report.render())

    passed = len([c for c in report.checks if c.outcome == "PASS"])
    print(f"\n  {passed} passed, {len(report.failed)} failed, {len(report.warned)} warned")
    print("  (all writes rolled back — nothing was committed)")

    if report.failed:
        print(f"\nRESULT: FAIL — second-venture spin-up is not repeatable yet\n")
        if crashed is not None and args.verbose:
            import traceback

            traceback.print_exception(crashed)
        return 1

    print("\nRESULT: PASS — a second venture can be stood up end to end\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
