"""
Vera — promise & discrepancy digest (VERA-v2.2 sub-task V4).

Runs once daily via `python -m src.agents.vera --promise-digest`, after V3
(07:36) and V2 (07:47) so the facts it reads are fresh. Two outputs, both in
Vera's constitution:

    1. Promise digest (standing job #3) — open/overdue commitments with owner,
       age, and MRR-at-risk, read from vera_promises.
    2. Discrepancy report (standing job #9, lite) — "Doc claims X; live state
       shows Y", built by reading the discrepancy verdicts V2/V3 already wrote
       to vera_facts. This module does NOT re-run those checks; it reports
       their findings in claim-vs-reality voice.

Plus the seeded-discrepancy acceptance harness (`--seed-check`): Vera's literal
build sign-off — plant a false deploy claim against real live state and prove
her checker catches it. No DB writes, no Stripe/prod dependency — runnable on
staging today.

Design mirrors revenue_truth.py: all DB reads happen in the orchestrator, pure
builders/renderer take plain pre-fetched data (the V3 lesson — a "pure"
renderer must never sneak in a DB round-trip).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Mapping, Optional

from sqlalchemy import text

from src.agents.vera.checks._shared import (
    html_headline,
    html_note,
    html_section,
    html_shell,
    html_table,
    report_recipients,
    validate_finding,
)
from src.agents.vera.config import FRESHNESS_STATIC, KILL_SWITCH_FEATURE
from src.agents.vera.db import vera_db
from src.agents.vera.facts import read_facts, write_fact
from src.agents.vera.promises import PromiseRow, open_promises

logger = logging.getLogger(__name__)

# Deterministic bogus SHA the seed-check plants as a false "prod is on this
# commit" claim. Never a real git object, so it can never accidentally match
# real prod HEAD — the discrepancy is guaranteed on every host, prod or not.
_SEEDED_BOGUS_SHA = "seededbogus0000000000000000000000000000"


# ─────────────────────────────────────────────────────────────────────────────
# Discrepancy model + builder (pure over pre-fetched facts)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Discrepancy:
    claim: str
    live: str
    source: str  # the vera_facts key (or check) this came from


def _row(rows: list) -> Optional[Mapping]:
    """First (newest, fresh) fact row or None — read_facts returns a list."""
    return rows[0] if rows else None


def build_discrepancies(
    deploy_drift: Optional[Mapping],
    pending_migrations: Optional[Mapping],
    stale_cron: list,
    paying_no_access: Optional[Mapping],
    access_not_paying: Optional[Mapping],
    mrr_drift: Optional[Mapping],
) -> list[Discrepancy]:
    """Pure. Translates each non-clean V2/V3 verdict into a claim-vs-live line.
    A missing fact (source check didn't run / went stale) is skipped here and
    surfaced separately as an 'unchecked' note by the renderer — never read as
    a false all-clear."""
    out: list[Discrepancy] = []

    # "unknown" is V2's abstention (repo not on host / git failed), NOT a
    # confirmed discrepancy — reporting it as one would assert the unverified,
    # which Vera's ACCURACY value forbids. Surfaced as "unchecked" by the
    # orchestrator instead. Only in_sync (clean) and unknown (abstain) are
    # non-discrepancies; behind / head_mismatch are real.
    if deploy_drift is not None and deploy_drift.get("fact_value") not in (None, "in_sync", "unknown"):
        out.append(Discrepancy(
            claim="Prod runs the latest deployed code.",
            live=f"Deploy drift = {deploy_drift['fact_value']}.",
            source="deploy.drift",
        ))

    if pending_migrations is not None:
        n = pending_migrations.get("value_numeric")
        if n is not None and int(n) > 0:
            out.append(Discrepancy(
                claim="All migrations are applied on prod.",
                live=f"{int(n)} migration(s) not applied.",
                source="deploy.pending_migrations",
            ))

    for beat in stale_cron:  # list of {label, fact_value}
        out.append(Discrepancy(
            claim=f"{beat['label']} scraper is running fresh.",
            live="Last successful run is past its SLA (stale).",
            source=f"cron.{beat['label']}.freshness",
        ))

    if paying_no_access is not None:
        n = paying_no_access.get("value_numeric")
        if n is not None and int(n) > 0:
            out.append(Discrepancy(
                claim="Every paying customer has access.",
                live=f"{int(n)} paying but no access (churn bomb).",
                source="revenue.reconciliation.paying_no_access.count",
            ))

    if access_not_paying is not None:
        n = access_not_paying.get("value_numeric")
        if n is not None and int(n) > 0:
            out.append(Discrepancy(
                claim="Every active account is paying.",
                live=f"{int(n)} have access but are not paying (free riders).",
                source="revenue.reconciliation.access_not_paying.count",
            ))

    if mrr_drift is not None:
        drift = mrr_drift.get("value_numeric")
        if drift is not None and int(drift) != 0:
            out.append(Discrepancy(
                claim="DB MRR matches Stripe.",
                live=f"MRR drift = ${int(drift) / 100:,.2f} (stripe - db).",
                source="revenue.mrr.drift_cents",
            ))

    return out


# ─────────────────────────────────────────────────────────────────────────────
# Promise digest (pure over pre-fetched promise rows)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class PromiseDigest:
    overdue: list = field(default_factory=list)   # list[PromiseRow]
    pending: list = field(default_factory=list)   # list[PromiseRow]

    @property
    def open_count(self) -> int:
        return len(self.overdue) + len(self.pending)


def _mrr_sort_key(p: PromiseRow, now: datetime) -> tuple:
    return (-(p.mrr_at_risk_cents or 0), -p.age_days(now))


def build_promise_digest(promises: list, now: Optional[datetime] = None) -> PromiseDigest:
    """Pure. Splits open promises into overdue vs pending, each sorted by
    MRR-at-risk desc then age desc (highest-leverage first)."""
    now = now or datetime.now(timezone.utc)
    overdue = sorted((p for p in promises if p.is_overdue(now)), key=lambda p: _mrr_sort_key(p, now))
    pending = sorted((p for p in promises if not p.is_overdue(now)), key=lambda p: _mrr_sort_key(p, now))
    return PromiseDigest(overdue=overdue, pending=pending)


# ─────────────────────────────────────────────────────────────────────────────
# Seeded-discrepancy acceptance harness (no DB writes)
# ─────────────────────────────────────────────────────────────────────────────

def detect_claim_discrepancy(claimed_sha: Optional[str], actual_sha: Optional[str]) -> Optional[Discrepancy]:
    """Pure. The assertable unit of the seed-check: a claim about prod's
    commit vs the real one. Returns a Discrepancy iff they differ."""
    if claimed_sha == actual_sha:
        return None
    return Discrepancy(
        claim=f"Prod is deployed at commit {claimed_sha}.",
        live=f"Real prod HEAD is {actual_sha or 'unknown (repo not on this host)'}.",
        source="seed-check:deploy.prod_sha",
    )


def run_seed_check() -> int:
    """Plant a false deploy claim against REAL live state, assert Vera's V2
    deploy checker catches it. Writes nothing. Returns 0 if caught, 1 if not."""
    from src.agents.vera.checks.live_state import check_deploy_drift

    deploy = check_deploy_drift()
    actual_sha = deploy.get("head_sha")
    caught = detect_claim_discrepancy(_SEEDED_BOGUS_SHA, actual_sha)

    if caught:
        logger.info(
            "[Vera] seed-check PASS — planted claim %s caught against real HEAD %s",
            _SEEDED_BOGUS_SHA, actual_sha or "unknown",
        )
        print(f"[Vera] seed-check PASS: {caught.claim} {caught.live}")
        return 0

    logger.error("[Vera] seed-check FAIL — planted false claim was NOT caught")
    print("[Vera] seed-check FAIL: planted discrepancy was not detected")
    return 1


# ─────────────────────────────────────────────────────────────────────────────
# Renderer
# ─────────────────────────────────────────────────────────────────────────────

def _mrr_label(cents: Optional[int]) -> str:
    return f"${cents / 100:,.2f}" if cents is not None else "—"


def render_digest_report(
    digest: PromiseDigest,
    discrepancies: list,
    unchecked: list,
    report_date: Optional[date] = None,
) -> tuple[str, str, str]:
    """Returns (subject, body, html_body). Numbers first, Vera's voice. Pure —
    no DB access; every input pre-fetched by the orchestrator."""
    report_date = report_date or datetime.now(timezone.utc).date()

    lines = [
        f"Vera — Promise & Discrepancy Digest — {report_date.isoformat()}",
        "=" * 60,
        "",
        f"OPEN DISCREPANCIES: {len(discrepancies)}",
        f"OPEN PROMISES: {digest.open_count} ({len(digest.overdue)} overdue)",
    ]

    # ── DISCREPANCIES ─────────────────────────────────────────────────────
    lines += ["", "DISCREPANCIES (Doc claims X; live state shows Y)"]
    if discrepancies:
        for d in discrepancies:
            lines.append(f"  - Claim: {d.claim}")
            lines.append(f"    Live:  {d.live}  [{d.source}]")
        disc_html = html_table(
            ["Claim", "Live state", "Source"],
            [(d.claim, d.live, d.source) for d in discrepancies],
        )
    else:
        lines.append("  none — every checked claim matches live state.")
        disc_html = html_note("none — every checked claim matches live state.")
    if unchecked:
        lines.append(f"  (not checked today — no fresh fact: {', '.join(unchecked)})")
        disc_html += html_note(
            "Not checked today (no fresh fact — the source check hasn't run): "
            + ", ".join(unchecked)
        )

    # ── OVERDUE PROMISES ──────────────────────────────────────────────────
    now = datetime.now(timezone.utc)
    lines += ["", "OVERDUE PROMISES"]
    if digest.overdue:
        for p in digest.overdue:
            lines.append(
                f"  - [{p.owner}] {p.description} — {p.age_days(now)}d old, "
                f"MRR-at-risk {_mrr_label(p.mrr_at_risk_cents)}"
            )
        overdue_html = html_table(
            ["Owner", "What", "Age", "Due", "MRR-at-risk"],
            [
                (p.owner, p.description, f"{p.age_days(now)}d",
                 p.due_at.date().isoformat() if p.due_at else "—",
                 _mrr_label(p.mrr_at_risk_cents))
                for p in digest.overdue
            ],
        )
    else:
        lines.append("  none")
        overdue_html = html_note("none")

    # ── OPEN (NOT YET OVERDUE) ────────────────────────────────────────────
    lines += ["", "OPEN PROMISES (not yet due)"]
    if digest.pending:
        for p in digest.pending:
            lines.append(
                f"  - [{p.owner}] {p.description} — {p.age_days(now)}d old, "
                f"MRR-at-risk {_mrr_label(p.mrr_at_risk_cents)}"
            )
        pending_html = html_table(
            ["Owner", "What", "Age", "Due", "MRR-at-risk"],
            [
                (p.owner, p.description, f"{p.age_days(now)}d",
                 p.due_at.date().isoformat() if p.due_at else "—",
                 _mrr_label(p.mrr_at_risk_cents))
                for p in digest.pending
            ],
        )
    else:
        lines.append("  none")
        pending_html = html_note("none")

    lines += ["", "— Vera."]
    body = "\n".join(lines)

    subject = (
        f"[Vera] Promise & Discrepancy Digest {report_date.isoformat()} — "
        f"{len(discrepancies)} discrepancy(ies), {len(digest.overdue)} overdue promise(s)"
    )

    html_body = html_shell(
        title="Vera — Promise & Discrepancy Digest",
        subtitle=report_date.isoformat(),
        body_html=(
            html_headline("OPEN DISCREPANCIES", str(len(discrepancies)))
            + html_section("Discrepancies (Doc claims X; live shows Y)", disc_html)
            + html_section("Overdue Promises", overdue_html)
            + html_section("Open Promises (not yet due)", pending_html)
        ),
    )
    return subject, body, html_body


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────

def _read_stale_cron() -> list:
    """Newest freshness verdict per (source, county) from vera_facts; keep the
    stale ones. Raw read (read-only session) — one query beats enumerating
    every heartbeat source key here."""
    with vera_db.session_scope() as session:
        rows = session.execute(
            text(
                "SELECT DISTINCT ON (fact_key, county_id) fact_key, county_id, fact_value "
                "FROM vera_facts WHERE fact_key LIKE 'cron.%.freshness' "
                "ORDER BY fact_key, county_id, observed_at DESC"
            )
        ).mappings().all()
    stale = []
    for r in rows:
        if r["fact_value"] != "stale":
            continue
        # fact_key = 'cron.<source>.freshness' -> '<source>' (+ county if present)
        source = r["fact_key"][len("cron."):-len(".freshness")]
        label = f"{source}/{r['county_id']}" if r["county_id"] else source
        stale.append({"label": label, "fact_value": r["fact_value"]})
    return stale


def _revenue_abstained(row: Optional[Mapping]) -> bool:
    """Pure. A row can exist with no value_numeric — V3's Stripe-outage
    abstention fact ("stripe unreachable") — which must read the same as no
    row at all, not as a verified zero."""
    return row is None or row.get("value_numeric") is None


def _compute_unchecked(
    deploy_drift: Optional[Mapping],
    paying_no_access: Optional[Mapping],
    access_not_paying: Optional[Mapping],
    mrr_drift: Optional[Mapping],
) -> list[str]:
    """Pure. Which sections had no fresh, real fact to check (V2/V3 didn't
    run / went stale, V2 abstained with drift='unknown', or V3 abstained
    with a Stripe-outage placeholder fact carrying no value_numeric)."""
    unchecked: list[str] = []
    drift_value = deploy_drift.get("fact_value") if deploy_drift else None
    if drift_value in (None, "unknown"):
        unchecked.append("deploy (V2)")
    if (
        _revenue_abstained(paying_no_access)
        and _revenue_abstained(access_not_paying)
        and _revenue_abstained(mrr_drift)
    ):
        unchecked.append("revenue (V3)")
    return unchecked


def run_discrepancy_digest() -> int:
    """Entry point for `python -m src.agents.vera --promise-digest`."""
    from src.services.kill_switch_service import get_kill_switch_status

    status = get_kill_switch_status(KILL_SWITCH_FEATURE)
    if status.get("color") == "red":
        logger.warning(
            "[Vera] kill switch [%s] = red — skipping promise digest", KILL_SWITCH_FEATURE
        )
        return 1

    # Pre-fetch every input (V3 lesson: no DB round-trip inside a pure builder).
    deploy_drift = _row(read_facts("deploy.drift", limit=1))
    pending_migrations = _row(read_facts("deploy.pending_migrations", limit=1))
    paying_no_access = _row(read_facts("revenue.reconciliation.paying_no_access.count", limit=1))
    access_not_paying = _row(read_facts("revenue.reconciliation.access_not_paying.count", limit=1))
    mrr_drift = _row(read_facts("revenue.mrr.drift_cents", limit=1))
    stale_cron = _read_stale_cron()
    promises = open_promises()

    unchecked = _compute_unchecked(deploy_drift, paying_no_access, access_not_paying, mrr_drift)

    discrepancies = build_discrepancies(
        deploy_drift, pending_migrations, stale_cron,
        paying_no_access, access_not_paying, mrr_drift,
    )
    digest = build_promise_digest(promises)

    # The digest's own output is itself a fact other agents can read.
    write_fact(
        "discrepancies.open.count", str(len(discrepancies)),
        value_numeric=Decimal(len(discrepancies)), source="vera_facts",
        method="count of non-clean V2/V3 verdicts + overdue promises",
        freshness_class=FRESHNESS_STATIC,
    )
    write_fact(
        "promises.open.count", str(digest.open_count),
        value_numeric=Decimal(digest.open_count), source="vera_promises",
        method="COUNT WHERE status='open'", freshness_class=FRESHNESS_STATIC,
    )
    write_fact(
        "promises.overdue.count", str(len(digest.overdue)),
        value_numeric=Decimal(len(digest.overdue)), source="vera_promises",
        method="open promises with due_at < now", freshness_class=FRESHNESS_STATIC,
    )

    subject, body, html_body = render_digest_report(digest, discrepancies, unchecked)

    from src.services.email import send_alert
    from src.services.vera_slack import post_vera_report

    recipients = report_recipients()
    if not recipients:
        logger.info("[Vera] no REPORT_RECIPIENTS configured — digest generated but not emailed")
    for addr in recipients:
        try:
            send_alert(subject, body, html_body=html_body, to=addr)
        except Exception as exc:
            logger.warning("[Vera] failed to send promise digest to %s: %s", addr, exc)

    post_vera_report(subject, body)

    # Validate actionable findings via Vera→Dev contract (spec §1.1.10).
    if discrepancies:
        validate_finding(
            {
                "issue": subject,
                "evidence": body[:600],
                "repro": "python -m src.agents.vera --promise-digest",
                "suspected_cause": (
                    "Webhook delivery gap, migration not applied, or data sync divergence"
                ),
                "proposed_fix": (
                    f"Investigate {len(discrepancies)} open discrepancy(ies): "
                    + ", ".join(d.source for d in discrepancies[:3])
                ),
                "effort": "low",
                "risk": "high",
            },
            source="discrepancy_digest",
        )

    logger.info(
        "[Vera] promise digest complete: discrepancies=%d open_promises=%d overdue=%d",
        len(discrepancies), digest.open_count, len(digest.overdue),
    )
    return 0
