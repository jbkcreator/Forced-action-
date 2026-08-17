"""
Vertical Autopilot — detection, scoring, and probe-loop pipeline (REVINT-v2.2 I4).

Implements the 6-dimension vertical fit rubric plus the full I4 probe loop:
    detect → score → compliance preflight → send stub → verdict → package

Public surface:
    score_vertical(vertical_name, evidence, db)         → VerticalCandidatePacket
    check_legal_status(vertical_name)                   → (legal_status, eligible_for_probe)
    evaluate_dim5(evidence)                             → int
    evaluate_dim6(vertical_name)                        → int
    run_probe(vertical_candidate_packet_id, db)         → VerticalProbe
    refresh_probe_replies(probe_id, db)                 → VerticalProbe
    evaluate_verdict(probe, db)                         → VerticalVerdict
    confirm_presell(verdict_id, db)                     → VerticalVerdict
    generate_package(packet, verdict, db)               → str (package_id)
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.vertical_fit_rubric import (
    LEGAL_RISK_ALLOWLIST,
    LEGAL_RISK_BLOCKLIST_CATEGORIES,
    MIN_MONTHLY_RECORDS,
    MONEY_EVIDENCE_SIGNALS,
    PROBE_CAMPAIGN_DAYS,
    PROBE_CAMPAIGN_SEND_FROM,
    PROBE_CAMPAIGN_SEND_TO,
    PROBE_CAMPAIGN_TIMEZONE,
    PROBE_DBPR_VERTICAL_MAP,
    PROBE_EMAIL_BODY,
    PROBE_EMAIL_SUBJECT,
    PROBE_KILL_THRESHOLD,
    PROBE_MAX_SENDS_PER_RUN,
    PROBE_MIN_SAMPLE_KILL,
    PROBE_MIN_SAMPLE_WIN,
    PROBE_SEND_CEILING,
    PROBE_WIN_THRESHOLD,
    URGENCY_EVIDENCE_SIGNALS,
    VERTICAL_AUTO_KILL_ENABLED,
    VERTICAL_FIT_THRESHOLD,
)
from src.core.models import VerticalCandidatePacket, VerticalProbe, VerticalVerdict

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Legal gate (Dim 6)
# ---------------------------------------------------------------------------

def check_legal_status(vertical_name: str) -> tuple[str, bool]:
    """Return (legal_status, eligible_for_probe) for a vertical.

    Rules:
      - allowlist match        → ("approved", True)
      - blocklist category key present in vertical_name
                               → ("blocked", False)
      - unknown                → ("pending_review", False)
    """
    if vertical_name in LEGAL_RISK_ALLOWLIST:
        return "approved", True

    for category in LEGAL_RISK_BLOCKLIST_CATEGORIES:
        if category in vertical_name:
            logger.warning(
                "vertical_autopilot: %r matched blocklist category %r — blocked",
                vertical_name,
                category,
            )
            return "blocked", False

    # Unknown vertical — flag for founder review; never auto-probe
    logger.info(
        "vertical_autopilot: %r not in allowlist or blocklist — pending_review",
        vertical_name,
    )
    return "pending_review", False


def evaluate_dim6(vertical_name: str) -> int:
    """Return Dim 6 binary score (1 = approved, 0 = blocked or pending)."""
    legal_status, _ = check_legal_status(vertical_name)
    return 1 if legal_status == "approved" else 0


# ---------------------------------------------------------------------------
# Dim 5 — Buyer evidence (requires BOTH money AND urgency)
# ---------------------------------------------------------------------------

def evaluate_dim5(evidence: dict[str, Any]) -> int:
    """Return 1 only when at least one money signal AND one urgency signal are present."""
    has_money = any(signal in evidence for signal in MONEY_EVIDENCE_SIGNALS)
    has_urgency = any(signal in evidence for signal in URGENCY_EVIDENCE_SIGNALS)
    return 1 if (has_money and has_urgency) else 0


# ---------------------------------------------------------------------------
# Full 6-dimension scorer
# ---------------------------------------------------------------------------

def _evaluate_dim1(evidence: dict[str, Any]) -> int:
    """Dim 1: monthly record volume >= MIN_MONTHLY_RECORDS."""
    return 1 if evidence.get("monthly_records", 0) >= MIN_MONTHLY_RECORDS else 0


def _evaluate_dim2(evidence: dict[str, Any]) -> int:
    """Dim 2: identifiable decision-maker (owner/contact reachable)."""
    return 1 if evidence.get("identifiable_decision_maker", False) else 0


def _evaluate_dim3(evidence: dict[str, Any]) -> int:
    """Dim 3: clear pain point / distress signal present."""
    return 1 if evidence.get("clear_pain_point", False) else 0


def _evaluate_dim4(evidence: dict[str, Any]) -> int:
    """Dim 4: FA has a solution that maps to this vertical."""
    return 1 if evidence.get("fa_solution_exists", False) else 0


def score_vertical(
    vertical_name: str,
    evidence: dict[str, Any],
    db: Session,
) -> VerticalCandidatePacket:
    """Evaluate all 6 dimensions and persist a VerticalCandidatePacket.

    Args:
        vertical_name: Canonical vertical identifier (e.g. "tax_lien").
        evidence: Dict of signal keys → values. See rubric config for signal names.
        db: SQLAlchemy session. Caller is responsible for commit.

    Returns:
        Persisted (but not yet committed) VerticalCandidatePacket.
    """
    dim1 = _evaluate_dim1(evidence)
    dim2 = _evaluate_dim2(evidence)
    dim3 = _evaluate_dim3(evidence)
    dim4 = _evaluate_dim4(evidence)
    dim5 = evaluate_dim5(evidence)
    dim6 = evaluate_dim6(vertical_name)

    total = dim1 + dim2 + dim3 + dim4 + dim5 + dim6
    legal_status, eligible_for_probe = check_legal_status(vertical_name)

    # Hard gate: pending_review must never reach probe stage
    if legal_status == "pending_review":
        eligible_for_probe = False

    status = "candidate" if total >= VERTICAL_FIT_THRESHOLD else "pending_legal" if legal_status == "pending_review" else "candidate"
    if legal_status == "blocked":
        status = "killed"

    evidence_record = {
        "dim1": {"monthly_records": evidence.get("monthly_records"), "passed": bool(dim1)},
        "dim2": {"identifiable_decision_maker": evidence.get("identifiable_decision_maker"), "passed": bool(dim2)},
        "dim3": {"clear_pain_point": evidence.get("clear_pain_point"), "passed": bool(dim3)},
        "dim4": {"fa_solution_exists": evidence.get("fa_solution_exists"), "passed": bool(dim4)},
        "dim5": {
            "money_signals": [s for s in MONEY_EVIDENCE_SIGNALS if s in evidence],
            "urgency_signals": [s for s in URGENCY_EVIDENCE_SIGNALS if s in evidence],
            "passed": bool(dim5),
        },
        "dim6": {"legal_status": legal_status, "passed": bool(dim6)},
    }

    packet = VerticalCandidatePacket(
        vertical_name=vertical_name,
        dim1_score=dim1,
        dim2_score=dim2,
        dim3_score=dim3,
        dim4_score=dim4,
        dim5_score=dim5,
        dim6_score=dim6,
        total_score=total,
        legal_status=legal_status,
        eligible_for_probe=eligible_for_probe,
        evidence=evidence_record,
        status=status,
        created_at=datetime.now(timezone.utc),
    )
    db.add(packet)
    db.flush()

    logger.info(
        "vertical_autopilot: scored %r — %d/6 dims, legal=%s, eligible_for_probe=%s, status=%s",
        vertical_name,
        total,
        legal_status,
        eligible_for_probe,
        status,
    )
    return packet


# ---------------------------------------------------------------------------
# Probe loop — I4
# ---------------------------------------------------------------------------

_PACKAGES_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "shared",
    "packages",
)


def _run_compliance_preflight(probe: VerticalProbe) -> bool:
    """Gate on kill switch; mark individual compliance checks NULL (not yet wired).

    Each boolean column is NULL until the real check is wired to its service
    (suppression list, TCPA, quiet-hours, etc.).  Writing NULL prevents the DB
    row from claiming a check passed when nothing was evaluated.  The kill-switch
    lookup is the only live check; all others are deferred until Relay is wired.
    """
    from src.services.kill_switch_service import get_kill_switch_status

    ks = get_kill_switch_status("vertical_probe")
    if ks.get("color") == "red":
        probe.kill_switch_active = True
        logger.warning(
            "vertical_autopilot: kill switch active for vertical=%r — aborting probe",
            probe.vertical_name,
        )
        return False

    probe.kill_switch_active = False
    # remaining preflight columns stay NULL until each check is wired to its service
    return True


def _execute_sends(probe: VerticalProbe, db: Session) -> None:
    """Create a dedicated Instantly campaign for this probe and add DBPR contacts as leads.

    Pulls up to PROBE_MAX_SENDS_PER_RUN contacts whose trade vertical maps to
    probe.vertical_name, preferring work_email over raw email. Creates a fresh
    Instantly campaign per probe run so reply attribution is clean (the shared
    Relay passthrough campaign's aggregate stats would conflate probe replies
    with all prior Relay sends).

    Sets probe.instantly_campaign_id and probe.sends_count. Reply ingestion
    happens separately via refresh_probe_replies().
    """
    from config.settings import get_settings
    from src.services import instantly_service

    settings = get_settings()
    sender_email = settings.relay_instantly_sender_email

    target_trades = PROBE_DBPR_VERTICAL_MAP.get(probe.vertical_name, [])
    if not target_trades:
        logger.warning(
            "vertical_autopilot._execute_sends: no DBPR trade mapping for vertical=%r — skipping sends",
            probe.vertical_name,
        )
        probe.sends_count = 0
        return

    placeholders = ", ".join(f":t{i}" for i in range(len(target_trades)))
    params: dict = {f"t{i}": v for i, v in enumerate(target_trades)}
    params["limit"] = PROBE_MAX_SENDS_PER_RUN

    rows = db.execute(
        text(
            f"SELECT id, full_name, COALESCE(work_email, email) AS send_email "
            f"FROM dbpr_contacts "
            f"WHERE vertical IN ({placeholders}) "
            f"  AND COALESCE(work_email, email) IS NOT NULL "
            f"  AND enrichment_status != 'dnc' "
            f"ORDER BY id "
            f"LIMIT :limit"
        ),
        params,
    ).fetchall()

    if not rows:
        logger.warning(
            "vertical_autopilot._execute_sends: no usable DBPR contacts for vertical=%r trades=%r",
            probe.vertical_name,
            target_trades,
        )
        probe.sends_count = 0
        return

    campaign_name = f"probe-{probe.vertical_name}-{probe.id}-{datetime.now(timezone.utc).date()}"
    schedule = {
        "schedules": [
            {
                "name": "Probe window",
                "timing": {"from": PROBE_CAMPAIGN_SEND_FROM, "to": PROBE_CAMPAIGN_SEND_TO},
                "days": PROBE_CAMPAIGN_DAYS,
                "timezone": PROBE_CAMPAIGN_TIMEZONE,
            }
        ],
        "start_date": datetime.now(timezone.utc).date().isoformat(),
    }
    sequence_steps = [
        {
            "step_number": 1,
            "subject": PROBE_EMAIL_SUBJECT.format(vertical=probe.vertical_name),
            "body": PROBE_EMAIL_BODY.format(
                first_name="{first_name}",  # Instantly personalisation token
                vertical=probe.vertical_name,
            ),
            "delay_days": 0,
        }
    ]

    campaign = instantly_service.create_campaign(
        name=campaign_name,
        schedule=schedule,
        sequence_steps=sequence_steps,
        email_list=[sender_email],
    )
    if not campaign or not campaign.get("id"):
        logger.error(
            "vertical_autopilot._execute_sends: Instantly campaign creation failed for probe=%d",
            probe.id,
        )
        raise RuntimeError(f"Instantly campaign creation failed for probe {probe.id}")

    campaign_id = campaign["id"]
    probe.instantly_campaign_id = campaign_id

    leads = []
    for row in rows:
        first_name = (row.full_name or "").split()[0] if row.full_name else ""
        leads.append({"email": row.send_email, "first_name": first_name})

    result = instantly_service.add_leads(campaign_id, leads)
    created = (result or {}).get("leads_created", len(leads))
    probe.sends_count = created

    logger.info(
        "vertical_autopilot._execute_sends: probe=%d vertical=%r campaign=%s leads_added=%d",
        probe.id,
        probe.vertical_name,
        campaign_id,
        created,
    )


def _cumulative_sends(packet_id: int, db: Session) -> tuple[int, int]:
    """Return (total_sends, total_replies) across all completed probes for a packet."""
    row = db.execute(
        text(
            "SELECT COALESCE(SUM(sends_count), 0), COALESCE(SUM(reply_count), 0) "
            "FROM vertical_probes "
            "WHERE vertical_candidate_packet_id = :pid AND status = 'completed'"
        ),
        {"pid": packet_id},
    ).one()
    return int(row[0]), int(row[1])


def refresh_probe_replies(probe_id: int, db: Session) -> VerticalProbe:
    """Poll Instantly analytics for a probe's campaign and update reply metrics.

    Fetches reply_count_unique from get_analytics_overview, writes reply_count
    and reply_rate back to the probe row, then re-evaluates the verdict using
    cumulative totals across all completed probes for the same packet.

    Safe to call repeatedly — idempotent on the analytics values returned.
    No-ops if probe has no instantly_campaign_id (sends not yet wired).
    """
    from sqlalchemy import select as _select
    from src.services import instantly_service

    probe: VerticalProbe = db.execute(
        _select(VerticalProbe).where(VerticalProbe.id == probe_id)
    ).scalar_one_or_none()
    if probe is None:
        raise ValueError(f"VerticalProbe {probe_id} not found")

    if not probe.instantly_campaign_id:
        logger.info(
            "vertical_autopilot.refresh_probe_replies: probe=%d has no campaign — skipping",
            probe_id,
        )
        return probe

    analytics = instantly_service.get_analytics_overview([probe.instantly_campaign_id])
    if not analytics:
        logger.warning(
            "vertical_autopilot.refresh_probe_replies: no analytics returned for probe=%d campaign=%s",
            probe_id,
            probe.instantly_campaign_id,
        )
        return probe

    row = analytics[0]
    reply_count = int(row.get("reply_count_unique") or row.get("replies") or 0)
    probe.reply_count = reply_count

    sends = probe.sends_count or 0
    probe.reply_rate = float(reply_count) / float(sends) if sends > 0 else 0.0

    db.flush()

    prior_sends, prior_replies = _cumulative_sends(probe.vertical_candidate_packet_id, db)
    cumulative_sends = prior_sends + sends
    cumulative_replies = prior_replies + reply_count
    if cumulative_sends > 0:
        probe.reply_rate = float(cumulative_replies) / float(cumulative_sends)
        db.flush()

    logger.info(
        "vertical_autopilot.refresh_probe_replies: probe=%d campaign=%s reply_count=%d reply_rate=%.4f",
        probe_id,
        probe.instantly_campaign_id,
        reply_count,
        probe.reply_rate,
    )

    evaluate_verdict(probe, db, cumulative_sends=cumulative_sends)
    return probe


def run_probe(vertical_candidate_packet_id: int, db: Session) -> VerticalProbe:
    """Orchestrate a single probe run (≤PROBE_MAX_SENDS_PER_RUN sends) for a candidate packet.

    Steps: load → ceiling check → compliance preflight → idempotency →
    create/reuse probe → execute sends → compute cumulative reply rate →
    update probe → evaluate verdict.
    """
    from sqlalchemy import select as _select

    # 1. Load candidate packet
    packet_obj: VerticalCandidatePacket = db.execute(
        _select(VerticalCandidatePacket).where(
            VerticalCandidatePacket.id == vertical_candidate_packet_id
        )
    ).scalar_one_or_none()
    if packet_obj is None:
        raise ValueError(f"VerticalCandidatePacket {vertical_candidate_packet_id} not found")

    if not packet_obj.eligible_for_probe:
        raise ValueError("Candidate not eligible for probe")

    # 2. Hard send-ceiling — refuse to start once cumulative sends reach PROBE_SEND_CEILING
    prior_sends, _ = _cumulative_sends(packet_obj.id, db)
    if prior_sends >= PROBE_SEND_CEILING:
        logger.info(
            "vertical_autopilot: packet %d at send ceiling (%d) — holding at awaiting_ruling",
            packet_obj.id, prior_sends,
        )
        if packet_obj.status != "awaiting_ruling":
            packet_obj.status = "awaiting_ruling"
            db.flush()
        raise ValueError(
            f"Packet {packet_obj.id} has reached the {PROBE_SEND_CEILING}-send ceiling; "
            "awaiting founder ruling before further probing."
        )

    # 3. Idempotency key (date-scoped per packet)
    idem_key = f"probe-{packet_obj.id}-{datetime.now(timezone.utc).date()}"

    # 4. Check idempotency — reuse existing probe including aborted (re-run preflight)
    existing = db.execute(
        _select(VerticalProbe).where(VerticalProbe.idempotency_key == idem_key)
    ).scalar_one_or_none()

    if existing and existing.status not in ("aborted",):
        logger.info(
            "vertical_autopilot: idempotency hit for key=%s — returning existing probe %d",
            idem_key,
            existing.id,
        )
        return existing

    if existing and existing.status == "aborted":
        # Reuse the row rather than inserting a second row with the same key.
        probe = existing
        probe.status = "running"
        probe.started_at = datetime.now(timezone.utc)
        probe.completed_at = None
        db.flush()
        logger.info(
            "vertical_autopilot: retrying aborted probe %d for key=%s",
            probe.id, idem_key,
        )
    else:
        # 5. Create new probe record
        probe = VerticalProbe(
            vertical_candidate_packet_id=packet_obj.id,
            vertical_name=packet_obj.vertical_name,
            idempotency_key=idem_key,
            status="running",
            started_at=datetime.now(timezone.utc),
        )
        db.add(probe)
        db.flush()

    # 6. Compliance preflight
    preflight_passed = _run_compliance_preflight(probe)
    if not preflight_passed:
        probe.status = "aborted"
        db.flush()
        logger.warning(
            "vertical_autopilot: probe %d aborted — compliance preflight failed",
            probe.id,
        )
        return probe

    # 7. Execute sends (stub — raises NotImplementedError outside tests)
    _execute_sends(probe, db)
    db.flush()

    # 8. Compute reply rate on cumulative basis across all completed probes + this run
    prior_sends_now, prior_replies = _cumulative_sends(packet_obj.id, db)
    total_sends = prior_sends_now + probe.sends_count
    total_replies = prior_replies + probe.reply_count
    probe.reply_rate = float(total_replies) / float(total_sends) if total_sends > 0 else 0.0

    # 9. Mark probe complete
    probe.completed_at = datetime.now(timezone.utc)
    probe.status = "completed"
    db.flush()

    logger.info(
        "vertical_autopilot: probe %d completed vertical=%r "
        "run_sends=%d cumulative_sends=%d reply_rate=%.4f",
        probe.id, probe.vertical_name, probe.sends_count, total_sends, probe.reply_rate,
    )

    # 10. Evaluate verdict using cumulative totals
    evaluate_verdict(probe, db, cumulative_sends=total_sends)

    return probe


def evaluate_verdict(
    probe: VerticalProbe,
    db: Session,
    cumulative_sends: int = 0,
) -> VerticalVerdict:
    """Evaluate probe reply rate against rubric thresholds and persist a VerticalVerdict.

    Verdict logic (evaluated in order):
      1. If cumulative_sends < PROBE_MIN_SAMPLE_WIN and rate > WIN_THRESHOLD → still running
         (not enough data to declare a win).
      2. If rate > WIN_THRESHOLD and cumulative_sends >= PROBE_MIN_SAMPLE_WIN → won.
      3. If rate < KILL_THRESHOLD and cumulative_sends >= PROBE_MIN_SAMPLE_KILL
         and VERTICAL_AUTO_KILL_ENABLED → killed.
      4. If rate < KILL_THRESHOLD but auto-kill is disabled OR below kill floor → awaiting_ruling.
      5. 3–8% gray band at or above kill floor → awaiting_ruling (founder must rule).
      6. Below any floor → running (keep collecting data).
    """
    from sqlalchemy import select as _select

    reply_rate = float(probe.reply_rate)
    at_kill_floor = cumulative_sends >= PROBE_MIN_SAMPLE_KILL
    at_win_floor = cumulative_sends >= PROBE_MIN_SAMPLE_WIN

    if reply_rate > PROBE_WIN_THRESHOLD and at_win_floor:
        verdict_val = "won"
        rule = "reply_rate_gt_8pct_at_min_sample"
    elif reply_rate < PROBE_KILL_THRESHOLD and at_kill_floor and VERTICAL_AUTO_KILL_ENABLED:
        verdict_val = "killed"
        rule = "reply_rate_lt_3pct_at_min_sample"
    elif at_kill_floor and reply_rate < PROBE_WIN_THRESHOLD:
        # Gray band (3–8%) at floor, or sub-3% with auto-kill disabled — founder rules.
        verdict_val = "awaiting_ruling"
        rule = "gray_band_at_min_sample" if reply_rate >= PROBE_KILL_THRESHOLD else "below_kill_threshold_auto_kill_disabled"
    else:
        # Below sample floor — keep collecting.
        verdict_val = "running"
        rule = "below_min_sample"

    packet_obj: VerticalCandidatePacket = db.execute(
        _select(VerticalCandidatePacket).where(
            VerticalCandidatePacket.id == probe.vertical_candidate_packet_id
        )
    ).scalar_one()

    verdict = VerticalVerdict(
        vertical_probe_id=probe.id,
        vertical_candidate_packet_id=probe.vertical_candidate_packet_id,
        vertical_name=probe.vertical_name,
        verdict=verdict_val,
        verdict_at=datetime.now(timezone.utc),
        rule_fired=rule,
        reply_rate_at_verdict=reply_rate,
        presell_confirmed=False,
        package_generated=False,
    )
    db.add(verdict)
    db.flush()

    if verdict_val == "killed":
        packet_obj.status = "killed"
        db.flush()
        _archive_probe_campaign(probe)
        logger.info(
            "vertical_autopilot: verdict=killed vertical=%r probe=%d cumulative_sends=%d",
            probe.vertical_name, probe.id, cumulative_sends,
        )
    elif verdict_val == "won":
        _on_won(verdict, packet_obj, db)
        _archive_probe_campaign(probe)
    elif verdict_val == "awaiting_ruling":
        packet_obj.status = "awaiting_ruling"
        db.flush()
        _archive_probe_campaign(probe)
        logger.info(
            "vertical_autopilot: verdict=awaiting_ruling vertical=%r probe=%d "
            "cumulative_sends=%d reply_rate=%.4f rule=%s",
            probe.vertical_name, probe.id, cumulative_sends, reply_rate, rule,
        )
    else:
        packet_obj.status = "probing"
        db.flush()
        logger.info(
            "vertical_autopilot: verdict=running vertical=%r probe=%d "
            "cumulative_sends=%d — continue probing",
            probe.vertical_name, probe.id, cumulative_sends,
        )

    return verdict


def _archive_probe_campaign(probe: VerticalProbe) -> None:
    """Pause the probe's Instantly campaign once a terminal verdict fires.

    Keeps the campaign visible in Instantly (named probe-<vertical>-<id>-<date>)
    but stops it from sending further, avoiding workspace clutter without deleting
    the send history needed for audit.
    """
    from src.services import instantly_service

    if not probe.instantly_campaign_id:
        return
    ok = instantly_service.update_campaign(probe.instantly_campaign_id, {"status": "paused"})
    if not ok:
        logger.warning(
            "vertical_autopilot: failed to archive campaign=%s for probe=%d — manual cleanup needed",
            probe.instantly_campaign_id,
            probe.id,
        )


def _on_won(verdict: VerticalVerdict, packet: VerticalCandidatePacket, db: Session) -> None:
    """Fire when verdict == 'won'. Generates package and sets deferred sell+clone payload."""
    package_id = generate_package(packet, verdict, db)

    verdict.handoff_payload = {
        "vertical": packet.vertical_name,
        "status": "ready_for_standard_cell",
        "clone_status": "deferred_until_county_2",
        "source_county": "hillsborough",
        "package_id": package_id,
    }
    verdict.clone_status = "deferred_until_county_2"
    verdict.source_county = "hillsborough"

    packet.status = "won"
    db.flush()

    logger.info(
        "vertical_autopilot: _on_won vertical=%r package_id=%s — presell_confirmed=False, dev queue blocked",
        packet.vertical_name,
        package_id,
    )


def _get_pricing_proposal(vertical: str) -> dict:
    """Stub pricing proposal. Real pricing config deferred."""
    return {
        "vertical": vertical,
        "offer": "subscription",
        "price_band": "$297–$497/mo",
        "notes": "placeholder — founder sets final price before launch",
    }


def _get_stripe_spec(vertical: str) -> dict:
    """Stub Stripe product spec. Real IDs deferred until founder creates products."""
    return {
        "product_name": vertical,
        "price_cents": None,
        "interval": "monthly",
    }


def _get_icp_sequence(vertical: str) -> list:
    """Stub 3-step ICP onboarding sequence. Real copy deferred."""
    return [
        {"step": 1, "type": "email", "subject": f"Welcome to {vertical} alerts", "body": "TBD"},
        {"step": 2, "type": "email", "subject": "Your first leads are ready", "body": "TBD"},
        {"step": 3, "type": "sms", "body": "Your leads are live — log in now."},
    ]


def generate_package(
    packet: VerticalCandidatePacket,
    verdict: VerticalVerdict,
    db: Session,
) -> str:
    """Generate and persist commercial package JSON for a won vertical. Returns package_id."""
    package_id = (
        f"PKG-{packet.vertical_name.upper()}-{datetime.now(timezone.utc).strftime('%Y%m%d')}"
    )
    package = {
        "package_id": package_id,
        "vertical": packet.vertical_name,
        "landing_page_param": packet.vertical_name,
        "pricing_proposal": _get_pricing_proposal(packet.vertical_name),
        "stripe_product_spec": _get_stripe_spec(packet.vertical_name),
        "icp_onboarding_sequence": _get_icp_sequence(packet.vertical_name),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    os.makedirs(_PACKAGES_DIR, exist_ok=True)
    package_path = os.path.join(_PACKAGES_DIR, f"{package_id}.json")
    with open(package_path, "w", encoding="utf-8") as fh:
        json.dump(package, fh, indent=2)

    verdict.package_id = package_id
    verdict.package_generated = True
    db.flush()

    logger.info(
        "vertical_autopilot: package generated vertical=%r package_id=%s path=%s",
        packet.vertical_name,
        package_id,
        package_path,
    )
    return package_id


def confirm_presell(verdict_id: int, db: Session) -> VerticalVerdict:
    """Set presell_confirmed=True on a verdict, unblocking dev queue entry."""
    from sqlalchemy import select as _select

    verdict = db.execute(
        _select(VerticalVerdict).where(VerticalVerdict.id == verdict_id)
    ).scalar_one_or_none()
    if verdict is None:
        raise ValueError(f"VerticalVerdict {verdict_id} not found")

    verdict.presell_confirmed = True
    db.flush()

    logger.info(
        "vertical_autopilot: presell confirmed verdict=%d vertical=%r — dev queue unblocked",
        verdict_id,
        verdict.vertical_name,
    )
    return verdict
