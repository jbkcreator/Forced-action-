"""
Stage 10 — 3-Variant Message Mutation Engine (fa055).

Manages a 3-slot (a/b/c) A/B test per named sequence:
  - Deterministic, traffic-capped variant assignment
  - Automated retirement of lowest-performing slot after 200 sends
  - Claude Haiku replacement copy generation
  - 200-send proving cycle: revert if replacement doesn't beat retired baseline
  - Auto-rollback if any slot drops >2σ below the best active slot

All DB I/O uses raw SQL via sa_text (repo convention).
Anthropic API call is synchronous; guarded by try/except so a Haiku outage
never blocks the retirement loop.

Usage:
    from src.services.variant_engine import (
        assign_variant, record_send, record_outcome,
        check_and_retire, check_replacement_performance,
        check_sigma_rollback,
    )
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.cora_guardrails import get_guardrail
from config.settings import get_settings
from config.stage10_config import VARIANT_TEST

logger = logging.getLogger(__name__)

SLOTS = ("a", "b", "c")


# ── helpers ──────────────────────────────────────────────────────────────────

def _hash_slot(subscriber_id: int, sequence_name: str, traffic_pct: int) -> Optional[str]:
    """Deterministic 3-way split within traffic_pct cap. Returns 'a'/'b'/'c'/None."""
    h = int(hashlib.md5(f"{sequence_name}{subscriber_id}".encode()).hexdigest(), 16) % 100
    if h >= traffic_pct:
        return None
    return SLOTS[h % 3]


def _conv_rate(sends: int, conversions: int) -> float:
    return conversions / sends if sends > 0 else 0.0


def _z_score(p_test: float, n_test: int, p_ctrl: float, n_ctrl: int) -> float:
    """One-sample proportional z-score of p_test vs p_ctrl."""
    p_pool = (p_test * n_test + p_ctrl * n_ctrl) / (n_test + n_ctrl)
    if p_pool in (0.0, 1.0) or n_test == 0 or n_ctrl == 0:
        return 0.0
    se = math.sqrt(p_pool * (1 - p_pool) * (1 / n_test + 1 / n_ctrl))
    return (p_test - p_ctrl) / se if se > 0 else 0.0


def _ikey(test_id: int, slot: str, action: str, round_tag: str = "1") -> str:
    return f"{test_id}:{slot}:{action}:r{round_tag}"


def _log_retirement(
    db: Session,
    *,
    test_id: int,
    action: str,
    slot: str,
    old_body: Optional[str],
    new_body: Optional[str],
    old_rate: Optional[float],
    new_rate: Optional[float],
    reason: str,
    idempotency_key: str,
) -> bool:
    """Insert a variant_retirement_log row. Returns False if already exists (idempotent)."""
    result = db.execute(sa_text("""
        INSERT INTO variant_retirement_log
            (test_id, action, slot, old_body, new_body,
             old_conversion_rate, new_conversion_rate,
             reason, idempotency_key, created_at)
        VALUES
            (:tid, :action, :slot, :old_body, :new_body,
             :old_rate, :new_rate,
             :reason, :ikey, NOW())
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING id
    """), {
        "tid": test_id,
        "action": action,
        "slot": slot,
        "old_body": old_body,
        "new_body": new_body,
        "old_rate": old_rate,
        "new_rate": new_rate,
        "reason": reason,
        "ikey": idempotency_key,
    })
    return result.rowcount > 0


# ── test lookup ──────────────────────────────────────────────────────────────

def get_test(sequence_name: str, db: Session) -> Optional[Any]:
    """Return the active message_variant_tests row or None."""
    return db.execute(sa_text("""
        SELECT * FROM message_variant_tests
        WHERE sequence_name = :name AND status = 'active'
        LIMIT 1
    """), {"name": sequence_name}).first()


def get_or_create_test(
    sequence_name: str,
    *,
    slot_a_body: str,
    slot_b_body: str,
    slot_c_body: str,
    segment: Optional[str] = None,
    traffic_pct: int = 10,
    db: Session,
) -> Any:
    """Idempotently register a 3-variant test. Returns the row."""
    cap = get_guardrail("ab_test_traffic_cap")["max_pct"]
    capped = min(traffic_pct, cap)

    existing = db.execute(sa_text("""
        SELECT * FROM message_variant_tests WHERE sequence_name = :name LIMIT 1
    """), {"name": sequence_name}).first()

    if existing:
        if existing.traffic_pct != capped:
            db.execute(sa_text("""
                UPDATE message_variant_tests
                SET traffic_pct = :pct, updated_at = NOW()
                WHERE id = :id
            """), {"pct": capped, "id": existing.id})
        return existing

    db.execute(sa_text("""
        INSERT INTO message_variant_tests
            (sequence_name, segment, traffic_pct,
             slot_a_body, slot_b_body, slot_c_body,
             status, created_at, updated_at)
        VALUES
            (:name, :seg, :pct,
             :a, :b, :c,
             'active', NOW(), NOW())
    """), {
        "name": sequence_name,
        "seg": segment,
        "pct": capped,
        "a": slot_a_body,
        "b": slot_b_body,
        "c": slot_c_body,
    })
    return db.execute(sa_text("""
        SELECT * FROM message_variant_tests WHERE sequence_name = :name LIMIT 1
    """), {"name": sequence_name}).first()


# ── assignment ───────────────────────────────────────────────────────────────

def assign_variant(
    subscriber_id: int,
    sequence_name: str,
    db: Session,
) -> Optional[tuple[str, str]]:
    """Return (slot, body) or None if subscriber is outside the traffic cap.

    If the assigned slot is retired, falls back to the best active slot so
    traffic is never lost.
    """
    test = get_test(sequence_name, db)
    if not test:
        return None

    slot = _hash_slot(subscriber_id, sequence_name, test.traffic_pct)
    if slot is None:
        return None

    # If hashed slot is retired, route to best active slot.
    if getattr(test, f"slot_{slot}_status") == "retired":
        slot = _best_active_slot(test)
        if slot is None:
            return None

    body = getattr(test, f"slot_{slot}_body")
    return slot, body


def _best_active_slot(test: Any) -> Optional[str]:
    """Return the slot with the highest conversion rate among active slots."""
    best_slot, best_rate = None, -1.0
    for s in SLOTS:
        if getattr(test, f"slot_{s}_status") != "active":
            continue
        sends = getattr(test, f"slot_{s}_sends")
        convs = getattr(test, f"slot_{s}_conversions")
        rate = _conv_rate(sends, convs)
        if rate > best_rate:
            best_rate, best_slot = rate, s
    return best_slot


# ── send / outcome recording ─────────────────────────────────────────────────

def record_send(sequence_name: str, slot: str, db: Session) -> None:
    """Increment the send counter for a slot. No-op if test not found."""
    if slot not in SLOTS:
        return
    db.execute(sa_text(f"""
        UPDATE message_variant_tests
        SET slot_{slot}_sends = slot_{slot}_sends + 1, updated_at = NOW()
        WHERE sequence_name = :name
    """), {"name": sequence_name})


def record_outcome(
    sequence_name: str,
    slot: str,
    outcome: str,
    db: Session,
) -> None:
    """Record a conversion or reply for a slot.

    outcome: 'converted' | 'replied' | 'ignored'
    """
    if slot not in SLOTS or outcome not in ("converted", "replied", "ignored"):
        return
    if outcome == "converted":
        db.execute(sa_text(f"""
            UPDATE message_variant_tests
            SET slot_{slot}_conversions = slot_{slot}_conversions + 1, updated_at = NOW()
            WHERE sequence_name = :name
        """), {"name": sequence_name})
    elif outcome == "replied":
        db.execute(sa_text(f"""
            UPDATE message_variant_tests
            SET slot_{slot}_replies = slot_{slot}_replies + 1, updated_at = NOW()
            WHERE sequence_name = :name
        """), {"name": sequence_name})


# ── retirement ───────────────────────────────────────────────────────────────

def _slot_stats(test: Any) -> list[dict]:
    stats = []
    for s in SLOTS:
        sends = getattr(test, f"slot_{s}_sends")
        convs = getattr(test, f"slot_{s}_conversions")
        stats.append({
            "slot": s,
            "status": getattr(test, f"slot_{s}_status"),
            "sends": sends,
            "conversions": convs,
            "conv_rate": _conv_rate(sends, convs),
            "body": getattr(test, f"slot_{s}_body"),
        })
    return stats


def check_and_retire(sequence_name: str, db: Session) -> dict:
    """Retirement loop step: retire lowest slot after 200 sends, generate replacement.

    Returns a dict describing the action taken (or 'no_op').
    Idempotent: safe to re-run on retry.
    """
    test = get_test(sequence_name, db)
    if not test:
        return {"status": "no_test", "sequence": sequence_name}

    retire_after = VARIANT_TEST["retire_after_sends"]
    min_sends = VARIANT_TEST["min_sends_for_comparison"]

    all_stats = _slot_stats(test)
    active = [s for s in all_stats if s["status"] == "active" and s["sends"] >= retire_after]

    if len(active) < 2:
        return {
            "status": "not_ready",
            "active_qualified": len(active),
            "needed": 2,
        }

    active.sort(key=lambda x: x["conv_rate"])
    loser = active[0]
    winner = active[-1]

    # Only retire if statistically significant — loser must have enough sends.
    if loser["sends"] < min_sends or winner["sends"] < min_sends:
        return {"status": "insufficient_sends", "loser_sends": loser["sends"]}

    z = _z_score(
        loser["conv_rate"], loser["sends"],
        winner["conv_rate"], winner["sends"],
    )
    sigma_threshold = VARIANT_TEST["rollback_sigma_threshold"]
    if z > -sigma_threshold:
        return {
            "status": "not_significantly_worse",
            "z_score": round(z, 3),
            "threshold": -sigma_threshold,
        }

    slot = loser["slot"]
    ikey = _ikey(test.id, slot, "retire")
    already_logged = db.execute(sa_text("""
        SELECT 1 FROM variant_retirement_log WHERE idempotency_key = :k
    """), {"k": ikey}).first()

    if already_logged:
        return {"status": "already_retired", "slot": slot}

    # Retire the loser.
    db.execute(sa_text(f"""
        UPDATE message_variant_tests
        SET slot_{slot}_status = 'retired',
            slot_{slot}_retired_at = NOW(),
            updated_at = NOW()
        WHERE id = :id
    """), {"id": test.id})

    # Generate replacement copy via Claude Haiku.
    new_body = _generate_replacement(sequence_name, loser, winner)

    # Install replacement in retired slot and enter proving cycle.
    db.execute(sa_text(f"""
        UPDATE message_variant_tests
        SET slot_{slot}_body = :body,
            slot_{slot}_status = 'active',
            slot_{slot}_sends = 0,
            slot_{slot}_conversions = 0,
            slot_{slot}_replies = 0,
            proving_slot = :slot,
            proving_baseline_conv_rate = :baseline,
            proving_started_at = NOW(),
            updated_at = NOW()
        WHERE id = :id
    """), {
        "body": new_body,
        "slot": slot,
        "baseline": loser["conv_rate"],
        "id": test.id,
    })

    _log_retirement(
        db,
        test_id=test.id,
        action="retired",
        slot=slot,
        old_body=loser["body"],
        new_body=new_body,
        old_rate=loser["conv_rate"],
        new_rate=None,
        reason=f"z={z:.3f} < -{sigma_threshold}σ; loser sends={loser['sends']}",
        idempotency_key=ikey,
    )

    logger.info(
        "[variant-engine] retired slot=%s seq=%s z=%.3f; replacement installed",
        slot, sequence_name, z,
    )
    return {
        "status": "retired",
        "slot": slot,
        "retired_rate": loser["conv_rate"],
        "winner_rate": winner["conv_rate"],
        "z_score": round(z, 3),
        "replacement_body": new_body[:80] + "...",
    }


# ── proving cycle ─────────────────────────────────────────────────────────────

def check_replacement_performance(sequence_name: str, db: Session) -> dict:
    """Proving-cycle step: after 200 sends, compare replacement to retired baseline.

    If replacement beats retired conv_rate → promote (clear proving state).
    If not → revert slot body to winner copy.
    """
    test = get_test(sequence_name, db)
    if not test or not test.proving_slot:
        return {"status": "no_proving_cycle"}

    slot = test.proving_slot
    prove_after = VARIANT_TEST["prove_within_sends"]
    slot_sends = getattr(test, f"slot_{slot}_sends")

    if slot_sends < prove_after:
        return {"status": "still_proving", "sends": slot_sends, "needed": prove_after}

    slot_convs = getattr(test, f"slot_{slot}_conversions")
    replacement_rate = _conv_rate(slot_sends, slot_convs)
    baseline_rate = float(test.proving_baseline_conv_rate or 0)

    if replacement_rate > baseline_rate:
        # Replacement wins — promote and clear proving state.
        ikey = _ikey(test.id, slot, "promoted")
        promoted = _log_retirement(
            db,
            test_id=test.id,
            action="promoted",
            slot=slot,
            old_body=None,
            new_body=getattr(test, f"slot_{slot}_body"),
            old_rate=baseline_rate,
            new_rate=replacement_rate,
            reason=f"replacement {replacement_rate:.3f} > baseline {baseline_rate:.3f}",
            idempotency_key=ikey,
        )
        if promoted:
            db.execute(sa_text("""
                UPDATE message_variant_tests
                SET proving_slot = NULL,
                    proving_baseline_conv_rate = NULL,
                    proving_started_at = NULL,
                    updated_at = NOW()
                WHERE id = :id
            """), {"id": test.id})
        return {
            "status": "promoted",
            "slot": slot,
            "replacement_rate": replacement_rate,
            "baseline_rate": baseline_rate,
        }
    else:
        # Replacement loses — revert to best active slot's copy.
        best = _best_active_slot(test)
        if best and best != slot:
            winner_body = getattr(test, f"slot_{best}_body")
            ikey = _ikey(test.id, slot, "reverted")
            reverted = _log_retirement(
                db,
                test_id=test.id,
                action="reverted",
                slot=slot,
                old_body=getattr(test, f"slot_{slot}_body"),
                new_body=winner_body,
                old_rate=replacement_rate,
                new_rate=None,
                reason=f"replacement {replacement_rate:.3f} <= baseline {baseline_rate:.3f}",
                idempotency_key=ikey,
            )
            if reverted:
                db.execute(sa_text(f"""
                    UPDATE message_variant_tests
                    SET slot_{slot}_body = :body,
                        slot_{slot}_sends = 0,
                        slot_{slot}_conversions = 0,
                        slot_{slot}_replies = 0,
                        proving_slot = NULL,
                        proving_baseline_conv_rate = NULL,
                        proving_started_at = NULL,
                        updated_at = NOW()
                    WHERE id = :id
                """), {"body": winner_body, "id": test.id})

        logger.info(
            "[variant-engine] replacement reverted slot=%s seq=%s "
            "replacement_rate=%.3f baseline=%.3f",
            slot, sequence_name, replacement_rate, baseline_rate,
        )
        return {
            "status": "reverted",
            "slot": slot,
            "replacement_rate": replacement_rate,
            "baseline_rate": baseline_rate,
        }


# ── sigma auto-rollback ───────────────────────────────────────────────────────

def check_sigma_rollback(sequence_name: str, db: Session) -> dict:
    """Pause any slot that drops >2σ below the best active slot.

    Called from the mutation job every run. Returns a list of paused slots.
    """
    test = get_test(sequence_name, db)
    if not test:
        return {"status": "no_test"}

    sigma_threshold = VARIANT_TEST["rollback_sigma_threshold"]
    min_sends = VARIANT_TEST["min_sends_for_comparison"]

    all_stats = _slot_stats(test)
    active = [s for s in all_stats if s["status"] == "active" and s["sends"] >= min_sends]

    if len(active) < 2:
        return {"status": "insufficient_data", "active": len(active)}

    best = max(active, key=lambda x: x["conv_rate"])
    paused = []

    for slot_stat in active:
        if slot_stat["slot"] == best["slot"]:
            continue
        z = _z_score(
            slot_stat["conv_rate"], slot_stat["sends"],
            best["conv_rate"], best["sends"],
        )
        if z < -sigma_threshold:
            ikey = _ikey(test.id, slot_stat["slot"], "rollback")
            wrote = _log_retirement(
                db,
                test_id=test.id,
                action="rollback",
                slot=slot_stat["slot"],
                old_body=slot_stat["body"],
                new_body=None,
                old_rate=slot_stat["conv_rate"],
                new_rate=None,
                reason=f"sigma rollback z={z:.3f} < -{sigma_threshold}σ",
                idempotency_key=ikey,
            )
            if wrote:
                db.execute(sa_text(f"""
                    UPDATE message_variant_tests
                    SET slot_{slot_stat['slot']}_status = 'retired',
                        slot_{slot_stat['slot']}_retired_at = NOW(),
                        updated_at = NOW()
                    WHERE id = :id
                """), {"id": test.id})
                logger.warning(
                    "[variant-engine] sigma-rollback: paused slot=%s seq=%s z=%.3f",
                    slot_stat["slot"], sequence_name, z,
                )
                paused.append({"slot": slot_stat["slot"], "z_score": round(z, 3)})

    return {"status": "checked", "paused": paused, "best_slot": best["slot"]}


# ── promote winner (self-healing hook) ───────────────────────────────────────

def promote_winner(sequence_name: str, db: Session) -> dict:
    """Pause the losing slot and promote the winner.

    Called by self-healing when first_payment_rate breaches threshold.
    Idempotent: writes to variant_retirement_log with unique key.
    """
    test = get_test(sequence_name, db)
    if not test:
        return {"status": "no_test"}

    all_stats = _slot_stats(test)
    active = [s for s in all_stats if s["status"] == "active"]
    if len(active) < 2:
        return {"status": "insufficient_active_slots", "active": len(active)}

    active.sort(key=lambda x: x["conv_rate"])
    loser = active[0]
    winner = active[-1]

    # Date-scoped key: only one auto-promotion per test per calendar day.
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ikey = f"{test.id}:sh_promote:{today}"
    wrote = _log_retirement(
        db,
        test_id=test.id,
        action="promoted",
        slot=loser["slot"],
        old_body=loser["body"],
        new_body=winner["body"],
        old_rate=loser["conv_rate"],
        new_rate=winner["conv_rate"],
        reason="self-healing auto-promote: losing variant paused, winner promoted",
        idempotency_key=ikey,
    )

    if not wrote:
        return {"status": "already_promoted"}

    db.execute(sa_text(f"""
        UPDATE message_variant_tests
        SET slot_{loser['slot']}_status = 'retired',
            slot_{loser['slot']}_retired_at = NOW(),
            updated_at = NOW()
        WHERE id = :id
    """), {"id": test.id})

    logger.info(
        "[variant-engine] self-heal promote: paused slot=%s winner=%s seq=%s",
        loser["slot"], winner["slot"], sequence_name,
    )
    return {
        "status": "promoted",
        "paused_slot": loser["slot"],
        "winner_slot": winner["slot"],
        "loser_rate": loser["conv_rate"],
        "winner_rate": winner["conv_rate"],
    }


# ── Claude Haiku replacement generation ─────────────────────────────────────

def _generate_replacement(
    sequence_name: str,
    loser: dict,
    winner: dict,
) -> str:
    """Call Claude Haiku to generate a replacement SMS body.

    Falls back to a simple template if the API call fails, so the retirement
    loop is never blocked by a transient Haiku outage.
    """
    try:
        import anthropic

        settings = get_settings()
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

        prompt = (
            f"You are Cora, an AI revenue operator for a distressed property platform.\n\n"
            f"Generate ONE new SMS message variant for the '{sequence_name}' sequence.\n\n"
            f"Retiring variant (conversion rate {loser['conv_rate']:.1%}, sending too few to measure):\n"
            f"{loser['body']}\n\n"
            f"Best-performing variant (conversion rate {winner['conv_rate']:.1%}):\n"
            f"{winner['body']}\n\n"
            f"Requirements:\n"
            f"- Under 160 characters\n"
            f"- No false urgency, no unsubstantiated pricing claims\n"
            f"- Different angle than both variants above\n"
            f"- Maintains TCPA compliance tone\n"
            f"- Speaks to a motivated property buyer/investor\n\n"
            f"Return ONLY the SMS body text, nothing else."
        )

        response = client.messages.create(
            model=VARIANT_TEST["haiku_model"],
            max_tokens=VARIANT_TEST["haiku_max_tokens"],
            messages=[{"role": "user", "content": prompt}],
        )
        body = response.content[0].text.strip()
        # Enforce SMS character limit.
        if len(body) > VARIANT_TEST["max_sms_chars"]:
            body = body[: VARIANT_TEST["max_sms_chars"]]
        logger.info(
            "[variant-engine] haiku generated replacement for seq=%s len=%d",
            sequence_name, len(body),
        )
        return body

    except Exception:
        logger.exception(
            "[variant-engine] haiku generation failed for seq=%s — using fallback template",
            sequence_name,
        )
        # Safe fallback: mirror the winner body to ensure traffic always has copy.
        return winner["body"]


# ── metrics snapshot (Prometheus / logging) ──────────────────────────────────

def get_variant_metrics_snapshot(sequence_name: str, db: Session) -> dict:
    """Return current slot performance stats. Used by Prometheus exporter and tests."""
    test = get_test(sequence_name, db)
    if not test:
        return {}
    return {
        "sequence_name": sequence_name,
        "status": test.status,
        "traffic_pct": test.traffic_pct,
        "proving_slot": test.proving_slot,
        "slots": {
            s: {
                "status": getattr(test, f"slot_{s}_status"),
                "sends": getattr(test, f"slot_{s}_sends"),
                "conversions": getattr(test, f"slot_{s}_conversions"),
                "replies": getattr(test, f"slot_{s}_replies"),
                "conv_rate": _conv_rate(
                    getattr(test, f"slot_{s}_sends"),
                    getattr(test, f"slot_{s}_conversions"),
                ),
            }
            for s in SLOTS
        },
    }
