"""R1 0.9 Shadow mode digest section builder.

``build_shadow_section`` is a pure function: rows in, text section out.  No DB, no Slack.
Called by the morning digest hook (added after #480 merges).

Metrics:
  * Day N/14 header; "14 days done, still in shadow" after day 14.
  * n always shown next to every rate.
  * Headline = risky-miss rate (judge said approve, Josh edited / rejected).
  * Match % + Cohen's kappa only once n ≥ 20 decided; else "too few to judge".
  * Baseline vs v1 vs always-approve reference side by side.
  * Top 3 mismatches: first name + message type + "judge: approve / Josh: rejected".
  * "Sent by shadow: 0" from pending_actions (must always be 0).

Volume note (from 0.9-GRILL-DECISIONS.md): ~1.5 cards/day at pre-launch, so n will likely stay
"too few to judge" the entire first run.  The section says so.
"""
from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Optional, Sequence

MATCH_N_THRESHOLD = 20   # D15: show match % and kappa only once this many decisions exist
SHADOW_WINDOW_DAYS = 14  # the spec's 14-day window; after this we say "14 days done, still in shadow"

# ── Cohen's kappa ────────────────────────────────────────────────────────────

def _kappa(rows: Sequence[dict]) -> Optional[float]:
    """Cohen's kappa for two-class (match / no-match) agreement.

    Returns None when n < MATCH_N_THRESHOLD or denominator is zero.
    """
    decided = [r for r in rows if r.get("actual") not in (None, "no_decision")]
    n = len(decided)
    if n < MATCH_N_THRESHOLD:
        return None
    matches = sum(1 for r in decided if r.get("decision_match"))
    p_o = matches / n  # observed agreement

    # Marginal distributions
    judge_approve = sum(1 for r in decided if (r.get("proposed") or {}).get("decision") == "approve") / n
    judge_other = 1 - judge_approve
    actual_approve = sum(1 for r in decided if r.get("actual") == "approve") / n
    actual_other = 1 - actual_approve

    p_e = judge_approve * actual_approve + judge_other * actual_other
    if abs(1 - p_e) < 1e-9:
        return None
    return round((p_o - p_e) / (1 - p_e), 3)


# ── Risky-miss rate ───────────────────────────────────────────────────────────

def _risky_misses(rows: Sequence[dict]) -> list[dict]:
    """Judge said 'approve', Josh edited or rejected — the only failure that matters pre-live."""
    return [
        r for r in rows
        if (r.get("proposed") or {}).get("decision") == "approve"
        and r.get("actual") in ("edit", "reject")
    ]


# ── Match rate helpers ───────────────────────────────────────────────────────

def _match_rate(rows: Sequence[dict]) -> Optional[float]:
    decided = [r for r in rows if r.get("actual") not in (None, "no_decision")]
    if len(decided) < MATCH_N_THRESHOLD:
        return None
    matches = sum(1 for r in decided if r.get("decision_match"))
    return round(matches / len(decided), 4)


def _always_approve_rate(rows: Sequence[dict]) -> Optional[float]:
    """The rate an always-approve baseline would achieve."""
    decided = [r for r in rows if r.get("actual") not in (None, "no_decision")]
    if len(decided) < MATCH_N_THRESHOLD:
        return None
    approvals = sum(1 for r in decided if r.get("actual") == "approve")
    return round(approvals / len(decided), 4)


def _risky_miss_rate(rows: Sequence[dict]) -> Optional[float]:
    decided = [r for r in rows if r.get("actual") not in (None, "no_decision")]
    if not decided:
        return None
    misses = _risky_misses(rows)
    return round(len(misses) / len(decided), 4)


# ── Top mismatches ────────────────────────────────────────────────────────────

def _top_mismatches(rows: Sequence[dict], limit: int = 3) -> list[str]:
    mismatches = [r for r in rows if r.get("actual") and r.get("actual") != "no_decision"
                  and not r.get("decision_match")]
    # sort by most surprising: risky misses first
    risky = [r for r in mismatches if (r.get("proposed") or {}).get("decision") == "approve"
             and r.get("actual") in ("edit", "reject")]
    other = [r for r in mismatches if r not in risky]
    ordered = risky + other

    lines: list[str] = []
    for r in ordered[:limit]:
        first_name = str(r.get("first_name") or "?")[:20]
        msg_type = str(r.get("message_type") or "message")
        judge_dec = (r.get("proposed") or {}).get("decision", "?")
        actual = r.get("actual", "?")
        lines.append(f"{first_name} · {msg_type} · judge: {judge_dec} / Josh: {actual}")
    return lines


# ── Percentage / kappa formatters ─────────────────────────────────────────────

def _pct(rate: Optional[float]) -> str:
    if rate is None:
        return "—"
    return f"{rate * 100:.1f}%"


def _kappa_str(k: Optional[float]) -> str:
    if k is None:
        return "—"
    return f"{k:.3f}"


# ── Main builder ─────────────────────────────────────────────────────────────

def build_shadow_section(
    *,
    class_name: str,
    class_rows: Sequence[dict],
    baseline_rows: Sequence[dict],
    day_number: int,
    shadow_sent_count: int,
) -> str:
    """Return the Slack-ready text section for one shadow class.

    Args:
        class_name:        The shadow class being reported (e.g. "auto-send-judge-v1").
        class_rows:        All shadow_decisions rows for this class since start.
        baseline_rows:     All shadow_decisions rows for the baseline class (same period).
        day_number:        Days since the class was started (1-based).
        shadow_sent_count: Count of pending_actions requested_by LIKE 'shadow:%'.  Must be 0.
    """
    if day_number <= SHADOW_WINDOW_DAYS:
        header = f"*Shadow mode — {class_name}* (day {day_number}/{SHADOW_WINDOW_DAYS})"
    else:
        header = f"*Shadow mode — {class_name}* (14 days done, still in shadow)"

    n_scored = len(class_rows)
    n_decided = sum(1 for r in class_rows if r.get("actual") not in (None, "no_decision"))
    n_no_decision = sum(1 for r in class_rows if r.get("actual") == "no_decision")

    # Yesterday's row subset (all rows in class_rows that have decided_at set to yesterday)
    # (caller passes all rows since start; "yesterday" is computed from decided_at)
    # For the section, we report cumulative stats (spec D15).

    risky = _risky_misses(class_rows)
    risky_rate = _risky_miss_rate(class_rows)
    match_rate_v1 = _match_rate(class_rows)
    kappa_v1 = _kappa(class_rows)
    match_rate_baseline = _match_rate(baseline_rows)
    kappa_baseline = _kappa(baseline_rows)
    always_approve = _always_approve_rate(class_rows)

    lines: list[str] = [header, ""]

    # Counts
    lines.append(f"Scored: {n_scored}  |  Decided: {n_decided}  |  No decision yet: {n_no_decision}")

    # Risky-miss rate (headline)
    if risky_rate is not None:
        lines.append(f"🔴 Risky misses (judge approve → Josh edit/reject): {_pct(risky_rate)} ({len(risky)}/{n_decided})")
    else:
        lines.append(f"🔴 Risky misses: —  (n={n_decided}; {MATCH_N_THRESHOLD} needed for rates)")

    # Match % table
    if n_decided >= MATCH_N_THRESHOLD:
        lines.append("")
        lines.append("```")
        lines.append(f"{'':28s}  match%   kappa   n")
        lines.append(f"{'always-approve (ref)':28s}  {_pct(always_approve):8s}  {'—':6s}  {n_decided}")
        lines.append(f"{'baseline':28s}  {_pct(match_rate_baseline):8s}  {_kappa_str(kappa_baseline):6s}  {n_decided}")
        lines.append(f"{class_name:28s}  {_pct(match_rate_v1):8s}  {_kappa_str(kappa_v1):6s}  {n_decided}")
        lines.append("```")
    else:
        lines.append(f"Match %: too few to judge (need {MATCH_N_THRESHOLD} decided, have {n_decided})")

    # Top 3 risky mismatches
    top = _top_mismatches(class_rows)
    if top:
        lines.append("")
        lines.append("*Risky misses — top cases:*")
        for t in top:
            lines.append(f"  • {t}")

    # Deal outcomes
    with_outcome = [r for r in class_rows if r.get("deal_outcome")]
    if with_outcome:
        lines.append("")
        funded = sum(1 for r in with_outcome if r["deal_outcome"] in ("funded", "paid"))
        declined = sum(1 for r in with_outcome if r["deal_outcome"] == "declined")
        lines.append(f"Deal outcomes on judged rows: funded/paid={funded}  declined={declined}")

    # Zero-sends proof (D14c)
    lines.append("")
    if shadow_sent_count == 0:
        lines.append("✅ Sent by shadow: 0")
    else:
        lines.append(f"🚨 Sent by shadow: {shadow_sent_count}  ← UNEXPECTED — investigate")

    # Volume note when n is too low
    if n_decided < MATCH_N_THRESHOLD:
        lines.append("")
        lines.append("_Note: at ~1.5 cards/day a 14-day run is ~20 decisions — not enough to judge accuracy. "
                      "The section reports counts faithfully._")

    return "\n".join(lines)
