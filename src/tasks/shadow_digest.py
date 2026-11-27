"""R1 0.9 shadow-mode section of Cora's morning digest: a pure function from shadow rows to Slack text."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional, Sequence
from zoneinfo import ZoneInfo

from config.lending_compliance import DEFAULT_TZ

MATCH_N_THRESHOLD = 20
SHADOW_WINDOW_DAYS = 14
_DECISIONS = ("approve", "edit", "reject")
_EASTERN = ZoneInfo(DEFAULT_TZ)
_PLAIN_TYPE = {"ghl_sms": "text", "ghl_email": "email", "instantly_email": "cold email", "ghl_note": "CRM note",
               "ghl_task": "CRM task", "ghl_tag": "CRM tag", "ghl_stage": "stage change"}


def _decided(rows: Sequence[dict]) -> list[dict]:
    return [r for r in rows if r.get("actual") in _DECISIONS]


def _proposed(row: dict) -> Optional[str]:
    return (row.get("proposed") or {}).get("decision")


def _eastern_date(at: Optional[datetime]) -> Optional[date]:
    return at.astimezone(_EASTERN).date() if at else None


def _kappa(rows: Sequence[dict]) -> Optional[float]:
    """Cohen's kappa over approve / edit / reject; None under the n threshold or when chance agreement is total."""
    decided = _decided(rows)
    n = len(decided)
    if n < MATCH_N_THRESHOLD:
        return None
    observed = sum(1 for r in decided if _proposed(r) == r["actual"]) / n
    chance = sum((sum(1 for r in decided if _proposed(r) == k) / n) * (sum(1 for r in decided if r["actual"] == k) / n)
                 for k in _DECISIONS)
    if abs(1 - chance) < 1e-9:
        return None
    return round((observed - chance) / (1 - chance), 3)


def _risky_misses(rows: Sequence[dict]) -> list[dict]:
    """Judge said approve, Josh edited or rejected: the only miss that matters before going live."""
    return [r for r in rows if _proposed(r) == "approve" and r.get("actual") in ("edit", "reject")]


def _match_rate(rows: Sequence[dict]) -> Optional[float]:
    decided = _decided(rows)
    if len(decided) < MATCH_N_THRESHOLD:
        return None
    return round(sum(1 for r in decided if r.get("decision_match")) / len(decided), 4)


def _always_approve_rate(rows: Sequence[dict]) -> Optional[float]:
    decided = _decided(rows)
    if len(decided) < MATCH_N_THRESHOLD:
        return None
    return round(sum(1 for r in decided if r["actual"] == "approve") / len(decided), 4)


def _risky_miss_rate(rows: Sequence[dict]) -> Optional[float]:
    decided = _decided(rows)
    return round(len(_risky_misses(rows)) / len(decided), 4) if decided else None


def _top_mismatches(rows: Sequence[dict], limit: int = 3) -> list[str]:
    """Risky misses first, then other mismatches; first name and phone last 4 only."""
    mismatches = [r for r in _decided(rows) if _proposed(r) != r["actual"]]
    mismatches.sort(key=lambda r: r not in _risky_misses([r]))
    lines = []
    for r in mismatches[:limit]:
        who = r.get("first_name") or "Unknown"
        if r.get("last4"):
            who += f" …{r['last4']}"
        kind = _PLAIN_TYPE.get(r.get("channel") or "", (r.get("channel") or "message").replace("_", " "))
        lines.append(f"{who} · {kind} · judge: {_proposed(r)} / Josh: {r['actual']}")
    return lines


def _pct(rate: Optional[float]) -> str:
    return "—" if rate is None else f"{rate * 100:.1f}%"


def _kappa_str(k: Optional[float]) -> str:
    return "—" if k is None else f"{k:.3f}"


def build_shadow_section(*, class_name: str, class_rows: Sequence[dict], baseline_rows: Sequence[dict],
                         day_number: int, shadow_sent_count: int, report_date: date) -> str:
    """One class's section: yesterday's counts, cumulative rates since start, and the zero-sends check.

    ``report_date`` is the Eastern day the digest posts; "yesterday" is the Eastern day before it.
    """
    yesterday = report_date - timedelta(days=1)
    if day_number <= SHADOW_WINDOW_DAYS:
        header = f"*Shadow mode — {class_name}* (day {day_number}/{SHADOW_WINDOW_DAYS})"
    else:
        header = f"*Shadow mode — {class_name}* (14 days done, still in shadow)"

    decided = _decided(class_rows)
    n_decided = len(decided)
    y_scored = sum(1 for r in class_rows if _eastern_date(r.get("created_at")) == yesterday)
    y_decided = sum(1 for r in decided if _eastern_date(r.get("decided_at")) == yesterday)
    no_decision = sum(1 for r in class_rows if r.get("actual") == "no_decision")
    waiting = sum(1 for r in class_rows if r.get("actual") is None)

    lines = [header, "",
             f"Yesterday: scored {y_scored}  |  decided {y_decided}",
             f"Since start: scored {len(class_rows)}  |  decided {n_decided}  |  no decision {no_decision}"
             f"  |  not yet checked {waiting}"]

    risky = _risky_misses(class_rows)
    risky_rate = _risky_miss_rate(class_rows)
    if risky_rate is None:
        lines.append("🔴 Risky misses (judge approve → Josh edit/reject): — (n=0)")
    else:
        lines.append(f"🔴 Risky misses (judge approve → Josh edit/reject): {_pct(risky_rate)} "
                     f"({len(risky)}/{n_decided})")

    if n_decided >= MATCH_N_THRESHOLD:
        n_baseline = len(_decided(baseline_rows))
        lines += ["", "```", f"{'':28s}  match%    kappa   n",
                  f"{'always-approve (ref)':28s}  {_pct(_always_approve_rate(class_rows)):8s}  {'—':6s}  {n_decided}",
                  f"{'baseline':28s}  {_pct(_match_rate(baseline_rows)):8s}  "
                  f"{_kappa_str(_kappa(baseline_rows)):6s}  {n_baseline}",
                  f"{class_name:28s}  {_pct(_match_rate(class_rows)):8s}  "
                  f"{_kappa_str(_kappa(class_rows)):6s}  {n_decided}",
                  "```"]
    else:
        lines.append(f"Match %: too few to judge (need {MATCH_N_THRESHOLD} decided, have {n_decided})")

    top = _top_mismatches(class_rows)
    if top:
        lines += ["", "*Top mismatches:*"] + [f"  • {t}" for t in top]

    settled = [r for r in class_rows if r.get("deal_outcome") and _eastern_date(r.get("outcome_at")) == yesterday]
    if settled:
        funded = sum(1 for r in settled if r["deal_outcome"] in ("funded", "paid"))
        declined = sum(1 for r in settled if r["deal_outcome"] == "declined")
        lines += ["", f"Deals settled yesterday on judged cards: funded {funded}  |  declined {declined}"]

    lines.append("")
    if shadow_sent_count == 0:
        lines.append("✅ Sent by shadow: 0")
    else:
        lines.append(f"🚨 Sent by shadow: {shadow_sent_count} ← must be 0, investigate")

    if n_decided < MATCH_N_THRESHOLD:
        lines += ["", "_At ~1.5 cards/day a 14-day run gives ~20 decisions: not enough to judge accuracy. "
                      "The counts above are reported as they are._"]
    return "\n".join(lines)
