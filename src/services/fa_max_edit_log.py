"""FA Max weekly edit log (WP-T3-2, client item 40).

The log is a derived view over relay_approval_queue: every human-decided row
in the edit-rate population that Josh revised before deciding. No table of
its own. Diffs and categories are computed on read from original_draft and
final_content.

Edit rates come from fa_max_autonomy — the same population and the same
material_edit count the Tier B graduation gate reads — so the number in the
report is the gate's number, never a recomputation.

Retraining agents on this corpus is deliberately out of scope (deferred until
a real corpus exists).
"""
from __future__ import annotations

import difflib
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.agents.fa_max.tool_registry import FA_MAX_AUTONOMY_POLICY
from src.services.fa_max_autonomy import (
    EDIT_RATE_POPULATION_SQL,
    FA_MAX_VENTURE,
    EditCounts,
    get_edit_counts_by_pair,
    iso_week_bounds,
)

MATERIAL_EDIT_THRESHOLD = 0.15
"""An edit is material when more than this share of the union of word tokens
changed. The single definition; the Revise handler stores material_edit with it."""

PRIOR_WEEKS = 4

_SHORTENED_RATIO = 0.80
_LENGTHENED_RATIO = 1.20
_TOP_CATEGORY_COUNT = 3

_WORD_RE = re.compile(r"\w+")
_URL_RE = re.compile(r"https?://\S+")
_NUMBER_RE = re.compile(r"\$?\d[\d,]*(?:\.\d+)?%?")

_TIER_B_GATE = FA_MAX_AUTONOMY_POLICY["B"]["max_edit_rate_exclusive"]


def token_change_ratio(old: str, new: str) -> float:
    """Symmetric difference over union of the two texts' lowercased word
    tokens. 0.0 when both are empty."""
    old_tokens = set(_WORD_RE.findall((old or "").lower()))
    new_tokens = set(_WORD_RE.findall((new or "").lower()))
    union = old_tokens | new_tokens
    if not union:
        return 0.0
    return len(old_tokens ^ new_tokens) / len(union)


def is_material_edit(old: str, new: str) -> bool:
    return token_change_ratio(old, new) > MATERIAL_EDIT_THRESHOLD


def word_diff(old: str, new: str) -> str:
    """Word-level diff rendered inline as [-removed-]{+added+}."""
    old_words = (old or "").split()
    new_words = (new or "").split()
    parts: list[str] = []
    matcher = difflib.SequenceMatcher(None, old_words, new_words, autojunk=False)
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            parts.extend(old_words[i1:i2])
            continue
        parts.extend(f"[-{w}-]" for w in old_words[i1:i2])
        parts.extend(f"{{+{w}+}}" for w in new_words[j1:j2])
    return " ".join(parts)


def _numbers(text_: str) -> set[str]:
    return {n.replace(",", "") for n in _NUMBER_RE.findall(text_ or "")}


def _lines(text_: str) -> list[str]:
    return [ln.strip() for ln in (text_ or "").splitlines() if ln.strip()]


def categorize(old: str, new: str) -> list[str]:
    """Cheap rule-based labels for what an edit changed, in fixed order:
    numbers, links, opening, sign_off, shortened, lengthened; ['wording']
    when none apply. opening/sign_off only apply to multi-line drafts — a
    one-line SMS has no separate greeting or sign-off."""
    labels: list[str] = []
    if _numbers(old) != _numbers(new):
        labels.append("numbers")
    if set(_URL_RE.findall(old or "")) != set(_URL_RE.findall(new or "")):
        labels.append("links")

    old_lines, new_lines = _lines(old), _lines(new)
    if len(old_lines) > 1 and len(new_lines) > 1:
        if old_lines[0] != new_lines[0]:
            labels.append("opening")
        if old_lines[-1] != new_lines[-1]:
            labels.append("sign_off")

    old_words = len((old or "").split())
    if old_words:
        length_ratio = len((new or "").split()) / old_words
        if length_ratio <= _SHORTENED_RATIO:
            labels.append("shortened")
        elif length_ratio >= _LENGTHENED_RATIO:
            labels.append("lengthened")

    return labels or ["wording"]


@dataclass(frozen=True)
class EditLogEntry:
    item_id: int
    agent_name: str
    tier: str
    channel: Optional[str]
    original_draft: str
    final_text: str
    diff: str
    change_ratio: float
    categories: list[str]
    material: bool
    revision_count: int
    last_revised_at: Optional[datetime]
    decided_at: Optional[datetime]
    status: str

    def to_dict(self, *, max_text: int = 1000) -> dict[str, Any]:
        return {
            "item_id": self.item_id,
            "agent_name": self.agent_name,
            "tier": self.tier,
            "channel": self.channel,
            "original_draft": _truncate(self.original_draft, max_text),
            "final_text": _truncate(self.final_text, max_text),
            "diff": _truncate(self.diff, max_text),
            "change_ratio": round(self.change_ratio, 4),
            "categories": self.categories,
            "material": self.material,
            "revision_count": self.revision_count,
            "last_revised_at": _iso(self.last_revised_at),
            "decided_at": _iso(self.decided_at),
            "status": self.status,
        }


@dataclass(frozen=True)
class AgentRollup:
    """One (agent, tier) line of the weekly edit log."""
    agent_name: str
    tier: str
    this_week: EditCounts
    prior_4w: Optional[EditCounts]  # None when the agent had no decisions in the prior 4 weeks
    top_categories: list[tuple[str, int]]
    biggest_edit: Optional[EditLogEntry]

    @property
    def rate_this_week(self) -> float:
        return self.this_week.rate

    @property
    def rate_prior_4w(self) -> Optional[float]:
        return self.prior_4w.rate if self.prior_4w else None

    @property
    def over_gate(self) -> bool:
        return self.rate_this_week >= _TIER_B_GATE

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent_name": self.agent_name,
            "tier": self.tier,
            "rate_this_week": round(self.rate_this_week, 4),
            "rate_prior_4w": None if self.rate_prior_4w is None else round(self.rate_prior_4w, 4),
            "n_decided": self.this_week.decided,
            "n_edited": self.this_week.revised,
            "n_material": self.this_week.material,
            "top_categories": [{"category": c, "count": n} for c, n in self.top_categories],
            "biggest_edit": self.biggest_edit.to_dict() if self.biggest_edit else None,
            "over_gate": self.over_gate,
        }


def _truncate(value: str, limit: int) -> str:
    return value if len(value) <= limit else value[: limit - 1] + "…"


def _iso(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value else None


def _entry_from_row(row) -> EditLogEntry:
    original = row["original_draft"] or ""
    final = row["final_content"] or ""
    return EditLogEntry(
        item_id=row["id"],
        agent_name=row["agent_name"],
        tier=row["autonomy_tier_at_send"],
        channel=row["channel"],
        original_draft=original,
        final_text=final,
        diff=word_diff(original, final),
        change_ratio=token_change_ratio(original, final),
        categories=categorize(original, final),
        material=bool(row["material_edit"]),
        revision_count=int(row["revision_count"] or 0),
        last_revised_at=row["last_revised_at"],
        decided_at=row["decided_at"],
        status=row["status"],
    )


def get_edit_log(
    session: Session,
    *,
    window_start: datetime,
    window_end: datetime,
    agent_name: Optional[str] = None,
    limit: Optional[int] = None,
) -> list[EditLogEntry]:
    """Revised rows in the edit-rate population decided in
    [window_start, window_end), newest first. Rows whose original draft was
    never captured are excluded here and counted by count_uncaptured()."""
    params: dict[str, Any] = {"v": FA_MAX_VENTURE, "start": window_start, "end": window_end}
    agent_sql = ""
    if agent_name:
        agent_sql = "AND agent_name = :agent "
        params["agent"] = agent_name
    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT :limit"
        params["limit"] = limit
    rows = session.execute(
        text(
            "SELECT id, agent_name, autonomy_tier_at_send, channel, original_draft, "
            "final_content, material_edit, revision_count, last_revised_at, decided_at, status "
            "FROM relay_approval_queue "
            f"WHERE {EDIT_RATE_POPULATION_SQL} "
            "AND revision_count > 0 AND original_draft IS NOT NULL "
            "AND decided_at >= :start AND decided_at < :end "
            f"{agent_sql}"
            f"ORDER BY decided_at DESC, id DESC {limit_sql}"
        ),
        params,
    ).mappings().all()
    return [_entry_from_row(r) for r in rows]


def count_uncaptured(session: Session, *, window_start: datetime, window_end: datetime) -> int:
    """Revised rows in the window whose original draft was never captured
    (rows revised before the WP-T3-2 capture fix)."""
    value = session.execute(
        text(
            "SELECT COUNT(*) FROM relay_approval_queue "
            f"WHERE {EDIT_RATE_POPULATION_SQL} "
            "AND revision_count > 0 AND original_draft IS NULL "
            "AND decided_at >= :start AND decided_at < :end"
        ),
        {"v": FA_MAX_VENTURE, "start": window_start, "end": window_end},
    ).scalar()
    return int(value or 0)


def build_rollup(session: Session, *, now: Optional[datetime] = None) -> list[AgentRollup]:
    """One AgentRollup per (agent, tier) with human decisions in the ISO week
    containing `now`. Three queries total, regardless of agent count."""
    now = now or datetime.now(timezone.utc)
    week_start, week_end = iso_week_bounds(now)
    prior_start, _ = iso_week_bounds(now, weeks_back=PRIOR_WEEKS)

    this_week = get_edit_counts_by_pair(session, window_start=week_start, window_end=week_end)
    prior = get_edit_counts_by_pair(session, window_start=prior_start, window_end=week_start)

    entries_by_pair: dict[tuple[str, str], list[EditLogEntry]] = defaultdict(list)
    for entry in get_edit_log(session, window_start=week_start, window_end=week_end):
        entries_by_pair[(entry.agent_name, entry.tier)].append(entry)

    rollups: list[AgentRollup] = []
    for pair, counts in this_week.items():
        entries = entries_by_pair.get(pair, [])
        categories = Counter(c for e in entries for c in e.categories)
        rollups.append(AgentRollup(
            agent_name=pair[0],
            tier=pair[1],
            this_week=counts,
            prior_4w=prior.get(pair),
            top_categories=categories.most_common(_TOP_CATEGORY_COUNT),
            biggest_edit=max(entries, key=lambda e: e.change_ratio, default=None),
        ))
    return rollups
