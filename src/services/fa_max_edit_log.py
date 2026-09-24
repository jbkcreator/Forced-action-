"""FA Max Weekly Edit Log (WP-T3-2).

Provides pure-function helpers (token_change_ratio, is_material_edit,
word_diff, categorize) and DB-backed queries (get_edit_log,
count_uncaptured, build_rollup) that produce the per-agent edit log for
the Friday weekly report and the Command Center get_edit_log tool.

Design (D1): the log is a derived view over relay_approval_queue. No new
table. The diff and categories are computed on read. The edit-rate number
comes from fa_max_autonomy.get_weekly_edit_rate — the same function that
gates Tier B graduation (D5).

token_change_ratio / is_material_edit are moved here from
admin_router._is_material_edit so the threshold has exactly one definition.
admin_router now delegates to is_material_edit.

All DB access uses sqlalchemy.text() per project convention.
"""
from __future__ import annotations

import difflib
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

MATERIAL_EDIT_THRESHOLD: float = 0.15
"""Symmetric token diff / union above which an edit is "material".
Matches the WP-T2-2 gate threshold. Single source of truth."""

_SHORTENED_THRESHOLD: float = 0.80   # final word count ≤ 80% of draft
_LENGTHENED_THRESHOLD: float = 1.20  # final word count ≥ 120% of draft

_URL_RE = re.compile(r"https?://\S+")
_NUMBER_TOKEN_RE = re.compile(r"[\d$%,]+")


# ─────────────────────────────────────────────────────────────────────────────
# Pure functions (no DB)
# ─────────────────────────────────────────────────────────────────────────────

def _word_tokens(text_: str) -> set[str]:
    return set(re.findall(r"\w+", (text_ or "").lower()))


def token_change_ratio(old: str, new: str) -> float:
    """Fraction of the union of lowercased \\w+ tokens that differ (symmetric
    difference / union). Returns 0.0 when both texts are empty."""
    old_t = _word_tokens(old)
    new_t = _word_tokens(new)
    union = old_t | new_t
    if not union:
        return 0.0
    diff = old_t.symmetric_difference(new_t)
    return len(diff) / len(union)


def is_material_edit(old: str, new: str) -> bool:
    """True when token_change_ratio > MATERIAL_EDIT_THRESHOLD (0.15).

    This is the single definition of "material edit" for WP-T3-2.
    admin_router._is_material_edit delegates here. The Tier B gate reads
    the stored material_edit flag, which is always set with this function.
    """
    return token_change_ratio(old, new) > MATERIAL_EDIT_THRESHOLD


def word_diff(old: str, new: str) -> str:
    """Word-level diff rendered as [-removed-]{+added+} inline.

    Uses difflib.SequenceMatcher on whitespace-split word lists so the output
    is human-readable in a Slack message.
    """
    old_words = (old or "").split()
    new_words = (new or "").split()
    matcher = difflib.SequenceMatcher(None, old_words, new_words, autojunk=False)
    parts: list[str] = []
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            parts.extend(old_words[i1:i2])
        elif tag == "delete":
            for w in old_words[i1:i2]:
                parts.append(f"[-{w}-]")
        elif tag == "insert":
            for w in new_words[j1:j2]:
                parts.append(f"{{+{w}+}}")
        elif tag == "replace":
            for w in old_words[i1:i2]:
                parts.append(f"[-{w}-]")
            for w in new_words[j1:j2]:
                parts.append(f"{{+{w}+}}")
    return " ".join(parts)


def categorize(old: str, new: str) -> list[str]:
    """Cheap rule-based labels for an edit. Order is fixed: numbers, links,
    opening, sign_off, shortened, lengthened. Returns ['wording'] when no
    other rule matches.

    Rules (D4):
    - numbers: set of number-like tokens (digits, $, %) differs
    - links: set of URLs differs
    - opening: first non-empty line differs
    - sign_off: last non-empty line differs
    - shortened: final word count ≤ 80% of draft
    - lengthened: final word count ≥ 120% of draft
    """
    labels: list[str] = []

    # numbers
    old_nums = set(_NUMBER_TOKEN_RE.findall(old or ""))
    new_nums = set(_NUMBER_TOKEN_RE.findall(new or ""))
    if old_nums != new_nums:
        labels.append("numbers")

    # links
    old_links = set(_URL_RE.findall(old or ""))
    new_links = set(_URL_RE.findall(new or ""))
    if old_links != new_links:
        labels.append("links")

    # opening / sign_off
    old_lines = [ln for ln in (old or "").splitlines() if ln.strip()]
    new_lines = [ln for ln in (new or "").splitlines() if ln.strip()]

    if old_lines or new_lines:
        old_first = old_lines[0] if old_lines else ""
        new_first = new_lines[0] if new_lines else ""
        if old_first != new_first:
            labels.append("opening")

        old_last = old_lines[-1] if old_lines else ""
        new_last = new_lines[-1] if new_lines else ""
        if old_last != new_last:
            labels.append("sign_off")

    # length
    old_wc = len((old or "").split())
    new_wc = len((new or "").split())
    if old_wc > 0:
        ratio = new_wc / old_wc
        if ratio <= _SHORTENED_THRESHOLD:
            labels.append("shortened")
        elif ratio >= _LENGTHENED_THRESHOLD:
            labels.append("lengthened")

    return labels if labels else ["wording"]


# ─────────────────────────────────────────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
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


@dataclass
class AgentRollup:
    agent_name: str
    tier: str
    rate_this_week: float
    rate_prior_4w: float          # pooled rate over the 4 preceding weeks; 0.0 if no data
    rate_prior_4w_has_data: bool  # False when prior-4w population is empty
    n_decided: int
    n_edited: int                 # any revision_count > 0
    n_material: int
    top_categories: list[str]     # up to 3
    biggest_edit: Optional[EditLogEntry]
    over_gate: bool


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

# The shared population SQL fragment — must match fa_max_autonomy._EDIT_RATE_POPULATION_SQL
# when that constant is extracted (D5). Defined here to avoid circular import
# until the autonomy module is refactored in the same PR.
_POPULATION_WHERE = (
    "venture_key = 'fa_max_lending' "
    "AND decided_at IS NOT NULL "
    "AND decided_by IS NOT NULL "
    "AND decided_by NOT LIKE 'system:autonomous:%' "
    "AND status IN ('approved', 'sent', 'failed', 'uncertain', 'skipped') "
    "AND agent_name IS NOT NULL "
    "AND autonomy_tier_at_send IS NOT NULL"
)


def _iso_week_start_utc(now: Optional[datetime] = None) -> datetime:
    """Monday 00:00 America/New_York as UTC, delegating to autonomy helper."""
    from src.services.fa_max_autonomy import _iso_week_start
    return _iso_week_start(now)


def _row_to_entry(row) -> EditLogEntry:
    orig = row["original_draft"] or ""
    final = row["final_content"] or ""
    return EditLogEntry(
        item_id=row["id"],
        agent_name=row["agent_name"],
        tier=row["autonomy_tier_at_send"],
        channel=row.get("channel"),
        original_draft=orig,
        final_text=final,
        diff=word_diff(orig, final),
        change_ratio=token_change_ratio(orig, final),
        categories=categorize(orig, final),
        material=bool(row["material_edit"]),
        revision_count=int(row["revision_count"] or 0),
        last_revised_at=row.get("last_revised_at"),
        decided_at=row.get("decided_at"),
        status=row["status"],
    )


def get_edit_log(
    session: Session,
    *,
    week_start: datetime,
    week_end: datetime,
    agent_name: Optional[str] = None,
    limit: Optional[int] = None,
) -> list[EditLogEntry]:
    """Return edited rows in the edit-rate population for the given window.

    Only rows with revision_count > 0 AND original_draft IS NOT NULL are
    included. Rows where original_draft was not captured (pre-fix rows from
    raw-INSERT paths) are counted separately by count_uncaptured().
    """
    agent_clause = "AND agent_name = :agent" if agent_name else ""
    limit_clause = "LIMIT :limit" if limit is not None else ""
    sql = text(
        f"SELECT id, agent_name, autonomy_tier_at_send, channel, "
        f"original_draft, final_content, material_edit, revision_count, "
        f"last_revised_at, decided_at, status "
        f"FROM relay_approval_queue "
        f"WHERE {_POPULATION_WHERE} "
        f"AND revision_count > 0 "
        f"AND original_draft IS NOT NULL "
        f"AND decided_at >= :week_start AND decided_at < :week_end "
        f"{agent_clause} "
        f"ORDER BY decided_at DESC "
        f"{limit_clause}"
    )
    params: dict = {"week_start": week_start, "week_end": week_end}
    if agent_name:
        params["agent"] = agent_name
    if limit is not None:
        params["limit"] = limit

    rows = session.execute(sql, params).mappings().all()
    return [_row_to_entry(r) for r in rows]


def count_uncaptured(session: Session, *, week_start: datetime, week_end: datetime) -> int:
    """Count revised rows whose original_draft was not captured (pre-fix rows).

    These are excluded from get_edit_log and shown as a footer note in the report.
    """
    row = session.execute(
        text(
            f"SELECT COUNT(*) FROM relay_approval_queue "
            f"WHERE {_POPULATION_WHERE} "
            f"AND revision_count > 0 "
            f"AND original_draft IS NULL "
            f"AND decided_at >= :week_start AND decided_at < :week_end"
        ),
        {"week_start": week_start, "week_end": week_end},
    ).scalar()
    return int(row or 0)


def build_rollup(
    session: Session,
    *,
    weeks_back: int = 4,
    now: Optional[datetime] = None,
) -> list[AgentRollup]:
    """Return one AgentRollup per (agent, tier) with human decisions this week.

    week_start is Monday 00:00 America/New_York, converted to UTC.
    rate_this_week uses fa_max_autonomy.get_weekly_edit_rate (same SQL as gate).
    rate_prior_4w is a single pooled rate over the preceding 4 ISO weeks.
    """
    from src.services.fa_max_autonomy import get_weekly_edit_rate, FA_MAX_AUTONOMY_POLICY
    from zoneinfo import ZoneInfo

    eastern = ZoneInfo("America/New_York")
    if now is None:
        now = datetime.now(timezone.utc)
    now_eastern = now.astimezone(eastern)
    week_start_eastern = (now_eastern - timedelta(days=now_eastern.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    week_start = week_start_eastern.astimezone(timezone.utc)
    week_end = week_start + timedelta(days=7)

    prior_start = week_start - timedelta(weeks=weeks_back)

    gate_threshold = FA_MAX_AUTONOMY_POLICY["B"]["max_edit_rate_exclusive"]

    # Discover active (agent, tier) pairs this week
    pairs_rows = session.execute(
        text(
            f"SELECT DISTINCT agent_name, autonomy_tier_at_send "
            f"FROM relay_approval_queue "
            f"WHERE {_POPULATION_WHERE} "
            f"AND decided_at >= :week_start AND decided_at < :week_end "
            f"ORDER BY agent_name, autonomy_tier_at_send"
        ),
        {"week_start": week_start, "week_end": week_end},
    ).all()

    rollups: list[AgentRollup] = []
    for pair in pairs_rows:
        agent = pair.agent_name
        tier = pair.autonomy_tier_at_send

        # This week
        rate_this_week = get_weekly_edit_rate(agent, tier, session)

        # Prior 4 weeks (pooled)
        prior_row = session.execute(
            text(
                f"SELECT "
                f"  COUNT(*) FILTER (WHERE material_edit IS TRUE) AS edited, "
                f"  COUNT(*) AS total "
                f"FROM relay_approval_queue "
                f"WHERE {_POPULATION_WHERE} "
                f"AND agent_name = :a AND autonomy_tier_at_send = :tier "
                f"AND decided_at >= :prior_start AND decided_at < :week_start"
            ),
            {"a": agent, "tier": tier, "prior_start": prior_start, "week_start": week_start},
        ).mappings().first()

        if prior_row and prior_row["total"]:
            rate_prior_4w = prior_row["edited"] / prior_row["total"]
            has_prior = True
        else:
            rate_prior_4w = 0.0
            has_prior = False

        # Counts
        counts_row = session.execute(
            text(
                f"SELECT "
                f"  COUNT(*) AS n_decided, "
                f"  COUNT(*) FILTER (WHERE revision_count > 0) AS n_edited, "
                f"  COUNT(*) FILTER (WHERE material_edit IS TRUE) AS n_material "
                f"FROM relay_approval_queue "
                f"WHERE {_POPULATION_WHERE} "
                f"AND agent_name = :a AND autonomy_tier_at_send = :tier "
                f"AND decided_at >= :week_start AND decided_at < :week_end"
            ),
            {"a": agent, "tier": tier, "week_start": week_start, "week_end": week_end},
        ).mappings().first()

        n_decided = int((counts_row and counts_row["n_decided"]) or 0)
        n_edited = int((counts_row and counts_row["n_edited"]) or 0)
        n_material = int((counts_row and counts_row["n_material"]) or 0)

        # Edit log entries for categories + biggest edit
        entries = get_edit_log(session, week_start=week_start, week_end=week_end,
                               agent_name=agent)

        # Top categories (up to 3, by frequency)
        cat_counts: dict[str, int] = {}
        for e in entries:
            for c in e.categories:
                cat_counts[c] = cat_counts.get(c, 0) + 1
        top_categories = sorted(cat_counts, key=lambda c: -cat_counts[c])[:3]

        biggest = max(entries, key=lambda e: e.change_ratio, default=None)

        rollups.append(AgentRollup(
            agent_name=agent,
            tier=tier,
            rate_this_week=rate_this_week,
            rate_prior_4w=rate_prior_4w,
            rate_prior_4w_has_data=has_prior,
            n_decided=n_decided,
            n_edited=n_edited,
            n_material=n_material,
            top_categories=top_categories,
            biggest_edit=biggest,
            over_gate=rate_this_week >= gate_threshold,
        ))

    return rollups
