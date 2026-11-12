"""P1.4 Standing Sends Tracker: card edits, approval counts per template version, and graduation to auto-send.

* **Card edits.** Every Revise on an approval card is stored in ``lending.card_edits`` as before and after states
  plus a line diff (``record_card_edit``, called from Cora's revise handler). Only the message fields are copied
  (subject, body, stage, tag, title), never the recipient.
* **Tracker.** ``template_stats`` counts, per template version (agent-job drafts carry one), the cards Josh approved,
  how many of those he edited first, the ones he rejected, and the ones sent automatically after graduation. The
  counts are read live from ``lending.pending_actions``, so they cannot drift or double-count.
* **Graduation.** Cora's minute sweep (``evaluate_graduations``) graduates a template the first time it has 25
  approvals with under 10% edited (``config/lending_standing_sends.py``), records the evidence in
  ``lending.template_graduations`` and raises it in Cora's inbox (posted in #cora). From then on the P1.1 job worker
  approves that template's jobs that do not require approval as ``auto:graduated`` (``is_graduated``); the relay
  still runs every send-time check (consent, 8am-8pm window, suppression, caps, lane, emergency halt).
* **Cora's own drafts** carry a template version too (brief section 7: every send carries one): ``CORA-SMS-v1``,
  ``CORA-EMAIL-v1``, ``INSTANTLY-REPLY-v1``. They are counted but never graduate: each is a message Josh asked for or
  an answer to one person's reply, so it always gets a card.
* **Undo.** ``python -m src.lending.standing_sends --revoke <template_version>``; a revoked template needs cards
  again and is never re-graduated automatically (``--restore`` lifts the revoke).

Usage:
    PYTHONPATH=. python -m src.lending.standing_sends --list
    PYTHONPATH=. python -m src.lending.standing_sends --revoke LF-D2-SMS-v1
    PYTHONPATH=. python -m src.lending.standing_sends --restore LF-D2-SMS-v1
"""
from __future__ import annotations

import argparse
import difflib
import json
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.lending_standing_sends import (
    APPROVAL_ONLY_TEMPLATES,
    AUTO_APPROVER_PREFIX,
    AUTO_GRADUATED_ACTOR,
    EDIT_FIELDS,
    GRADUATION_MAX_EDIT_RATE,
    GRADUATION_MIN_APPROVALS,
    MORNING_CARD_LIMIT,
)
from src.lending.cora_inbox import add_to_inbox_safely, slack_safe

logger = logging.getLogger(__name__)

# The sweep's incremental check re-reads this far back from its previous run, and does a full pass this often.
INCREMENTAL_OVERLAP = timedelta(minutes=5)
FULL_PASS_EVERY = timedelta(hours=24)


# ── card edits ───────────────────────────────────────────────────────────────

def _message_fields(payload: Mapping[str, Any]) -> dict[str, str]:
    return {name: str(payload[name]) for name in EDIT_FIELDS if isinstance(payload.get(name), str)}


def field_diff(before: Mapping[str, str], after: Mapping[str, str]) -> str:
    """A unified line diff of every field that changed, one block per field."""
    blocks: list[str] = []
    for name in EDIT_FIELDS:
        old, new = before.get(name, ""), after.get(name, "")
        if old == new:
            continue
        lines = difflib.unified_diff(old.splitlines(), new.splitlines(), fromfile=f"{name} (before)",
                                     tofile=f"{name} (after)", lineterm="")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


_INSERT_EDIT = text("""
    INSERT INTO lending.card_edits (action_id, revision_no, template_version, channel, tool_name, before, after, diff,
                                    instruction, edited_by)
    VALUES (:action_id, :revision_no, :template_version, :channel, :tool_name, CAST(:before AS jsonb),
            CAST(:after AS jsonb), :diff, :instruction, :edited_by)
    ON CONFLICT (action_id, revision_no) DO NOTHING
    RETURNING id
""")


def _edit_rows(action_id: int, tool_name: str, channel: str, payload: Mapping[str, Any],
               revisions: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """One row per revision. Revision i's before is the draft it replaced; its after is the next revision's before,
    or the current draft for the last one."""
    rows = []
    for index, revision in enumerate(revisions):
        before = _message_fields(revision.get("payload") or {})
        after_payload = (revisions[index + 1].get("payload") or {}) if index + 1 < len(revisions) else payload
        after = _message_fields(after_payload)
        rows.append({
            "action_id": action_id, "revision_no": index + 1,
            "template_version": payload.get("template_version") or None, "channel": channel, "tool_name": tool_name,
            "before": json.dumps(before), "after": json.dumps(after), "diff": field_diff(before, after),
            "instruction": revision.get("note"), "edited_by": revision.get("revised_by"),
        })
    return rows


def _insert_edits(engine: Engine, rows: list[dict[str, Any]]) -> int:
    added = 0
    with engine.begin() as conn:
        for row in rows:
            if conn.execute(_INSERT_EDIT, row).scalar() is not None:
                added += 1
    return added


def record_card_edit(engine: Engine, action: Any) -> int:
    """Store every Revise of ``action`` (a ``PendingAction`` read after the revise) not stored yet, so an edit missed
    by a crash or a failed write is filled in by the next one. Returns how many rows were added. Raises on a
    database error."""
    if action is None or not action.revisions:
        return 0
    rows = _edit_rows(action.action_id, action.tool_name, action.channel, action.payload, list(action.revisions))
    added = _insert_edits(engine, rows)
    if added:
        logger.info("[standing-sends] %s card edit(s) recorded action=%s template=%s", added, action.action_id,
                    rows[0]["template_version"] or "-")
    return added


# Drafts whose latest revision has no card_edits row yet: a crash or a failed write right after a Revise.
_MISSING_EDITS = text("""
    SELECT p.action_id, p.tool_name, p.channel, p.payload, p.revisions
      FROM lending.pending_actions p
     WHERE (CAST(:since AS timestamptz) IS NULL OR p.updated_at >= :since)
       AND jsonb_array_length(COALESCE(p.revisions, '[]'::jsonb)) > 0
       AND NOT EXISTS (SELECT 1 FROM lending.card_edits e
                        WHERE e.action_id = p.action_id AND e.revision_no = jsonb_array_length(p.revisions))
     ORDER BY p.updated_at DESC
     LIMIT :limit
""")


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def backfill_card_edits(engine: Engine, limit: int = 50, since: Optional[Any] = None) -> int:
    """Cora's sweep: record edits the revise handler missed (only drafts changed since ``since`` when given). Returns
    how many rows were added."""
    with engine.connect() as conn:
        missing = [dict(row) for row in conn.execute(_MISSING_EDITS, {"limit": limit, "since": since}).mappings()]
    added = sum(_insert_edits(engine, _edit_rows(row["action_id"], row["tool_name"], row["channel"],
                                                 _json(row["payload"]) or {}, _json(row["revisions"]) or []))
                for row in missing)
    if added:
        logger.warning("[standing-sends] backfilled %s card edit(s) the revise handler missed", added)
    return added


def make_edit_recorder(engine: Engine, get_action: Callable[[int], Any]) -> Callable[[int], None]:
    """The revise handler's hook: records the edit and never raises (a failed write must not undo the revise)."""

    def record(action_id: int) -> None:
        try:
            record_card_edit(engine, get_action(action_id))
        except Exception as exc:  # class only: SQL errors embed the message text
            logger.error("[standing-sends] card edit for action %s not recorded (%s)", action_id, type(exc).__name__)

    return record


# ── tracker ──────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class TemplateStats:
    template_version: str
    approved: int
    edited: int
    rejected: int
    auto_sent: int
    graduated: bool = False
    revoked: bool = False

    @property
    def edit_rate(self) -> float:
        return self.edited / self.approved if self.approved else 0.0

    @property
    def approval_only(self) -> bool:
        """Cora's own drafts (brief section 7 template versions): tracked, never graduated."""
        return self.template_version in APPROVAL_ONLY_TEMPLATES

    @property
    def meets_rule(self) -> bool:
        return (not self.approval_only and self.approved >= GRADUATION_MIN_APPROVALS
                and self.edit_rate < GRADUATION_MAX_EDIT_RATE)

    @property
    def state(self) -> str:
        if self.approval_only:
            return "approval_only"
        if self.revoked:
            return "revoked"
        if self.graduated:
            return "graduated"
        return "ready" if self.meets_rule else "collecting"


# Approved by a person: decided by someone who is not an automatic approver, and not rejected. Sent, failed, blocked
# at send time and approved-then-expired all count: Josh approved the wording.
_STATS = text("""
    WITH templated AS (
        SELECT payload->>'template_version' AS template_version, status, decided_by, revisions
          FROM lending.pending_actions
         WHERE COALESCE(payload->>'template_version', '') <> ''
           AND (CAST(:versions AS text[]) IS NULL OR payload->>'template_version' = ANY(CAST(:versions AS text[])))
    ), counted AS (
        SELECT template_version,
               count(*) FILTER (WHERE decided_by IS NOT NULL AND decided_by NOT LIKE :auto_prefix
                                  AND status <> 'rejected') AS approved,
               count(*) FILTER (WHERE decided_by IS NOT NULL AND decided_by NOT LIKE :auto_prefix
                                  AND status <> 'rejected'
                                  AND jsonb_array_length(COALESCE(revisions, '[]'::jsonb)) > 0) AS edited,
               count(*) FILTER (WHERE status = 'rejected') AS rejected,
               count(*) FILTER (WHERE decided_by = :auto_actor) AS auto_sent
          FROM templated GROUP BY template_version
    )
    SELECT c.*, g.template_version IS NOT NULL AS graduated, g.revoked_at IS NOT NULL AS revoked
      FROM counted c LEFT JOIN lending.template_graduations g USING (template_version)
     ORDER BY (g.template_version IS NOT NULL AND g.revoked_at IS NULL) DESC, c.approved DESC, c.template_version
""")


def template_stats(conn: Any, versions: Optional[list[str]] = None) -> list[TemplateStats]:
    """Counts per template version: every one, or only ``versions``."""
    rows = conn.execute(_STATS, {"auto_actor": AUTO_GRADUATED_ACTOR, "auto_prefix": f"{AUTO_APPROVER_PREFIX}%",
                                 "versions": versions}).mappings()
    return [TemplateStats(template_version=row["template_version"], approved=int(row["approved"]),
                          edited=int(row["edited"]), rejected=int(row["rejected"]), auto_sent=int(row["auto_sent"]),
                          graduated=bool(row["graduated"]), revoked=bool(row["revoked"])) for row in rows]


def tracker_line(stats: TemplateStats) -> str:
    label = slack_safe(stats.template_version)
    counts = (f"{stats.approved}/{GRADUATION_MIN_APPROVALS} approvals · {stats.edit_rate:.0%} edited"
              + (f" · {stats.rejected} rejected" if stats.rejected else ""))
    if stats.state == "approval_only":
        return f"• {label}: drafted by Cora, always needs approval · {stats.approved} approved · {stats.edit_rate:.0%} edited" + (
            f" · {stats.rejected} rejected" if stats.rejected else "")
    if stats.state == "graduated":
        return f"• :white_check_mark: *{label}*: sending on its own ({stats.auto_sent} approved automatically) · {counts}"
    if stats.state == "revoked":
        return f"• :no_entry_sign: *{label}*: graduation revoked, needs approval · {counts}"
    if stats.state == "ready":
        return f"• :tada: *{label}*: ready, graduates on the next check · {counts}"
    return f"• {label}: {counts}"


def morning_card_lines(stats: list[TemplateStats]) -> list[str]:
    lines = [tracker_line(s) for s in stats[:MORNING_CARD_LIMIT]]
    if len(stats) > MORNING_CARD_LIMIT:
        lines.append(f"…and {len(stats) - MORNING_CARD_LIMIT} more message types")
    return lines


# ── graduation ───────────────────────────────────────────────────────────────

_GRADUATE = text("""
    INSERT INTO lending.template_graduations (template_version, approvals, edited, rejected, edit_rate)
    VALUES (:template_version, :approvals, :edited, :rejected, :edit_rate)
    ON CONFLICT (template_version) DO NOTHING
    RETURNING graduated_at
""")


# Template versions with a decision (approve / reject) since a moment: the only ones whose counts can have changed.
_DECIDED_SINCE = text("""
    SELECT DISTINCT payload->>'template_version' AS template_version
      FROM lending.pending_actions
     WHERE decided_at >= :since AND COALESCE(payload->>'template_version', '') <> ''
""")


def evaluate_graduations(engine: Engine, since: Optional[Any] = None) -> list[TemplateStats]:
    """Graduate every template that meets the rule and has never graduated (a revoked one stays revoked). With
    ``since``, only templates with a decision since then are counted (the sweep's incremental check); without it,
    all of them. Each new graduation is raised in Cora's inbox in the same transaction. Returns the newly graduated
    templates."""
    graduated: list[TemplateStats] = []
    with engine.begin() as conn:
        versions = None
        if since is not None:
            versions = [row[0] for row in conn.execute(_DECIDED_SINCE, {"since": since})]
            if not versions:
                return graduated
        for stats in template_stats(conn, versions):
            if stats.graduated or not stats.meets_rule:
                continue
            new = conn.execute(_GRADUATE, {"template_version": stats.template_version, "approvals": stats.approved,
                                           "edited": stats.edited, "rejected": stats.rejected,
                                           "edit_rate": round(stats.edit_rate, 4)}).scalar()
            if new is None:
                continue
            graduated.append(stats)
            add_to_inbox_safely(
                conn, event_type="template_graduated", dedup_key=stats.template_version[:200],
                summary=(f"{stats.template_version} graduated to standing auto-send: {stats.approved} approvals, "
                         f"{stats.edit_rate:.0%} edited. Agent jobs of this type that don't require approval now send "
                         "without a card (every send check still runs)."),
                payload={"template_version": stats.template_version, "approvals": stats.approved,
                         "edited": stats.edited, "rejected": stats.rejected, "edit_rate": round(stats.edit_rate, 4)},
            )
            logger.info("[standing-sends] %s graduated approvals=%s edited=%s", stats.template_version,
                        stats.approved, stats.edited)
    return graduated


class StandingSendsStep:
    """Cora's minute sweep step. Each run looks only at drafts decided or revised since the previous run (with an
    overlap, so a slow commit is not missed); a full pass runs on the first call after a start and then once a day,
    as a safety net. Returns the newly graduated templates."""

    def __init__(self, engine: Engine, clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> None:
        self._engine = engine
        self._clock = clock
        self._last_run: Optional[datetime] = None
        self._last_full: Optional[datetime] = None

    def __call__(self) -> list[TemplateStats]:
        now = self._clock()
        full = self._last_full is None or now - self._last_full >= FULL_PASS_EVERY
        since = None if full else self._last_run - INCREMENTAL_OVERLAP
        backfill_card_edits(self._engine, since=since)
        graduated = evaluate_graduations(self._engine, since=since)
        self._last_run = now
        if full:
            self._last_full = now
        return graduated


_IS_GRADUATED = text("""
    SELECT EXISTS (SELECT 1 FROM lending.template_graduations
                    WHERE template_version = :template_version AND revoked_at IS NULL)
""")


def make_is_graduated(engine: Engine) -> Callable[[str], bool]:
    """``JobQueueWorker``'s graduation check. Fails closed: an unreadable answer means not graduated (a card)."""

    def is_graduated(template_version: str) -> bool:
        if not template_version:
            return False
        try:
            with engine.connect() as conn:
                return bool(conn.execute(_IS_GRADUATED, {"template_version": template_version}).scalar())
        except Exception as exc:  # class only
            logger.error("[standing-sends] graduation check failed (%s); treating %s as not graduated",
                         type(exc).__name__, template_version)
            return False

    return is_graduated


_REVOKE = text("""
    UPDATE lending.template_graduations SET revoked_at = now(), revoked_by = :by
     WHERE template_version = :template_version AND revoked_at IS NULL
""")
_RESTORE = text("""
    UPDATE lending.template_graduations SET revoked_at = NULL, revoked_by = NULL
     WHERE template_version = :template_version AND revoked_at IS NOT NULL
""")


def set_revoked(engine: Engine, template_version: str, *, revoked: bool, by: str) -> bool:
    with engine.begin() as conn:
        statement, params = (_REVOKE, {"template_version": template_version, "by": by[:64]}) if revoked else (
            _RESTORE, {"template_version": template_version})
        return conn.execute(statement, params).rowcount == 1


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Standing Sends Tracker (P1.4)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--list", action="store_true", help="print approvals, edit rate and state per template")
    group.add_argument("--revoke", metavar="TEMPLATE_VERSION", help="stop auto-sending a graduated template")
    group.add_argument("--restore", metavar="TEMPLATE_VERSION", help="lift a revoke")
    parser.add_argument("--by", default="cli", help="who is revoking (stored with the revoke)")
    args = parser.parse_args(argv)
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    from config.settings import get_settings

    engine = create_engine(get_settings().database_url, pool_pre_ping=True)
    if args.list:
        with engine.connect() as conn:
            stats = template_stats(conn)
        if not stats:
            logger.info("No agent message types yet.")
        for row in stats:
            logger.info("%s: %s | approved %s | edited %s (%.0f%%) | rejected %s | approved automatically %s",
                        row.template_version, row.state, row.approved, row.edited, row.edit_rate * 100, row.rejected,
                        row.auto_sent)
        return 0
    template, revoke = (args.revoke, True) if args.revoke else (args.restore, False)
    changed = set_revoked(engine, template, revoked=revoke, by=args.by)
    if changed:
        logger.info("%s: %s", template, "revoked" if revoke else "restored")
    else:
        logger.warning("%s: no change (not graduated, or already %s)", template, "revoked" if revoke else "active")
    return 0 if changed else 1


if __name__ == "__main__":
    raise SystemExit(main())
