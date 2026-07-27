"""
Cora's business-record store — interim, file-based, append-only.

No new Postgres tables/migrations are permitted on this branch (a separate,
unreviewed rename branch touches src/core/models.py at 148 scattered lines;
adding a table here risks a real merge conflict later). This store is the
documented stand-in for the eventual OutboundDraft/Reply/PreCallBrief tables
— see docs/plans/cora_v2_2_interim_build_decisions.md for the full rationale.

Design, deliberately mirroring existing patterns rather than inventing new
ones:
  - Append-only JSON Lines, one record per line, under data/cora/ (already
    gitignored — see .gitignore's `data/` entry — so this never enters git
    history regardless of the rename-branch situation).
  - "Last line for a given id wins" — a status transition is a *new*
    appended line carrying the same id, never an in-place rewrite. Safe
    against partial writes; maps cleanly onto a future UPDATE statement.
  - Staleness is computed at READ time from a fixed max-age constant, never
    a persisted expires_at column — exact shape of
    src.agents.vera.facts.is_stale(freshness_class, observed_at). Cora never
    writes to vera_facts; this is Cora's own, separate constant.
  - Concurrency: a single in-process threading.Lock per file. NOT race-safe
    across multiple processes/replicas — the best available substitute
    given a real DB unique constraint (the insert-then-catch-IntegrityError
    pattern used elsewhere, e.g. src/services/owner_alert.py:_claim_alert)
    isn't available without a migration. Documented limitation, closeable
    once a real table exists.
"""
from __future__ import annotations

import json
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

DATA_DIR = Path(__file__).resolve().parents[3] / "data" / "cora"

DRAFT_MAX_AGE_HOURS = 72
CORA_FACT_MAX_AGE_HOURS: Dict[str, int] = {
    # freshness_class -> max age before a fact_used entry is considered stale.
    # Distinct from src.agents.vera.config.FRESHNESS_MAX_AGE_HOURS — these
    # gate BuyerEntity snapshot fields Cora reads, not VeraFact rows.
    "whale_snapshot": 24 * 14,   # BuyerEntity.confidence_score / is_whale etc — 14 days
    "auction_event": 24 * 3,     # a specific auction win/loss fact — 3 days
    "generic": 24 * 30,          # anything without a declared class — 30 days
}

DraftStatus = Literal["draft", "rejected", "expired", "superseded", "approved_pending_send"]
OpportunityStatus = Literal["targeted", "touched", "replied", "call", "proposal", "closed"]
ReplyStatus = Literal["pending_approval", "manual_review", "suppressed"]

_locks: Dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _lock_for(path: Path) -> threading.Lock:
    key = str(path)
    with _locks_guard:
        if key not in _locks:
            _locks[key] = threading.Lock()
        return _locks[key]


def _ensure_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _append_line(path: Path, record: Dict[str, Any]) -> None:
    _ensure_dir()
    lock = _lock_for(path)
    with lock:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str) + "\n")


def _read_latest_by_id(path: Path, id_field: str) -> Dict[str, Dict[str, Any]]:
    """Read every line, reduce to the latest record per id_field value."""
    if not path.exists():
        return {}
    latest: Dict[str, Dict[str, Any]] = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            rec_id = rec.get(id_field)
            if rec_id is None:
                continue
            latest[rec_id] = rec
    return latest


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(value))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def now() -> datetime:
    return _now()


def parse_dt(value: Any) -> Optional[datetime]:
    return _parse_dt(value)


def is_stale(freshness_class: str, observed_at: Any) -> bool:
    """Mirrors src.agents.vera.facts.is_stale() exactly — computed at read time."""
    max_age_hours = CORA_FACT_MAX_AGE_HOURS.get(freshness_class)
    if max_age_hours is None:
        return False
    observed = _parse_dt(observed_at)
    if observed is None:
        return True  # unparseable timestamp is treated as stale, not fresh
    return _now() - observed > timedelta(hours=max_age_hours)


# ─────────────────────────────────────────────────────────────────────────────
# OutboundDraft
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class FactUsed:
    fact_key: str
    value: str
    source_ref: str
    observed_at: str
    freshness_class: str = "generic"


@dataclass
class OutboundDraftRecord:
    draft_id: str
    opportunity_thread_id: str
    buyer_entity_id: int
    cell_id: str
    offer: str
    avenue: str
    angle: str
    subject: str
    body: str
    facts_used: List[Dict[str, Any]]
    source_refs: List[str]
    recommended_channel: str
    confidence_score: int
    status: DraftStatus = "draft"
    booking_link: Optional[str] = None
    payment_link: Optional[str] = None
    reject_reason: Optional[str] = None
    created_at: str = field(default_factory=lambda: _now().isoformat())
    schema_version: int = 1
    published: bool = False
    is_followup: bool = False
    followup_sequence: Optional[int] = None


_DRAFTS_FILE = DATA_DIR / "outbound_drafts.jsonl"


def append_draft(record: OutboundDraftRecord) -> None:
    _append_line(_DRAFTS_FILE, asdict(record))


def new_draft_id() -> str:
    return str(uuid.uuid4())


def read_drafts(
    opportunity_thread_id: Optional[str] = None,
    cell_id: Optional[str] = None,
    status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    latest = _read_latest_by_id(_DRAFTS_FILE, "draft_id")
    rows = list(latest.values())
    if opportunity_thread_id is not None:
        rows = [r for r in rows if r.get("opportunity_thread_id") == opportunity_thread_id]
    if cell_id is not None:
        rows = [r for r in rows if r.get("cell_id") == cell_id]
    if status is not None:
        rows = [r for r in rows if r.get("status") == status]
    return rows


def is_draft_expired(draft: Dict[str, Any]) -> bool:
    created = _parse_dt(draft.get("created_at"))
    if created is None:
        return True
    return _now() - created > timedelta(hours=DRAFT_MAX_AGE_HOURS)


def read_active_drafts(
    opportunity_thread_id: Optional[str] = None,
    cell_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """'Active' == status in {draft, approved_pending_send} and not expired."""
    rows = read_drafts(opportunity_thread_id=opportunity_thread_id, cell_id=cell_id)
    active = []
    for r in rows:
        if r.get("status") not in ("draft", "approved_pending_send"):
            continue
        if is_draft_expired(r):
            continue
        active.append(r)
    return active


def has_duplicate_actionable_draft(opportunity_thread_id: str, cell_id: str) -> bool:
    return len(read_active_drafts(opportunity_thread_id=opportunity_thread_id, cell_id=cell_id)) > 0


def expire_stale_drafts() -> int:
    """Append an 'expired' transition line for any active draft past DRAFT_MAX_AGE_HOURS."""
    latest = _read_latest_by_id(_DRAFTS_FILE, "draft_id")
    count = 0
    for rec in latest.values():
        if rec.get("status") not in ("draft", "approved_pending_send"):
            continue
        if not is_draft_expired(rec):
            continue
        expired = dict(rec)
        expired["status"] = "expired"
        _append_line(_DRAFTS_FILE, expired)
        count += 1
    return count


def mark_draft_published(draft_id: str) -> None:
    latest = _read_latest_by_id(_DRAFTS_FILE, "draft_id")
    rec = latest.get(draft_id)
    if rec is None:
        return
    updated = dict(rec)
    updated["published"] = True
    _append_line(_DRAFTS_FILE, updated)


# ─────────────────────────────────────────────────────────────────────────────
# Opportunity state machine
# ─────────────────────────────────────────────────────────────────────────────

_OPPORTUNITY_STATE_FILE = DATA_DIR / "opportunity_state.jsonl"

_VALID_TRANSITIONS: Dict[OpportunityStatus, tuple] = {
    # "closed" is reachable from every non-terminal state, not just the
    # later ones — an unsubscribe/hostile reply can close an opportunity
    # that never got past "targeted" or "touched", not only ones that
    # advanced to "replied"/"call"/"proposal" first.
    "targeted": ("touched", "closed"),
    "touched": ("replied", "call", "closed"),
    "replied": ("call", "closed"),
    "call": ("proposal", "closed"),
    "proposal": ("closed",),
    "closed": (),
}


def current_opportunity_status(opportunity_thread_id: str) -> Optional[OpportunityStatus]:
    latest = _read_latest_by_id(_OPPORTUNITY_STATE_FILE, "opportunity_thread_id")
    rec = latest.get(opportunity_thread_id)
    return rec.get("status") if rec else None


def list_opportunities_by_status(status: OpportunityStatus) -> List[str]:
    """All opportunity_thread_ids whose latest transition is `status` — used by followup_scheduler."""
    latest = _read_latest_by_id(_OPPORTUNITY_STATE_FILE, "opportunity_thread_id")
    return [tid for tid, rec in latest.items() if rec.get("status") == status]


def transition_opportunity(
    opportunity_thread_id: str,
    new_status: OpportunityStatus,
    reason: str,
) -> bool:
    """Append a state transition. Returns False (no-op) if the transition is invalid."""
    current = current_opportunity_status(opportunity_thread_id)
    if current is not None and new_status not in _VALID_TRANSITIONS.get(current, ()):
        if current != new_status:
            return False
    _append_line(
        _OPPORTUNITY_STATE_FILE,
        {
            "opportunity_thread_id": opportunity_thread_id,
            "status": new_status,
            "reason": reason,
            "changed_at": _now().isoformat(),
        },
    )
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Replies (C3)
# ─────────────────────────────────────────────────────────────────────────────

_REPLIES_FILE = DATA_DIR / "replies.jsonl"


@dataclass
class ReplyRecord:
    reply_id: str
    opportunity_thread_id: Optional[str]
    from_address: str
    subject: str
    body_text: str
    received_at: str
    intent: Optional[str] = None
    subtype: Optional[str] = None
    response_draft_id: Optional[str] = None
    status: ReplyStatus = "manual_review"
    created_at: str = field(default_factory=lambda: _now().isoformat())
    published: bool = False


def new_reply_id() -> str:
    return str(uuid.uuid4())


def append_reply(record: ReplyRecord) -> None:
    _append_line(_REPLIES_FILE, asdict(record))


def read_replies(opportunity_thread_id: Optional[str] = None) -> List[Dict[str, Any]]:
    latest = _read_latest_by_id(_REPLIES_FILE, "reply_id")
    rows = list(latest.values())
    if opportunity_thread_id is not None:
        rows = [r for r in rows if r.get("opportunity_thread_id") == opportunity_thread_id]
    rows.sort(key=lambda r: r.get("received_at") or "")
    return rows


def read_conversation(opportunity_thread_id: str) -> List[Dict[str, Any]]:
    """Prior drafts + replies for a thread, ordered by time — 'load prior conversation'."""
    drafts = [
        {"kind": "draft", "at": d.get("created_at"), "record": d}
        for d in read_drafts(opportunity_thread_id=opportunity_thread_id)
    ]
    replies = [
        {"kind": "reply", "at": r.get("received_at"), "record": r}
        for r in read_replies(opportunity_thread_id=opportunity_thread_id)
    ]
    combined = drafts + replies
    combined.sort(key=lambda x: x.get("at") or "")
    return combined


# ─────────────────────────────────────────────────────────────────────────────
# Pre-call briefs (C4)
# ─────────────────────────────────────────────────────────────────────────────

_PRE_CALL_BRIEFS_FILE = DATA_DIR / "pre_call_briefs.jsonl"


@dataclass
class PreCallBriefRecord:
    brief_id: str
    opportunity_thread_id: str
    call_booked_at: str
    content: Dict[str, Any]
    created_at: str = field(default_factory=lambda: _now().isoformat())
    published: bool = False


def new_brief_id() -> str:
    return str(uuid.uuid4())


def append_pre_call_brief(record: PreCallBriefRecord) -> None:
    _append_line(_PRE_CALL_BRIEFS_FILE, asdict(record))


def read_pre_call_briefs(opportunity_thread_id: Optional[str] = None) -> List[Dict[str, Any]]:
    latest = _read_latest_by_id(_PRE_CALL_BRIEFS_FILE, "brief_id")
    rows = list(latest.values())
    if opportunity_thread_id is not None:
        rows = [r for r in rows if r.get("opportunity_thread_id") == opportunity_thread_id]
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Test-only reset helper
# ─────────────────────────────────────────────────────────────────────────────

def _reset_store_for_tests() -> None:
    """Deletes all jsonl files under DATA_DIR. Test-harness use only."""
    for f in (_DRAFTS_FILE, _OPPORTUNITY_STATE_FILE, _REPLIES_FILE, _PRE_CALL_BRIEFS_FILE):
        if f.exists():
            f.unlink()
