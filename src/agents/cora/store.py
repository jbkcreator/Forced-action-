"""
Cora's business-record store.

OutboundDraft lives in the real `outbound_drafts` Postgres table
(src/core/models.py, migrations/apply_outbound_draft.py) — status
transitions are plain UPDATEs, draft_id is a real primary key. Reply,
PreCallBrief, and opportunity-state are still the original interim,
file-based, append-only JSON-Lines store; migrating those wasn't asked for
and nothing in the app layer needed their full transition history, so they
stay as-is for now.

Design for the JSON-Lines records (Reply/PreCallBrief/opportunity-state),
deliberately mirroring existing patterns rather than inventing new ones:
  - Append-only JSON Lines, one record per line, under data/cora/ (already
    gitignored — see .gitignore's `data/` entry).
  - "Last line for a given id wins" — a status transition is a *new*
    appended line carrying the same id, never an in-place rewrite. Safe
    against partial writes.
  - Staleness is computed at READ time from a fixed max-age constant, never
    a persisted expires_at column — exact shape of
    src.agents.vera.facts.is_stale(freshness_class, observed_at). Cora never
    writes to vera_facts; this is Cora's own, separate constant.
  - Concurrency: a single in-process threading.Lock per file. NOT race-safe
    across multiple processes/replicas — documented limitation, unrelated to
    OutboundDraft (which uses real DB semantics instead).
"""
from __future__ import annotations

import json
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from config.venture_template import DEFAULT_VENTURE_KEY

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

# "pending_channel_support": approved by the founder, but the draft's channel has
# no registered Relay dispatcher yet (today: sms). Parked off 'draft' so the batch
# builder stops re-selecting it every sweep; retryable once the dispatcher lands.
DraftStatus = Literal[
    "draft", "rejected", "expired", "superseded", "approved_pending_send",
    "pending_channel_support",
]
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
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    # Which venture produced this draft (CLONE-v2.2 / CL4). Defaults to venture
    # #1, so nothing about the existing single-venture path changes.
    #
    # It has to be written here rather than inferred downstream: per-cell and
    # per-venture reply rate is read off this column
    # (src/services/venture_ladder.py:cell_reply_rates), and that number is what
    # the auto-double rule scales real sending volume on. A second venture whose
    # drafts all carried venture #1's key would have a permanently empty reply
    # rate and could never scale.
    venture_key: str = DEFAULT_VENTURE_KEY
    # LEARN-v2.2 Layer 1 — the price fact cited in this draft (if the offer
    # has a configured price band, price_assignment.is_respa_excluded() is
    # False, and PRICE_BAND_TESTING_ENABLED — currently False everywhere,
    # so this is the floor price today) and the AgentLaneExperimentAssignment
    # id it came from, if any. Layer 2's attribution join reads this to
    # connect a later reply/booking/payment back to the arm that produced
    # this draft.
    price_cents: Optional[int] = None
    experiment_assignment_id: Optional[int] = None


def new_draft_id() -> str:
    return str(uuid.uuid4())


def venture_key_for_county(db: Any, county_id: Optional[str]) -> str:
    """The venture `county_id` belongs to, or DEFAULT_VENTURE_KEY if unresolvable.

    Draft-persistence call sites (outreach.py, post_call_recap.py) must call
    this rather than trust OutboundDraftRecord.venture_key's default. Per-cell
    and per-venture reply rate is read off the venture_key column
    (src/services/venture_ladder.py:cell_reply_rates), and a second venture's
    drafts that silently defaulted to venture #1 would have a permanently
    empty reply rate — and could never advance the cell rung or auto-double.
    """
    if not county_id:
        return DEFAULT_VENTURE_KEY
    from sqlalchemy import text

    row = db.execute(
        text("SELECT venture_key FROM counties WHERE county_id = :county_id"),
        {"county_id": county_id},
    ).first()
    return row.venture_key if row and row.venture_key else DEFAULT_VENTURE_KEY


def _draft_row_to_dict(row: Any) -> Dict[str, Any]:
    d = dict(row)
    if d.get("created_at") is not None:
        d["created_at"] = d["created_at"].isoformat() if hasattr(d["created_at"], "isoformat") else d["created_at"]
    return d


_DRAFT_COLUMNS = (
    "draft_id, opportunity_thread_id, buyer_entity_id, cell_id, offer, avenue, angle, "
    "subject, body, facts_used, source_refs, recommended_channel, confidence_score, "
    "status, booking_link, payment_link, reject_reason, created_at, schema_version, "
    "published, is_followup, followup_sequence, contact_email, contact_phone, "
    "venture_key, price_cents, experiment_assignment_id"
)


def append_draft(db: Any, record: OutboundDraftRecord) -> None:
    from sqlalchemy import text
    db.execute(
        text(f"""
            INSERT INTO outbound_drafts ({_DRAFT_COLUMNS})
            VALUES (
                :draft_id, :opportunity_thread_id, :buyer_entity_id, :cell_id, :offer, :avenue, :angle,
                :subject, :body, :facts_used, :source_refs, :recommended_channel, :confidence_score,
                :status, :booking_link, :payment_link, :reject_reason, :created_at, :schema_version,
                :published, :is_followup, :followup_sequence, :contact_email, :contact_phone,
                :venture_key, :price_cents, :experiment_assignment_id
            )
        """),
        {
            **asdict(record),
            "facts_used": json.dumps(record.facts_used),
            "source_refs": json.dumps(record.source_refs),
        },
    )


def read_drafts(
    db: Any,
    opportunity_thread_id: Optional[str] = None,
    cell_id: Optional[str] = None,
    status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    from sqlalchemy import text
    clauses, params = [], {}
    if opportunity_thread_id is not None:
        clauses.append("opportunity_thread_id = :opportunity_thread_id")
        params["opportunity_thread_id"] = opportunity_thread_id
    if cell_id is not None:
        clauses.append("cell_id = :cell_id")
        params["cell_id"] = cell_id
    if status is not None:
        clauses.append("status = :status")
        params["status"] = status
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = db.execute(
        text(f"SELECT {_DRAFT_COLUMNS} FROM outbound_drafts {where} ORDER BY created_at ASC"),
        params,
    ).mappings().all()
    return [_draft_row_to_dict(r) for r in rows]


def is_draft_expired(draft: Dict[str, Any]) -> bool:
    created = _parse_dt(draft.get("created_at"))
    if created is None:
        return True
    return _now() - created > timedelta(hours=DRAFT_MAX_AGE_HOURS)


def read_active_drafts(
    db: Any,
    opportunity_thread_id: Optional[str] = None,
    cell_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """'Active' == status in {draft, approved_pending_send} and not expired."""
    rows = read_drafts(db, opportunity_thread_id=opportunity_thread_id, cell_id=cell_id)
    active = []
    for r in rows:
        if r.get("status") not in ("draft", "approved_pending_send"):
            continue
        if is_draft_expired(r):
            continue
        active.append(r)
    return active


def has_duplicate_actionable_draft(db: Any, opportunity_thread_id: str, cell_id: str) -> bool:
    return len(read_active_drafts(db, opportunity_thread_id=opportunity_thread_id, cell_id=cell_id)) > 0


_EMAIL_INDEX_PREFIX = "cora:email_thread_index:"


def index_contact_email(contact_email: Optional[str], opportunity_thread_id: str) -> None:
    """
    Write-time index: O(1) lookup accelerator for find_opportunity_thread_id_by_email,
    called once per draft at persist time (outreach.py._node_persist). No TTL —
    a contact_email -> opportunity_thread_id mapping doesn't go stale on its own;
    it's a pure performance cache over the JSON-Lines file (the source of truth),
    never the only place this mapping is recorded. A miss here always falls back
    to the file scan, so a cold Redis / a draft written before this index existed
    is still found correctly, just slower.
    """
    if not contact_email:
        return
    from src.core.redis_client import get_redis, redis_available
    if not redis_available():
        return
    get_redis().set(f"{_EMAIL_INDEX_PREFIX}{contact_email.strip().lower()}", opportunity_thread_id)


def find_opportunity_thread_id_by_email(db: Any, contact_email: str) -> Optional[str]:
    """
    Reply-matching lookup: which opportunity_thread_id did we send TO this
    address? Checks the Redis index first (O(1)); on a miss, falls back to
    a DB query (not thread-filtered — the whole point is we don't know the
    thread yet) and returns the most recently created match. Case-insensitive,
    since email addresses are. None if no draft was ever sent to this address
    — the caller (the reply pipeline, or the mailbox poller upstream of it)
    treats that as unmatched.
    """
    if not contact_email:
        return None
    needle = contact_email.strip().lower()

    from src.core.redis_client import get_redis, redis_available
    if redis_available():
        indexed = get_redis().get(f"{_EMAIL_INDEX_PREFIX}{needle}")
        if indexed:
            return indexed

    from sqlalchemy import text
    row = db.execute(
        text(
            "SELECT opportunity_thread_id FROM outbound_drafts "
            "WHERE lower(contact_email) = :needle ORDER BY created_at DESC LIMIT 1"
        ),
        {"needle": needle},
    ).first()
    return row[0] if row else None


def expire_stale_drafts(db: Any) -> int:
    """Marks every active draft past DRAFT_MAX_AGE_HOURS as 'expired'. Returns the count updated."""
    from sqlalchemy import text
    result = db.execute(
        text(
            "UPDATE outbound_drafts SET status = 'expired' "
            "WHERE status IN ('draft', 'approved_pending_send') "
            "AND created_at < now() - make_interval(hours => :max_age_hours)"
        ),
        {"max_age_hours": DRAFT_MAX_AGE_HOURS},
    )
    return result.rowcount


def mark_draft_published(db: Any, draft_id: str) -> None:
    from sqlalchemy import text
    db.execute(
        text("UPDATE outbound_drafts SET published = true WHERE draft_id = :draft_id"),
        {"draft_id": draft_id},
    )


def mark_draft_status(db: Any, draft_id: str, status: DraftStatus, reject_reason: Optional[str] = None) -> None:
    """Additive helper — nothing before THROUGH-v2.2 ever needed to flip a
    draft's status directly (mark_draft_published only toggles the separate
    `published` bool). Used by THROUGH's batch-approval decisions.py to move
    a draft to 'approved_pending_send' on approval or 'rejected' on an
    exception-reject within a batch."""
    from sqlalchemy import text
    db.execute(
        text(
            "UPDATE outbound_drafts SET status = :status, reject_reason = :reject_reason "
            "WHERE draft_id = :draft_id"
        ),
        {"status": status, "reject_reason": reject_reason, "draft_id": draft_id},
    )


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
    response_subject: Optional[str] = None
    response_body: Optional[str] = None
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


def mark_reply_published(reply_id: str) -> None:
    """
    Flips a persisted ReplyRecord's `published` flag to True — used only for a
    BOOKING_REQUEST reply whose call.booked publish failed at persist time and
    was later retried successfully (see reply.py's retry_unpublished_call_booked).
    Append-only + latest-line-wins-by-id (same convention as every other
    JSON-lines record in this store), so this is a full re-append of the
    record with one field changed, not an in-place edit.
    """
    latest = _read_latest_by_id(_REPLIES_FILE, "reply_id")
    record = latest.get(reply_id)
    if record is None:
        return
    record["published"] = True
    _append_line(_REPLIES_FILE, record)


def read_conversation(db: Any, opportunity_thread_id: str) -> List[Dict[str, Any]]:
    """Prior drafts + replies for a thread, ordered by time — 'load prior conversation'."""
    drafts = [
        {"kind": "draft", "at": d.get("created_at"), "record": d}
        for d in read_drafts(db, opportunity_thread_id=opportunity_thread_id)
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
    """Deletes all jsonl files under DATA_DIR. Test-harness use only. Drafts live in Postgres now — cleared via the fresh_db rollback, not here."""
    for f in (_OPPORTUNITY_STATE_FILE, _REPLIES_FILE, _PRE_CALL_BRIEFS_FILE):
        if f.exists():
            f.unlink()
