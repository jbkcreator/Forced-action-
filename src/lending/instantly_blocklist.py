"""P1.3: every opt-out with an email also goes onto NDL's Instantly block list, retried until Instantly accepts it.

``compliance._propagate`` calls :func:`enqueue` for each opted-out address, in the opt-out's own transaction (no HTTP
there). Cora's minute sweep runs :class:`BlockListSweep`, which sends the pending addresses to Instantly's block list
(``instantly_client.block_address``), a few per pass, and keeps trying a failed one with a growing wait until it
succeeds. After ``MAX_ATTEMPTS`` failures the address is marked ``failed`` and an item goes to Cora's inbox (no address
in it), so a person adds it by hand. Rows stay pending while the NDL Instantly account is not configured.

Enqueue never raises: if the table is missing (migration not run) or the write fails, the opt-out itself is still
recorded and the problem is logged. Run ``migrations/apply_lending_instantly_block_queue.py`` BEFORE deploying.
"""
from __future__ import annotations

import hashlib
import logging
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.lending.cora_inbox import add_to_inbox_safely
from src.lending.instantly_client import InstantlyAccount, block_address, lending_instantly_account

logger = logging.getLogger(__name__)

REARM_DONE_AFTER_SECONDS = 86400
BATCH_SIZE = 10  # addresses sent to Instantly per sweep pass
BUDGET_SECONDS = 30.0  # each call can take up to 20 s; no new call starts after this
MAX_ATTEMPTS = 12
BASE_BACKOFF_SECONDS = 60
MAX_BACKOFF_SECONDS = 3600

# DO NOTHING (never DO UPDATE) so an opt-out never waits for a row the sweep holds locked while it calls Instantly.
_ENQUEUE = text("""
    INSERT INTO lending.instantly_block_queue (email) VALUES (:email) ON CONFLICT (email) DO NOTHING
""")
# A failed row, or one done more than REARM_DONE_AFTER ago (someone may have taken the address off Instantly's list since),
# is queued again by a new opt-out. A pending row is untouched, and the WHERE never matches one the sweep has locked.
_REARM = text("""
    UPDATE lending.instantly_block_queue SET status = 'pending', attempts = 0, next_attempt_at = now(), last_error = NULL
     WHERE email = ANY(:emails)
       AND (status = 'failed' OR (status = 'done' AND done_at < now() - make_interval(secs => :done_after)))
""")
_CLAIM = text("""
    SELECT id, email, attempts FROM lending.instantly_block_queue
     WHERE status = 'pending' AND next_attempt_at <= now()
     ORDER BY id LIMIT :limit FOR UPDATE SKIP LOCKED
""")
_DONE = text("UPDATE lending.instantly_block_queue SET status = 'done', done_at = now(), last_error = NULL WHERE id = :id")
_RETRY = text("""
    UPDATE lending.instantly_block_queue SET attempts = attempts + 1, last_error = :error,
           next_attempt_at = now() + make_interval(secs => :wait) WHERE id = :id
""")
_FAIL = text("UPDATE lending.instantly_block_queue SET status = 'failed', attempts = attempts + 1, last_error = :error WHERE id = :id")


def enqueue(db: Any, emails: Iterable[str]) -> int:
    """Queue each address for the block list (idempotent per address). Runs in the caller's transaction, in a
    savepoint, and never raises: returns how many addresses were offered, 0 when the write failed."""
    unique = sorted({e.strip().lower() for e in emails if e and e.strip()})
    if not unique:
        return 0
    try:
        with db.begin_nested():
            db.execute(_ENQUEUE, [{"email": e} for e in unique])
            db.execute(_REARM, {"emails": unique, "done_after": REARM_DONE_AFTER_SECONDS})
    except Exception as exc:  # class only: SQL errors embed the address; the opt-out itself is already recorded
        logger.error("[instantly-blocklist] %d opt-out address(es) not queued for the block list (%s)", len(unique),
                     type(exc).__name__)
        return 0
    return len(unique)


def _wait(attempts: int) -> int:
    return min(BASE_BACKOFF_SECONDS * (2 ** attempts), MAX_BACKOFF_SECONDS)


@dataclass(frozen=True)
class BlockListResult:
    blocked: int = 0
    retrying: int = 0
    failed: int = 0


class BlockListSweep:
    """One pass of Cora's sweep over the pending block-list rows."""

    def __init__(self, engine: Engine, *, account: Callable[[], Optional[InstantlyAccount]] = lending_instantly_account,
                 block: Callable[[InstantlyAccount, str], bool] = block_address,
                 clock: Callable[[], float] = time.monotonic,
                 session_factory: Optional[Callable[[], Any]] = None) -> None:
        self._session_factory = session_factory or (lambda: Session(engine))
        self._account = account
        self._block = block
        self._clock = clock

    def __call__(self) -> BlockListResult:
        account = self._account()
        if account is None:
            return BlockListResult()
        blocked = retrying = failed = 0
        deadline = self._clock() + BUDGET_SECONDS
        with self._session_factory() as db:
            for row in db.execute(_CLAIM, {"limit": BATCH_SIZE}).mappings().all():
                if self._clock() > deadline:
                    break
                try:
                    ok = bool(self._block(account, row["email"]))
                except Exception as exc:  # class only: the request carries the address
                    ok, error = False, type(exc).__name__
                else:
                    error = "Instantly refused or was unreachable"
                if ok:
                    db.execute(_DONE, {"id": row["id"]})
                    blocked += 1
                elif row["attempts"] + 1 >= MAX_ATTEMPTS:
                    db.execute(_FAIL, {"id": row["id"], "error": error})
                    failed += 1
                    self._alert(db, row)
                else:
                    db.execute(_RETRY, {"id": row["id"], "error": error, "wait": _wait(row["attempts"])})
                    retrying += 1
            db.commit()
        if blocked or retrying or failed:
            logger.info("[instantly-blocklist] blocked=%d retrying=%d failed=%d", blocked, retrying, failed)
        return BlockListResult(blocked, retrying, failed)

    @staticmethod
    def _alert(db: Session, row: Any) -> None:
        digest = hashlib.sha256(row["email"].encode()).hexdigest()[:16]
        logger.error("[instantly-blocklist] an opt-out address could not be added to the Instantly block list after "
                     "%d attempts; add it by hand", MAX_ATTEMPTS)
        add_to_inbox_safely(
            db, event_type="instantly_block_failed", dedup_key=f"block:{digest}", target_channel="ops",
            summary=("An opted-out address could not be added to NDL's Instantly block list after "
                     f"{MAX_ATTEMPTS} tries: add it by hand (it is suppressed in our own stores)"),
            payload={"queue_id": row["id"]})
