"""
FA Max Qualification Agent worker (WP-T3-7).

Consumes the 'fa_max_qualification' durable work queue. Each work item carries:
  {opportunity_id, facts_revision, checklist_version}

Worker loop (mirrors FaMaxWorker from worker.py):
  claim_next_work_item(queue_name=FA_MAX_QUAL_QUEUE_NAME)
    -> _process_qualification_item()
       1. Freshness check at claim time: current facts_revision matches item.
       2. evaluate_sufficiency()
       3. Write fa_max_qualification_decisions row (inside evaluate_sufficiency).
       4. Dispatch on verdict:
          a. sufficient  → CAS-commit then enqueue 'fa_max_quote_ready'.
             Transitions opportunity qualifying→scoping on first sufficiency.
             For subsequent sufficiency (already in scoping or later): only
             enqueues the builder work (no state transition).
          b. insufficient / pending_enrichment → route gaps to EXCEPTIONS via
             enqueue_and_attempt(). Cancels the previous gap-hash alert for
             this opportunity when the gap set changes.
    -> complete_work_item(status='done'|'failed')
    -> periodic reclaim sweep for abandoned leases

Freshness guarantee (stale-handoff window):
  The CAS at enqueue_quote_ready_work wraps the enqueue_work_item inside
  a transaction that also re-reads facts_revision (WHERE opportunity_id=:oid
  AND facts_revision = :claimed_revision). A concurrent set_facts() that bumped
  the revision between claim and here produces a revision mismatch; this worker
  skips enqueuing the builder work and instead logs a stale-claim warning.
  The concurrent set_facts() will have enqueued a new qualification recheck
  for the bumped revision, which this or another worker will pick up next.

Autonomy: internal agent — no outbound contact, no tier gate.

Usage:
  python -m src.agents.fa_max.qualification_worker
"""
from __future__ import annotations

import logging
import os
import signal
import socket
import time
import uuid
from typing import Any, Dict, Optional

from sqlalchemy import text
from src.core.database import get_db_context
from src.services.state_engine import (
    claim_next_work_item,
    complete_work_item,
    enqueue_work_item,
    get_opportunity_state,
    reactivate_failed_work_items,
    reclaim_expired_work_items,
    transition,
    TransitionOutcome,
    make_idempotency_key,
    ensure_entity_registry,
)
from src.services.fa_max_qualification import (
    FA_MAX_QUAL_QUEUE_NAME,
    FA_MAX_QUOTE_READY_QUEUE_NAME,
    CHECKLIST_VERSION as _CURRENT_CHECKLIST_VERSION,
    SufficiencyResult,
    evaluate_sufficiency,
    enqueue_quote_ready_work,
    enqueue_qualification_recheck,
    get_opportunity_facts,
)
from config.fa_max_qualification import CHECKLIST_VERSION

logger = logging.getLogger(__name__)

DEFAULT_LEASE_SECONDS = 120
IDLE_POLL_SECONDS = 5
RECLAIM_SWEEP_EVERY_N_LOOPS = 12

# A transient error (a DB hiccup, a Slack timeout inside enqueue_and_attempt)
# should be retried, not permanently failed on the first exception —
# complete_work_item(status='failed') is terminal; reclaim_expired_work_items
# never revisits a 'failed' row (code-review finding, 2026-09: every
# unhandled exception previously marked the item permanently failed on
# attempt 1, with no retry path at all). Below this bound, an exception
# leaves the item claimed and lets the lease simply expire so
# reclaim_expired_work_items returns it to 'available' for another attempt.
# attempt_count is incremented by claim_next_work_item on every claim
# (including reclaimed ones), so this bound is enforced across restarts.
MAX_TRANSIENT_ATTEMPTS = 5

_FA_MAX_VENTURE_KEY = "fa_max_lending"


def _worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}:qual"


class FaMaxQualificationWorker:
    def __init__(self, worker_id: Optional[str] = None) -> None:
        self.worker_id = worker_id or _worker_id()
        self._stop = False
        self._loop_count = 0

    def request_stop(self, *_args: Any) -> None:
        logger.info(
            "fa_max.qual_worker: shutdown requested (worker=%s)", self.worker_id
        )
        self._stop = True

    def install_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)

    def _claim(self) -> Optional[Dict[str, Any]]:
        with get_db_context() as session:
            return claim_next_work_item(
                session=session,
                queue_name=FA_MAX_QUAL_QUEUE_NAME,
                worker_id=self.worker_id,
                lease_seconds=DEFAULT_LEASE_SECONDS,
            )

    def _complete(self, work_item_id: str, status: str) -> None:
        with get_db_context() as session:
            completed = complete_work_item(
                session=session,
                work_item_id=work_item_id,
                worker_id=self.worker_id,
                status=status,
            )
        if not completed:
            logger.warning(
                "fa_max.qual_worker: complete_work_item no-op for"
                " work_item_id=%s status=%s (lease likely expired)",
                work_item_id, status,
            )

    def _process_one(self, item: Dict[str, Any]) -> None:
        work_item_id: str = item["work_item_id"]
        payload: Dict[str, Any] = item.get("payload") or {}

        opportunity_id: Optional[str] = payload.get("opportunity_id")
        claimed_revision: Optional[int] = payload.get("facts_revision")
        claimed_checklist: Optional[str] = payload.get("checklist_version")
        attempt_count: int = item.get("attempt_count") or 1

        if not opportunity_id or claimed_revision is None:
            # Malformed payload can never succeed on retry — this is the one
            # case that is correctly a permanent failure on first sight.
            logger.error(
                "fa_max.qual_worker: invalid payload in work_item_id=%s"
                " (missing opportunity_id or facts_revision) — failing item",
                work_item_id,
            )
            self._complete(work_item_id, "failed")
            return

        try:
            _process_qualification_item(
                opportunity_id=opportunity_id,
                claimed_revision=claimed_revision,
                claimed_checklist=claimed_checklist or CHECKLIST_VERSION,
                worker_id=self.worker_id,
            )
        except Exception:
            if attempt_count < MAX_TRANSIENT_ATTEMPTS:
                logger.exception(
                    "fa_max.qual_worker: error for work_item_id=%s"
                    " opportunity_id=%s (attempt %d/%d) — leaving claimed for"
                    " lease expiry so reclaim_expired_work_items retries it",
                    work_item_id, opportunity_id, attempt_count, MAX_TRANSIENT_ATTEMPTS,
                )
                # Deliberately do NOT call self._complete() — the claim
                # simply expires (DEFAULT_LEASE_SECONDS) and
                # reclaim_expired_work_items() returns it to 'available'.
                return
            logger.exception(
                "fa_max.qual_worker: work_item_id=%s opportunity_id=%s"
                " failed permanently after %d attempts — failing item",
                work_item_id, opportunity_id, attempt_count,
            )
            self._complete(work_item_id, "failed")
            return

        self._complete(work_item_id, "done")
        logger.info(
            "fa_max.qual_worker: completed work_item_id=%s opportunity_id=%s",
            work_item_id, opportunity_id,
        )

    def _sweep_expired(self) -> None:
        with get_db_context() as session:
            reclaim_expired_work_items(
                session=session, queue_name=FA_MAX_QUAL_QUEUE_NAME
            )

    def run_forever(self, idle_poll_seconds: int = IDLE_POLL_SECONDS) -> None:
        logger.info("fa_max.qual_worker: starting (worker=%s)", self.worker_id)
        reactivate_failed_qualification_items()
        run_checklist_version_backstop_sweep()
        while not self._stop:
            self._loop_count += 1
            try:
                if self._loop_count % RECLAIM_SWEEP_EVERY_N_LOOPS == 0:
                    self._sweep_expired()

                item = self._claim()
                if item is None:
                    time.sleep(idle_poll_seconds)
                    continue

                self._process_one(item)
            except Exception:
                logger.exception(
                    "fa_max.qual_worker: main loop iteration failed — continuing"
                )

        logger.info("fa_max.qual_worker: stopped (worker=%s)", self.worker_id)


# ---------------------------------------------------------------------------
# Core qualification processing — separated for testability
# ---------------------------------------------------------------------------

def _process_qualification_item(
    *,
    opportunity_id: str,
    claimed_revision: int,
    claimed_checklist: str,
    worker_id: str,
) -> None:
    """End-to-end qualification logic for one work item.

    Does not touch the work-queue row itself — the worker loop handles
    claim/complete around this call.
    """
    with get_db_context() as session:
        # 1. Freshness check at claim time: verify revision hasn't moved
        #    since this work item was enqueued. A concurrent set_facts() may
        #    have bumped the revision and already enqueued a newer recheck.
        facts = get_opportunity_facts(session=session, opportunity_id=opportunity_id)
        if facts is None:
            logger.warning(
                "fa_max.qual_worker: no facts row for opportunity=%s — "
                "creating empty row and evaluating as incomplete",
                opportunity_id,
            )
            from src.services.fa_max_qualification import _ensure_facts_row
            _ensure_facts_row(session, opportunity_id)
            session.commit()
            facts = {"facts_revision": 0}

        current_revision: int = facts.get("facts_revision", 0)
        if current_revision != claimed_revision:
            logger.info(
                "fa_max.qual_worker: stale claim — opportunity=%s "
                "claimed_revision=%d current_revision=%d; "
                "a newer recheck is already enqueued, skipping",
                opportunity_id, claimed_revision, current_revision,
            )
            return

        # 2. Load opportunity to get type and current stage
        opp = get_opportunity_state(session=session, opportunity_id=opportunity_id)
        if opp is None:
            logger.error(
                "fa_max.qual_worker: opportunity=%s not found", opportunity_id
            )
            raise ValueError(f"opportunity_not_found:{opportunity_id}")

        opportunity_type: str = opp["opportunity_type"]
        current_stage: str = opp["current_stage"]
        state_version: int = opp["state_version"]
        person_id: str = opp["person_id"]

        # 3. Evaluate sufficiency
        result: SufficiencyResult = evaluate_sufficiency(
            session=session,
            opportunity_id=opportunity_id,
            opportunity_type=opportunity_type,
            facts=facts,
            facts_revision=claimed_revision,
        )
        session.commit()

    # 4. Act on the verdict (separate sessions for each side-effect so a
    #    Slack failure doesn't abort the state transition or vice versa).

    if result.verdict == "sufficient":
        _handle_sufficient(
            opportunity_id=opportunity_id,
            person_id=person_id,
            current_stage=current_stage,
            state_version=state_version,
            facts_revision=claimed_revision,
            result=result,
        )
    elif result.verdict == "sufficient_pending_contract":
        # Facts are complete per the checklist, but this opportunity_type's
        # checklist is unconfirmed against a real downstream consumer —
        # deliberately NO stage transition and NO builder enqueue (the
        # blocker is a cross-team contract, not a missing fact). The
        # decision row itself (written above, inside evaluate_sufficiency)
        # is the durable, queryable record of this state — visible to an
        # operator without paging anyone. See
        # config.PENDING_CONTRACT_APPROVAL_TYPES.
        #
        # Cancellation IS still correct here, unlike the transition/enqueue:
        # the client's facts genuinely are complete now (a real client_gap
        # they were asked to fill has been resolved), so any EXCEPTIONS
        # alert from an EARLIER insufficient evaluation of this same
        # opportunity is stale and must not keep paging Josh about a gap
        # that's been filled — this branch was missing that call entirely
        # (code-review finding, fourth round, 2026-09 flagged the gap; sixth
        # round replaces the unlocked freshness pre-check with the same
        # locked check/cancel-atomically pattern used everywhere else).
        with get_db_context() as session:
            is_current, _ = _lock_and_check_eligibility(session, opportunity_id, claimed_revision)
            if is_current:
                _cancel_all_gap_alerts_for_opportunity(session=session, opportunity_id=opportunity_id)
            session.commit()

        logger.info(
            "fa_max.qual_worker: opportunity=%s type=%s facts complete but"
            " checklist unconfirmed with Dev 4 (WP-8A/8B) — no handoff;"
            " see config.fa_max_qualification.PENDING_CONTRACT_APPROVAL_TYPES",
            opportunity_id, opportunity_type,
        )
    else:
        _handle_insufficient(
            opportunity_id=opportunity_id,
            opportunity_type=opportunity_type,
            current_stage=current_stage,
            result=result,
        )


def _lock_and_check_eligibility(session, opportunity_id: str, expected_revision: int):
    """FOR UPDATE both the facts row and the opportunity row, in that FIXED
    order everywhere this is called (avoids a lock-ordering deadlock
    between two workers processing the same opportunity), and validate
    both against their CURRENT values.

    Returns (is_current, outcome):
      is_current=False → expected_revision no longer matches the DB; the
        caller must abort ALL side effects (transition, enqueue, cancel,
        alert) — outcome is meaningless in this case.
      is_current=True, outcome=<current outcome> → the caller may proceed,
        but must still check outcome == 'open' itself (what "not open"
        should do differs by caller — _handle_sufficient skips the
        transition/enqueue but still cancels a resolved alert;
        _handle_insufficient skips posting a NEW alert for a closed
        opportunity but the caller decides that).

    Both locks are held for the rest of the CALLER's transaction — a
    concurrent set_facts() (which takes the same facts-row lock) or a
    concurrent opportunity closure (a plain UPDATE against the SAME
    opportunity row, which Postgres blocks against ANY existing lock on
    that row regardless of how the lock was acquired) both wait until this
    transaction commits or rolls back. Introduced (code-review finding,
    sixth round, 2026-09) to replace the previous unlocked "check now, might
    go stale before we act" pattern used by _facts_revision_is_current — an
    acknowledged residual race that the reviewer correctly flagged as not
    actually closing the underlying problem.
    """
    db_revision = session.execute(
        text(
            "SELECT facts_revision FROM fa_max_opportunity_facts"
            " WHERE opportunity_id = :oid ::uuid"
            " FOR UPDATE"
        ),
        {"oid": opportunity_id},
    ).scalar()
    if db_revision != expected_revision:
        return False, None

    outcome = session.execute(
        text(
            "SELECT outcome FROM fa_max_opportunities"
            " WHERE opportunity_id = :oid ::uuid"
            " FOR UPDATE"
        ),
        {"oid": opportunity_id},
    ).scalar()
    return True, outcome


def _handle_sufficient(
    *,
    opportunity_id: str,
    person_id: str,
    current_stage: str,
    state_version: int,
    facts_revision: int,
    result: SufficiencyResult,
) -> None:
    """On a sufficient verdict:
    1. Lock BOTH the facts row and the opportunity row (in that fixed
       order — matches _lock_and_check_eligibility's order everywhere it's
       called, avoiding a deadlock between two workers locking the same
       two rows in opposite orders) and re-validate facts_revision AND
       outcome='open' against their CURRENT values, not the stale ones read
       before evaluate_sufficiency ran.
    2. If in 'qualifying', transition to 'scoping' (first sufficiency).
    3. Enqueue 'fa_max_quote_ready' work.
    4. Cancel any still-pending EXCEPTIONS gap alerts for this opportunity,
       IN THE SAME TRANSACTION (not a separate one after commit) — it is
       now sufficient, so any prior "missing fact" alert is resolved.

    Locking BOTH rows (code-review finding, sixth round, 2026-09): locking
    only the facts row does not block a concurrent opportunity CLOSURE (a
    write to fa_max_opportunities.outcome is a different row) — an
    opportunity could still close between the outcome read and this
    transaction's commit. A plain UPDATE against a row already FOR-UPDATE-
    locked by this transaction blocks until this transaction ends, exactly
    like a concurrent set_facts() blocks on the facts-row lock — so holding
    both locks for the transaction's duration closes that gap too, without
    requiring any change to whatever closes an opportunity elsewhere.

    Cancellation moved INSIDE this transaction (code-review finding, sixth
    round, 2026-09): the earlier fix re-checked freshness with an UNLOCKED
    read taken after this transaction's lock had already released — real,
    but narrower than the original problem, not closed. Cancellation here
    is a durable state mutation with no Slack call in it, so there is no
    reason it needs to happen outside the lock at all.
    """
    with get_db_context() as session:
        is_current, current_outcome = _lock_and_check_eligibility(
            session, opportunity_id, facts_revision
        )
        if not is_current:
            logger.info(
                "fa_max.qual_worker: stale-handoff abort — opportunity=%s "
                "evaluated revision=%d no longer current; skipping BOTH "
                "the stage transition and builder enqueue "
                "(newer recheck will follow)",
                opportunity_id, facts_revision,
            )
            session.commit()
            return
        if current_outcome != "open":
            logger.info(
                "fa_max.qual_worker: opportunity=%s closed (outcome=%s) since"
                " claim — skipping stage transition and builder enqueue",
                opportunity_id, current_outcome,
            )
            session.commit()
            return

        # Transition qualifying → scoping on first sufficiency only.
        if current_stage == "qualifying":
            entity_uuid = ensure_entity_registry(
                session=session, entity_type="opportunity", native_id=opportunity_id
            )
            ikey = make_idempotency_key(
                entity_uuid, "qualifying", "scoping",
                "agent:qualification",
            )
            trans_result = transition(
                session=session,
                entity_type="opportunity",
                entity_uuid=entity_uuid,
                from_state="qualifying",
                to_state="scoping",
                actor="agent:qualification",
                source_component="src.agents.fa_max.qualification_worker",
                idempotency_key=ikey,
                state_version=state_version,
                context={
                    "facts_revision": facts_revision,
                    "checklist_version": result.checklist_version,
                    "verdict": result.verdict,
                },
            )
            if trans_result.outcome not in (
                TransitionOutcome.succeeded, TransitionOutcome.idempotent_skip
            ):
                logger.warning(
                    "fa_max.qual_worker: qualifying→scoping transition refused"
                    " outcome=%s for opportunity=%s — skipping builder enqueue",
                    trans_result.outcome.value, opportunity_id,
                )
                session.commit()
                return

        work_item_id = enqueue_quote_ready_work(
            session=session,
            opportunity_id=opportunity_id,
            facts_revision=facts_revision,
            person_id=person_id,
        )

        # Resolved: this opportunity is now sufficient, so any EXCEPTIONS
        # alert still pending for a prior gap set is stale — cancel it here,
        # still under the SAME lock validated above, not in a separate
        # transaction after the lock releases.
        _cancel_all_gap_alerts_for_opportunity(session=session, opportunity_id=opportunity_id)

        # Compute + persist the Scenario Builder result for EVERY sufficient
        # evaluation, not only the first (code-review finding, ninth round,
        # 2026-09): the state_engine.transition() hook only fires on the
        # qualifying→scoping STAGE CHANGE — a later correction that keeps
        # the opportunity in 'scoping' (or beyond) produces no new stage
        # transition, so the dossier Josh reviews never reflected it. This
        # is the primary, reliable trigger for BOTH first sufficiency and
        # every later correction; enqueue_quote_ready_work above remains
        # for whichever future consumer eventually reads that queue.
        #
        # Persist here, under the SAME lock validated above (so this result
        # is provably computed from the exact facts just validated); Slack
        # delivery happens AFTER commit below, never inside this
        # transaction, so a later failure in this same transaction can
        # never leave a posted card with no committed row behind it.
        #
        # Isolated in its own savepoint (matching state_engine.py's
        # existing _maybe_trigger_quote_ready_review pattern): a Scenario
        # Builder compute/persist failure must never poison the stage
        # transition, EXCEPTIONS cancellation, and builder-queue enqueue
        # already done above in this same transaction.
        pending_dossier_result_id = None
        sp = session.begin_nested()
        try:
            from src.services.quote_ready.dossier import compute_and_persist_quote_ready
            pending_dossier_result_id = compute_and_persist_quote_ready(
                session, opportunity_id=opportunity_id
            )
            sp.commit()
        except Exception:
            sp.rollback()
            logger.warning(
                "fa_max.qual_worker: Scenario Builder compute/persist failed for"
                " opportunity=%s — stage transition/cancellation above are unaffected",
                opportunity_id, exc_info=True,
            )

        session.commit()

    if pending_dossier_result_id:
        from src.services.quote_ready.dossier import post_quote_ready_dossier
        with get_db_context() as delivery_session:
            post_quote_ready_dossier(delivery_session, pending_dossier_result_id)

    if work_item_id:
        logger.info(
            "fa_max.qual_worker: opportunity=%s sufficient at revision=%d"
            " → enqueued fa_max_quote_ready work_item=%s"
            " (DEPENDENCY: Dev 4 WP-8A/8B consumer not yet built)",
            opportunity_id, facts_revision, work_item_id,
        )
    else:
        logger.info(
            "fa_max.qual_worker: opportunity=%s sufficient at revision=%d"
            " → builder work already enqueued (idempotent skip)",
            opportunity_id, facts_revision,
        )


def _handle_insufficient(
    *,
    opportunity_id: str,
    opportunity_type: str,
    current_stage: str,
    result: SufficiencyResult,
) -> None:
    """On an insufficient or pending_enrichment verdict, route ONLY the
    client-actionable gaps to EXCEPTIONS — never a pending_enrichment gap.

    Per WP-T3-7 scope ("no autonomous chasing of missing numbers in v1 —
    routes the gap to the client instead"), the EXCEPTIONS lane is for facts
    only the client can supply. A pending_enrichment gap is, by definition,
    a fact the enrichment pipeline should be able to fill on its own — the
    checklist's own gap_reason text says as much ("will be auto-populated
    from existing enrichment data if available"). Previously every gap,
    including pure pending_enrichment ones, paged Josh identically to a
    real client_gap (code-review finding, 2026-09) — before enrichment had
    even had a chance to run. T3-8 owns the eventual enrichment-exhaustion
    promotion policy (pending_enrichment → client_gap after retries are
    exhausted); until that exists, a pure pending_enrichment verdict is
    correctly silent here, not silently dropped.

    Locked, atomic durable state, delivery after commit (code-review
    finding, sixth round, 2026-09 — replaces the previous unlocked
    _facts_revision_is_current pre-check, which the reviewer correctly
    identified as narrowing but not closing the out-of-order race):
    1. FOR UPDATE both the facts row and the opportunity row
       (_lock_and_check_eligibility) — aborts entirely if the evaluated
       revision is stale, or cancels-and-exits if the opportunity closed.
    2. Cancel any superseded-hash alert AND durably record the new pending
       alert row (enqueue_pending) — both inside this SAME locked
       transaction, so they're atomic with the revision/outcome check.
    3. Commit — releases both locks.
    4. Only AFTER commit, attempt Slack delivery (attempt_delivery) — never
       inside the lock, so a slow/failed Slack call never blocks a
       concurrent set_facts() or opportunity closure.

    A genuine failure to durably INSERT the new row now propagates as an
    ordinary exception from inside the transaction (no more manual
    "does a row exist afterward?" check) — the caller
    (_process_qualification_item → the worker's bounded-retry path) handles
    it the same way it handles any other exception. A Slack delivery
    FAILURE (post-commit) is not re-raised: the row is already durably
    'pending', so the existing drain_pending() cron retries it — that is
    its job, not this function's.
    """
    from src.services.relay.exceptions_alert_queue import enqueue_pending, attempt_delivery

    with get_db_context() as session:
        is_current, current_outcome = _lock_and_check_eligibility(
            session, opportunity_id, result.facts_revision
        )
        if not is_current:
            logger.info(
                "fa_max.qual_worker: stale insufficient result for opportunity=%s"
                " (evaluated revision=%d no longer current) — skipping EXCEPTIONS"
                " entirely; a newer evaluation's outcome is authoritative",
                opportunity_id, result.facts_revision,
            )
            session.commit()
            return

        if current_outcome != "open":
            # Closed opportunities don't need Josh chasing a gap anymore.
            _cancel_all_gap_alerts_for_opportunity(session=session, opportunity_id=opportunity_id)
            logger.info(
                "fa_max.qual_worker: opportunity=%s closed (outcome=%s) —"
                " cancelling any pending gap alert, not posting a new one",
                opportunity_id, current_outcome,
            )
            session.commit()
            return

        if not result.gaps:
            logger.warning(
                "fa_max.qual_worker: verdict=%s but no gaps for opportunity=%s"
                " — skipping EXCEPTIONS alert",
                result.verdict, opportunity_id,
            )
            session.commit()
            return

        client_gaps = [g for g in result.gaps if g.gap_type == "client_gap"]
        enrichment_gaps = [g for g in result.gaps if g.gap_type == "pending_enrichment"]

        if not client_gaps:
            # Pure pending_enrichment verdict: nothing NEW for Josh to act
            # on. Do NOT alert — wait for enrichment (or T3-8's eventual
            # exhaustion policy). But a PRIOR evaluation may have had
            # client_gaps now resolved while enrichment gaps remain — that
            # earlier alert is stale and must still be cancelled (code-review
            # finding, second round, 2026-09).
            _cancel_all_gap_alerts_for_opportunity(session=session, opportunity_id=opportunity_id)
            session.commit()
            logger.info(
                "fa_max.qual_worker: opportunity=%s verdict=pending_enrichment"
                " (%d enrichment-sourceable gap(s), 0 client gaps) — no"
                " EXCEPTIONS alert; any prior client-gap alert cancelled;"
                " awaiting enrichment",
                opportunity_id, len(enrichment_gaps),
            )
            return

        client_hash = _gap_subset_hash(client_gaps)
        new_rule = f"qualification_gap:{opportunity_id}:{client_hash}"

        # Cancel any previous gap-hash alert whose hash doesn't match the
        # current client-gap set, then durably record the new one — both
        # under the SAME lock validated above.
        _cancel_superseded_gap_alerts(session=session, opportunity_id=opportunity_id, current_hash=client_hash)

        gap_lines = "\n".join(
            f"  • {g.display_name}: {g.reason}" for g in client_gaps
        )
        enrichment_note = (
            f"\n\n_Also pending enrichment (not routed to you): "
            f"{', '.join(g.display_name for g in enrichment_gaps)}_"
            if enrichment_gaps else ""
        )
        message = (
            f"Qualification gap for opportunity {opportunity_id} "
            f"[type={opportunity_type}, stage={current_stage}]:\n"
            f"{gap_lines}"
            f"{enrichment_note}\n\n"
            f"_facts_revision={result.facts_revision}"
            f"  checklist={result.checklist_version}"
            f"  verdict={result.verdict}_"
        )

        row_id = enqueue_pending(
            session=session, venture_key=_FA_MAX_VENTURE_KEY, rule=new_rule, message=message,
        )
        session.commit()

    if row_id is None:
        logger.info(
            "fa_max.qual_worker: opportunity=%s verdict=%s client_gaps=%d"
            " enrichment_gaps=%d rule=%s already durably queued/sent — skipping delivery",
            opportunity_id, result.verdict, len(client_gaps), len(enrichment_gaps), new_rule,
        )
        return

    delivered = attempt_delivery(
        row_id, venture_key=_FA_MAX_VENTURE_KEY, rule=new_rule, message=message,
    )
    logger.info(
        "fa_max.qual_worker: opportunity=%s verdict=%s client_gaps=%d"
        " enrichment_gaps=%d EXCEPTIONS alert delivered=%s rule=%s",
        opportunity_id, result.verdict, len(client_gaps), len(enrichment_gaps),
        delivered, new_rule,
    )


def _gap_subset_hash(gaps: list) -> Optional[str]:
    """Same hashing scheme as SufficiencyResult.gap_content_hash, but over an
    arbitrary gap subset (here: client_gap-only) rather than all gaps."""
    import hashlib
    import json

    if not gaps:
        return None
    sorted_pairs = sorted((g.fact_key, g.reason) for g in gaps)
    raw = json.dumps(sorted_pairs, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


def _cancel_superseded_gap_alerts(
    *, session=None, opportunity_id: str, current_hash: Optional[str]
) -> None:
    """Cancel any pending EXCEPTIONS alerts for this opportunity whose gap
    hash differs from current_hash. These represent a prior gap set that has
    since been superseded by a correction (the gap set changed).

    Only cancels 'pending', unclaimed rows — does not race with an in-flight
    _attempt() that already holds the claim (that delivery may still complete,
    which is the accepted duplicate-delivery trade-off).

    session: when provided, runs as part of the CALLER's own transaction
    (no commit here) — used by _handle_sufficient/_handle_insufficient so
    this cancellation is atomic with the facts-revision/outcome lock they
    already hold (code-review finding, sixth round, 2026-09: cancelling in
    a separate, later transaction after that lock released left a window
    for a concurrent evaluation to act on stale information). When omitted,
    opens and commits its own — unchanged standalone behavior for any
    other caller.
    """
    rule_prefix = f"qualification_gap:{opportunity_id}:"

    def _run(session) -> None:
        rows = session.execute(
            text(
                "SELECT id, rule FROM fa_max_exceptions_alert_queue"
                " WHERE rule LIKE :prefix"
                " AND status = 'pending'"
                " AND (claimed_until IS NULL OR claimed_until < now())"
                " AND rule <> :current_rule"
            ),
            {
                "prefix": rule_prefix + "%",
                "current_rule": (
                    f"{rule_prefix}{current_hash}" if current_hash else ""
                ),
            },
        ).mappings().all()

        if not rows:
            return

        ids_to_cancel = [r["id"] for r in rows]
        session.execute(
            text(
                "UPDATE fa_max_exceptions_alert_queue"
                " SET status = 'cancelled'"
                " WHERE id = ANY(:ids)"
                " AND status = 'pending'"
                " AND (claimed_until IS NULL OR claimed_until < now())"
            ),
            {"ids": ids_to_cancel},
        )
        logger.info(
            "fa_max.qual_worker: cancelled %d superseded gap alert(s)"
            " for opportunity=%s (current_hash=%s)",
            len(ids_to_cancel), opportunity_id, current_hash,
        )

    if session is not None:
        _run(session)
    else:
        with get_db_context() as owned_session:
            _run(owned_session)
            owned_session.commit()


def _cancel_all_gap_alerts_for_opportunity(*, session=None, opportunity_id: str) -> None:
    """Cancel every still-pending gap alert for this opportunity — called
    from _handle_sufficient() once a verdict resolves to 'sufficient', since
    any prior "missing fact" alert is now stale (code-review finding,
    2026-09: the success path never cancelled anything, so a resolved gap
    alert stayed 'pending' until happenstance superseded it with a
    different, still-insufficient gap set).

    current_hash=None makes _cancel_superseded_gap_alerts's exclusion
    filter match nothing, so every pending row for this opportunity's
    rule prefix is cancelled — reuses the same tested cancellation path
    rather than a parallel implementation. session: see
    _cancel_superseded_gap_alerts's docstring.
    """
    _cancel_superseded_gap_alerts(session=session, opportunity_id=opportunity_id, current_hash=None)


_REACTIVATE_BATCH_SIZE = 500
_REACTIVATE_MAX_TOTAL = 20000


def reactivate_failed_qualification_items(
    *, batch_size: int = _REACTIVATE_BATCH_SIZE, max_total: int = _REACTIVATE_MAX_TOTAL
) -> int:
    """Return every terminally-'failed' fa_max_qualification work item to
    'available' at worker startup.

    A permanently-failed item's idempotency_key ("qual:{opp}:{rev}:{ver}")
    is UNIQUE with no status scoping, so a NEW enqueue attempt for the exact
    same (opportunity, revision, checklist_version) silently does nothing
    once the original attempt has failed — including from
    run_checklist_version_backstop_sweep(), which could correctly identify
    the opportunity as needing reevaluation and still never actually
    recover it (code-review finding, fourth round, 2026-09). This also
    covers the case where evaluate_sufficiency() successfully wrote a
    decision row (so the opportunity looks "evaluated" to that sweep) but
    the SAME work item later failed permanently trying to durably record
    the EXCEPTIONS alert — the decision row and the alert delivery are
    decoupled, and only the work-queue's own 'failed' status reliably
    marks that case.

    Reactivating the existing row (not inserting a new one) sidesteps the
    unique constraint — see reactivate_failed_work_items()'s docstring.
    Paginated the same way as the checklist-version sweep, for the same
    reason (never load or lock an unbounded row set in one call).
    """
    total = 0
    with get_db_context() as session:
        while total < max_total:
            n = reactivate_failed_work_items(
                session=session,
                queue_name=FA_MAX_QUAL_QUEUE_NAME,
                limit=min(batch_size, max_total - total),
            )
            session.commit()
            total += n
            if n < batch_size:
                break

    if total:
        logger.info(
            "fa_max.qual_worker: reactivated %d permanently-failed"
            " qualification work item(s) for retry", total,
        )
    return total


_BACKSTOP_SWEEP_BATCH_SIZE = 500
# Overall cap across all batches in one invocation — bounds startup time.
# A checklist edit affecting more opportunities than this needs a repeated
# manual invocation to finish backfilling (logged explicitly when hit).
_BACKSTOP_SWEEP_MAX_TOTAL = 20000


def run_checklist_version_backstop_sweep(
    *, batch_size: int = _BACKSTOP_SWEEP_BATCH_SIZE, max_total: int = _BACKSTOP_SWEEP_MAX_TOTAL
) -> int:
    """Re-enqueue a qualification recheck for every non-terminal opportunity
    that needs reevaluation: its latest decision (if any) used a stale
    checklist_version, its facts_revision has moved past its latest decision
    (a recheck for that revision was lost — e.g. the work item exhausted
    MAX_TRANSIENT_ATTEMPTS), or it has a facts row but NO decision at all
    (its original recheck was lost the same way, before ever producing one).

    A checklist edit changes what "sufficient" means; without this sweep, an
    opportunity that was marked sufficient (or insufficient) under the old
    checklist is never re-evaluated against the new one until its facts
    happen to change again — which may be never. This was documented as
    required ("CHECKLIST_VERSION must be bumped... triggers a backstop
    sweep") but never implemented (code-review finding, 2026-09).

    Broadened twice since the initial implementation (code-review finding,
    third round, 2026-09):
    - LEFT JOIN (not INNER JOIN) on the latest decision — an opportunity
      whose qualification recheck was permanently lost before EVER
      producing a decision row was previously invisible to this sweep
      entirely, since it only compared an EXISTING decision's
      checklist_version. Its own retry mechanism (MAX_TRANSIENT_ATTEMPTS)
      makes this rare, but not impossible, and this sweep is exactly the
      backstop for "rare, not impossible" gaps.
    - Also matches on `f.facts_revision > latest.facts_revision` (or no
      decision at all) — not just a stale checklist_version — so a lost
      recheck for the CURRENT checklist version is caught too, not only a
      stale-version one.

    Run once at worker startup (config/fa_max_qualification.py's
    CHECKLIST_VERSION bumps are rare, deliberate, deploy-time events — this
    does not need to run on every poll loop). Paginates in `batch_size`
    chunks, each its own transaction, up to `max_total` overall — never
    loads an unbounded row set into one transaction. If `max_total` is hit,
    logs at WARNING so an operator knows to re-run rather than assuming
    full coverage silently.
    """
    total_enqueued = 0
    total_seen = 0
    # Keyset cursor, NOT offset/repeat-LIMIT: enqueuing a recheck does not
    # itself change fa_max_qualification_decisions (a worker processes the
    # enqueued item asynchronously, later) — a plain "ORDER BY ... LIMIT N"
    # re-run inside this same loop therefore matched the IDENTICAL top-N
    # rows every iteration, silently repeat-visiting opportunity #1..N and
    # never reaching #N+1 onward (code-review finding, fourth round,
    # 2026-09). A keyset cursor on opportunity_id guarantees forward
    # progress through the eligible set regardless of whether anything
    # downstream has processed yet.
    last_seen_id: Optional[str] = None

    while total_seen < max_total:
        remaining = min(batch_size, max_total - total_seen)
        with get_db_context() as session:
            rows = session.execute(
                text(
                    """
                    SELECT o.opportunity_id::text AS opportunity_id,
                           o.person_id::text AS person_id,
                           f.facts_revision AS facts_revision
                    FROM fa_max_opportunities o
                    JOIN fa_max_opportunity_facts f
                        ON f.opportunity_id = o.opportunity_id
                    LEFT JOIN LATERAL (
                        SELECT checklist_version, facts_revision
                        FROM fa_max_qualification_decisions d
                        WHERE d.opportunity_id = o.opportunity_id
                        ORDER BY d.decided_at DESC
                        LIMIT 1
                    ) latest ON TRUE
                    WHERE o.outcome = 'open'
                      AND (:last_seen_id ::uuid IS NULL OR o.opportunity_id > :last_seen_id ::uuid)
                      AND (
                          latest.checklist_version IS NULL
                          OR latest.checklist_version IS DISTINCT FROM :current_version
                          OR f.facts_revision > latest.facts_revision
                      )
                    ORDER BY o.opportunity_id
                    LIMIT :batch_size
                    """
                ),
                {
                    "current_version": _CURRENT_CHECKLIST_VERSION,
                    "batch_size": remaining,
                    "last_seen_id": last_seen_id,
                },
            ).mappings().all()

            for row in rows:
                work_item_id = enqueue_qualification_recheck(
                    session=session,
                    opportunity_id=row["opportunity_id"],
                    facts_revision=row["facts_revision"],
                    person_id=row["person_id"],
                )
                if work_item_id:
                    total_enqueued += 1
                last_seen_id = row["opportunity_id"]
            session.commit()

        total_seen += len(rows)
        if len(rows) < remaining:
            break  # fewer than a full page → no more rows to sweep

    if total_seen >= max_total:
        logger.warning(
            "fa_max.qual_worker: checklist-version backstop sweep hit"
            " max_total=%d — coverage may be incomplete, re-run manually",
            max_total,
        )

    logger.info(
        "fa_max.qual_worker: checklist-version backstop sweep — %d/%d"
        " opportunity(ies) needing reevaluation re-enqueued (current_version=%s)",
        total_enqueued, total_seen, _CURRENT_CHECKLIST_VERSION,
    )
    return total_enqueued
    return enqueued


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    worker = FaMaxQualificationWorker()
    worker.install_signal_handlers()
    worker.run_forever()


if __name__ == "__main__":
    main()
