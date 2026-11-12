"""P1.1 job queue worker, run once a minute inside Cora's sweep (``cora_agent.run_sweep``).

Three phases, each isolated so one failing never skips the next:

1. **Intake**: each new file in "NDL Jobs/inbox" (oldest first, at most MAX_JOBS_PER_SWEEP) is recorded in
   ``lending.agent_jobs`` under its ``job_id`` (a repeated job_id is ignored), moved to "inbox/processed", then
   evaluated.
2. **Release**: held jobs whose ``release_at`` has come are evaluated again.
3. **Finish**: a carded job whose draft is decided gets its receipt; then every finished job whose receipt is not yet
   in "NDL Jobs/done" is written (retried each sweep until Drive accepts it).

Evaluating a job reads the contact from GHL and runs the checks: a block finishes the job with that status, a hold
parks it, and a go submits a normal Cora draft through the send gate (an #approvals card). The draft's idempotency key
is ``agent_job:{job_id}``, so a crash between the card and the row update never makes a second draft or card. A job
whose template has graduated (P1.4, ``is_graduated``) and that does not require approval is approved here as
``auto:graduated``; the relay sends it with all its send-time checks. Until P1.4 nothing has graduated.

Cora runs as exactly one instance, so rows are not locked; every state change is a conditional UPDATE.
"""
from __future__ import annotations

import argparse
import json
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, NamedTuple, Optional

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from config.lending_agent_jobs import COLD_EMAIL_CHANNEL, MAX_JOBS_PER_SWEEP, REQUESTED_BY, STUCK_SENDING_AFTER
from config.lending_standing_sends import AUTO_GRADUATED_ACTOR
from packages.agent_core.pending_actions import ActionStatus, PendingActionQueue
from packages.agent_core.send_gate import SendGate
from packages.agent_core.store import utc_now
from packages.agent_core.tools import ToolContext
from src.lending.agent_jobs.checks import Verdict, check_job
from src.lending.agent_jobs.drafts import build_draft
from src.lending.agent_jobs.drive import DriveFile, FakeJobFolders, JobFolders
from src.lending.agent_jobs.job import AgentJob, JobFormatError, parse_job
from src.lending.agent_jobs.receipts import build_receipt, receipt_for
from src.lending.ghl_crm import GhlContact

logger = logging.getLogger(__name__)

Check = Callable[[AgentJob, Optional[GhlContact], datetime], Verdict]
ReadContact = Callable[[str], Optional[GhlContact]]
_GHL_RETRY = STUCK_SENDING_AFTER  # a GHL read that failed is retried after this long


class WorkerStats(NamedTuple):
    received: int = 0
    carded: int = 0
    held: int = 0
    blocked: int = 0
    finished: int = 0
    errors: int = 0


def _load_json(raw: Any) -> Any:
    return json.loads(raw) if isinstance(raw, (str, bytes)) else raw


def _load_time(raw: Any) -> Optional[datetime]:
    if raw is None or isinstance(raw, datetime):
        return raw
    return datetime.fromisoformat(str(raw))


def _draft_key(job: AgentJob) -> str:
    return f"agent_job:{job.job_id}"


def make_check(engine: Engine, *, instantly_ready: bool) -> Check:
    """``check_job`` on a short read-only session of its own."""

    def check(job: AgentJob, contact: Optional[GhlContact], now: datetime) -> Verdict:
        with Session(engine) as db:
            try:
                return check_job(db, job, contact, now, instantly_ready=instantly_ready)
            finally:
                db.rollback()

    return check


class JobQueueWorker:
    def __init__(self, engine: Engine, folders: JobFolders, gate: SendGate, queue: PendingActionQueue,
                 read_contact: ReadContact, *, approval_channel: str, check: Check,
                 is_graduated: Callable[[str], bool] = lambda _: False,
                 clock: Callable[[], datetime] = utc_now, jobs_table: str = "lending.agent_jobs",
                 raise_alert: Optional[Callable[..., Any]] = None) -> None:
        self._engine = engine
        self._folders = folders
        self._gate = gate
        self._queue = queue
        self._read_contact = read_contact
        self._approval_channel = approval_channel
        self._check = check
        self._is_graduated = is_graduated
        self._clock = clock
        self._table = jobs_table
        self._postgres = engine.dialect.name == "postgresql"
        self._raise_alert = raise_alert or _inbox_alert
        self._sending_since: dict[int, datetime] = {}
        self._stuck_logged: set[int] = set()
        self._counts: dict[str, int] = {}

    # -- storage ---------------------------------------------------------------------------------------------

    @contextmanager
    def _tx(self) -> Iterator[Any]:
        with self._engine.begin() as conn:
            yield conn

    def _json(self, name: str) -> str:
        return f"CAST(:{name} AS jsonb)" if self._postgres else f":{name}"

    def _rows(self, where: str, params: Mapping[str, Any], *, limited: bool = True) -> list[dict[str, Any]]:
        limit = " LIMIT :limit" if limited else ""
        with self._tx() as conn:
            rows = conn.execute(text(f"SELECT * FROM {self._table} WHERE {where} ORDER BY received_at{limit}"),
                                {**params, "limit": MAX_JOBS_PER_SWEEP}).mappings().all()
        return [dict(row) for row in rows]

    def _update(self, job_id: str, from_states: tuple[str, ...], **values: Any) -> bool:
        assignments = ", ".join(f"{key} = {self._json(key) if key == 'result' else ':' + key}" for key in values)
        params = {key: json.dumps(value) if key == "result" else value for key, value in values.items()}
        statement = text(f"UPDATE {self._table} SET {assignments}, updated_at = :updated_at "
                         "WHERE job_id = :job_id AND state IN :from_states").bindparams(
            bindparam("from_states", expanding=True))
        with self._tx() as conn:
            moved = conn.execute(statement, {**params, "updated_at": self._clock(), "job_id": job_id,
                                             "from_states": list(from_states)}).rowcount
        return bool(moved)

    def _count(self, key: str) -> None:
        self._counts[key] = self._counts.get(key, 0) + 1

    # -- phases ----------------------------------------------------------------------------------------------

    def run_once(self) -> WorkerStats:
        self._counts = {}
        for phase in (self._intake, self._release, self._finish):
            try:
                phase()
            except Exception as exc:  # class only: one phase failing never skips the next
                logger.error("[agent-jobs] %s phase failed (%s)", phase.__name__.strip("_"), type(exc).__name__)
                self._count("errors")
        return WorkerStats(**{field: self._counts.get(field, 0) for field in WorkerStats._fields})

    def _intake(self) -> None:
        files = self._folders.list_inbox(MAX_JOBS_PER_SWEEP)
        if not files:
            return
        statement = text(f"SELECT drive_file_id FROM {self._table} WHERE drive_file_id IN :ids").bindparams(
            bindparam("ids", expanding=True))
        with self._tx() as conn:
            known = {row[0] for row in conn.execute(statement, {"ids": [f.file_id for f in files]})}
        for drive_file in files:
            try:
                self._take(drive_file, already_recorded=drive_file.file_id in known)
            except Exception as exc:  # class only; the file stays in the inbox and is retried next sweep
                logger.error("[agent-jobs] file %s could not be taken (%s)", drive_file.file_id, type(exc).__name__)
                self._count("errors")

    def _take(self, drive_file: DriveFile, *, already_recorded: bool) -> None:
        if already_recorded:  # recorded on an earlier sweep that died before the move
            self._folders.move_to_processed(drive_file.file_id)
            return
        now = self._clock()
        try:
            job: Optional[AgentJob] = parse_job(self._folders.read(drive_file.file_id), drive_file.name)
            job_id, error = str(job.job_id), None
        except JobFormatError as exc:
            job, job_id, error = None, exc.job_id, exc.reason
        if job_id is None:
            logger.error("[agent-jobs] file %s could not be read as a job and has no job_id: %s",
                         drive_file.file_id, error)
            self._folders.move_to_processed(drive_file.file_id)
            self._count("errors")
            return
        self._count("received")
        inserted = self._insert(drive_file, job_id, job, now)
        self._folders.move_to_processed(drive_file.file_id)
        if not inserted:
            logger.warning("[agent-jobs] job %s was already received; file %s ignored", job_id, drive_file.file_id)
            return
        if job is None:
            self._finish_row(job_id, ("held",), "error", reason=error)
            return
        self._evaluate(self._rows("job_id = :job_id", {"job_id": job_id})[0])

    def _insert(self, drive_file: DriveFile, job_id: str, job: Optional[AgentJob], now: datetime) -> bool:
        values = {"job_id": job_id, "drive_file_id": drive_file.file_id, "file_name": drive_file.name,
                  "created_by": job.created_by if job else None, "job_type": job.type if job else None,
                  "template_version": job.template_version if job else None,
                  "ghl_contact_id": job.ghl_contact_id if job else None,
                  "job": json.dumps(job.model_dump(mode="json") if job else {}), "now": now}
        with self._tx() as conn:
            inserted = conn.execute(text(
                f"INSERT INTO {self._table} (job_id, drive_file_id, file_name, created_by, job_type, template_version, "
                f"ghl_contact_id, job, state, release_at, received_at, updated_at) VALUES (:job_id, :drive_file_id, "
                f":file_name, :created_by, :job_type, :template_version, :ghl_contact_id, {self._json('job')}, 'held', "
                ":now, :now, :now) ON CONFLICT DO NOTHING"), values).rowcount
        return bool(inserted)

    def _release(self) -> None:
        for row in self._rows("state = 'held' AND release_at <= :now", {"now": self._clock()}):
            try:
                self._evaluate(row)
            except Exception as exc:  # class only; the job stays held and is retried later
                logger.error("[agent-jobs] job %s could not be evaluated (%s)", row["job_id"], type(exc).__name__)
                self._update(row["job_id"], ("held",), release_at=self._clock() + _GHL_RETRY)
                self._count("errors")

    def _evaluate(self, row: Mapping[str, Any]) -> None:
        job = AgentJob.model_validate(_load_json(row["job"]))
        existing = self._queue.find_by_idempotency_key(_draft_key(job))
        if existing is not None:
            # A sweep died after the card posted: adopt the live draft. Re-running the checks could finish the job
            # with a different status (the cold cap counts this very draft) while its card is still approvable.
            rail = "instantly" if existing.channel == COLD_EMAIL_CHANNEL else "ghl"
            self._update(row["job_id"], ("held",), state="carded", action_id=existing.action_id, rail=rail,
                         release_at=None)
            self._count("carded")
            return
        now = self._clock()
        contact = self._read_contact(job.ghl_contact_id)
        verdict = self._check(job, contact, now)
        if verdict.outcome == "block":
            status = verdict.status or "error"
            if status == "expired" and row.get("held_reason") == "hours":
                status = "blocked_hours"
            self._finish_row(row["job_id"], ("held",), status, reason=verdict.reason, rail=verdict.rail)
            self._count("blocked")
            return
        if verdict.outcome == "hold":
            self._update(row["job_id"], ("held",), release_at=verdict.release_at, held_reason=verdict.held_reason,
                         rail=verdict.rail)
            self._count("held")
            return
        draft = build_draft(job, contact, verdict.rail or "ghl")
        context = ToolContext(user_id=REQUESTED_BY, is_approver=False, channel=self._approval_channel, thread_ts="",
                              tool_use_id=_draft_key(job))
        queued = self._gate.submit(draft, tool_name=f"agent_job:{job.type}", context=context,
                                   ttl=job.expires_at - now)
        if not job.requires_approval and self._is_graduated(job.template_version):
            self._queue.approve(queued.action_id, AUTO_GRADUATED_ACTOR)
        self._update(row["job_id"], ("held",), state="carded", action_id=queued.action_id, rail=verdict.rail,
                     release_at=None)
        self._count("carded")

    def _finish_row(self, job_id: str, from_states: tuple[str, ...], status: str, *, reason: Optional[str] = None,
                    rail: Optional[str] = None, receipt: Optional[dict] = None) -> None:
        row = self._rows("job_id = :job_id", {"job_id": job_id})[0]
        if rail:
            row["rail"] = rail
        result = receipt or build_receipt(row, status, now=self._clock(), reason=reason)
        values: dict[str, Any] = {"state": "done", "result_status": result["status"], "result": result}
        if rail:
            values["rail"] = rail
        self._update(job_id, from_states, **values)

    def _finish(self) -> None:
        now = self._clock()
        # Every carded job, not the oldest 50: undecided cards must never hide a newer job Josh has decided.
        for row in self._rows("state = 'carded'", {}, limited=False):
            action = self._queue.get(row["action_id"]) if row["action_id"] is not None else None
            self._watch_sending(action, row["job_id"], now)
            receipt = receipt_for(row, action, now=now)
            if receipt is not None:
                self._finish_row(row["job_id"], ("carded",), receipt["status"], receipt=receipt)
        for row in self._rows("state = 'done' AND result_written_at IS NULL", {}):
            try:
                self._folders.write_result(row["job_id"], _load_json(row["result"]))
            except Exception as exc:  # class only; retried next sweep
                logger.error("[agent-jobs] receipt for job %s not written (%s); retrying next sweep",
                             row["job_id"], type(exc).__name__)
                self._count("errors")
                continue
            self._update(row["job_id"], ("done",), result_written_at=self._clock())
            self._count("finished")

    def _watch_sending(self, action: Any, job_id: str, now: datetime) -> None:
        """An approved send still 'sending' after STUCK_SENDING_AFTER has an unknown outcome: never retried (a
        duplicate send is worse than a missing one), so a person must check GHL or Instantly. Raised once in Cora's
        inbox (deduplicated by action id, so a restart never raises it twice) and logged."""
        if action is None or action.status is not ActionStatus.SENDING:
            return
        since = self._sending_since.setdefault(action.action_id, now)
        if now - since < STUCK_SENDING_AFTER or action.action_id in self._stuck_logged:
            return
        self._stuck_logged.add(action.action_id)
        logger.error("[agent-jobs] action %s has been sending for %s; outcome unknown, check GHL or Instantly",
                     action.action_id, now - since)
        self._raise_alert(event_type="agent_job_send_unknown", dedup_key=str(action.action_id),
                          summary=(f"Agent job send outcome unknown: action {action.action_id} stopped mid-send. "
                                   "Check GHL or Instantly before re-sending; it is never retried."),
                          payload={"action_id": action.action_id, "job_id": job_id})


def _inbox_alert(**event: Any) -> None:
    from src.lending.cora_inbox import add_to_inbox_in_own_session

    add_to_inbox_in_own_session(**event)


def dry_run(directory: Path) -> WorkerStats:
    """Parse every *.json in a local folder as a job file; nothing is stored, carded or sent."""
    folders = FakeJobFolders()
    for path in sorted(directory.glob("*.json")):
        folders.add(path.name, path.read_bytes())
    received = invalid = 0
    for drive_file in folders.list_inbox(10_000):
        try:
            parse_job(folders.read(drive_file.file_id), drive_file.name)
            received += 1
        except JobFormatError as exc:
            invalid += 1
            logger.warning("[agent-jobs] %s: %s", drive_file.name, exc.reason)
    return WorkerStats(received=received, errors=invalid)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="P1.1 agent job queue (the live worker runs inside fa-lending-cora)")
    parser.add_argument("--dry-run", action="store_true", help="only parse job files from --from-dir")
    parser.add_argument("--from-dir", type=Path, help="local folder of job files")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if not (args.dry_run and args.from_dir):
        parser.error("only --dry-run --from-dir <folder> is supported; the live worker runs inside fa-lending-cora")
    stats = dry_run(args.from_dir)
    logger.info("received=%s carded=0 invalid=%s", stats.received, stats.errors)
    return 0 if stats.errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
