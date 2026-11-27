"""The DLQ replay CLI lists, shows and replays saved webhook failures by hand, never printing payload values.

No database: the session is a fake holding the saved rows; handlers are scripted.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

from src.lending import webhook_failures
from src.lending.inbound import dlq_handler
from src.lending.webhook_failures import GHL_OPT_OUT, GHL_TEXT_CONSENT

PHONE = "+18135559301"
BODY = {"contact_id": "ghl-c-42", "phone": PHONE, "message": "please stop texting me"}
CREATED = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)
GAVE_UP = datetime(2026, 11, 20, 12, 25, tzinfo=timezone.utc)


class DatabaseDown(Exception):
    pass


def _row(failure_id, endpoint=GHL_OPT_OUT, attempts=5, gave_up_at=GAVE_UP, resolved_at=None, last_error="DatabaseDown"):
    return SimpleNamespace(id=failure_id, endpoint=endpoint, payload=dict(BODY), ghl_contact_id="ghl-c-42",
                           attempts=attempts, last_error=last_error, created_at=CREATED, gave_up_at=gave_up_at,
                           resolved_at=resolved_at)


class FakeTable:
    """lending.webhook_failures in memory: answers the CLI's selects and applies its updates to the rows."""

    def __init__(self, rows):
        self.rows = {row.id: row for row in rows}

    @contextmanager
    def begin_nested(self):
        yield

    def commit(self):
        pass

    def execute(self, statement, params=None):
        sql, params = " ".join(str(statement).split()), dict(params or {})
        if sql.startswith("SELECT"):
            rows = sorted(self.rows.values(), key=lambda r: r.id)
            if "WHERE id = :id" in sql:
                rows = [r for r in rows if r.id == params["id"]]
            if "endpoint = :endpoint" in sql:
                rows = [r for r in rows if r.endpoint == params["endpoint"] and r.resolved_at is None]
            if "after" in params:
                rows = [r for r in rows if r.id > params["after"]][:params["limit"]]
            return SimpleNamespace(all=lambda: rows, first=lambda: rows[0] if rows else None)
        row = self.rows[params["id"]]
        if "SET resolved_at = now()" in sql:
            row.resolved_at = "now"
        elif "attempts = :attempts" in sql:
            row.attempts, row.last_error = params["attempts"], params["error"]
            row.gave_up_at = "now" if params["give_up"] else None
        elif "gave_up_at = NULL" in sql:
            row.gave_up_at = None
        else:
            raise AssertionError(f"unexpected statement: {sql}")
        return SimpleNamespace()


def _run(monkeypatch, caplog, rows, argv, handler=None):
    caplog.set_level(logging.INFO, logger=dlq_handler.logger.name)
    table = FakeTable(rows)

    @contextmanager
    def session():
        yield table

    if handler is not None:
        monkeypatch.setitem(webhook_failures.ENDPOINT_HANDLERS, GHL_OPT_OUT, handler)
    code = dlq_handler.main(argv, session_factory=session)
    return code, table, caplog.text


def test_replay_runs_a_given_up_failure_once_now_and_resolves_it(monkeypatch, caplog):
    replayed: list[dict] = []
    code, table, out = _run(monkeypatch, caplog, [_row(7)], ["--replay", "7"],
                            handler=lambda db, body: replayed.append(body))
    assert code == 0 and replayed == [BODY]
    assert table.rows[7].resolved_at == "now" and table.rows[7].gave_up_at is None
    assert "#7" in out and "resolved" in out and PHONE not in out


def test_a_replay_that_fails_again_exits_non_zero_and_stays_given_up(monkeypatch, caplog):
    def still_down(db, body):
        raise DatabaseDown("conn refused 8135559301")

    code, table, out = _run(monkeypatch, caplog, [_row(7, attempts=5)], ["--replay", "7"], handler=still_down)
    assert code == 1 and table.rows[7].resolved_at is None
    assert table.rows[7].attempts == 6 and table.rows[7].gave_up_at == "now"
    assert "#7" in out and "8135559301" not in out and "stop texting" not in out


def test_replaying_an_unknown_id_exits_non_zero(monkeypatch, caplog):
    code, _, out = _run(monkeypatch, caplog, [], ["--replay", "99"])
    assert code == 1 and "#99 not found" in out


def test_an_already_resolved_failure_is_not_replayed(monkeypatch, caplog):
    replayed: list[dict] = []
    code, _, out = _run(monkeypatch, caplog, [_row(7, gave_up_at=None, resolved_at=GAVE_UP)], ["--replay", "7"],
                        handler=lambda db, body: replayed.append(body))
    assert code == 0 and replayed == [] and "already resolved" in out


def test_replay_all_runs_every_open_or_given_up_failure_of_one_endpoint(monkeypatch, caplog):
    rows = [_row(1), _row(2, attempts=2, gave_up_at=None), _row(3, gave_up_at=None, resolved_at=GAVE_UP),
            _row(4, endpoint=GHL_TEXT_CONSENT)]
    replayed: list[int] = []
    code, table, _ = _run(monkeypatch, caplog, rows, ["--replay-all", GHL_OPT_OUT],
                          handler=lambda db, body: replayed.append(body))
    assert code == 0 and len(replayed) == 2
    assert table.rows[1].resolved_at == "now" and table.rows[2].resolved_at == "now"
    assert table.rows[4].resolved_at is None


def test_replay_all_exits_non_zero_when_any_replay_fails(monkeypatch, caplog):
    monkeypatch.setattr(dlq_handler, "REPLAY_BATCH_SIZE", 1)
    calls: list[int] = []

    def first_fails(db, body):
        calls.append(1)
        if len(calls) == 1:
            raise DatabaseDown()

    code, table, _ = _run(monkeypatch, caplog, [_row(1), _row(2)], ["--replay-all", GHL_OPT_OUT], handler=first_fails)
    assert code == 1 and table.rows[1].resolved_at is None and table.rows[2].resolved_at == "now"


def test_show_prints_the_failure_and_its_payload_keys_never_the_values(monkeypatch, caplog):
    code, _, out = _run(monkeypatch, caplog, [_row(7)], ["--show", "7"])
    assert code == 0
    for expected in ("#7", GHL_OPT_OUT, "ghl-c-42", "attempts 5", "DatabaseDown", "2026-11-20",
                     "contact_id, message, phone"):
        assert expected in out
    assert PHONE not in out and "stop texting" not in out


def test_show_of_an_unknown_id_exits_non_zero(monkeypatch, caplog):
    code, _, out = _run(monkeypatch, caplog, [], ["--show", "99"])
    assert code == 1 and "#99 not found" in out


def test_list_prints_one_line_per_failure_with_its_state(monkeypatch, caplog):
    rows = [_row(1), _row(2, attempts=2, gave_up_at=None), _row(3, gave_up_at=None, resolved_at=GAVE_UP)]
    code, _, out = _run(monkeypatch, caplog, rows, ["--list"])
    lines = {line.split("[dlq] ")[1].split()[0]: line for line in out.splitlines() if "[dlq] #" in line}
    assert code == 0 and set(lines) == {"#1", "#2", "#3"}
    assert "gave up" in lines["#1"] and "open" in lines["#2"] and "resolved" in lines["#3"]
    assert PHONE not in out and "stop texting" not in out
