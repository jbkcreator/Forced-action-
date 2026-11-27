"""Retention purge (R1 0.5 D11): bodies of processed journal rows and resolved webhook failures are emptied after
30 days, in batches; open / failed / given-up rows keep their body.

No database: the session is a fake that records each statement and answers with a scripted rowcount.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from types import SimpleNamespace

from src.lending.inbound import retention


class FakeLending:
    """Records every statement; each UPDATE answers the next scripted rowcount for its table (0 when exhausted)."""

    def __init__(self, events=(), failures=()):
        self.counts = {"inbound_events": list(events), "webhook_failures": list(failures)}
        self.statements: list[tuple[str, dict]] = []
        self.commits = 0

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        self.statements.append((sql, dict(params or {})))
        table = "inbound_events" if "lending.inbound_events" in sql else "webhook_failures"
        queue = self.counts[table]
        return SimpleNamespace(rowcount=queue.pop(0) if queue else 0)

    @contextmanager
    def session(self):
        yield self
        self.commits += 1

    def sql_for(self, table):
        return [(sql, params) for sql, params in self.statements if f"lending.{table}" in sql]


def test_processed_journal_rows_lose_raw_after_30_days_and_keep_metadata():
    fake = FakeLending(events=[3])
    result = retention.purge(session_factory=fake.session)

    assert result.events_purged == 3
    sql, params = fake.sql_for("inbound_events")[0]
    assert sql.startswith("UPDATE lending.inbound_events SET raw = NULL")
    assert "status = 'processed'" in sql
    assert "raw IS NOT NULL" in sql
    assert "processed_at < now() - make_interval(days => :days)" in sql
    assert params["days"] == 30
    assert "FOR UPDATE SKIP LOCKED" in sql


def test_resolved_failures_get_empty_payload_since_the_column_is_not_null():
    fake = FakeLending(failures=[2])
    result = retention.purge(session_factory=fake.session)

    assert result.failures_purged == 2
    sql, params = fake.sql_for("webhook_failures")[0]
    assert sql.startswith("UPDATE lending.webhook_failures SET payload = '{}'::jsonb")
    assert "resolved_at < now() - make_interval(days => :days)" in sql
    assert "payload <> '{}'::jsonb" in sql
    assert params["days"] == 30


def test_open_failed_and_given_up_rows_are_never_selected():
    fake = FakeLending()
    retention.purge(session_factory=fake.session)

    events_sql = fake.sql_for("inbound_events")[0][0]
    assert "'failed'" not in events_sql and "'received'" not in events_sql
    failures_sql = fake.sql_for("webhook_failures")[0][0]
    # only resolved rows qualify: an open or given-up row has resolved_at NULL, which never satisfies "<"
    assert "resolved_at <" in failures_sql
    assert "gave_up_at" not in failures_sql


def test_purges_in_batches_until_a_short_batch_each_in_its_own_transaction():
    fake = FakeLending(events=[2, 2, 1], failures=[2, 0])
    result = retention.purge(session_factory=fake.session, batch_size=2)

    assert (result.events_purged, result.failures_purged) == (5, 2)
    assert len(fake.sql_for("inbound_events")) == 3
    assert len(fake.sql_for("webhook_failures")) == 2
    assert all(params["limit"] == 2 for _, params in fake.statements)
    assert fake.commits == 5


def test_batch_cap_stops_a_runaway_loop():
    fake = FakeLending(events=[2] * 50)
    result = retention.purge(session_factory=fake.session, batch_size=2, max_batches=4)

    assert result.events_purged == 8
    assert len(fake.sql_for("inbound_events")) == 4


def test_main_does_nothing_while_the_journal_is_off(monkeypatch):
    fake = FakeLending(events=[1])
    monkeypatch.setattr(retention, "journal_enabled", lambda: False)

    assert retention.main([], session_factory=fake.session) == 0
    assert fake.statements == []


def test_main_purges_and_logs_counts_only(monkeypatch, caplog):
    fake = FakeLending(events=[4], failures=[1])
    monkeypatch.setattr(retention, "journal_enabled", lambda: True)

    with caplog.at_level(logging.INFO):
        assert retention.main([], session_factory=fake.session) == 0
    assert "events=4" in caplog.text and "failures=1" in caplog.text


def test_main_returns_1_and_logs_class_only_when_the_database_fails(monkeypatch, caplog):
    class DatabaseDown(Exception):
        pass

    @contextmanager
    def broken():
        raise DatabaseDown("postgres://secret@host")
        yield  # pragma: no cover

    monkeypatch.setattr(retention, "journal_enabled", lambda: True)
    assert retention.main([], session_factory=broken) == 1
    assert "DatabaseDown" in caplog.text
    assert "secret" not in caplog.text
