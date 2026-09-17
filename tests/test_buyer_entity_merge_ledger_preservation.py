"""Regression tests for PR #268/#271 review findings:

merge_entities must reassign the absorbed entity's buyer_entity_links,
borrower_ledger_events, borrower_monitor_log, and closer_calls rows to the
surviving entity BEFORE deleting the absorbed buyer_entities row (the first
three FK it ON DELETE CASCADE, closer_calls has no ondelete clause at all) —
otherwise the append-only audit trail is silently wiped and unmerge cannot
bring it back.

Session is faked (the repo's borrower tests are mock-based); these assert the
reassign SQL is issued before the delete and that the moved ids round-trip
through the merge log so unmerge can reverse them by exact id, never a
timestamp heuristic.
"""
from datetime import datetime, timezone

from src.services import buyer_entity_merge as m


class _FakeResult:
    def __init__(self, rows=None, one=None, rowcount=0):
        self._rows = rows if rows is not None else []
        self._one = one
        self.rowcount = rowcount

    def mappings(self):
        return self

    def one_or_none(self):
        return self._one

    def all(self):
        return self._rows

    def fetchall(self):
        return self._rows


class _RecordingSession:
    """Records executed SQL (as text) in order; returns canned results by keyword."""

    def __init__(self, responder):
        self._responder = responder
        self.executed = []  # (sql_text, params)
        self.added = []

    def execute(self, clause, params=None):
        sql = str(clause)
        self.executed.append((sql, params))
        return self._responder(sql, params)

    def add(self, obj):
        self.added.append(obj)

    def flush(self):
        pass

    def _index_of(self, needle):
        for i, (sql, _) in enumerate(self.executed):
            if needle in sql:
                return i
        return -1


def _merge_responder(absorbed_row, surviving_row, link_ids, ledger_ids, monitor_ids, closer_ids):
    def respond(sql, params):
        if "FROM buyer_entities WHERE id" in sql and "FOR UPDATE" in sql:
            pid = params["id"]
            return _FakeResult(one=surviving_row if pid == surviving_row["id"] else absorbed_row)
        if "UPDATE buyer_entity_links" in sql:
            return _FakeResult(rows=[(i,) for i in link_ids])
        if "UPDATE borrower_ledger_events" in sql:
            return _FakeResult(rows=[(i,) for i in ledger_ids])
        if "UPDATE borrower_monitor_log" in sql:
            return _FakeResult(rows=[(i,) for i in monitor_ids])
        if "UPDATE closer_calls" in sql:
            return _FakeResult(rows=[(i,) for i in closer_ids])
        return _FakeResult(rowcount=1)

    return respond


def test_merge_reassigns_ledger_and_monitor_before_delete():
    absorbed = {"id": 2, "canonical_name": "ABSORBED LLC"}
    surviving = {"id": 1, "canonical_name": "SURVIVING LLC"}
    sess = _RecordingSession(_merge_responder(absorbed, surviving, [30], [10, 11], [20], [40]))

    log = m.merge_entities(sess, surviving_id=1, absorbed_id=2, merged_by="admin")

    ledger_update = sess._index_of("UPDATE borrower_ledger_events")
    monitor_update = sess._index_of("UPDATE borrower_monitor_log")
    delete_idx = sess._index_of("DELETE FROM buyer_entities")

    assert ledger_update != -1, "ledger events must be reassigned"
    assert monitor_update != -1, "monitor log must be reassigned"
    assert delete_idx != -1
    # The audit trail must move BEFORE the cascade-triggering delete.
    assert ledger_update < delete_idx
    assert monitor_update < delete_idx
    # Moved ids are captured on the log so unmerge can reverse precisely.
    assert log.moved_ledger_event_ids == [10, 11]
    assert log.moved_monitor_log_ids == [20]
    assert log.moved_link_ids == [30]
    assert log.moved_closer_call_ids == [40]


def test_merge_with_no_history_records_empty_lists():
    absorbed = {"id": 2}
    surviving = {"id": 1}
    sess = _RecordingSession(_merge_responder(absorbed, surviving, [], [], [], []))

    log = m.merge_entities(sess, surviving_id=1, absorbed_id=2, merged_by="admin")

    assert log.moved_ledger_event_ids == []
    assert log.moved_monitor_log_ids == []


def test_unmerge_moves_history_back_to_restored_entity():
    merged_at = datetime(2026, 9, 16, tzinfo=timezone.utc)
    log_row = {
        "id": 5,
        "surviving_id": 1,
        "absorbed_id": 2,
        "absorbed_snapshot": {"id": 2, "canonical_name": "ABSORBED LLC"},
        "merged_at": merged_at,
        "reversed_at": None,
        "moved_link_ids": [30],
        "moved_ledger_event_ids": [10, 11],
        "moved_monitor_log_ids": [20],
        "moved_closer_call_ids": [40],
    }

    def respond(sql, params):
        if "FROM buyer_entity_merge_log WHERE id" in sql:
            return _FakeResult(one=log_row)
        if "UPDATE buyer_entity_links" in sql:
            return _FakeResult(rows=[(i,) for i in log_row["moved_link_ids"]])
        return _FakeResult(rowcount=1)

    sess = _RecordingSession(respond)

    # add() assigns no id in the fake; patch flush to stamp the restored entity.
    def _flush():
        for obj in sess.added:
            if getattr(obj, "id", None) is None:
                obj.id = 99
    sess.flush = _flush

    m.unmerge_entity(sess, merge_log_id=5, reversed_by="admin")

    assert sess._index_of("UPDATE borrower_ledger_events") != -1
    assert sess._index_of("UPDATE borrower_monitor_log") != -1
    # Both reassign to the restored id (99).
    ledger_call = next(p for s, p in sess.executed if "UPDATE borrower_ledger_events" in s)
    assert ledger_call["restored_id"] == 99
    assert ledger_call["moved_ledger_event_ids"] == [10, 11]
