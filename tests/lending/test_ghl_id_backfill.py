"""GHL id backfill for dialer load records: lookup counting and batched writes. No database, no GHL."""
from __future__ import annotations

from src.lending import ghl_id_backfill as bf


def test_look_up_keeps_found_ids_and_counts_misses_and_failures():
    def find(phone):
        if phone == "+3":
            raise TimeoutError()
        return {"+1": "g1"}.get(phone)

    waits = []
    found, counts = bf.look_up(["+1", "+2", "+3"], find, sleep=waits.append)
    assert found == {"+1": "g1"}
    assert counts == {"found": 1, "not_in_ghl": 1, "failed": 1}
    assert len(waits) == 2  # a gap between calls, none before the first


class FakeDb:
    def __init__(self):
        self.batches, self.commits = [], 0

    def execute(self, _query, params):
        self.batches.append(params["phones"])
        return type("R", (), {"rowcount": len(params["phones"])})()

    def commit(self):
        self.commits += 1


def test_write_batches_and_commits_each_batch(monkeypatch):
    monkeypatch.setattr(bf, "WRITE_BATCH", 2)
    db = FakeDb()
    assert bf.write(db, {"+1": "a", "+2": "b", "+3": "c"}) == 3
    assert [len(b) for b in db.batches] == [2, 1] and db.commits == 2


def test_write_nothing_found_does_nothing():
    db = FakeDb()
    assert bf.write(db, {}) == 0 and db.batches == []
