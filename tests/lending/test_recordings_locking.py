"""check_pending holds no transaction or row lock across an HTTP call. Database-free (fake session)."""
from datetime import datetime, timezone

from src.lending.recordings import check_pending

NOW = datetime(2026, 10, 6, 16, 0, tzinfo=timezone.utc)


class FakeDb:
    def __init__(self, rows):
        self.rows, self.events = rows, []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.events.append(("exec", sql))
        db = self

        class Result:
            def mappings(self):
                return self

            def all(self):
                return db.rows
        return Result()

    def commit(self):
        self.events.append(("commit", ""))


def test_no_lock_and_no_open_transaction_during_the_http_call():
    rows = [{"id": i, "dialer_call_id": f"c{i}", "recording_ref": f"https://x/{i}", "recording_status": "pending"}
            for i in (1, 2)]
    db = FakeDb(rows)
    seen = []

    def head(url):
        seen.append(db.events[-1][0])  # the last thing before the HTTP call must be a commit
        return 200 if url.endswith("1") else 403

    stats = check_pending(db, head, now=NOW)
    assert seen == ["commit", "commit"]
    assert all("FOR UPDATE" not in sql for kind, sql in db.events if kind == "exec")
    updates = [sql for kind, sql in db.events if kind == "exec" and sql.lstrip().startswith("UPDATE")]
    assert len(updates) == 2 and all("recording_status IN ('pending', 'forbidden')" in u for u in updates)
    assert (stats.readable, stats.forbidden) == (1, 1)
