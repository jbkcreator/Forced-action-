import os
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from src.lending.recordings import check_pending

pytestmark = pytest.mark.skipif(not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL")

NOW = datetime(2026, 10, 6, 16, 0, tzinfo=timezone.utc)
URL = "https://app.batchdialer.com/api/callrecording/1"


def _row(db, call_id="c1", ref=URL, status="pending"):
    db.execute(text(
        "INSERT INTO lending.call_dispositions (dialer_call_id, recording_ref, recording_status, call_ended_at, raw_event) "
        "VALUES (:c, :r, :s, :e, '{}'::jsonb)"), {"c": call_id, "r": ref, "s": status, "e": NOW})


def _status(db, call_id="c1"):
    return db.execute(text("SELECT recording_status FROM lending.call_dispositions WHERE dialer_call_id = :c"),
                      {"c": call_id}).scalar()


def test_200_is_readable_and_is_not_checked_again(lending_db):
    _row(lending_db)
    check_pending(lending_db, lambda url: 200, now=NOW)
    calls = []
    check_pending(lending_db, lambda url: calls.append(url) or 200, now=NOW + timedelta(hours=2))
    assert _status(lending_db) == "readable" and calls == []


def test_403_stays_forbidden_waits_then_flips_when_permission_is_enabled(lending_db):
    _row(lending_db)
    check_pending(lending_db, lambda url: 403, now=NOW)
    assert _status(lending_db) == "forbidden"
    calls = []
    check_pending(lending_db, lambda url: calls.append(url) or 403, now=NOW + timedelta(minutes=5))
    assert calls == []  # too soon
    check_pending(lending_db, lambda url: 200, now=NOW + timedelta(minutes=31))
    assert _status(lending_db) == "readable"  # no code change needed


def test_403_never_gives_up(lending_db):
    _row(lending_db)
    for i in range(10):
        check_pending(lending_db, lambda url: 403, now=NOW + timedelta(minutes=31 * i))
    assert _status(lending_db) == "forbidden"


def test_404_is_missing_and_5xx_or_network_error_stays_pending(lending_db):
    _row(lending_db, "c1", ref=URL)
    _row(lending_db, "c2", ref=URL + "2")

    def head(url):
        if url.endswith("2"):
            raise OSError("dns")
        return 404

    check_pending(lending_db, head, now=NOW)
    assert _status(lending_db, "c1") == "missing" and _status(lending_db, "c2") == "pending"
    check_pending(lending_db, lambda url: 503, now=NOW + timedelta(minutes=31))
    assert _status(lending_db, "c2") == "pending"


def test_calls_without_a_recording_are_ignored(lending_db):
    _row(lending_db, ref=None, status=None)
    calls = []
    check_pending(lending_db, lambda url: calls.append(url) or 200, now=NOW)
    assert calls == []
