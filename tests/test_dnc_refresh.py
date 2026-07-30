from unittest.mock import patch

from src.tasks import dnc_refresh


def _stats():
    return {
        "total": 0,
        "dnc_hits": 0,
        "litigator_hits": 0,
        "suppressed": 0,
        "failed": 0,
        "normalize_skipped": 0,
        "already_suppressed": 0,
        "fresh_clean_skipped": 0,
        "unmatched_csv": 0,
        "tracerfy_unknown": 0,
        "skipped": False,
    }


def test_run_scrub_skips_already_suppressed_without_api_call():
    stats = _stats()

    with patch.object(dnc_refresh, "_submit_scrub_batch") as submit:
        dnc_refresh._run_scrub(
            label="owners",
            rows=[(1, "+18135550100")],
            api_key="key",
            stats=stats,
            suppressed_phones={"+18135550100"},
        )

    submit.assert_not_called()
    assert stats["already_suppressed"] == 1


def test_run_scrub_skips_fresh_clean_without_api_call():
    stats = _stats()

    with patch.object(dnc_refresh, "_submit_scrub_batch") as submit:
        dnc_refresh._run_scrub(
            label="subscribers",
            rows=[(1, "+18135550101")],
            api_key="key",
            stats=stats,
            fresh_clean_phones={"+18135550101"},
        )

    submit.assert_not_called()
    assert stats["fresh_clean_skipped"] == 1


def test_run_scrub_counts_tracerfy_unknown_when_csv_omits_phone():
    stats = _stats()

    with patch.object(dnc_refresh, "_submit_scrub_batch", return_value="queue_1"), \
         patch.object(dnc_refresh, "_poll_queue", return_value=[]), \
         patch.object(dnc_refresh, "get_db_context") as ctx:
        dnc_refresh._run_scrub(
            label="owners",
            rows=[(1, "+18135550102")],
            api_key="key",
            stats=stats,
        )

    ctx.assert_called_once()
    assert stats["tracerfy_unknown"] == 1
    assert stats["suppressed"] == 0


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _CaptureSession:
    def __init__(self):
        self.sql = None
        self.params = None

    def execute(self, sql, params):
        self.sql = str(sql)
        self.params = params
        return _Rows([(7, "+18135550107")])


def test_collect_owner_phones_filters_by_enriched_contact_source_when_given():
    from datetime import datetime, timezone

    session = _CaptureSession()
    rows = dnc_refresh._collect_owner_phones(
        session,
        "hillsborough",
        datetime.now(timezone.utc),
        source="tracerfy",
    )

    assert rows == [(7, "+18135550107")]
    assert "FROM enriched_contacts ec" in session.sql
    assert "ec.source = :source" in session.sql
    assert "o.phone_1 IN (ec.mobile_phone, ec.landline)" in session.sql
    assert session.params["source"] == "tracerfy"


def test_collect_owner_phones_does_not_join_enriched_contacts_without_source():
    from datetime import datetime, timezone

    session = _CaptureSession()
    dnc_refresh._collect_owner_phones(
        session,
        "hillsborough",
        datetime.now(timezone.utc),
    )

    assert "FROM enriched_contacts ec" not in session.sql
    assert "source" not in session.params


# ---------------------------------------------------------------------------
# _reenqueue_dnc_blocked_subscriber_calls
# ---------------------------------------------------------------------------

class _ReenqueueSession:
    """Fake session that returns configurable rows from execute()."""

    def __init__(self, rows):
        self._rows = rows

    def execute(self, sql, params=None):
        return _Rows(self._rows)


def test_reenqueue_dispatches_for_dnc_blocked_subscriber_with_clean_result():
    session = _ReenqueueSession([(42, "+18135550200")])

    published = []
    with patch("src.agents.events.ingestion.publish_lifecycle_event", side_effect=published.append):
        count = dnc_refresh._reenqueue_dnc_blocked_subscriber_calls(session)

    assert count == 1
    assert published[0]["event_type"] == "new_lead_signup"
    assert published[0]["subscriber_id"] == 42
    assert published[0]["source"] == "dnc_refresh_retry"


def test_reenqueue_returns_zero_when_no_blocked_decisions():
    session = _ReenqueueSession([])

    with patch("src.agents.events.ingestion.publish_lifecycle_event") as pub:
        count = dnc_refresh._reenqueue_dnc_blocked_subscriber_calls(session)

    assert count == 0
    pub.assert_not_called()


def test_reenqueue_continues_after_publish_failure():
    """A single publish error must not abort re-enqueue for remaining subscribers."""
    session = _ReenqueueSession([
        (10, "+18135550201"),
        (11, "+18135550202"),
    ])

    call_count = 0

    def _flaky(event):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("Redis unavailable")

    with patch("src.agents.events.ingestion.publish_lifecycle_event", side_effect=_flaky):
        count = dnc_refresh._reenqueue_dnc_blocked_subscriber_calls(session)

    # subscriber 10 failed, subscriber 11 succeeded — count reflects successes only
    assert count == 1
