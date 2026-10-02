from unittest.mock import MagicMock

from src.lending import cdr_poller
from src.lending.cdr_poll import IngestStats


def test_cycle_polls_new_every_time_and_rescans_only_when_due(monkeypatch):
    poll, rescan = MagicMock(return_value=IngestStats()), MagicMock(return_value=IngestStats())
    monkeypatch.setattr(cdr_poller, "poll_new", poll)
    monkeypatch.setattr(cdr_poller, "rescan_today", rescan)
    retry = MagicMock(return_value=0)
    monkeypatch.setattr(cdr_poller, "retry_unpropagated_dnc", retry)
    cdr_poller.run_cycle("db", "http", rescan=False)
    cdr_poller.run_cycle("db", "http", rescan=True)
    assert poll.call_count == 2 and rescan.call_count == 1 and retry.call_count == 2


def test_poller_refuses_to_start_without_an_api_key(monkeypatch):
    monkeypatch.setattr(cdr_poller, "get_http", lambda: None)
    assert cdr_poller.main(["--once"]) == 2


def _run_main(monkeypatch, argv, campaigns):
    from contextlib import contextmanager

    db = MagicMock()
    db.execute.return_value.scalar.return_value = True

    @contextmanager
    def session():
        yield db

    rescan, cycle = MagicMock(return_value=IngestStats()), MagicMock(return_value=IngestStats())
    monkeypatch.setattr(cdr_poller, "get_http", lambda: object())
    monkeypatch.setattr(cdr_poller, "lending_session", session)
    monkeypatch.setattr(cdr_poller, "lending_campaign_ids", lambda: campaigns)
    monkeypatch.setattr(cdr_poller, "rescan_today", rescan)
    monkeypatch.setattr(cdr_poller, "run_cycle", cycle)
    return cdr_poller.main(argv), rescan, cycle


def test_backfill_days_rescans_that_many_days_first(monkeypatch):
    code, rescan, cycle = _run_main(monkeypatch, ["--once", "--backfill-days", "5"], frozenset({"55"}))
    assert code == 0 and rescan.call_args.kwargs["days"] == 5 and not cycle.called


def test_empty_campaign_list_refuses_to_start(monkeypatch, caplog):
    with caplog.at_level("ERROR"):
        code, _, cycle = _run_main(monkeypatch, ["--once"], frozenset())
    assert code == 2 and not cycle.called
    assert "LENDING_DIALER_CAMPAIGN_IDS is empty" in caplog.text
