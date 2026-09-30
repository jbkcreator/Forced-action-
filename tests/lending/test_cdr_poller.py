from unittest.mock import MagicMock

from src.lending import cdr_poller
from src.lending.cdr_poll import IngestStats


def test_cycle_polls_new_every_time_and_rescans_only_when_due(monkeypatch):
    poll, rescan = MagicMock(return_value=IngestStats()), MagicMock(return_value=IngestStats())
    monkeypatch.setattr(cdr_poller, "poll_new", poll)
    monkeypatch.setattr(cdr_poller, "rescan_today", rescan)
    cdr_poller.run_cycle("db", "http", rescan=False)
    cdr_poller.run_cycle("db", "http", rescan=True)
    assert poll.call_count == 2 and rescan.call_count == 1


def test_poller_refuses_to_start_without_an_api_key(monkeypatch):
    monkeypatch.setattr(cdr_poller, "get_http", lambda: None)
    assert cdr_poller.main(["--once"]) == 2
