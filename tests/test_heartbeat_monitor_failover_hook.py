from datetime import datetime, timezone
from unittest import mock

import src.tasks.heartbeat_monitor as hm
from src.tasks.heartbeat_monitor import Heartbeat


def test_trigger_failover_check_calls_maybe_failover_with_db_session():
    b = Heartbeat("foreclosures", 1500, None, None, True, county_id="hillsborough")
    fake_session = mock.MagicMock()
    fake_ctx = mock.MagicMock()
    fake_ctx.__enter__.return_value = fake_session
    with mock.patch.object(hm, "get_db_context", return_value=fake_ctx), \
         mock.patch.object(hm, "maybe_failover") as maybe_failover:
        hm._trigger_failover_check(b)
    maybe_failover.assert_called_once_with(fake_session, "foreclosures", "hillsborough")
    fake_session.commit.assert_called_once()


def test_trigger_failover_check_swallows_exceptions():
    b = Heartbeat("foreclosures", 1500, None, None, True, county_id="hillsborough")
    with mock.patch.object(hm, "get_db_context", side_effect=RuntimeError("db down")):
        hm._trigger_failover_check(b)  # must not raise


def test_mark_recovered_sources_for_failover_only_calls_for_recovered_beats():
    stale_beat = Heartbeat("foreclosures", 1500, None, None, True, county_id="hillsborough")
    recovered_beat = Heartbeat("permits", 1500, datetime.now(timezone.utc), 5, False, county_id="hillsborough")
    fake_session = mock.MagicMock()
    fake_ctx = mock.MagicMock()
    fake_ctx.__enter__.return_value = fake_session
    with mock.patch.object(hm, "get_db_context", return_value=fake_ctx), \
         mock.patch.object(hm, "mark_recovered") as mark_recovered:
        hm._mark_recovered_sources_for_failover([stale_beat, recovered_beat])
    mark_recovered.assert_called_once_with(fake_session, "permits", "hillsborough")


def test_mark_recovered_sources_for_failover_noop_when_nothing_recovered():
    stale_beat = Heartbeat("foreclosures", 1500, None, None, True, county_id="hillsborough")
    with mock.patch.object(hm, "get_db_context") as ctx, \
         mock.patch.object(hm, "mark_recovered") as mark_recovered:
        hm._mark_recovered_sources_for_failover([stale_beat])
    ctx.assert_not_called()
    mark_recovered.assert_not_called()


def test_run_once_triggers_failover_only_for_newly_alerted_stale_sources():
    """New alert (not recently alerted) -> failover check fires.
    Already-alerted (cooldown active) -> failover check must NOT fire again
    (rides on the existing dedup, per this plan's Architecture)."""
    new_stale = Heartbeat("foreclosures", 1500, None, None, True, county_id="hillsborough")
    already_alerted_stale = Heartbeat("permits", 1500, None, None, True, county_id="hillsborough")

    def fake_recently_alerted(source_type, county_id):
        return source_type == "permits"

    with mock.patch.object(hm, "compute_heartbeats", return_value=[new_stale, already_alerted_stale]), \
         mock.patch.object(hm, "_recently_alerted", side_effect=fake_recently_alerted), \
         mock.patch.object(hm, "_clear_dedup_for_recovered_sources"), \
         mock.patch.object(hm, "_mark_recovered_sources_for_failover"), \
         mock.patch.object(hm, "_trigger_failover_check") as trigger, \
         mock.patch.object(hm, "send_alert", return_value=True), \
         mock.patch.object(hm, "_record_alerted"):
        hm.run_once()

    trigger.assert_called_once_with(new_stale)


def test_run_once_dry_run_never_triggers_failover():
    new_stale = Heartbeat("foreclosures", 1500, None, None, True, county_id="hillsborough")
    with mock.patch.object(hm, "compute_heartbeats", return_value=[new_stale]), \
         mock.patch.object(hm, "_recently_alerted", return_value=False), \
         mock.patch.object(hm, "_trigger_failover_check") as trigger, \
         mock.patch.object(hm, "_mark_recovered_sources_for_failover") as recover:
        hm.run_once(dry_run=True)
    trigger.assert_not_called()
    recover.assert_not_called()
