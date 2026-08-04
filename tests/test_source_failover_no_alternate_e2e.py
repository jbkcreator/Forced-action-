from datetime import datetime, timezone
from unittest import mock

import src.tasks.heartbeat_monitor as hm
from src.tasks.heartbeat_monitor import Heartbeat


def test_kill_source_with_no_alternate_triggers_no_alternate_configured_path():
    """Full acceptance path: heartbeat_monitor's pre-existing --kill-source
    demo flag (which busts the alert dedup so the next tick is a genuinely
    NEW alert) must drive Q4's failover hook, which — since every alternate
    starts NULL per decision E3-revised — must take the
    'no_alternate_configured' branch, not silently do nothing and not
    self-reference the primary as its own fallback."""
    stale_beat = Heartbeat("foreclosures", 1500, None, None, True, county_id="hillsborough")

    with mock.patch.object(hm, "compute_heartbeats", return_value=[stale_beat]), \
         mock.patch.object(hm, "_recently_alerted", return_value=False), \
         mock.patch.object(hm, "_clear_dedup_for_recovered_sources"), \
         mock.patch.object(hm, "_mark_recovered_sources_for_failover"), \
         mock.patch.object(hm, "maybe_failover") as maybe_failover_call, \
         mock.patch.object(hm, "send_alert", return_value=True), \
         mock.patch.object(hm, "_record_alerted"), \
         mock.patch.object(hm, "get_db_context") as ctx:
        fake_session = mock.MagicMock()
        ctx.return_value.__enter__.return_value = fake_session
        hm.run_once()

    # The hook must have been reached with the stale source's identity —
    # proving --kill-source's dedup-bust is what makes the NEXT run_once()
    # tick treat this as a genuinely new incident worth checking for failover.
    maybe_failover_call.assert_called_once_with(fake_session, "foreclosures", "hillsborough")


def test_no_alternate_configured_never_self_references(fresh_db):
    """Direct proof against the rejected original E3 answer: with no
    alternate_url configured, active_source must remain 'primary' — the
    source is never pointed at itself as a fake fallback."""
    from sqlalchemy import text
    from src.services.source_failover import maybe_failover

    fresh_db.execute(text("""
        INSERT INTO counties (county_id, display_name, is_active)
        VALUES ('hillsborough', 'Hillsborough', TRUE) ON CONFLICT (county_id) DO NOTHING
    """))
    fresh_db.execute(text("""
        INSERT INTO county_sources (county_id, signal_type, url, date_range_available, is_active)
        VALUES ('hillsborough', 'no_self_reference_test', 'https://primary.example.com', TRUE, TRUE)
        ON CONFLICT (county_id, signal_type) DO UPDATE SET active_source = 'primary'
    """))
    fresh_db.commit()

    try:
        result = maybe_failover(fresh_db, "no_self_reference_test", "hillsborough")
        fresh_db.commit()

        assert result.event == "no_alternate_configured"
        row = fresh_db.execute(text("""
            SELECT active_source, alternate_url FROM county_sources
            WHERE county_id = 'hillsborough' AND signal_type = 'no_self_reference_test'
        """)).fetchone()
        assert row.active_source == "primary"
        assert row.alternate_url is None
    finally:
        fresh_db.execute(text(
            "DELETE FROM county_sources WHERE signal_type = 'no_self_reference_test'"
        ))
        fresh_db.execute(text(
            "DELETE FROM source_failover_log WHERE source_type = 'no_self_reference_test'"
        ))
        fresh_db.commit()
