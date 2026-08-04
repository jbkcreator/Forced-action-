from sqlalchemy import text

from src.services.source_failover import (
    confidence_penalty_for,
    label_backup_sourced,
    maybe_failover,
    mark_recovered,
)


def _seed_county_source(db, source_type: str, county_id: str, alternate_url: str | None = None):
    db.execute(text("""
        INSERT INTO counties (county_id, display_name, is_active)
        VALUES (:county_id, :county_id, TRUE)
        ON CONFLICT (county_id) DO NOTHING
    """), {"county_id": county_id})
    db.execute(text("""
        INSERT INTO county_sources (
            county_id, signal_type, url, alternate_url, alternate_source_name,
            date_range_available, is_active
        )
        VALUES (:county_id, :signal_type, 'https://primary.example.com', :alt_url,
                CASE WHEN :alt_url IS NOT NULL THEN 'Backup Portal' ELSE NULL END,
                TRUE, TRUE)
        ON CONFLICT (county_id, signal_type) DO UPDATE
            SET alternate_url = EXCLUDED.alternate_url,
                alternate_source_name = EXCLUDED.alternate_source_name,
                active_source = 'primary'
    """), {"county_id": county_id, "signal_type": source_type, "alt_url": alternate_url})
    db.commit()


def test_maybe_failover_no_county_sources_row_returns_none(fresh_db):
    result = maybe_failover(fresh_db, "totally_unconfigured_source", "hillsborough")
    assert result is None


def test_maybe_failover_no_alternate_logs_and_alerts(fresh_db):
    _seed_county_source(fresh_db, "failover_test_no_alt", "hillsborough", alternate_url=None)

    result = maybe_failover(fresh_db, "failover_test_no_alt", "hillsborough")
    fresh_db.commit()

    assert result is not None
    assert result.event == "no_alternate_configured"

    row = fresh_db.execute(text("""
        SELECT event_type, detail FROM source_failover_log
        WHERE source_type = 'failover_test_no_alt' AND county_id = 'hillsborough'
        ORDER BY id DESC LIMIT 1
    """)).fetchone()
    assert row.event_type == "no_alternate_configured"
    assert "no alternate configured" in row.detail.lower()

    # active_source must NOT have flipped — no self-referencing fallback (E3-revised)
    src_row = fresh_db.execute(text("""
        SELECT active_source FROM county_sources
        WHERE county_id = 'hillsborough' AND signal_type = 'failover_test_no_alt'
    """)).fetchone()
    assert src_row.active_source == "primary"


def test_maybe_failover_switches_when_alternate_configured(fresh_db):
    _seed_county_source(fresh_db, "failover_test_with_alt", "hillsborough",
                         alternate_url="https://backup.example.com")

    result = maybe_failover(fresh_db, "failover_test_with_alt", "hillsborough")
    fresh_db.commit()

    assert result.event == "switched_to_alternate"
    src_row = fresh_db.execute(text("""
        SELECT active_source, switched_to_alternate_at FROM county_sources
        WHERE county_id = 'hillsborough' AND signal_type = 'failover_test_with_alt'
    """)).fetchone()
    assert src_row.active_source == "alternate"
    assert src_row.switched_to_alternate_at is not None


def test_maybe_failover_is_idempotent_once_already_on_alternate(fresh_db):
    _seed_county_source(fresh_db, "failover_test_idempotent", "hillsborough",
                         alternate_url="https://backup.example.com")
    maybe_failover(fresh_db, "failover_test_idempotent", "hillsborough")
    fresh_db.commit()

    # second call: already on alternate — must be a no-op (no duplicate log row)
    before = fresh_db.execute(text(
        "SELECT COUNT(*) AS c FROM source_failover_log WHERE source_type = 'failover_test_idempotent'"
    )).fetchone().c
    result2 = maybe_failover(fresh_db, "failover_test_idempotent", "hillsborough")
    fresh_db.commit()
    after = fresh_db.execute(text(
        "SELECT COUNT(*) AS c FROM source_failover_log WHERE source_type = 'failover_test_idempotent'"
    )).fetchone().c
    assert result2 is None
    assert after == before


def test_mark_recovered_switches_back_to_primary(fresh_db):
    _seed_county_source(fresh_db, "failover_test_recover", "hillsborough",
                         alternate_url="https://backup.example.com")
    maybe_failover(fresh_db, "failover_test_recover", "hillsborough")
    fresh_db.commit()

    mark_recovered(fresh_db, "failover_test_recover", "hillsborough")
    fresh_db.commit()

    src_row = fresh_db.execute(text("""
        SELECT active_source, switched_to_alternate_at FROM county_sources
        WHERE county_id = 'hillsborough' AND signal_type = 'failover_test_recover'
    """)).fetchone()
    assert src_row.active_source == "primary"
    assert src_row.switched_to_alternate_at is None

    row = fresh_db.execute(text("""
        SELECT event_type FROM source_failover_log
        WHERE source_type = 'failover_test_recover' ORDER BY id DESC LIMIT 1
    """)).fetchone()
    assert row.event_type == "switched_back_to_primary"


def test_confidence_penalty_zero_on_primary(fresh_db):
    _seed_county_source(fresh_db, "failover_test_penalty_primary", "hillsborough",
                         alternate_url="https://backup.example.com")
    assert confidence_penalty_for(fresh_db, "failover_test_penalty_primary", "hillsborough") == 0


def test_confidence_penalty_nonzero_on_alternate(fresh_db):
    _seed_county_source(fresh_db, "failover_test_penalty_alt", "hillsborough",
                         alternate_url="https://backup.example.com")
    maybe_failover(fresh_db, "failover_test_penalty_alt", "hillsborough")
    fresh_db.commit()
    assert confidence_penalty_for(fresh_db, "failover_test_penalty_alt", "hillsborough") == 20


def test_label_backup_sourced_does_not_mutate_input():
    record = {"address": "123 Main St"}
    labeled = label_backup_sourced(record, "foreclosures", "hillsborough", 20)
    assert "source_label" not in record
    assert labeled["source_label"] == "backup:foreclosures/hillsborough"
    assert labeled["confidence_penalty_applied"] == 20
