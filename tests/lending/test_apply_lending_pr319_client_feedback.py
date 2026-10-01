from sqlalchemy import text

from migrations.apply_lending_pr319_client_feedback import apply_to


def test_migration_is_idempotent(lending_db):
    conn = lending_db.connection()
    apply_to(conn)
    apply_to(conn)
    cols = {r[0] for r in conn.execute(text(
        "SELECT column_name FROM information_schema.columns WHERE table_schema='lending' AND table_name='call_dispositions'"))}
    assert {"consent_checked_at", "recording_status", "recording_checked_at", "dialer_campaign_id"} <= cols
    assert conn.execute(text("SELECT to_regclass('lending.text_consents')")).scalar() is not None


def test_recording_backfill_marks_existing_links_pending(lending_db):
    conn = lending_db.connection()
    conn.execute(text(
        "INSERT INTO lending.call_dispositions (dialer_call_id, recording_ref, raw_event) "
        "VALUES ('bf1', 'https://x/rec', '{}'::jsonb), ('bf2', NULL, '{}'::jsonb)"))
    apply_to(conn)
    apply_to(conn)
    got = dict(conn.execute(text(
        "SELECT dialer_call_id, recording_status FROM lending.call_dispositions WHERE dialer_call_id IN ('bf1','bf2')")).all())
    assert got == {"bf1": "pending", "bf2": None}


def test_campaign_id_backfills_from_the_raw_event_and_rerun_is_stable(lending_db):
    conn = lending_db.connection()
    conn.execute(text(
        "INSERT INTO lending.call_dispositions (dialer_call_id, raw_event) VALUES "
        "('cb1', '{\"campaign\": {\"id\": 11}}'::jsonb), ('cb2', '{\"campaign_id\": \"12\"}'::jsonb), "
        "('cb3', '{}'::jsonb)"))
    apply_to(conn)
    apply_to(conn)
    got = dict(conn.execute(text(
        "SELECT dialer_call_id, dialer_campaign_id FROM lending.call_dispositions "
        "WHERE dialer_call_id IN ('cb1','cb2','cb3')")).all())
    assert got == {"cb1": "11", "cb2": "12", "cb3": None}
