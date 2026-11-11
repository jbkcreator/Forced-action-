from sqlalchemy import text

from migrations.apply_lending_recording_view_url import apply_to


def test_column_added_and_rerun_is_safe(lending_db):
    conn = lending_db.connection()
    apply_to(conn)
    apply_to(conn)
    cols = {r[0] for r in conn.execute(text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='lending' AND table_name='call_dispositions'"))}
    assert "recording_view_url" in cols


def test_existing_column_takes_no_lock():
    """deploy.sh re-runs this on a busy table: with the column there it must not issue the ALTER (exclusive lock)."""
    from unittest.mock import MagicMock

    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = 1
    apply_to(conn)
    assert not any("ALTER TABLE" in str(c.args[0]) for c in conn.execute.call_args_list)
