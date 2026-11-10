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
