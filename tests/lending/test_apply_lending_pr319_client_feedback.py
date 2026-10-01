from sqlalchemy import text

from migrations.apply_lending_pr319_client_feedback import apply_to


def test_migration_is_idempotent(lending_db):
    conn = lending_db.connection()
    apply_to(conn)
    apply_to(conn)
    cols = {r[0] for r in conn.execute(text(
        "SELECT column_name FROM information_schema.columns WHERE table_schema='lending' AND table_name='call_dispositions'"))}
    assert "consent_checked_at" in cols
    assert conn.execute(text("SELECT to_regclass('lending.text_consents')")).scalar() is not None
