from sqlalchemy import text

from config.lending_agent_jobs import COLD_EMAIL_CHANNEL, JOB_TYPES, RESULT_STATUSES


def test_apply_creates_agent_jobs_table(lending_db):
    from migrations.apply_lending_agent_jobs import apply_to

    schema = "agent_jobs_migration_test"  # its own schema inside the test's rolled-back transaction
    conn = lending_db.connection()
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    apply_to(conn, schema)
    apply_to(conn, schema)
    cols = {r[0] for r in conn.execute(text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = :s AND table_name='agent_jobs'"), {"s": schema})}
    assert {"job_id", "drive_file_id", "rail", "state", "action_id", "result_status", "result_written_at"} <= cols
    indexes = {r[0] for r in conn.execute(text(
        "SELECT indexname FROM pg_indexes WHERE schemaname = :s AND tablename='agent_jobs'"), {"s": schema})}
    assert {"ix_agent_jobs_state_release", "ix_agent_jobs_action"} <= indexes


def test_job_types_map_to_cora_channels():
    assert set(JOB_TYPES) == {"send_sms", "send_email", "create_task", "add_tag", "move_stage", "add_note"}
    assert set(JOB_TYPES.values()) == {"ghl_sms", "ghl_email", "ghl_task", "ghl_tag", "ghl_stage", "ghl_note"}
    assert COLD_EMAIL_CHANNEL == "instantly_cold_email"  # "instantly_email" is P1.3's thread answer
    assert RESULT_STATUSES == ("sent", "edited_sent", "rejected", "blocked_consent", "blocked_hours",
                               "blocked_lane", "blocked_cap", "expired", "error")
