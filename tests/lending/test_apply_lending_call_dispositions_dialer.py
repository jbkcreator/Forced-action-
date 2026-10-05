"""apply_lending_call_dispositions_dialer: upgrades the Aircall-era table, and is re-runnable."""
from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text

pytestmark = pytest.mark.skipif(not os.environ.get("DATABASE_URL"), reason="requires a live Postgres DATABASE_URL")

OLD_TABLE = """
CREATE TABLE "{s}".call_dispositions (
  id serial PRIMARY KEY, aircall_call_id varchar(64) NOT NULL UNIQUE, direction varchar(10), phone varchar(20),
  caller_seat varchar(100), caller_name varchar(120), caller_line varchar(40), campaign_tag varchar(40),
  aircall_contact_id varchar(40), disposition varchar(30), disposition_tag_raw varchar(80),
  multiple_dispositions boolean NOT NULL DEFAULT false, talk_duration_sec int, call_started_at timestamptz,
  call_ended_at timestamptz, disposition_at timestamptz, recording_disclosure_logged boolean NOT NULL DEFAULT false,
  sheet_synced_at timestamptz, sheet_synced_disposition varchar(30), slack_posted_at timestamptz,
  slack_posted_disposition varchar(30), slack_ts varchar(40), opt_out_propagated_at timestamptz,
  raw_event jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ck_lending_call_dispositions_disposition CHECK (disposition IS NULL OR disposition IN ('CONNECTED','BAD_NUMBER')))
"""


@pytest.fixture
def env():
    engine = create_engine(os.environ["DATABASE_URL"])
    schema = f"lending_t_{uuid.uuid4().hex[:8]}"
    yield engine, schema
    with engine.begin() as c:
        c.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
    engine.dispose()


def _columns(engine, schema, table="call_dispositions"):
    with engine.connect() as c:
        return {r[0] for r in c.execute(text(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = :s AND table_name = :t"),
            {"s": schema, "t": table})}


def test_upgrade_renames_widens_drops_the_check_and_is_rerunnable(env):
    from migrations.apply_lending_call_dispositions_dialer import apply

    engine, schema = env
    with engine.begin() as c:
        c.execute(text(f'CREATE SCHEMA "{schema}"'))
        c.execute(text(OLD_TABLE.format(s=schema)))
        c.execute(text(f"INSERT INTO \"{schema}\".call_dispositions (aircall_call_id, raw_event) VALUES ('old1', '{{}}')"))
    apply(engine=engine, schema=schema)
    apply(engine=engine, schema=schema)  # second run changes nothing and does not fail

    cols = _columns(engine, schema)
    assert {"dialer_call_id", "dialer_contact_id", "caller_id_number", "disposition_raw", "unfunded_cause",
            "recording_ref", "disposition_missing_alerted_at", "booking_blocked"} <= cols
    assert not ({"aircall_call_id", "aircall_contact_id", "caller_line", "disposition_tag_raw", "multiple_dispositions"} & cols)
    assert _columns(engine, schema, "missed_call_events")
    with engine.begin() as c:
        assert c.execute(text(f'SELECT dialer_call_id FROM "{schema}".call_dispositions')).scalar() == "old1"
        c.execute(text(f"UPDATE \"{schema}\".call_dispositions SET disposition = 'GATE_FAILED_NURTURE'"))  # old CHECK is gone


def test_missing_table_is_a_clear_error(env):
    from migrations.apply_lending_call_dispositions_dialer import apply_to

    engine, schema = env
    with engine.begin() as c:
        c.execute(text(f'CREATE SCHEMA "{schema}"'))
        with pytest.raises(RuntimeError, match="apply_lending_call_dispositions.py"):
            apply_to(c, schema)
