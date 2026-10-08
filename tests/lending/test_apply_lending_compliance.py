"""tests/lending/test_apply_lending_compliance.py

Wave 0 Dev 2 (WP-W0-2/3/8) lending compliance schema. Runs against a throwaway
schema pair (never the shared `lending` schema) and drops it afterwards.
"""
from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL"),
    reason="requires a live Postgres DATABASE_URL",
)

TABLES = {"suppression_list", "contacts", "opt_out_events", "load_exclusions", "dnc_scrubs", "dialer_holds",
          "dialer_load_records", "call_dispositions", "missed_call_events", "missed_call_texts", "text_consents", "web_leads",
          "dialer_unconfirmed_creates"}


@pytest.fixture
def env():
    """Throwaway target schema + a source schema holding copies of FA opt-out tables."""
    engine = create_engine(os.environ["DATABASE_URL"])
    tag = uuid.uuid4().hex[:8]
    target, source = f"lending_t_{tag}", f"lending_src_{tag}"
    with engine.begin() as c:
        c.execute(text(f'CREATE SCHEMA "{source}"'))
        for t in ("sms_opt_outs", "email_opt_outs", "dnc_phone_checks"):
            c.execute(text(f'CREATE TABLE "{source}".{t} (LIKE public.{t} INCLUDING DEFAULTS)'))
    yield engine, target, source
    with engine.begin() as c:
        c.execute(text(f'DROP SCHEMA IF EXISTS "{target}" CASCADE'))
        c.execute(text(f'DROP SCHEMA IF EXISTS "{source}" CASCADE'))
    engine.dispose()


def _tables(engine, schema):
    with engine.connect() as c:
        return {
            r[0]
            for r in c.execute(
                text("SELECT table_name FROM information_schema.tables WHERE table_schema = :s"),
                {"s": schema},
            )
        }


def test_apply_creates_lending_tables_in_target_schema(env):
    from migrations.apply_lending_compliance import apply

    engine, target, source = env
    apply(engine=engine, schema=target, source_schema=source)

    assert _tables(engine, target) == TABLES


def _seed_sources(engine, source):
    with engine.begin() as c:
        c.execute(text(f"""
            INSERT INTO "{source}".sms_opt_outs (phone, keyword_used, source, opted_out_at) VALUES
              ('+18135550101', 'STOP', 'manual', now()),
              ('+18135550102', 'DNC', 'tracerfy_dnc_refresh', now()),
              ('+18135550103', 'DNC', 'tracerfy_dnc_refresh', now()),
              ('(813) 555-0106', 'STOP', 'inbound_sms', now())
        """))
        c.execute(text(f"""
            INSERT INTO "{source}".email_opt_outs (email, source, opted_out_at)
            VALUES ('Optout@Example.com', 'unsubscribe_link', now()),
                   ('bounced@example.com', 'mandrill_hard_bounce', now())
        """))
        c.execute(text(f"""
            INSERT INTO "{source}".dnc_phone_checks (phone, national_dnc, litigator, checked_at, source) VALUES
              ('+18135550103', true,  true,  now(), 'tracerfy_dnc_refresh'),
              ('+18135550104', true,  false, now(), 'tracerfy_dnc_refresh'),
              ('+18135550105', false, false, now(), 'tracerfy_dnc_refresh')
        """))


def _suppressed(engine, schema):
    with engine.connect() as c:
        rows = c.execute(text(f'SELECT phone, email, reason FROM "{schema}".suppression_list'))
        return {(r[0] or r[1]): r[2] for r in rows}


def test_backfill_takes_real_opt_outs_and_litigators_only(env):
    from migrations.apply_lending_compliance import apply

    engine, target, source = env
    _seed_sources(engine, source)
    apply(engine=engine, schema=target, source_schema=source)

    assert _suppressed(engine, target) == {
        "+18135550101": "OPT_OUT",      # manual STOP
        "optout@example.com": "OPT_OUT",  # email, lower-cased
        "+18135550103": "LITIGATOR",    # Tracerfy-flagged litigator
        "+18135550106": "OPT_OUT",      # legacy raw format, normalized on backfill
        # bounced@example.com excluded: a bounce is not an opt-out
        # 0102 (DNC-only sms row), 0104 (national DNC only), 0105 (clean) excluded:
        # national DNC is re-checked every 31 days, never permanent.
    }


def test_apply_twice_creates_no_duplicates(env):
    from migrations.apply_lending_compliance import apply

    engine, target, source = env
    _seed_sources(engine, source)
    apply(engine=engine, schema=target, source_schema=source)
    first = _suppressed(engine, target)
    apply(engine=engine, schema=target, source_schema=source)

    assert _suppressed(engine, target) == first
