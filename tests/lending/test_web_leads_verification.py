"""WP-GL-11 verification: real concurrency, boundaries, suppression bypass attempts, restart and
failure behaviour, and structural compliance checks.

These tests COMMIT rows (threads need separate real transactions), so they run only against a
disposable database: set NDL_DISPOSABLE_DB=1. They never run against the shared database.
"""
from __future__ import annotations

import ast
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from config.lending_web import DEDUP_WINDOW_MINUTES, GHL_MAX_ATTEMPTS, GHL_RETRY_AFTER_MINUTES, SMS_CONSENT_TEXT
from src.lending.consent import has_text_consent
from src.lending.web_leads import DeliveryError, PushResult, build_input, deliver_pending, save_web_lead

pytestmark = pytest.mark.skipif(os.environ.get("NDL_DISPOSABLE_DB") != "1",
                                reason="commits rows: needs a disposable database (NDL_DISPOSABLE_DB=1)")

ROOT = Path(__file__).resolve().parents[2]
PHONE = "+18135550142"


@pytest.fixture(scope="module")
def factory():
    from config.settings import get_settings

    engine = create_engine(str(get_settings().database_url), pool_size=12, max_overflow=12)
    yield sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    engine.dispose()


@pytest.fixture(autouse=True)
def clean(factory):
    def wipe():
        with factory() as s:
            for table in ("web_leads", "text_consents", "suppression_list", "contacts"):
                s.execute(text(f"DELETE FROM lending.{table}"))
            s.commit()

    wipe()
    yield
    wipe()


def _data(**o):
    form = {"name": "Dana Builder", "phone": "(813) 555-0142", "email": "dana@example.com"}
    form.update(o)
    return build_input(form, ip_address="203.0.113.9", user_agent="pytest")


class Sink:
    def __init__(self, error=None, delay=0.0):
        self.error, self.delay, self.pushed, self._lock = error, delay, [], threading.Lock()

    def push(self, lead):
        import time

        time.sleep(self.delay)
        with self._lock:
            self.pushed.append(lead["id"])
        if self.error:
            raise self.error
        return PushResult("ghl_1", True)


# ---------------------------------------------------------------- concurrency

def test_simultaneous_identical_submissions_make_exactly_one_lead(factory, monkeypatch):
    import time

    import src.lending.web_leads as module

    original = module._recent_duplicate

    def slow_check(db, data):
        found = original(db, data)
        time.sleep(0.3)  # widen the gap between "no duplicate seen" and the insert so the race is certain
        return found

    monkeypatch.setattr(module, "_recent_duplicate", slow_check)
    barrier = threading.Barrier(8)

    def submit(_):
        with factory() as s:
            barrier.wait()
            lead_id, created = save_web_lead(s, _data(sms_consent="yes"))
            s.commit()
            return lead_id, created

    with ThreadPoolExecutor(8) as pool:
        results = list(pool.map(submit, range(8)))
    with factory() as s:
        rows = s.execute(text("SELECT count(*) FROM lending.web_leads")).scalar()
        consents = s.execute(text("SELECT count(*) FROM lending.text_consents")).scalar()
    assert rows == 1 and sum(1 for _, created in results if created) == 1 and consents == 1
    assert len({lead_id for lead_id, _ in results}) == 1


def test_concurrent_sweeps_deliver_every_lead_exactly_once(factory):
    with factory() as s:
        for n in range(20):
            save_web_lead(s, _data(phone=f"(813) 555-02{n:02d}"))
        s.commit()
    sink = Sink(delay=0.02)
    barrier = threading.Barrier(4)

    def sweep(_):
        with factory() as s:
            barrier.wait()
            count = deliver_pending(s, sink)
            s.commit()
            return count

    with ThreadPoolExecutor(4) as pool:
        counts = list(pool.map(sweep, range(4)))
    assert sum(counts) == 20 and len(sink.pushed) == 20 and len(set(sink.pushed)) == 20
    with factory() as s:
        assert s.execute(text("SELECT count(*) FROM lending.web_leads WHERE ghl_status = 'synced'")).scalar() == 20


def test_a_worker_killed_mid_delivery_loses_nothing_and_a_new_instance_resumes(factory):
    with factory() as s:
        lead_id, _ = save_web_lead(s, _data())
        s.commit()
    crashed = Sink()
    with factory() as s:
        deliver_pending(s, crashed, lead_id=lead_id)
        s.rollback()  # the process dies after GHL answered but before the status commit
    with factory() as s:
        row = s.execute(text("SELECT ghl_status, ghl_attempts FROM lending.web_leads WHERE id = :i"), {"i": lead_id}).one()
    assert tuple(row) == ("pending", 0)  # nothing lost, nothing half-recorded
    survivor = Sink()
    with factory() as s:
        assert deliver_pending(s, survivor) == 1
        s.commit()
    with factory() as s:
        assert s.execute(text("SELECT ghl_status FROM lending.web_leads WHERE id = :i"), {"i": lead_id}).scalar() == "synced"
    assert crashed.pushed == survivor.pushed == [lead_id]  # at-least-once: GHL upsert/tags are idempotent


# ----------------------------------------------------------------- boundaries

@pytest.mark.parametrize("age,is_duplicate", [
    (timedelta(minutes=DEDUP_WINDOW_MINUTES) - timedelta(seconds=1), True),
    (timedelta(minutes=DEDUP_WINDOW_MINUTES) + timedelta(seconds=1), False),
])
def test_dedup_window_boundary(factory, age, is_duplicate):
    with factory() as s:
        first, _ = save_web_lead(s, _data())
        s.execute(text("UPDATE lending.web_leads SET received_at = now() - make_interval(secs => :a)"), {"a": age.total_seconds()})
        second, created = save_web_lead(s, _data())
        s.commit()
    assert (not created) is is_duplicate and (second == first) is is_duplicate


@pytest.mark.parametrize("attempts,deliverable", [(GHL_MAX_ATTEMPTS - 1, True), (GHL_MAX_ATTEMPTS, False)])
def test_attempt_cap_boundary(factory, attempts, deliverable):
    with factory() as s:
        lead_id, _ = save_web_lead(s, _data())
        s.execute(text("UPDATE lending.web_leads SET ghl_status = 'failed', ghl_attempts = :n"), {"n": attempts})
        s.commit()
    sink = Sink()
    with factory() as s:
        deliver_pending(s, sink, now=datetime.now(timezone.utc) + timedelta(hours=1))
        s.commit()
    assert (sink.pushed == [lead_id]) is deliverable


@pytest.mark.parametrize("extra_seconds,retried", [(0, False), (1, True)])
def test_retry_wait_boundary(factory, extra_seconds, retried):
    now = datetime.now(timezone.utc)
    with factory() as s:
        save_web_lead(s, _data())
        s.execute(text("UPDATE lending.web_leads SET ghl_status = 'failed', ghl_attempts = 1, ghl_last_attempt_at = :t"),
                  {"t": now - timedelta(minutes=GHL_RETRY_AFTER_MINUTES, seconds=extra_seconds)})
        s.commit()
    sink = Sink()
    with factory() as s:
        deliver_pending(s, sink, now=now)
        s.commit()
    assert bool(sink.pushed) is retried


def test_field_length_boundary_truncates_instead_of_rejecting():
    assert len(_data(name="n" * 120).name) == 120
    assert len(_data(name="n" * 121).name) == 120


# ------------------------------------------------- suppression: bypass attempts

@pytest.mark.parametrize("typed", ["813-555-0142", "(813) 555-0142", "8135550142", "+1 813 555 0142", "1-813-555-0142"])
def test_suppressed_number_cannot_gain_consent_however_it_is_typed(factory, typed):
    with factory() as s:
        s.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES (:p, 'OPT_OUT', 'sms')"), {"p": PHONE})
        lead_id, _ = save_web_lead(s, _data(phone=typed, sms_consent="yes", consent_text=SMS_CONSENT_TEXT))
        s.commit()
        assert s.execute(text("SELECT suppressed FROM lending.web_leads WHERE id = :i"), {"i": lead_id}).scalar() is True
        assert has_text_consent(s, PHONE) is False


def test_suppressed_email_in_any_case_blocks_consent(factory):
    with factory() as s:
        s.execute(text("INSERT INTO lending.suppression_list (email, reason, source_channel) VALUES ('dana@example.com', 'OPT_OUT', 'email')"))
        lead_id, _ = save_web_lead(s, _data(phone="(813) 555-0188", email="DANA@EXAMPLE.COM", sms_consent="yes"))
        s.commit()
        assert s.execute(text("SELECT suppressed FROM lending.web_leads WHERE id = :i"), {"i": lead_id}).scalar() is True


def test_do_not_contact_flag_also_blocks_consent_and_the_ghl_consent_tag(factory):
    with factory() as s:
        s.execute(text("INSERT INTO lending.contacts (phone, do_not_contact) VALUES (:p, true)"), {"p": PHONE})
        lead_id, _ = save_web_lead(s, _data(sms_consent="yes"))
        s.commit()
        assert s.execute(text("SELECT suppressed FROM lending.web_leads WHERE id = :i"), {"i": lead_id}).scalar() is True
        assert has_text_consent(s, PHONE) is False


def test_an_opt_out_after_the_form_revokes_the_consent(factory):
    with factory() as s:
        save_web_lead(s, _data(sms_consent="yes"))
        assert has_text_consent(s, PHONE) is True
        s.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES (:p, 'OPT_OUT', 'sms')"), {"p": PHONE})
        assert has_text_consent(s, PHONE) is False


def test_the_response_does_not_reveal_whether_a_number_is_suppressed(factory):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.api.deps import get_db
    from src.api.lending_web_router import router
    from src.services.rate_limit import reset_local_buckets

    def override():
        with factory() as s:
            yield s

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = override
    client = TestClient(app)
    reset_local_buckets()
    form = {"name": "Dana", "phone": "(813) 555-0142", "sms_consent": "yes"}
    clean_resp = client.post("/api/lending/web-leads", data=form)
    with factory() as s:
        s.execute(text("DELETE FROM lending.web_leads"))
        s.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) VALUES (:p, 'OPT_OUT', 'sms')"), {"p": PHONE})
        s.commit()
    suppressed_resp = client.post("/api/lending/web-leads", data=form)
    assert (clean_resp.status_code, clean_resp.json()) == (suppressed_resp.status_code, suppressed_resp.json())


# --------------------------------------------- failure / retry / restart (HTTP path)

def _client(factory, monkeypatch, sink):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    import src.api.lending_web_router as router_mod
    from src.api.deps import get_db
    from src.services.rate_limit import reset_local_buckets

    def override():
        with factory() as s:
            yield s

    from contextlib import contextmanager

    @contextmanager
    def session():
        with factory() as s:
            yield s
            s.commit()

    monkeypatch.setattr(router_mod, "get_live_sink", lambda: sink)
    monkeypatch.setattr(router_mod, "lending_session", session)
    app = FastAPI()
    app.include_router(router_mod.router)
    app.dependency_overrides[get_db] = override
    reset_local_buckets()
    return TestClient(app), session


def _row(factory):
    with factory() as s:
        return s.execute(text("SELECT ghl_status, ghl_attempts, ghl_last_error FROM lending.web_leads")).one()


def test_ghl_down_loses_no_lead_and_the_sweep_delivers_it_later(factory, monkeypatch):
    client, session = _client(factory, monkeypatch, Sink(error=DeliveryError("contact upsert: HTTP 503")))
    response = client.post("/api/lending/web-leads", data={"name": "Dana", "phone": "(813) 555-0142"})
    assert response.status_code == 200  # the visitor is never told about a GHL outage
    assert tuple(_row(factory)) == ("failed", 1, "contact upsert: HTTP 503")

    import src.tasks.lending_web_lead_sweep as sweep

    good = Sink()
    monkeypatch.setattr(sweep, "get_live_sink", lambda: good)
    monkeypatch.setattr(sweep, "lending_session", session)
    with factory() as s:
        s.execute(text("UPDATE lending.web_leads SET ghl_last_attempt_at = now() - interval '10 minutes'"))
        s.commit()
    assert sweep.run() == 1
    assert _row(factory)[0] == "synced"


def test_ghl_not_configured_keeps_the_lead_pending_and_still_answers_200(factory, monkeypatch):
    client, _ = _client(factory, monkeypatch, None)
    assert client.post("/api/lending/web-leads", data={"name": "Dana", "phone": "(813) 555-0142"}).status_code == 200
    assert tuple(_row(factory)) == ("pending", 0, None)


def test_a_lead_that_exhausts_its_attempts_stays_failed_and_is_logged_at_error(factory, caplog):
    import logging

    with factory() as s:
        lead_id, _ = save_web_lead(s, _data())
        s.execute(text("UPDATE lending.web_leads SET ghl_status = 'failed', ghl_attempts = :n"), {"n": GHL_MAX_ATTEMPTS - 1})
        s.commit()
    with caplog.at_level(logging.ERROR, logger="src.lending.web_leads"):
        with factory() as s:
            deliver_pending(s, Sink(error=DeliveryError("contact upsert: HTTP 500")), now=datetime.now(timezone.utc) + timedelta(hours=1))
            s.commit()
    assert tuple(_row(factory)[:2]) == ("failed", GHL_MAX_ATTEMPTS)
    assert any(r.levelno == logging.ERROR and str(lead_id) in r.getMessage() for r in caplog.records)
    assert "8135550142" not in caplog.text and "dana@example.com" not in caplog.text  # no PII in logs


# -------------------------------------------------------- compliance (structural)

FORBIDDEN_COLUMN = re.compile(r"credit|fico|score|income|ssn|bank|tax|salary|liquid|reserve|balance|net_worth|asset", re.I)


def test_no_borrower_financial_field_exists_on_the_new_table(factory):
    with factory() as s:
        cols = [r[0] for r in s.execute(text(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='lending' AND table_name='web_leads'"))]
    assert cols and not [c for c in cols if FORBIDDEN_COLUMN.search(c)], cols


NEW_MODULES = ["src/lending/web_leads.py", "src/lending/web_lead_ghl.py", "src/api/lending_web_router.py",
               "src/tasks/lending_web_lead_sweep.py"]


def test_no_new_module_can_send_a_text_or_voice_message():
    banned = re.compile(r"sms_compliance|telnyx|send_sms|synthflow|twilio", re.I)
    for rel in NEW_MODULES:
        tree = ast.parse((ROOT / rel).read_text(encoding="utf-8"))
        names = [n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)]
        names += [a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names]
        calls = [getattr(n.func, "attr", getattr(n.func, "id", "")) for n in ast.walk(tree) if isinstance(n, ast.Call)]
        assert not [x for x in names + calls if banned.search(x)], rel


def test_nothing_the_form_writes_to_ghl_states_a_rate_term_or_commitment():
    from src.lending.web_lead_ghl import _note, _tags_to_add

    lead = {"id": 1, "name": "Dana", "phone": PHONE, "email": None, "property_city": "Tampa", "deal_type": "Fix and flip",
            "completed_projects_3y": "1 to 2", "sms_consent": True, "deal_drop_optin": True, "suppressed": False,
            "received_at": datetime.now(timezone.utc)}
    written = _note(lead) + " ".join(_tags_to_add(lead, True))
    assert not re.search(r"\brate\b|\bapr\b|interest|\bltv\b|\bltc\b|quote|approved|guarantee|%", written, re.I), written


def test_the_endpoint_replies_with_no_price_term_or_commitment(factory, monkeypatch):
    client, _ = _client(factory, monkeypatch, None)
    body = client.post("/api/lending/web-leads", data={"name": "Dana", "phone": "(813) 555-0142"}).json()
    assert body == {"received": True}
