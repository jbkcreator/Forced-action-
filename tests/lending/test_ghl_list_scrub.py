"""D1/D4: DNC scrub of the numbers on Josh's GHL call lists (website and cold leads, never LendingFlow)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from src.lending import ghl_list_scrub as job
from src.lending.compliance import ScrubResult
from src.lending.ghl_list_scrub import due_phones, list_scrub

NOW = datetime(2026, 11, 4, 8, 0, tzinfo=timezone.utc)


class Scrubber:
    def __init__(self, dnc=(), fail=False):
        self.calls, self.dnc, self.fail = [], set(dnc), fail

    def __call__(self, phones):
        self.calls.append(list(phones))
        if self.fail:
            raise RuntimeError("tracerfy down")
        return [{"phone": p, "national_dnc": "Yes" if p in self.dnc else "No",
                 "litigator": "No", "state_dnc": "No", "phone_type": "mobile"} for p in phones]


def _stub_storage(monkeypatch, due):
    """list_scrub without a database: the due list is given, _scrub turns the scrubber's rows into results."""
    stamped, nurtured = [], []
    monkeypatch.setattr(job, "due_phones", lambda db, phones, *, now: list(due))

    def fake_scrub(db, phones, scrubber):
        try:
            rows = scrubber(phones)
        except Exception:
            return {}
        return {r["phone"]: ScrubResult(r["national_dnc"] == "Yes", False, False, NOW) for r in rows}

    monkeypatch.setattr(job, "_scrub", fake_scrub)
    monkeypatch.setattr(job, "_stamp_contacts", lambda db, scrubs: stamped.extend(scrubs))
    monkeypatch.setattr(job, "_flag_nurture", lambda db, phones: nurtured.extend(phones))
    return stamped, nurtured


def test_due_numbers_are_scrubbed_in_batches_and_dnc_hits_are_counted_and_flagged(monkeypatch):
    due = [f"+1813555{n}" for n in range(3000, 3005)]
    stamped, nurtured = _stub_storage(monkeypatch, due)
    scrubber = Scrubber(dnc={due[1]})

    result = list_scrub(None, due, scrubber=scrubber, max_credits=100, batch_size=2, now=NOW)

    assert [len(c) for c in scrubber.calls] == [2, 2, 1]
    assert (result.scrubbed, result.credits_used, result.blocked) == (5, 5, 1)
    assert sorted(stamped) == sorted(due)
    assert nurtured == [due[1]]


def test_the_credit_cap_stops_before_a_batch_that_would_exceed_it(monkeypatch):
    due = [f"+1813555{n}" for n in range(3100, 3105)]
    _stub_storage(monkeypatch, due)
    scrubber = Scrubber()
    result = list_scrub(None, due, scrubber=scrubber, max_credits=3, batch_size=2, now=NOW)
    assert result.credits_used == 2 and result.aborted and result.left_unscrubbed == 3
    assert len(scrubber.calls) == 1


def test_a_failed_scrub_batch_spends_no_credits(monkeypatch):
    due = ["+18135553201"]
    stamped, _ = _stub_storage(monkeypatch, due)
    result = list_scrub(None, due, scrubber=Scrubber(fail=True), max_credits=10, now=NOW)
    assert (result.credits_used, result.scrub_batch_failures, stamped) == (0, 1, [])


def test_main_is_idle_while_the_switch_is_off(monkeypatch):
    class Off:
        lending_ghl_list_scrub_enabled = False

    monkeypatch.setattr(job, "get_settings", lambda: Off())
    monkeypatch.setattr(job, "_listed_phones", lambda: (_ for _ in ()).throw(AssertionError("read the lists")))
    assert job.main([]) == 0


# ── database ─────────────────────────────────────────────────────────────────


def _scrub_row(db, phone, age_days):
    db.execute(text("INSERT INTO lending.dnc_scrubs (phone, national_dnc, litigator, state_dnc, checked_at) "
                    "VALUES (:p, false, false, false, :at)"), {"p": phone, "at": NOW - timedelta(days=age_days)})


def test_due_numbers_are_unscrubbed_or_six_days_old_never_lendingflow_or_suppressed(lending_db):
    never, old, fresh, lendingflow, opted_out = (
        "+18135553301", "+18135553302", "+18135553303", "+18135553304", "+18135553305")
    _scrub_row(lending_db, old, 6)
    _scrub_row(lending_db, fresh, 2)
    lending_db.execute(text("INSERT INTO lending.lendingflow_leads (vendor_lead_id, dedupe_hash, raw_payload, phone) "
                            "VALUES ('v-ls', 'h-ls', '{}'::jsonb, :p)"), {"p": lendingflow})
    lending_db.execute(text("INSERT INTO lending.suppression_list (phone, reason, source_channel) "
                            "VALUES (:p, 'OPT_OUT', 'sms')"), {"p": opted_out})

    assert due_phones(lending_db, [never, old, fresh, lendingflow, opted_out], now=NOW) == [never, old]
