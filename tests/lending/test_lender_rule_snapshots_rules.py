"""R1 0.7: write-time rule validation, snapshot diffs, the rollback endpoint and the backup helpers. No database."""
from __future__ import annotations

import os
import time
from dataclasses import replace
from decimal import Decimal

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy.engine import make_url

from config.lender_matrix import LENDER_MATRIX
from scripts import pg_backup, verify_backup_restore
from src.api import lending_lender_rules_router as rules_router
from src.lending import lender_rules_store as store
from src.lending.contracts import FieldStatus

LENDER = LENDER_MATRIX[0].key


def _check(field, value, status="confirmed", matrix=LENDER_MATRIX, lender=LENDER):
    return store.check_rule_change(matrix, lender, field, value, status)


# ── Hard-invalid values are refused; valid-but-wrong ones are not ──────────────

@pytest.mark.parametrize("value", [9.0, 1.01, 0, -0.5])
def test_leverage_outside_zero_to_one_is_refused(value):
    for field in ("max_ltv", "max_ltc"):
        with pytest.raises(store.RuleValueError):
            _check(field, value)


@pytest.mark.parametrize("value", [0.99, 0.9, 1, 0.01])
def test_a_wrong_but_valid_leverage_is_accepted(value):
    decoded, status = _check("max_ltv", value)
    assert decoded == Decimal(str(value)) and status is FieldStatus.CONFIRMED


def test_a_leverage_that_is_unknown_stays_none():
    assert _check("max_ltv", None)[0] is None


@pytest.mark.parametrize("field,value", [("max_ltv", "high"), ("max_ltv", True), ("credit_floor", 12.5),
                                         ("requires_ground_up_build", "yes"), ("approved_states", 7)])
def test_the_wrong_type_is_refused(field, value):
    with pytest.raises(store.RuleValueError):
        _check(field, value)


def test_an_unknown_lender_field_or_status_is_refused():
    with pytest.raises(store.RuleValueError):
        _check("max_ltv", 0.8, lender="nobody")
    with pytest.raises(store.RuleValueError):
        _check("max_pizza", 0.8)
    with pytest.raises(store.RuleValueError):
        _check("max_ltv", 0.8, status="sure")


def test_a_negative_amount_is_refused():
    with pytest.raises(store.RuleValueError):
        _check("min_loan_amount", -1)


def test_a_minimum_above_the_maximum_is_refused():
    base = tuple(replace(r, min_loan_amount=Decimal(100000)) if r.key == LENDER else r for r in LENDER_MATRIX)
    with pytest.raises(store.RuleValueError):
        _check("max_loan_amount", 50000, matrix=base)
    assert _check("max_loan_amount", 200000, matrix=base)[0] == Decimal(200000)


# ── Diffs ──────────────────────────────────────────────────────────────────────

def _row(lender, field, value, status="confirmed"):
    return {"lender_key": lender, "field": field, "value": value, "status": status}


def test_diff_names_every_changed_added_and_removed_rule():
    old = [_row("a", "max_ltv", 0.9), _row("a", "max_ltc", 0.85), _row("b", "credit_floor", 680)]
    new = [_row("a", "max_ltv", 0.99), _row("a", "max_ltc", 0.85), _row("c", "hold_months", 6)]
    assert store.diff_rules(old, new) == [
        'a.max_ltv: 0.9 [confirmed] -> 0.99 [confirmed]',
        'b.credit_floor: 680 [confirmed] -> (none)',
        'c.hold_months: (none) -> 6 [confirmed]',
    ]


def test_a_status_change_alone_is_a_difference():
    assert store.diff_rules([_row("a", "max_ltv", 0.9, "placeholder")], [_row("a", "max_ltv", 0.9)]) == [
        "a.max_ltv: 0.9 [placeholder] -> 0.9 [confirmed]"]


def test_identical_rule_sets_have_no_diff():
    rows = [_row("a", "max_ltv", 0.9)]
    assert store.diff_rules(rows, list(rows)) == []


def test_a_rollback_to_the_same_rules_is_not_a_change():
    assert store.RollbackResult(5, 3, 5).changed is False
    assert store.RollbackResult(5, 3, 6).changed is True


# ── The endpoint ───────────────────────────────────────────────────────────────

class _Db:
    commits = 0
    rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


@pytest.fixture
def api(monkeypatch):
    from config.settings import get_settings

    monkeypatch.setattr(get_settings(), "lending_admin_token", SecretStr("secret-token"), raising=False)
    db = _Db()
    app = FastAPI()
    app.include_router(rules_router.router)
    app.dependency_overrides[rules_router.get_lending_db] = lambda: db
    client = TestClient(app, raise_server_exceptions=False)
    client.db = db
    return client


HEADERS = {"X-Admin-Token": "secret-token"}


def test_the_endpoints_need_the_admin_token(api):
    assert api.get("/api/lending/lender-rules/versions").status_code == 401
    assert api.post("/api/lending/lender-rules/rollback", json={"version": 1, "actor": "a"},
                    headers={"X-Admin-Token": "wrong"}).status_code == 401


def test_versions_are_listed(api, monkeypatch):
    from datetime import datetime, timezone

    monkeypatch.setattr(store, "list_snapshots", lambda db, limit: [
        {"version_no": 3, "created_by": "josh", "reason": "rollback to v1", "created_at": datetime.now(timezone.utc)}])
    response = api.get("/api/lending/lender-rules/versions", headers=HEADERS)
    assert response.status_code == 200 and response.json()[0]["version_no"] == 3


def test_a_rollback_is_committed_and_reports_the_new_version(api, monkeypatch):
    monkeypatch.setattr(store, "rollback_to", lambda db, v, actor, reason=None: store.RollbackResult(2, v, 3))
    response = api.post("/api/lending/lender-rules/rollback", json={"version": 1, "actor": "josh"}, headers=HEADERS)
    assert response.json() == {"from_version": 2, "to_version": 1, "new_version": 3, "changed": True}
    assert api.db.commits == 1


def test_an_unknown_version_is_404(api, monkeypatch):
    def missing(db, v, actor, reason=None):
        raise store.SnapshotNotFound("no snapshot")

    monkeypatch.setattr(store, "rollback_to", missing)
    response = api.post("/api/lending/lender-rules/rollback", json={"version": 99, "actor": "josh"}, headers=HEADERS)
    assert response.status_code == 404 and response.json() == {"detail": "Version not found"}
    assert api.db.rollbacks == 1


def test_a_failure_is_500_without_the_cause(api, monkeypatch):
    def boom(db, v, actor, reason=None):
        raise RuntimeError("password=hunter2")

    monkeypatch.setattr(store, "rollback_to", boom)
    response = api.post("/api/lending/lender-rules/rollback", json={"version": 1, "actor": "josh"}, headers=HEADERS)
    assert response.status_code == 500 and "hunter2" not in response.text


def test_a_bad_body_is_422(api):
    for body in ({"version": 0, "actor": "a"}, {"version": 1}, {"version": 1, "actor": "a", "extra": 1}):
        assert api.post("/api/lending/lender-rules/rollback", json=body, headers=HEADERS).status_code == 422


# ── Backup helpers ─────────────────────────────────────────────────────────────

def test_the_password_goes_in_the_environment_not_the_command_line():
    env = pg_backup.pg_env(make_url("postgresql://app:p%40ss@db.example:5433/distress_db"))
    assert env == {"PGHOST": "db.example", "PGPORT": "5433", "PGUSER": "app", "PGDATABASE": "distress_db",
                   "PGPASSWORD": "p@ss"}


def test_prune_keeps_the_newest_dump_and_recent_ones(tmp_path):
    now = time.time()
    for name, age_days in (("distress_db_new.dump", 0), ("distress_db_mid.dump", 3), ("distress_db_old.dump", 9),
                           ("distress_db_older.dump", 20)):
        dump = tmp_path / name
        dump.write_bytes(b"x")
        (tmp_path / f"{name}.counts.json").write_text("{}")
        os.utime(dump, (now - age_days * 86400,) * 2)
    removed = pg_backup.prune(tmp_path, "distress_db", keep_days=7, now=now)
    assert {p.name for p in removed} == {"distress_db_old.dump", "distress_db_older.dump"}
    assert sorted(p.name for p in tmp_path.glob("*.dump")) == ["distress_db_mid.dump", "distress_db_new.dump"]
    assert not list(tmp_path.glob("distress_db_old*.counts.json"))


def test_prune_never_deletes_the_only_dump_however_old(tmp_path):
    dump = tmp_path / "distress_db_only.dump"
    dump.write_bytes(b"x")
    os.utime(dump, (time.time() - 90 * 86400,) * 2)
    assert pg_backup.prune(tmp_path, "distress_db", keep_days=7) == [] and dump.exists()


def test_a_restore_must_sit_between_the_dump_time_count_and_production_now():
    compare = verify_backup_restore.compare_counts
    assert compare({"t": 5}, {"t": 5}, {"t": 9}) == []
    assert compare({"t": 5}, {"t": 8}, {"t": 9}) == []
    assert "restored 4 rows, 5 existed" in compare({"t": 5}, {"t": 4}, {"t": 9})[0]
    assert "production holds only 9" in compare({"t": 5}, {"t": 10}, {"t": 9})[0]
    assert "missing after restore" in compare({"t": 5}, {}, {"t": 9})[0]


def test_the_restore_check_refuses_production_and_unmarked_databases():
    prod = make_url("postgresql://u:p@db/distress_db")
    with pytest.raises(SystemExit):
        verify_backup_restore.check_scratch(make_url("postgresql://u:p@db/distress_db"), prod)
    with pytest.raises(SystemExit):
        verify_backup_restore.check_scratch(make_url("postgresql://u:p@db/restore_check"), prod)
    verify_backup_restore.check_scratch(make_url("postgresql://u:p@db/restore_check_test"), prod)
    with pytest.raises(SystemExit):  # the same database under a 'test' name is still the production database
        verify_backup_restore.check_scratch(make_url("postgresql://u:p@db/distress_db_test"), make_url("postgresql://u:p@db/distress_db_test"))


def test_the_newest_dump_is_found_and_an_empty_folder_is_refused(tmp_path):
    dump = tmp_path / "distress_db_old.dump"
    dump.write_bytes(b"x")
    os.utime(dump, (time.time() - 30 * 3600,) * 2)
    assert verify_backup_restore.latest_dump(tmp_path, "distress_db") == dump
    with pytest.raises(SystemExit):
        verify_backup_restore.latest_dump(tmp_path / "empty", "distress_db")
