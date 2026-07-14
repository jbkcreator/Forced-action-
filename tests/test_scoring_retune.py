"""Orchestrator (Stage B->C->D->E->F) fail-fast + gating behavior.

Mocks every subprocess stage and the DB (advisory lock, shadow-table clear) so
these run with no real subprocess, sklearn, or network dependency — pure
control-flow tests for run_pipeline's fail-fast chain and exit-code mapping.
"""
from __future__ import annotations

import contextlib

import pytest

from src.tasks import scoring_retune as sr


class _FakeResult:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class _FakeLockSession:
    """Stands in for the get_db_context() session used for the advisory lock."""

    def __init__(self, lock_available=True):
        self.lock_available = lock_available
        self.executed: list[str] = []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.executed.append(sql)
        if "pg_try_advisory_lock" in sql:
            return _FakeResult(self.lock_available)
        return _FakeResult(None)


@pytest.fixture
def lock_session(monkeypatch):
    """Default: lock is available. Tests can flip `.lock_available` before calling."""
    session = _FakeLockSession(lock_available=True)

    @contextlib.contextmanager
    def fake_ctx():
        yield session

    import src.core.database as db
    monkeypatch.setattr(db, "get_db_context", fake_ctx)
    monkeypatch.setattr(sr, "_clear_shadow_table", lambda: None)
    monkeypatch.setattr(sr, "_prune_old_artifacts", lambda: None)
    return session


@pytest.fixture
def stage_dirs(tmp_path, monkeypatch):
    training = tmp_path / "training"
    fit = tmp_path / "fit"
    validation = tmp_path / "validation"
    monkeypatch.setattr(sr, "TRAINING_DIR", training)
    monkeypatch.setattr(sr, "FIT_DIR", fit)
    monkeypatch.setattr(sr, "VALIDATION_DIR", validation)
    return {"training": training, "fit": fit, "validation": validation}


def _install_stub_run(monkeypatch, dirs, *, codes, create_files=True):
    """Patch sr._run with a stub keyed by the module's trailing name component.

    `codes` maps short module name -> exit code. Missing stages default to 0.
    When create_files is True, a successful Stage B/C run touches the file the
    real subprocess would have produced (read from the --run-id arg it was
    called with), so the is_file() checks in run_pipeline pass.
    """
    calls: list[tuple[str, list[str]]] = []

    def fake_run(module, args):
        calls.append((module, list(args)))
        short = module.rsplit(".", 1)[-1]
        code = codes.get(short, 0)
        if create_files and code == 0 and "--run-id" in args:
            run_id = args[args.index("--run-id") + 1]
            if short == "scoring_training_data":
                dirs["training"].mkdir(parents=True, exist_ok=True)
                (dirs["training"] / f"{run_id}.csv").write_text("x")
            elif short == "scoring_fit":
                dirs["fit"].mkdir(parents=True, exist_ok=True)
                (dirs["fit"] / f"{run_id}.json").write_text("{}")
        return code

    monkeypatch.setattr(sr, "_run", fake_run)
    return calls


def _stage_names(calls):
    return [module.rsplit(".", 1)[-1] for module, _ in calls]


def test_all_stages_run_and_promote(lock_session, stage_dirs, monkeypatch):
    calls = _install_stub_run(monkeypatch, stage_dirs, codes={})  # all 0
    code = sr.run_pipeline(window_days=90, since=None, county=None)
    assert code == 0
    assert _stage_names(calls) == [
        "scoring_training_data", "scoring_fit", "cds_engine",
        "scoring_validation_report", "scoring_cutover",
    ]
    # Stage F call carries the shared run_id
    f_args = calls[-1][1]
    assert "--run-id" in f_args


def test_stage_b_fails_aborts(lock_session, stage_dirs, monkeypatch):
    calls = _install_stub_run(monkeypatch, stage_dirs, codes={"scoring_training_data": 1})
    code = sr.run_pipeline(window_days=90, since=None, county=None)
    assert code == 2
    assert _stage_names(calls) == ["scoring_training_data"]


def test_stage_b_produces_no_csv_aborts(lock_session, stage_dirs, monkeypatch):
    calls = _install_stub_run(monkeypatch, stage_dirs, codes={}, create_files=False)
    code = sr.run_pipeline(window_days=90, since=None, county=None)
    assert code == 2
    assert _stage_names(calls) == ["scoring_training_data"]


def test_stage_c_fails_aborts(lock_session, stage_dirs, monkeypatch):
    calls = _install_stub_run(monkeypatch, stage_dirs, codes={"scoring_fit": 1})
    code = sr.run_pipeline(window_days=90, since=None, county=None)
    assert code == 2
    assert _stage_names(calls) == ["scoring_training_data", "scoring_fit"]


def test_shadow_clear_failure_aborts(lock_session, stage_dirs, monkeypatch):
    def raise_clear():
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(sr, "_clear_shadow_table", raise_clear)
    calls = _install_stub_run(monkeypatch, stage_dirs, codes={})
    code = sr.run_pipeline(window_days=90, since=None, county=None)
    assert code == 2
    assert _stage_names(calls) == ["scoring_training_data", "scoring_fit"]


def test_stage_d_fails_aborts(lock_session, stage_dirs, monkeypatch):
    calls = _install_stub_run(monkeypatch, stage_dirs, codes={"cds_engine": 1})
    code = sr.run_pipeline(window_days=90, since=None, county=None)
    assert code == 2
    assert _stage_names(calls) == ["scoring_training_data", "scoring_fit", "cds_engine"]


def test_stage_f_blocked_returns_1(lock_session, stage_dirs, monkeypatch):
    calls = _install_stub_run(monkeypatch, stage_dirs, codes={"scoring_cutover": 1})
    code = sr.run_pipeline(window_days=90, since=None, county=None)
    assert code == 1
    assert _stage_names(calls) == [
        "scoring_training_data", "scoring_fit", "cds_engine",
        "scoring_validation_report", "scoring_cutover",
    ]


def test_stage_f_bad_input_returns_2_distinct_from_blocked(lock_session, stage_dirs, monkeypatch):
    """Stage F exit=2 (bad input) must NOT collapse into the same 1 as a gate block."""
    calls = _install_stub_run(monkeypatch, stage_dirs, codes={"scoring_cutover": 2})
    code = sr.run_pipeline(window_days=90, since=None, county=None)
    assert code == 2


def test_lock_already_held_aborts_without_running_stages(lock_session, stage_dirs, monkeypatch):
    lock_session.lock_available = False

    def fail_if_called(module, args):
        raise AssertionError(f"_run should not be invoked when the lock is held: {module}")

    monkeypatch.setattr(sr, "_run", fail_if_called)
    code = sr.run_pipeline(window_days=90, since=None, county=None)
    assert code == 2


def test_lock_released_after_pipeline_completes(lock_session, stage_dirs, monkeypatch):
    _install_stub_run(monkeypatch, stage_dirs, codes={})
    sr.run_pipeline(window_days=90, since=None, county=None)
    assert any("pg_advisory_unlock" in s for s in lock_session.executed)
