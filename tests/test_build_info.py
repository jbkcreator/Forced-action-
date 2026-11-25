"""R1 0.10: the commit SHA and migration head a process reports on /health."""
from __future__ import annotations

import subprocess

import src.utils.build_info as build_info_module
from src.utils.build_info import UNKNOWN, BuildInfo, build_info, read_build_info

SHA = "e1636938" + "0" * 32


def fake_git(outputs):
    def run(args):
        out = outputs[args[0]]
        if isinstance(out, Exception):
            raise out
        return out
    return run


def test_reads_sha_and_newest_migration():
    info = read_build_info(fake_git({"rev-parse": SHA, "log": "migrations/apply_lending_lender_rule_snapshots.py"}))
    assert info == BuildInfo(SHA, "apply_lending_lender_rule_snapshots")


def test_migration_head_is_last_in_sort_order():
    info = read_build_info(fake_git({"rev-parse": SHA, "log": "migrations/apply_b.py\nmigrations/apply_a.py\n"}))
    assert info.migration_head == "apply_b"


def test_git_failure_gives_unknown():
    info = read_build_info(fake_git({"rev-parse": FileNotFoundError(), "log": subprocess.TimeoutExpired("git", 5)}))
    assert info == BuildInfo(UNKNOWN, UNKNOWN)


def test_non_hex_sha_gives_unknown():
    assert read_build_info(fake_git({"rev-parse": "fatal: not a git repository", "log": ""})).commit_sha == UNKNOWN


def test_one_failing_field_does_not_take_the_other_down():
    info = read_build_info(fake_git({"rev-parse": SHA, "log": subprocess.CalledProcessError(128, "git")}))
    assert info == BuildInfo(SHA, UNKNOWN)


def test_build_info_is_read_once_per_process(monkeypatch):
    calls = []
    monkeypatch.setattr(build_info_module, "_git",
                        lambda args: calls.append(args) or (SHA if args[0] == "rev-parse" else ""))
    build_info.cache_clear()
    try:
        assert build_info() is build_info()
        assert len(calls) == 2
    finally:
        build_info.cache_clear()


def test_the_real_repo_reports_its_head():
    head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True,
                          cwd=build_info_module.REPO_ROOT).stdout.strip()
    info = read_build_info()
    assert info.commit_sha == head
    assert info.migration_head.startswith("apply_")
