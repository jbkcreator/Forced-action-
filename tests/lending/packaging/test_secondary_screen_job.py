"""The morning job: a card that failed to post is retried on the next run, and a retired package's card is updated."""
from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from src.lending.packaging.screen import FileRef, Screened
from src.tasks import secondary_screen_job as job
from tests.lending.packaging.fakes import FakeSession


@pytest.fixture
def world(monkeypatch):
    state = {"db": FakeSession(), "screened": Screened([11], []), "posted": [], "refreshed": [], "fail_post": True}
    settings = SimpleNamespace(lending_secondary_screen_enabled=True, lending_ops_channel="C-OPS")
    monkeypatch.setattr(job, "get_settings", lambda: settings)
    monkeypatch.setattr(job, "cora_client", lambda: object())
    monkeypatch.setattr(job, "secondary_matrix", lambda: ((), "v1"))
    monkeypatch.setattr(job, "eligible_files", lambda db: [FileRef("loan_file", 7)])
    monkeypatch.setattr(job, "screen_file", lambda db, ref, **k: state["screened"])
    monkeypatch.setattr(job, "compile_package",
                        lambda db, pid, **k: SimpleNamespace(package_id=pid, lender_key="corevest", status="ready"))

    def post(db, client, channel, result):
        if state["fail_post"]:
            raise ConnectionError("slack down")
        state["posted"].append(result.package_id)

    monkeypatch.setattr(job, "post_card", post)
    monkeypatch.setattr(job, "refresh_card", lambda client, db, pid, note, today: state["refreshed"].append(pid))

    @contextmanager
    def session():
        yield state["db"]

    monkeypatch.setattr(job, "lending_session", session)
    return state


def test_a_card_that_failed_to_post_is_posted_on_the_next_run_with_unchanged_facts(world):
    assert job.run() == 1 and world["posted"] == []
    world["fail_post"] = False
    world["screened"] = Screened([], [])  # same facts, same rules: nothing re-drafted
    world["db"].answers["COALESCE(card_ts, '') = ''"] = [{"id": 11}]
    assert job.run() == 0  # a retry is not a new draft
    assert world["posted"] == [11]


def test_a_package_in_both_lists_is_posted_once(world):
    world["fail_post"] = False
    world["db"].answers["COALESCE(card_ts, '') = ''"] = [{"id": 11}]
    job.run()
    assert world["posted"] == [11]


def test_retired_packages_have_their_cards_updated(world):
    world["fail_post"] = False
    world["screened"] = Screened([], [41])
    job.run()
    assert world["refreshed"] == [41]


def test_dry_run_posts_and_retries_nothing(world):
    world["fail_post"] = False
    world["db"].answers["COALESCE(card_ts, '') = ''"] = [{"id": 12}]
    job.run(dry_run=True)
    assert world["posted"] == [] and not world["db"].ran("COALESCE(card_ts, '') = ''")
