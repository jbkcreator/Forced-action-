"""The test run never uses the live database (Q41)."""
import pytest

from _database_guard import UNCONFIGURED_URL, UnsafeTestDatabase, choose_database_url

LIVE = "postgresql://app:secret@10.0.0.5:5432/distress_db"
THROWAWAY = "postgresql://postgres:x@127.0.0.1:58123/fa_test"


def test_without_a_test_url_the_tests_get_an_address_nothing_listens_on():
    assert choose_database_url(None, LIVE) == UNCONFIGURED_URL
    assert choose_database_url("", LIVE) == UNCONFIGURED_URL
    assert "@127.0.0.1:1/" in UNCONFIGURED_URL  # port 1: a connection is refused at once


def test_a_throwaway_database_is_used_as_given():
    assert choose_database_url(THROWAWAY, LIVE) == THROWAWAY
    assert choose_database_url(THROWAWAY, None) == THROWAWAY


def test_a_database_whose_name_has_no_test_in_it_is_refused():
    with pytest.raises(UnsafeTestDatabase, match="must contain 'test'"):
        choose_database_url("postgresql://u:p@127.0.0.1:5432/distress_db", None)


def test_the_live_database_is_refused_even_when_its_name_contains_test():
    live_test_named = "postgresql://u:p@10.0.0.5:5432/distress_test"
    with pytest.raises(UnsafeTestDatabase, match="same database"):
        choose_database_url(live_test_named, "postgresql+psycopg2://other:pw@10.0.0.5/distress_test")


def test_the_same_name_on_another_host_is_a_different_database():
    assert choose_database_url(THROWAWAY, "postgresql://u:p@10.0.0.5:58123/fa_test") == THROWAWAY


def test_an_unparsable_url_is_refused():
    with pytest.raises(UnsafeTestDatabase):
        choose_database_url("not a url", None)


def test_the_run_itself_is_not_on_a_live_database():
    import os

    from sqlalchemy.engine import make_url

    url = make_url(os.environ["DATABASE_URL"])
    assert "test" in (url.database or "").lower()
    assert os.environ["VERA_DATABASE_URL"] == ""
