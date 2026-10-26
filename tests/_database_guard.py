"""Point the test run at a throwaway database, never the live one.

Imported first by tests/conftest.py, before anything reads DATABASE_URL.

* ``TEST_DATABASE_URL`` set: DATABASE_URL replaces itself with it for this process (VERA_DATABASE_URL is blanked, it
  is a live read-only role). The run is refused when the database name does not contain "test", or when it is the
  database DATABASE_URL points to outside the tests.
* ``TEST_DATABASE_URL`` not set: DATABASE_URL is set to an address nothing listens on (it beats ``.env``), so the
  tests that need Postgres skip instead of touching a live database.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping, Optional

import pytest
from sqlalchemy.engine import make_url

# A valid address nothing listens on: the app builds its engine from DATABASE_URL when it is imported, so the value
# cannot be empty. Fixtures that connect skip on the refused connection; modules that read DATABASE_URL straight
# from the environment are skipped by ``pytest_collection_modifyitems`` below.
UNCONFIGURED_URL = "postgresql://unconfigured:unconfigured@127.0.0.1:1/unconfigured_test"
ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


class UnsafeTestDatabase(Exception):
    """TEST_DATABASE_URL is not a database the tests may use."""


def _same_database(first: str, second: str) -> bool:
    try:
        a, b = make_url(first), make_url(second)
    except Exception:
        return False
    return (a.host, a.port or 5432, a.database) == (b.host, b.port or 5432, b.database)


def choose_database_url(test_url: Optional[str], configured_url: Optional[str]) -> str:
    """The URL the tests run on: ``test_url`` when it is safe, an unreachable address when none is given."""
    if not test_url:
        return UNCONFIGURED_URL
    try:
        database = make_url(test_url).database or ""
    except Exception as exc:
        raise UnsafeTestDatabase("TEST_DATABASE_URL is not a valid database URL") from exc
    if "test" not in database.lower():
        raise UnsafeTestDatabase("the name of the database in TEST_DATABASE_URL must contain 'test'")
    if configured_url and _same_database(test_url, configured_url):
        raise UnsafeTestDatabase("TEST_DATABASE_URL is the same database as DATABASE_URL: point it at a throwaway one")
    return test_url


def _configured_url(env: Mapping[str, str]) -> Optional[str]:
    """The DATABASE_URL the app would use: the environment first, then ``.env``."""
    if env.get("DATABASE_URL"):
        return env["DATABASE_URL"]
    try:
        from dotenv import dotenv_values

        return dotenv_values(ENV_FILE).get("DATABASE_URL") or None
    except Exception:
        return None


def apply(env: Optional[dict] = None) -> str:
    env = os.environ if env is None else env
    try:
        url = choose_database_url(env.get("TEST_DATABASE_URL"), _configured_url(env))
    except UnsafeTestDatabase as exc:
        pytest.exit(f"Refusing to run the tests: {exc}.", returncode=2)
    env["DATABASE_URL"] = url
    env["VERA_DATABASE_URL"] = ""
    return url


_DATABASE_URL = apply()
_ENV_DATABASE_READERS = ('environ["DATABASE_URL"]', "environ.get(\"DATABASE_URL\")", "environ.get('DATABASE_URL')")
_reads_env_database: dict[Path, bool] = {}


def _reads_database_url_from_env(path: Path) -> bool:
    if path not in _reads_env_database:
        try:
            source = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            source = ""
        _reads_env_database[path] = any(reader in source for reader in _ENV_DATABASE_READERS)
    return _reads_env_database[path]


def pytest_collection_modifyitems(config, items):
    """Registered through tests/conftest.py. Without TEST_DATABASE_URL, skip every test module that reads
    DATABASE_URL from the environment, so none of them can reach a database."""
    if _DATABASE_URL != UNCONFIGURED_URL:
        return
    skip = pytest.mark.skip(reason="TEST_DATABASE_URL is not set: database tests never run on the live database")
    for item in items:
        if _reads_database_url_from_env(Path(str(item.path))):
            item.add_marker(skip)
