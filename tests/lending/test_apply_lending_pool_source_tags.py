"""PR 318 re-review #9: the pool_name CHECK is replaced whichever name the old one carries. No database."""
from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_migration():
    path = Path(__file__).resolve().parents[2] / "migrations" / "apply_lending_pool_source_tags.py"
    spec = importlib.util.spec_from_file_location("apply_lending_pool_source_tags", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _RecordingSession:
    def __init__(self):
        self.statements = []

    def execute(self, statement):
        self.statements.append(str(statement))


def test_both_possible_names_of_the_old_pool_name_check_are_dropped_before_the_new_one_is_added():
    session = _RecordingSession()
    _load_migration().apply(session)
    drops = [i for i, s in enumerate(session.statements) if "DROP CONSTRAINT IF EXISTS" in s]
    add = next(i for i, s in enumerate(session.statements) if "ADD CONSTRAINT" in s)
    dropped = " ".join(session.statements[i] for i in drops)
    assert "lending_calling_pool_staging_pool_name_check" in dropped
    assert " calling_pool_staging_pool_name_check" in dropped     # name from a fresh, migrations-only build
    assert all(i < add for i in drops)
    assert "auction_winner" in session.statements[add] and "permit_owner" in session.statements[add]
