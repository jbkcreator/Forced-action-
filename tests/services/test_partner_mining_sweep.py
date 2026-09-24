"""
Regression tests for the partner_mining_sweep cron crashes (2026-09-23/24):
deed query read a non-existent properties.homestead_exempt, wholesaler
resolution inserted duplicate links from a join fan-out, dry-run committed
resolution links, and run stats were rejected by the CHECK constraint.
"""
from contextlib import contextmanager
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from migrations import apply_partner_mining_run_stats_source_type as mig
from src.agents.vera.checks.live_state import _MODULE_TO_SOURCE_TYPES
from src.services.partner_mining import resolution
from src.tasks import partner_mining_sweep

_AS_OF = date(2026, 9, 24)


def _empty_deed_session() -> MagicMock:
    session = MagicMock()
    session.execute.return_value.yield_per.return_value = iter([])
    return session


def _patched_db(session: MagicMock):
    @contextmanager
    def _ctx():
        yield session
    return patch("src.core.database.get_db_context", _ctx)


@pytest.mark.parametrize("dry_run, expected_calls", [(True, 0), (False, 1)])
def test_resolution_skipped_only_in_dry_run(dry_run, expected_calls):
    session = _empty_deed_session()
    with _patched_db(session), \
         patch.object(resolution, "run_counterparty_resolution") as lender, \
         patch.object(resolution, "run_wholesaler_resolution") as wholesaler:
        partner_mining_sweep._run_county("pinellas", dry_run=dry_run,
                                         as_of=_AS_OF)
    assert lender.call_count == expected_calls
    assert wholesaler.call_count == expected_calls


def test_deed_query_reads_homestead_from_financials():
    session = _empty_deed_session()
    with _patched_db(session):
        partner_mining_sweep._run_county("pinellas", dry_run=True,
                                         as_of=_AS_OF)
    sql = str(session.execute.call_args_list[0].args[0])
    assert "f.homestead_exempt" in sql
    assert "LEFT JOIN financials f" in sql
    assert "p.homestead_exempt" not in sql


def test_wholesaler_query_cannot_fan_out_per_sell_deed():
    # A JOIN on sell deeds returns one row per qualifying sell; EXISTS returns
    # each buy deed once, so one run never inserts the same source_id twice.
    sql = " ".join(resolution._WHOLESALER_QUERY.split())
    assert "JOIN deeds sell" not in sql
    assert "EXISTS ( SELECT 1 FROM deeds sell" in sql


def test_vera_maps_partner_mining_cron_module():
    assert _MODULE_TO_SOURCE_TYPES["src.tasks.partner_mining_sweep"] == {"partner_mining"}


def test_migration_preserves_existing_constraint_values():
    conn = MagicMock()
    conn.execute.return_value.first.return_value = MagicMock(
        _mapping={"def": "CHECK (source_type IN ('deeds','lis_pendens_outcomes'))"}
    )
    current = mig._existing_check_values(conn, "check_run_stats_source_type", "scraper_run_stats")
    assert current | mig.REQUIRED_SOURCE_TYPES == {"deeds", "lis_pendens_outcomes", "partner_mining"}


def _county_outcomes(outcomes: dict):
    def _fake(cid, *, dry_run, as_of):
        value = outcomes[cid]
        if isinstance(value, Exception):
            raise value
        return value
    return _fake


@pytest.mark.parametrize("outcomes, expected_exit", [
    ({"hillsborough": 87, "pinellas": 34, "pasco": 5}, 0),
    ({"hillsborough": 87, "pinellas": 34, "pasco": RuntimeError("portal down")}, 0),
    ({"hillsborough": 0, "pinellas": 0, "pasco": RuntimeError("portal down")}, 1),
    ({"hillsborough": RuntimeError("pasco parcel lookup failed"), "pinellas": 34, "pasco": 5}, 1),
    ({"hillsborough": 87, "pinellas": RuntimeError("boom"), "pasco": RuntimeError("down")}, 1),
])
def test_exit_code_follows_partial_success_rule(outcomes, expected_exit):
    with patch.object(partner_mining_sweep, "_run_county", side_effect=_county_outcomes(outcomes)):
        with pytest.raises(SystemExit) as exc:
            partner_mining_sweep.main(["--dry-run"])
    assert exc.value.code == expected_exit
