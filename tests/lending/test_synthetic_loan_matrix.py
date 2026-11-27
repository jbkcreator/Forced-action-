"""R1 0.10: the synthetic loan matrix, 3 loan types x 7 edge states, run on every PR in CI.

Each fixture (tests/fixtures/synthetic_loans.py) goes through the check that already exists for its edge state.
The expired-document and cross-borrower fixtures carry their data now; the validator that blocks them is R1 1.7
(packaging/packet_validator.py), which adds its tests over ``loans(edge_case=...)``.
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import NoReturn

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from config.lending_compliance import ReasonCode
from config.lending_instant_quote import REFINANCE_RULE_PENDING
from packages.agent_core.lane_guard import lane_violation
from src.lending.compliance import filter_loadable
from src.lending.contracts import LoanFile
from src.lending.instant_quote import Guarantor, quote
from src.lending.state_machine import ingest_loan_file
from tests.fixtures.synthetic_loans import AS_OF, EDGE_CASES, PRODUCTS, all_loans, loans
from tests.lending.test_loan_state_machine_db import sm_db  # noqa: F401  (fixture: throwaway schema, rolled back)

IN_CI = os.environ.get("CI") == "true"  # GitHub Actions sets CI=true
NOW = datetime(2026, 11, 2, 12, 0, tzinfo=timezone.utc)


def test_matrix_is_complete_and_deterministic():
    first, second = all_loans(), all_loans()
    assert len(first) == 21
    assert {(l.product, l.edge_case) for l in first} == {(p, c) for p in PRODUCTS for c in EDGE_CASES}
    assert first == second
    assert len({l.loan_file.borrower.phone for l in first}) == 21
    assert len({l.loan_file.property.address for l in first}) == 21


@pytest.mark.parametrize("loan", all_loans(), ids=lambda l: l.key)
def test_every_fixture_round_trips_the_strict_contract(loan):
    assert LoanFile.model_validate(loan.loan_file.model_dump()) == loan.loan_file
    assert loan.loan_file.schema_version == 1


@pytest.mark.parametrize("loan", loans(edge_case="incomplete"), ids=lambda l: l.key)
def test_incomplete_fixtures_keep_unknown_inputs_none(loan):
    f = loan.loan_file
    assert (f.loan_amount, f.purchase_price, f.rehab_budget, f.arv, f.payoff_balance) == (None,) * 5
    assert loan.quote_request is None


@pytest.mark.parametrize("loan", loans(edge_case="conflicting_fico"), ids=lambda l: l.key)
def test_quote_prices_off_the_lowest_guarantor_fico(loan, monkeypatch):
    monkeypatch.setattr("src.lending.instant_quote.engine.REFINANCE_QUOTES_ENABLED", True)
    both = quote(loan.quote_request)
    low = quote(replace(loan.quote_request, guarantors=(Guarantor(640, 4, 2),)))
    high = quote(replace(loan.quote_request, guarantors=(Guarantor(760, 4, 2),)))
    assert both.status == "indicative"
    assert (both.loan_amount, both.fee_surcharge_pct, both.conditions) == (
        low.loan_amount, low.fee_surcharge_pct, low.conditions)
    assert (both.loan_amount, both.fee_surcharge_pct) != (high.loan_amount, high.fee_surcharge_pct)


@pytest.mark.parametrize("loan", loans(edge_case="clean"), ids=lambda l: l.key)
def test_clean_fixtures_quote_as_the_engine_allows_today(loan):
    result = quote(loan.quote_request)
    if loan.product == "refinance":
        assert (result.status, result.reason) == ("rejected", REFINANCE_RULE_PENDING)
    else:
        assert result.status == "indicative"


def test_only_backflip_fixtures_trip_the_lane_guard():
    hits = {l.key for l in all_loans()
            if lane_violation({"file": l.loan_file.model_dump(mode="json"), "draft": l.outbound_draft})}
    assert hits == {l.key for l in loans(edge_case="backflip_tagged")}


@pytest.mark.parametrize("loan", loans(edge_case="expired_documents"), ids=lambda l: l.key)
def test_expired_document_fixtures_hold_a_stale_statement_and_an_expired_pricing_sheet(loan):
    docs = {d.kind: d for d in loan.documents}
    assert (AS_OF - docs["bank_statement"].dated).days > 60
    assert docs["pricing_sheet"].expires_on < AS_OF


@pytest.mark.parametrize("loan", loans(edge_case="cross_borrower_document"), ids=lambda l: l.key)
def test_cross_borrower_fixtures_hold_another_persons_statement(loan):
    b = loan.loan_file.borrower
    assert any(d.owner_name != f"{b.first_name} {b.last_name}" for d in loan.documents if d.kind == "bank_statement")


def test_fixtures_outside_the_document_cases_carry_only_current_own_documents():
    for l in all_loans():
        if l.edge_case in ("expired_documents", "cross_borrower_document"):
            continue
        b = l.loan_file.borrower
        for d in l.documents:
            assert d.owner_name == f"{b.first_name} {b.last_name}", l.key
            assert (AS_OF - d.dated).days <= 60 and d.expires_on is None, l.key


def _skip_or_fail(reason: str) -> NoReturn:
    """No database: skip on a laptop, fail in CI, so the matrix can never pass green on skipped DB tests."""
    if IN_CI:
        pytest.fail(f"synthetic matrix DB tests cannot run in CI: {reason}")
    pytest.skip(reason)


def test_skip_or_fail_fails_in_ci_and_skips_elsewhere(monkeypatch):
    monkeypatch.setattr(sys.modules[__name__], "IN_CI", True)
    with pytest.raises(pytest.fail.Exception):
        _skip_or_fail("no db")
    monkeypatch.setattr(sys.modules[__name__], "IN_CI", False)
    with pytest.raises(pytest.skip.Exception):
        _skip_or_fail("no db")


@pytest.fixture
def matrix_db(request):
    """sm_db (the 0.1 throwaway schema), but its skip goes through _skip_or_fail."""
    try:
        return request.getfixturevalue("sm_db")
    except pytest.skip.Exception as exc:
        _skip_or_fail(str(exc))


@pytest.fixture
def gate_db():
    """The real lending tables inside one rolled-back transaction (the pattern of test_compliance_filter.py)."""
    from config.settings import get_settings

    url = get_settings().database_url
    if not url:
        _skip_or_fail("no DATABASE_URL")
    engine = create_engine(str(url), connect_args={"connect_timeout": 20})
    try:
        conn = engine.connect()
    except Exception:
        engine.dispose()
        _skip_or_fail("Postgres is not reachable")
    tx = conn.begin()
    conn.execute(text("SET LOCAL lock_timeout = '5s'"))
    session = Session(bind=conn, join_transaction_mode="create_savepoint")
    yield session
    session.close()
    tx.rollback()
    conn.close()
    engine.dispose()


def test_every_fixture_ingests_once_with_unknown_inputs_null(matrix_db):
    ids = {l.key: ingest_loan_file(matrix_db, l.loan_file, actor="test").loan_file_id for l in all_loans()}
    assert len(set(ids.values())) == 21
    assert matrix_db.execute(text("SELECT count(*) FROM lending.loan_files")).scalar_one() == 21
    for l in loans(edge_case="incomplete"):
        row = matrix_db.execute(
            text("SELECT loan_amount, purchase_price, rehab_budget, arv, payoff_balance FROM lending.loan_files "
                 "WHERE id = :id"), {"id": ids[l.key]}).one()
        assert tuple(row) == (None,) * 5, l.key
    refi = loans(product="refinance", edge_case="clean")[0]
    row = matrix_db.execute(text("SELECT product, payoff_balance FROM lending.loan_files WHERE id = :id"),
                            {"id": ids[refi.key]}).one()
    assert tuple(row) == ("other", 150000)


def _no_scrub(phones):
    raise AssertionError(f"the scrubber must not be called, got {phones}")


def test_only_dnc_flagged_fixtures_are_blocked_by_the_load_filter(gate_db):
    # One fresh scrub row per fixture, so the Tracerfy scrubber is never called.
    for l in all_loans():
        gate_db.execute(
            text("INSERT INTO dnc_phone_checks (phone, national_dnc, litigator, checked_at, source, raw_result) "
                 "VALUES (:p, :n, :l, :at, 'test', CAST(:raw AS jsonb))"),
            {"p": l.loan_file.borrower.phone, "n": l.dnc_flag == "national_dnc", "l": l.dnc_flag == "litigator",
             "at": NOW - timedelta(days=1), "raw": json.dumps({"state_dnc": "Y" if l.dnc_flag == "state_dnc" else "N"})})
    results = filter_loadable([{"phone": l.loan_file.borrower.phone} for l in all_loans()],
                              gate_db, now=NOW, scrubber=_no_scrub)
    by_key = {l.key: r for l, r in zip(all_loans(), results)}
    flagged = loans(edge_case="dnc_flagged")
    assert {k for k, r in by_key.items() if not r.allowed} == {l.key for l in flagged}
    expected = {"national_dnc": ReasonCode.NATIONAL_DNC, "litigator": ReasonCode.LITIGATOR,
                "state_dnc": ReasonCode.STATE_DNC}
    for l in flagged:
        assert by_key[l.key].reason == expected[l.dnc_flag], l.key
