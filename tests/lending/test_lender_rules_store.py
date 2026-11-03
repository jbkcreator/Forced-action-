"""T-05 (Josh A1): lender rules as data. The table overrides the config per field; nothing else changes."""
from __future__ import annotations

import logging
from decimal import Decimal

import pytest
from sqlalchemy import text

from config.lender_matrix import LENDER_MATRIX, PARAMETER_FIELDS, fully_confirmed, field_status_of
from migrations.apply_lending_lender_rule_values import apply_to, seed_rows
from src.lending import lender_rules_store as store
from src.lending.contracts import BorrowerProfile, DeclineReason, FieldStatus, LoanRequest, LoanType
from src.lending.lender_fit import evaluate_lender_fit
from src.lending.lender_rules_store import RuleValueError, apply_rows, current_matrix, decode_value, encode_value

FLIP = "generic_backflip_flip"


def _by_key(matrix, key):
    return next(rules for rules in matrix if rules.key == key)


def _row(field, value, status="confirmed", key=FLIP):
    return (key, field, value, status)


def test_no_rows_returns_the_config_unchanged():
    assert apply_rows(LENDER_MATRIX, []) is not None
    assert apply_rows(LENDER_MATRIX, []) == LENDER_MATRIX


def test_a_row_overrides_one_value_and_its_status():
    matrix = apply_rows(LENDER_MATRIX, [_row("credit_floor", 700, "confirmed")])
    flip = _by_key(matrix, FLIP)
    assert flip.credit_floor == 700
    assert field_status_of(flip, "credit_floor") is FieldStatus.CONFIRMED
    assert field_status_of(flip, "max_ltv") is FieldStatus.PLACEHOLDER


def test_other_lenders_are_untouched():
    matrix = apply_rows(LENDER_MATRIX, [_row("credit_floor", 700)])
    assert _by_key(matrix, "rcn_capital") == _by_key(LENDER_MATRIX, "rcn_capital")


def test_a_lender_is_verified_only_when_every_field_is_confirmed():
    every_field = [_row(name, getattr(_by_key(LENDER_MATRIX, FLIP), name)) for name in PARAMETER_FIELDS]
    rows = [(k, f, encode_value(v), s) for k, f, v, s in every_field]
    flip = _by_key(apply_rows(LENDER_MATRIX, rows), FLIP)
    assert flip.verified is True and fully_confirmed(flip)

    one_short = apply_rows(LENDER_MATRIX, rows[:-1] + [_row(PARAMETER_FIELDS[-1], 12, "placeholder")])
    assert _by_key(one_short, FLIP).verified is False


def test_confirm_at_submission_is_kept_and_is_not_confirmed():
    flip = _by_key(apply_rows(LENDER_MATRIX, [_row("max_ltc", 0.8, "confirm_at_submission")]), FLIP)
    assert field_status_of(flip, "max_ltc") is FieldStatus.CONFIRM_AT_SUBMISSION
    assert not fully_confirmed(flip)


@pytest.mark.parametrize("field, value", [
    ("credit_floor", "high"),
    ("credit_floor", -5),
    ("max_ltc", True),
    ("max_ltc", "abc"),
    ("loan_types", ["NOT_A_LOAN_TYPE"]),
    ("loan_types", "FIX_AND_FLIP"),
    ("hold_months", 0),
    ("min_completed_projects", 1.5),
    ("requires_ground_up_build", "yes"),
    ("not_a_field", 1),
])
def test_an_invalid_row_is_skipped_and_logged_not_applied(field, value, caplog):
    with caplog.at_level(logging.ERROR):
        matrix = apply_rows(LENDER_MATRIX, [_row(field, value)])
    assert matrix == LENDER_MATRIX
    assert "row skipped" in caplog.text


def test_an_unknown_status_is_skipped(caplog):
    with caplog.at_level(logging.ERROR):
        matrix = apply_rows(LENDER_MATRIX, [_row("credit_floor", 700, "sure")])
    assert matrix == LENDER_MATRIX


def test_an_unknown_lender_is_ignored(caplog):
    with caplog.at_level(logging.WARNING):
        matrix = apply_rows(LENDER_MATRIX, [_row("credit_floor", 700, key="brand_new_lender")])
    assert matrix == LENDER_MATRIX
    assert "unknown lender" in caplog.text


def test_a_merged_matrix_that_fails_validation_falls_back_to_the_config(caplog):
    rows = [_row("min_loan_amount", 5_000_000), _row("max_loan_amount", 1_000)]
    with caplog.at_level(logging.ERROR):
        assert apply_rows(LENDER_MATRIX, rows) == LENDER_MATRIX
    assert "table ignored" in caplog.text


def test_sets_decimals_and_nulls_decode():
    assert decode_value("approved_states", ["fl", " ga "]) == frozenset({"FL", "GA"})
    assert decode_value("loan_types", ["BRIDGE"]) == frozenset({LoanType.BRIDGE})
    assert decode_value("max_ltv", 0.7) == Decimal("0.7")
    assert decode_value("max_ltv", "0.75") == Decimal("0.75")
    assert decode_value("max_loan_amount", None) is None
    assert decode_value("credit_floor", None) is None
    with pytest.raises(RuleValueError):
        decode_value("hold_months", None)


@pytest.mark.parametrize("rules", LENDER_MATRIX, ids=lambda r: r.key)
def test_every_seed_value_round_trips_through_the_loader(rules):
    for name in PARAMETER_FIELDS:
        original = getattr(rules, name)
        assert decode_value(name, encode_value(original)) == original


def test_seed_has_one_row_per_lender_and_field_all_with_a_status():
    rows = seed_rows()
    assert len(rows) == len(LENDER_MATRIX) * len(PARAMETER_FIELDS)
    assert len({(r["lender_key"], r["field"]) for r in rows}) == len(rows)
    assert {r["status"] for r in rows} <= {s.value for s in FieldStatus}


def test_evaluator_uses_an_edited_value_end_to_end():
    request = LoanRequest(loan_type=LoanType.FIX_AND_FLIP, loan_amount=Decimal("300_000"), state="FL",
                          purchase_price=Decimal("300_000"), rehab_budget=Decimal("100_000"), arv=Decimal("600_000"))
    borrower = BorrowerProfile(credit_band_min_fico=700, state="FL")  # above the 680 safe-set floor (Q1)

    before = evaluate_lender_fit(borrower, request, matrix=LENDER_MATRIX)
    assert FLIP in [fit.lender_key for fit in before.fitting]

    edited = apply_rows(LENDER_MATRIX, [_row("max_ltv", 0.4, "confirmed")])  # 300K / 600K ARV = 50% LTV
    after = evaluate_lender_fit(borrower, request, matrix=edited)
    miss = next(miss for miss in after.non_fitting if miss.lender_key == FLIP)
    assert DeclineReason.LTV_OVER_MAX in miss.reason_codes


def test_the_config_is_used_when_the_flag_is_off(monkeypatch):
    class _Off:
        lending_rules_from_db = False

    monkeypatch.setattr(store, "get_settings", lambda: _Off())
    assert current_matrix() is LENDER_MATRIX


def test_the_config_is_used_when_the_database_is_unavailable(monkeypatch, caplog):
    class _On:
        lending_rules_from_db = True

    def _boom():
        raise RuntimeError("no database")

    monkeypatch.setattr(store, "get_settings", lambda: _On())
    monkeypatch.setattr("src.core.database.get_db_context", _boom)
    with caplog.at_level(logging.ERROR):
        assert current_matrix() is LENDER_MATRIX


def test_the_table_is_used_when_the_flag_is_on_and_the_database_answers(monkeypatch):
    """Dynamic DB overrides via lending.lender_rule_values: with
    LENDING_RULES_FROM_DB on, the evaluators read the table's matrix, not the config."""
    from contextlib import contextmanager

    class _On:
        lending_rules_from_db = True

    sentinel = (LENDER_MATRIX[0],)

    @contextmanager
    def _db():
        yield object()

    monkeypatch.setattr(store, "get_settings", lambda: _On())
    from src.core import database
    monkeypatch.setattr(database, "get_db_context", _db)
    monkeypatch.setattr(store, "load_matrix", lambda session: sentinel)
    assert current_matrix() is sentinel


def test_the_database_round_trip_and_rerun_never_overwrites_an_edit(lending_db):
    conn = lending_db.connection()
    assert apply_to(conn) == len(seed_rows())
    assert apply_to(conn) == 0

    assert store.load_matrix(lending_db) == LENDER_MATRIX

    conn.execute(text(
        "UPDATE lending.lender_rule_values SET value = '700'::jsonb, status = 'confirmed', updated_by = 'josh' "
        "WHERE lender_key = :k AND field = 'credit_floor'"), {"k": FLIP})
    assert apply_to(conn) == 0
    flip = _by_key(store.load_matrix(lending_db), FLIP)
    assert flip.credit_floor == 700
    assert field_status_of(flip, "credit_floor") is FieldStatus.CONFIRMED


def test_a_bad_edit_in_the_database_is_skipped(lending_db, caplog):
    conn = lending_db.connection()
    apply_to(conn)
    conn.execute(text(
        "UPDATE lending.lender_rule_values SET value = '\"high\"'::jsonb WHERE lender_key = :k AND field = 'credit_floor'"),
        {"k": FLIP})
    with caplog.at_level(logging.ERROR):
        assert _by_key(store.load_matrix(lending_db), FLIP).credit_floor is None  # config value: safe set fills it
    assert "row skipped" in caplog.text


def test_the_status_check_rejects_an_unknown_status(lending_db):
    from sqlalchemy.exc import IntegrityError

    conn = lending_db.connection()
    apply_to(conn)
    with pytest.raises(IntegrityError):
        with lending_db.begin_nested():
            lending_db.execute(text(
                "UPDATE lending.lender_rule_values SET status = 'sure' WHERE lender_key = :k AND field = 'credit_floor'"),
                {"k": FLIP})


def test_a_missing_table_falls_back_to_the_config(lending_db, caplog):
    lending_db.execute(text("DROP TABLE IF EXISTS lending.lender_rule_values"))
    with caplog.at_level(logging.ERROR):
        assert store.load_matrix(lending_db) == LENDER_MATRIX
    assert "could not read" in caplog.text


# ── L1: Central Lending, unverified placeholders, effective dates ─────────────

PLACEHOLDER_LENDERS = ("rcn_capital", "easy_street", "kiavi_affiliate", "abl", "central_lending")


@pytest.mark.parametrize("key", PLACEHOLDER_LENDERS)
def test_the_placeholder_lenders_are_unverified_and_dated(key):
    from config.lender_matrix import RULES_EFFECTIVE_FROM, has_placeholder_rules

    rules = _by_key(LENDER_MATRIX, key)
    assert not rules.verified and has_placeholder_rules(rules)
    assert all(field_status_of(rules, name) is FieldStatus.PLACEHOLDER for name in PARAMETER_FIELDS)
    assert RULES_EFFECTIVE_FROM[key].isoformat() == "2026-11-03"


def test_every_seed_row_carries_its_lenders_effective_date():
    from config.lender_matrix import RULES_EFFECTIVE_FROM

    assert all(row["effective_from"] == RULES_EFFECTIVE_FROM[row["lender_key"]] for row in seed_rows())
    assert {row["lender_key"] for row in seed_rows()} >= {"central_lending"}


def test_a_row_dated_in_the_future_is_not_applied_until_then(lending_db):
    conn = lending_db.connection()
    apply_to(conn)
    conn.execute(text(
        "UPDATE lending.lender_rule_values SET value = '700'::jsonb, status = 'confirmed',"
        " effective_from = CURRENT_DATE + 30 WHERE lender_key = :k AND field = 'credit_floor'"), {"k": FLIP})
    flip = _by_key(store.load_matrix(lending_db), FLIP)
    assert flip.credit_floor == 640 and field_status_of(flip, "credit_floor") is FieldStatus.PLACEHOLDER

    conn.execute(text(
        "UPDATE lending.lender_rule_values SET effective_from = CURRENT_DATE - 1"
        " WHERE lender_key = :k AND field = 'credit_floor'"), {"k": FLIP})
    assert _by_key(store.load_matrix(lending_db), FLIP).credit_floor == 700


def test_a_table_from_before_l1_gains_the_date_column_from_its_last_change(lending_db):
    conn = lending_db.connection()
    apply_to(conn)
    conn.execute(text("ALTER TABLE lending.lender_rule_values DROP COLUMN effective_from"))
    conn.execute(text("UPDATE lending.lender_rule_values SET updated_at = '2026-10-10 15:00+00'"))
    assert apply_to(conn) == 0
    dates = conn.execute(text("SELECT DISTINCT effective_from FROM lending.lender_rule_values")).scalars().all()
    assert [d.isoformat() for d in dates] == ["2026-10-10"]
    assert apply_to(conn) == 0  # re-run leaves the dates alone


def test_a_new_row_defaults_to_todays_et_date_the_day_the_reader_compares(lending_db):
    conn = lending_db.connection()
    apply_to(conn)
    conn.execute(text("ALTER TABLE lending.lender_rule_values ALTER COLUMN effective_from SET DEFAULT CURRENT_DATE"))
    apply_to(conn)  # a re-run resets an older default
    default = conn.execute(text(
        "SELECT column_default FROM information_schema.columns WHERE table_schema = 'lending'"
        " AND table_name = 'lender_rule_values' AND column_name = 'effective_from'")).scalar_one()
    assert "America/New_York" in default
    conn.execute(text("DELETE FROM lending.lender_rule_values WHERE lender_key = :k AND field = 'credit_floor'"),
                 {"k": FLIP})
    conn.execute(text(
        "INSERT INTO lending.lender_rule_values (lender_key, field, value, status)"
        " VALUES (:k, 'credit_floor', '700'::jsonb, 'confirmed')"), {"k": FLIP})
    assert _by_key(store.load_matrix(lending_db), FLIP).credit_floor == 700


def test_the_safe_set_migration_resets_seeded_backflip_rows_but_keeps_hand_edits(lending_db):
    """Client list Nov 2 (Q1): a database seeded with Backflip's Oct 1 values is put on the safe set."""
    from migrations.apply_lending_lender_rules_safe_set import apply_to as reset_to_safe_set

    conn = lending_db.connection()
    lending_db.execute(text("DELETE FROM lending.lender_rule_values WHERE lender_key LIKE 'generic_backflip%'"))
    apply_to(conn)
    old = {"credit_floor": "640", "max_ltv": "0.75", "min_loan_amount": "100000"}
    for field, value in old.items():  # what the old seed wrote
        conn.execute(text("UPDATE lending.lender_rule_values SET value = CAST(:v AS jsonb) "
                          "WHERE lender_key = :k AND field = :f"), {"v": value, "k": FLIP, "f": field})
    conn.execute(text("UPDATE lending.lender_rule_values SET value = '0.6'::jsonb, status = 'confirmed', "
                      "updated_by = 'josh' WHERE lender_key = :k AND field = 'max_ltc'"), {"k": FLIP})

    assert reset_to_safe_set(conn) == len(old)
    assert reset_to_safe_set(conn) == 0
    flip = _by_key(store.load_matrix(lending_db), FLIP)
    assert flip.credit_floor is None and flip.max_ltv is None and flip.min_loan_amount is None
    assert flip.max_ltc == Decimal("0.6")  # edited by hand: left as is
