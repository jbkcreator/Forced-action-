"""Per-borrower Backflip conflict check.

Pure-logic tests with mocked sessions; no database. Covers the planted-contact
acceptance test (one Backflip contact per match criterion is caught and
blocked), fail-closed behaviour, near-miss rejection, the batch audit write,
and the feed/migration changes that store the new identifier kinds.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.services.backflip_conflict_check import (
    AUDIT_GATE,
    CRITERION_EMAIL_DOMAIN,
    CRITERION_ENTITY_NAME,
    CRITERION_PARCEL_ID,
    CRITERION_PHONE_HASH,
    CRITERION_PRIMARY_EMAIL,
    REASON_CONFLICT,
    REASON_FEED_STALE,
    REASON_FEED_UNAVAILABLE,
    REASON_NO_IDENTIFIERS,
    BackflipIdentifierIndex,
    BorrowerRecord,
    find_borrower_conflicts,
    hash_phone,
    load_backflip_identifier_index,
    record_dialer_conflict_decisions,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

BACKFLIP_PHONE = "+18135550100"
BACKFLIP_EMAIL = "john@smithbuilders.com"
BACKFLIP_ENTITY = "SMITH HOLDINGS LLC"
BACKFLIP_PARCEL = "122916000000000000"


def _index(**overrides) -> BackflipIdentifierIndex:
    fields = {
        "block_reason": None,
        "phone_hashes": frozenset({hash_phone(BACKFLIP_PHONE)}),
        "emails": frozenset({BACKFLIP_EMAIL}),
        "email_domains": frozenset({"smithbuilders.com"}),
        "entity_names": frozenset({BACKFLIP_ENTITY}),
        "parcel_ids": frozenset({BACKFLIP_PARCEL}),
    }
    fields.update(overrides)
    return BackflipIdentifierIndex(**fields)


def _check(record: BorrowerRecord, index: BackflipIdentifierIndex | None = None, **kwargs):
    return find_borrower_conflicts([record], index or _index(), **kwargs)[0]


def _session_returning(fresh, rows=()) -> MagicMock:
    freshness = MagicMock()
    freshness.scalar_one_or_none.return_value = fresh
    contacts = MagicMock()
    contacts.fetchall.return_value = list(rows)
    session = MagicMock()
    session.execute.side_effect = [freshness, contacts]
    return session


# ---------------------------------------------------------------------------
# Planted Backflip contact is caught on each criterion
# ---------------------------------------------------------------------------

class TestPlantedContactCaught:
    def test_phone_hash_matches_across_formatting(self):
        decision = _check(BorrowerRecord("r1", phone="(813) 555-0100"))
        assert decision.blocked
        assert decision.reason == REASON_CONFLICT
        assert decision.matched_criteria == (CRITERION_PHONE_HASH,)

    def test_primary_email_matches_case_insensitively(self):
        decision = _check(BorrowerRecord("r1", email="  John@SmithBuilders.COM "))
        assert decision.blocked
        assert decision.matched_criteria == (CRITERION_PRIMARY_EMAIL,)

    def test_email_domain_matches_when_enabled(self):
        decision = _check(BorrowerRecord("r1", email="mary@smithbuilders.com"), match_email_domain=True)
        assert decision.blocked
        assert decision.matched_criteria == (CRITERION_EMAIL_DOMAIN,)

    def test_entity_name_matches_across_punctuation_and_spacing(self):
        decision = _check(BorrowerRecord("r1", entity_name="Smith  Holdings, L.L.C."))
        assert decision.blocked
        assert decision.matched_criteria == (CRITERION_ENTITY_NAME,)

    def test_parcel_id_matches_across_separators(self):
        decision = _check(BorrowerRecord("r1", parcel_id="12-29-16-00000-000-0000"))
        assert decision.blocked
        assert decision.matched_criteria == (CRITERION_PARCEL_ID,)

    def test_every_matching_criterion_is_reported(self):
        decision = _check(
            BorrowerRecord(
                "r1",
                phone=BACKFLIP_PHONE,
                email=BACKFLIP_EMAIL,
                entity_name=BACKFLIP_ENTITY,
                parcel_id=BACKFLIP_PARCEL,
            ),
            match_email_domain=True,
        )
        assert decision.matched_criteria == (
            CRITERION_PHONE_HASH,
            CRITERION_PRIMARY_EMAIL,
            CRITERION_EMAIL_DOMAIN,
            CRITERION_ENTITY_NAME,
            CRITERION_PARCEL_ID,
        )


# ---------------------------------------------------------------------------
# Clear records and near misses
# ---------------------------------------------------------------------------

class TestClearAndNearMiss:
    def test_unrelated_record_is_clear(self):
        decision = _check(
            BorrowerRecord("r1", phone="(813) 555-0199", email="amy@other.com",
                           entity_name="OTHER LLC", parcel_id="999"),
        )
        assert not decision.blocked
        assert decision.reason is None
        assert decision.matched_criteria == ()

    def test_email_domain_ignored_by_default(self):
        decision = _check(BorrowerRecord("r1", email="mary@smithbuilders.com"))
        assert not decision.blocked

    def test_entity_legal_suffix_is_significant(self):
        index = _index(entity_names=frozenset({"FYLOS LLC"}))
        assert not _check(BorrowerRecord("r1", entity_name="Fylos Inc"), index).blocked

    def test_entity_name_is_not_prefix_matched(self):
        assert not _check(BorrowerRecord("r1", entity_name="Smith Holdings LLC II")).blocked

    def test_parcel_leading_zero_is_significant(self):
        assert not _check(BorrowerRecord("r1", parcel_id="0" + BACKFLIP_PARCEL)).blocked

    def test_decisions_keep_input_order(self):
        records = [
            BorrowerRecord("clear", email="amy@other.com"),
            BorrowerRecord("hit", email=BACKFLIP_EMAIL),
        ]
        decisions = find_borrower_conflicts(records, _index())
        assert [d.record_ref for d in decisions] == ["clear", "hit"]
        assert [d.blocked for d in decisions] == [False, True]


# ---------------------------------------------------------------------------
# Fail closed
# ---------------------------------------------------------------------------

class TestFailClosed:
    @pytest.mark.parametrize("reason", [REASON_FEED_STALE, REASON_FEED_UNAVAILABLE])
    def test_untrusted_feed_blocks_every_record(self, reason):
        index = BackflipIdentifierIndex(block_reason=reason)
        decisions = find_borrower_conflicts(
            [BorrowerRecord("r1", email="amy@other.com"), BorrowerRecord("r2", phone="(813) 555-0199")],
            index,
        )
        assert all(d.blocked and d.reason == reason and d.matched_criteria == () for d in decisions)

    def test_record_without_identifiers_is_blocked(self):
        decision = _check(BorrowerRecord("r1"))
        assert decision.blocked
        assert decision.reason == REASON_NO_IDENTIFIERS

    def test_unparseable_identifiers_count_as_absent(self):
        decision = _check(BorrowerRecord("r1", phone="not a phone", email="no-at-sign", entity_name=" ,. "))
        assert decision.blocked
        assert decision.reason == REASON_NO_IDENTIFIERS


# ---------------------------------------------------------------------------
# Loading the Backflip snapshot
# ---------------------------------------------------------------------------

class TestLoadIndex:
    def _load(self, session):
        settings = MagicMock(fa_max_backflip_feed_max_age_hours=24)
        with patch("src.services.backflip_conflict_check.get_settings", return_value=settings):
            return load_backflip_identifier_index(session)

    def test_missing_feed_row_is_unavailable(self):
        index = self._load(_session_returning(None))
        assert index.block_reason == REASON_FEED_UNAVAILABLE

    def test_stale_feed_is_blocked_without_reading_contacts(self):
        session = _session_returning(False)
        index = self._load(session)
        assert index.block_reason == REASON_FEED_STALE
        assert session.execute.call_count == 1

    def test_fresh_feed_indexes_each_kind(self):
        rows = [
            ("phone", BACKFLIP_PHONE),
            ("email", BACKFLIP_EMAIL),
            ("entity_name", BACKFLIP_ENTITY),
            ("parcel_id", BACKFLIP_PARCEL),
        ]
        index = self._load(_session_returning(True, rows))
        assert index.block_reason is None
        assert index.phone_hashes == {hash_phone(BACKFLIP_PHONE)}
        assert index.emails == {BACKFLIP_EMAIL}
        assert index.email_domains == {"smithbuilders.com"}
        assert index.entity_names == {BACKFLIP_ENTITY}
        assert index.parcel_ids == {BACKFLIP_PARCEL}

    def test_snapshot_is_read_in_two_queries(self):
        session = _session_returning(True, [("email", BACKFLIP_EMAIL)])
        self._load(session)
        assert session.execute.call_count == 2
        assert "WHERE active" in str(session.execute.call_args_list[1].args[0])


# ---------------------------------------------------------------------------
# Audit write
# ---------------------------------------------------------------------------

class TestAuditWrite:
    def _write(self, decisions):
        session = MagicMock()
        cm = MagicMock()
        cm.__enter__.return_value = session
        with patch("src.services.backflip_conflict_check.get_db_context", return_value=cm):
            record_dialer_conflict_decisions(decisions)
        return session

    def test_all_decisions_written_in_one_batch(self):
        decisions = find_borrower_conflicts(
            [BorrowerRecord("hit", phone=BACKFLIP_PHONE), BorrowerRecord("clear", email="amy@other.com")],
            _index(),
        )
        session = self._write(decisions)
        assert session.execute.call_count == 1
        params = session.execute.call_args.args[1]
        assert [p["subject_ref"] for p in params] == ["hit", "clear"]
        assert all(p["gate"] == AUDIT_GATE for p in params)
        assert params[0]["suppressed"] is True
        assert params[0]["criteria"] == [CRITERION_PHONE_HASH]
        assert params[1]["suppressed"] is False
        assert params[1]["criteria"] is None

    def test_audit_rows_hold_no_raw_identifiers(self):
        decisions = find_borrower_conflicts(
            [BorrowerRecord("hit", phone=BACKFLIP_PHONE, email=BACKFLIP_EMAIL)], _index(),
        )
        params = self._write(decisions).execute.call_args.args[1][0]
        flattened = " ".join(str(value) for value in params.values())
        assert BACKFLIP_PHONE not in flattened
        assert BACKFLIP_EMAIL not in flattened
        assert params["masked"] == "...0100"
        assert params["digest"] == hash_phone(BACKFLIP_PHONE)

    def test_empty_decisions_skip_the_database(self):
        with patch("src.services.backflip_conflict_check.get_db_context") as ctx:
            record_dialer_conflict_decisions([])
        ctx.assert_not_called()

    def test_database_failure_propagates(self):
        session = MagicMock()
        session.execute.side_effect = RuntimeError("db down")
        cm = MagicMock()
        cm.__enter__.return_value = session
        decisions = find_borrower_conflicts([BorrowerRecord("r1", email=BACKFLIP_EMAIL)], _index())
        with patch("src.services.backflip_conflict_check.get_db_context", return_value=cm):
            with pytest.raises(RuntimeError):
                record_dialer_conflict_decisions(decisions)


# ---------------------------------------------------------------------------
# Feed import and schema for the new identifier kinds
# ---------------------------------------------------------------------------

class TestFeedIdentifierKinds:
    def test_csv_parses_entity_name_and_parcel_id(self, tmp_path):
        from src.services.fa_max_backflip_feed import parse_backflip_csv

        csv_path = tmp_path / "backflip.csv"
        csv_path.write_text(
            "email,phone,entity_name,parcel_id\n"
            "John@SmithBuilders.com,(813) 555-0100,\"Smith Holdings, L.L.C.\",12-29-16-00000-000-0000\n",
            encoding="utf-8",
        )
        assert parse_backflip_csv(csv_path) == {
            ("email", BACKFLIP_EMAIL),
            ("phone", BACKFLIP_PHONE),
            ("entity_name", BACKFLIP_ENTITY),
            ("parcel_id", BACKFLIP_PARCEL),
        }

    def test_csv_with_only_entity_column_is_accepted(self, tmp_path):
        from src.services.fa_max_backflip_feed import parse_backflip_csv

        csv_path = tmp_path / "backflip.csv"
        csv_path.write_text("entity_name\nSmith Holdings LLC\n", encoding="utf-8")
        assert parse_backflip_csv(csv_path) == {("entity_name", BACKFLIP_ENTITY)}

    def test_csv_without_known_columns_is_rejected(self, tmp_path):
        from src.services.fa_max_backflip_feed import parse_backflip_csv

        csv_path = tmp_path / "backflip.csv"
        csv_path.write_text("name\nSomeone\n", encoding="utf-8")
        with pytest.raises(ValueError, match="CSV needs"):
            parse_backflip_csv(csv_path)

    def test_snapshot_writer_normalizes_new_kinds(self):
        from src.services.fa_max_backflip_feed import replace_backflip_snapshot

        session = MagicMock()
        cm = MagicMock()
        cm.__enter__.return_value = session
        with patch("src.services.fa_max_backflip_feed.get_db_context", return_value=cm):
            count = replace_backflip_snapshot(
                {("entity_name", "Smith Holdings, L.L.C."), ("parcel_id", "12-29-16-00000-000-0000")},
            )
        assert count == 2
        values = {call.args[1]["value"] for call in session.execute.call_args_list
                  if len(call.args) > 1 and "value" in call.args[1]}
        assert values == {BACKFLIP_ENTITY, BACKFLIP_PARCEL}

    @pytest.mark.parametrize("identifier", [("fax", "123"), ("entity_name", " ,. "), ("parcel_id", "--")])
    def test_snapshot_writer_rejects_unknown_or_empty_identifiers(self, identifier):
        from src.services.fa_max_backflip_feed import replace_backflip_snapshot

        with patch("src.services.fa_max_backflip_feed.get_db_context") as ctx:
            with pytest.raises(ValueError):
                replace_backflip_snapshot({identifier})
        ctx.assert_not_called()

    def test_migration_is_idempotent_and_widens_both_constraints(self):
        source = (REPO_ROOT / "migrations" / "apply_backflip_conflict_identifiers.py").read_text(encoding="utf-8")
        assert "DROP CONSTRAINT IF EXISTS ck_fa_max_backflip_identifier_kind" in source
        assert "'entity_name', 'parcel_id'" in source
        assert "DROP CONSTRAINT IF EXISTS ck_fa_max_bsd_gate" in source
        assert "'dialer'" in source
        assert "ADD COLUMN IF NOT EXISTS subject_ref" in source
        assert "ADD COLUMN IF NOT EXISTS matched_criteria" in source
