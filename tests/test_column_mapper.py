"""
Unit tests for src/loaders/column_mapper.py

No DB, no LLM calls — both are mocked.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from src.loaders.column_mapper import ColumnMapper, LLMColumnMapper, SIGNAL_SCHEMAS, SkipMapping


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mapper():
    with patch("src.loaders.column_mapper.get_settings") as mock_settings:
        mock_settings.return_value.anthropic_api_key.get_secret_value.return_value = "sk-test"
        with patch("src.loaders.column_mapper.anthropic.Anthropic"):
            return LLMColumnMapper()


# ---------------------------------------------------------------------------
# SIGNAL_SCHEMAS
# ---------------------------------------------------------------------------

class TestSignalSchemas:

    def test_all_expected_signal_types_present(self):
        for sig in ("foreclosures", "liens", "violations", "permits",
                    "court_records", "tax_delinquency", "deeds"):
            assert sig in SIGNAL_SCHEMAS

    def test_schemas_are_non_empty_lists(self):
        for sig, cols in SIGNAL_SCHEMAS.items():
            assert isinstance(cols, list) and len(cols) > 0, f"Empty schema for {sig}"

    def test_liens_has_record_date(self):
        assert "RecordDate" in SIGNAL_SCHEMAS["liens"]

    def test_foreclosures_has_auction_date(self):
        assert "Auction Start Date/Time" in SIGNAL_SCHEMAS["foreclosures"]


# ---------------------------------------------------------------------------
# apply (static method)
# ---------------------------------------------------------------------------

class TestApply:

    def test_renames_columns_per_mapping(self):
        df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        mapping = {"A": "case_number", "B": "filing_date"}
        result = ColumnMapper.apply(df, mapping)
        assert list(result.columns) == ["case_number", "filing_date"]

    def test_pass_through_columns_unchanged(self):
        df = pd.DataFrame({"known": [1], "extra_col": [2]})
        mapping = {"known": "permit_number", "extra_col": "extra_col"}
        result = ColumnMapper.apply(df, mapping)
        assert "extra_col" in result.columns

    def test_data_preserved_after_rename(self):
        df = pd.DataFrame({"raw_date": ["2024-01-01"]})
        result = ColumnMapper.apply(df, {"raw_date": "filing_date"})
        assert result["filing_date"].iloc[0] == "2024-01-01"


# ---------------------------------------------------------------------------
# get_or_create — approved / non-rejected pending cache hit
# ---------------------------------------------------------------------------

class TestGetOrCreateCacheHit:

    def test_uses_approved_mapping_no_llm(self):
        mapper = _make_mapper()
        df = pd.DataFrame({"Date Filed": ["2024-01-01"], "Parcel": ["123"]})
        approved_mapping = {"Date Filed": "filing_date", "Parcel": "parcel_id"}

        with patch.object(mapper, "_fetch_best", return_value=approved_mapping):
            with patch.object(mapper, "_call_llm") as mock_llm:
                result = mapper.get_or_create("foreclosures", 1, df)

        mock_llm.assert_not_called()
        assert result == approved_mapping

    def test_cache_hit_returns_mapping_dict(self):
        mapper = _make_mapper()
        df = pd.DataFrame({"Date Filed": ["2024-01-01", "2024-01-02"]})
        cached = {"Date Filed": "filing_date"}
        with patch.object(mapper, "_fetch_best", return_value=cached):
            result = mapper.get_or_create("foreclosures", 1, df)
        assert result == cached


# ---------------------------------------------------------------------------
# get_or_create — cache miss → LLM → save pending
# ---------------------------------------------------------------------------

class TestGetOrCreateCacheMissLLM:

    def test_calls_llm_on_cache_miss(self):
        mapper = _make_mapper()
        df = pd.DataFrame({"Auction Date": ["2024-01-01"], "Case No": ["2024-CA-001"]})
        llm_mapping = {"Auction Date": "auction_date", "Case No": "case_number"}

        with patch.object(mapper, "_fetch_best", return_value=None):
            with patch.object(mapper, "_fetch_reject_feedback", return_value=None):
                with patch.object(mapper, "_call_llm", return_value=llm_mapping) as mock_llm:
                    with patch.object(mapper, "_save_pending") as mock_save:
                        result = mapper.get_or_create("foreclosures", 2, df)

        mock_llm.assert_called_once()
        mock_save.assert_called_once()
        assert result == llm_mapping

    def test_saves_pending_called_with_raw_columns(self):
        mapper = _make_mapper()
        df = pd.DataFrame({"Z Col": [1], "A Col": [2]})
        llm_mapping = {"Z Col": "Z Col", "A Col": "A Col"}

        with patch.object(mapper, "_fetch_best", return_value=None):
            with patch.object(mapper, "_fetch_reject_feedback", return_value=None):
                with patch.object(mapper, "_call_llm", return_value=llm_mapping):
                    with patch.object(mapper, "_save_pending") as mock_save:
                        mapper.get_or_create("violations", 3, df)

        call_args = mock_save.call_args
        assert set(call_args[0][1]) == {"Z Col", "A Col"}

    def test_llm_mapping_returned(self):
        mapper = _make_mapper()
        df = pd.DataFrame({"ViolationType": ["overgrown"], "OpenDate": ["2024-01-01"]})
        llm_mapping = {"ViolationType": "violation_type", "OpenDate": "opened_date"}

        with patch.object(mapper, "_fetch_best", return_value=None):
            with patch.object(mapper, "_fetch_reject_feedback", return_value=None):
                with patch.object(mapper, "_call_llm", return_value=llm_mapping):
                    with patch.object(mapper, "_save_pending"):
                        result = mapper.get_or_create("violations", 4, df)

        assert result == llm_mapping

    def test_reject_feedback_passed_to_llm(self):
        mapper = _make_mapper()
        df = pd.DataFrame({"Col": ["val"]})
        feedback = "FOLIO was mapped wrong — it's the parcel column, not the owner"
        llm_mapping = {"Col": "Col"}

        with patch.object(mapper, "_fetch_best", return_value=None):
            with patch.object(mapper, "_fetch_reject_feedback", return_value=feedback):
                with patch.object(mapper, "_call_llm", return_value=llm_mapping) as mock_llm:
                    with patch.object(mapper, "_save_pending"):
                        mapper.get_or_create("master_data", 5, df)

        _, kwargs = mock_llm.call_args
        assert kwargs.get("prior_feedback") == feedback


# ---------------------------------------------------------------------------
# get_or_create — unknown signal type
# ---------------------------------------------------------------------------

class TestGetOrCreateUnknownSignalType:

    def test_raises_skip_mapping_for_unknown_type(self):
        mapper = _make_mapper()
        df = pd.DataFrame({"col": [1]})
        with pytest.raises(SkipMapping, match="fire"):
            mapper.get_or_create("fire", 5, df)


# ---------------------------------------------------------------------------
# LLMColumnMapper.map() — backwards-compat wrapper
# ---------------------------------------------------------------------------

class TestLLMColumnMapperMap:

    def test_map_applies_mapping_to_df(self):
        mapper = _make_mapper()
        df = pd.DataFrame({"Auction Date": ["2024-01-01"], "Case No": ["2024-CA-001"]})
        llm_mapping = {"Auction Date": "auction_date", "Case No": "case_number"}

        with patch.object(mapper, "_fetch_best", return_value=llm_mapping):
            result = mapper.map(df, "foreclosures", source_id=1)

        assert "auction_date" in result.columns
        assert "case_number" in result.columns
        assert len(result) == 1


# ---------------------------------------------------------------------------
# _call_llm — LLM response handling
# ---------------------------------------------------------------------------

class TestCallLlm:

    def _mapper_with_llm_response(self, response_text: str):
        with patch("src.loaders.column_mapper.get_settings") as mock_settings:
            mock_settings.return_value.anthropic_api_key.get_secret_value.return_value = "sk-test"
            client_mock = MagicMock()
            msg_mock = MagicMock()
            msg_mock.content[0].text = response_text
            client_mock.messages.create.return_value = msg_mock
            with patch("src.loaders.column_mapper.anthropic.Anthropic", return_value=client_mock):
                mapper = LLMColumnMapper()
        return mapper

    def test_valid_json_mapping(self):
        import json
        raw_cols = ["Auction Date", "Case No"]
        schema = SIGNAL_SCHEMAS["foreclosures"]
        response = json.dumps({"Auction Date": "Auction Start Date/Time", "Case No": "Case Number"})
        mapper = self._mapper_with_llm_response(response)
        result = mapper._call_llm(raw_cols, schema, "foreclosures")
        assert result["Auction Date"] == "Auction Start Date/Time"
        assert result["Case No"] == "Case Number"

    def test_invalid_target_falls_back_to_passthrough(self):
        import json
        raw_cols = ["MyCol"]
        schema = SIGNAL_SCHEMAS["foreclosures"]
        response = json.dumps({"MyCol": "not_a_real_column"})
        mapper = self._mapper_with_llm_response(response)
        result = mapper._call_llm(raw_cols, schema, "foreclosures")
        assert result["MyCol"] == "MyCol"

    def test_non_json_response_falls_back_to_identity(self):
        raw_cols = ["ColA", "ColB"]
        schema = SIGNAL_SCHEMAS["liens"]
        mapper = self._mapper_with_llm_response("not json at all")
        result = mapper._call_llm(raw_cols, schema, "liens")
        assert result == {"ColA": "ColA", "ColB": "ColB"}

    def test_markdown_code_fence_stripped(self):
        import json
        raw_cols = ["Filing Date"]
        schema = SIGNAL_SCHEMAS["foreclosures"]
        inner = json.dumps({"Filing Date": "Auction Start Date/Time"})
        mapper = self._mapper_with_llm_response(f"```json\n{inner}\n```")
        result = mapper._call_llm(raw_cols, schema, "foreclosures")
        assert result["Filing Date"] == "Auction Start Date/Time"

    def test_all_raw_cols_present_in_output(self):
        import json
        raw_cols = ["A", "B", "C"]
        schema = SIGNAL_SCHEMAS["violations"]
        # LLM only maps A and B — C is missing
        response = json.dumps({"A": "record_number", "B": "opened_date"})
        mapper = self._mapper_with_llm_response(response)
        result = mapper._call_llm(raw_cols, schema, "violations")
        assert "C" in result
        assert result["C"] == "C"  # pass-through

    def test_prior_feedback_included_in_prompt(self):
        import json
        raw_cols = ["ParcelNum"]
        schema = SIGNAL_SCHEMAS["master_data"]
        response = json.dumps({"ParcelNum": "FOLIO"})
        mapper = self._mapper_with_llm_response(response)
        feedback = "ParcelNum maps to FOLIO not OWNER"
        result = mapper._call_llm(raw_cols, schema, "master_data", prior_feedback=feedback)
        # The LLM client was called — verify the prompt contained the feedback
        call_args = mapper._client.messages.create.call_args
        prompt_text = call_args[1]["messages"][0]["content"]
        assert "PRIOR ADMIN REJECTION FEEDBACK" in prompt_text
        assert feedback in prompt_text
