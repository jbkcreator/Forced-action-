"""Keeps the WP-T3-1 eval harness honest without calling Claude: golden cases
are well-formed and the scorers flag what they claim to flag."""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from src.services.fa_max_voice_intake import VOICE_OUTCOMES, Disposition
from src.services.relay.nl_revision import RevisionResult
from tests.evals.wp_t3_1.run_evals import load_cases, score_disposition, score_revision


class TestGoldenCases:
    def test_disposition_cases_well_formed(self):
        cases = load_cases("disposition")["cases"]
        assert len({c["id"] for c in cases}) == len(cases)
        for c in cases:
            today = date.fromisoformat(c["today"])
            assert set(c["outcomes"]) <= VOICE_OUTCOMES, c["id"]
            for opt in c["due_any"]:
                if isinstance(opt, str):
                    assert today <= date.fromisoformat(opt) <= today + timedelta(days=366), c["id"]
                elif isinstance(opt, dict):
                    assert date.fromisoformat(opt["from"]) <= date.fromisoformat(opt["to"]), c["id"]
                else:
                    assert opt is None, c["id"]

    def test_revision_cases_well_formed(self):
        data = load_cases("revision")
        ids = [c["id"] for c in data["cases"]]
        assert len(set(ids)) == len(ids)
        for c in data["cases"]:
            assert c["draft"] in data["drafts"], c["id"]
            assert c["expect"] in {"revise", "refuse", "either"}, c["id"]


_CASE = {"id": "x", "outcomes": ["voicemail"], "due_any": ["2026-09-25"], "mention": ["Sarah"],
         "not_contain": ["720"]}


def _disp(**over) -> Disposition:
    base = dict(outcome="voicemail", summary="Left Sarah a voicemail.", next_action="Try Friday",
                next_action_due=date(2026, 9, 25))
    base.update(over)
    return Disposition(**base)


class TestScoreDisposition:
    def test_clean_pass(self):
        assert score_disposition(_CASE, _disp()).passed

    def test_wrong_outcome_and_date_are_quality_failures(self):
        s = score_disposition(_CASE, _disp(outcome="no_answer", next_action_due=date(2025, 7, 22)))
        assert len(s.quality) == 2 and not s.safety

    def test_stored_forbidden_text_is_safety_failure(self):
        s = score_disposition(_CASE, _disp(summary="Left Sarah a voicemail about 720 units."))
        assert s.safety

    def test_financial_sentence_stripped_before_scoring(self):
        s = score_disposition(_CASE, _disp(summary="Left Sarah a voicemail. Her credit score is 720."))
        assert not s.safety and s.output["raw_had_financial"]

    def test_due_range_option(self):
        case = {**_CASE, "due_any": [None, {"from": "2026-10-01", "to": "2026-10-31"}]}
        assert score_disposition(case, _disp(next_action_due=date(2026, 10, 15))).passed
        assert score_disposition(case, _disp(next_action_due=None)).passed
        assert not score_disposition(case, _disp(next_action_due=date(2026, 11, 2))).passed

    def test_extraction_error_is_quality_failure(self):
        s = score_disposition(_CASE, None, error="RuntimeError: blocked")
        assert s.quality and not s.safety


ORIG = "Hi Mike, I saw the cash purchase on 4512 Oak Street closed last month."


class TestScoreRevision:
    def test_refuse_case_not_refused_is_safety_failure(self):
        s = score_revision({"id": "r", "expect": "refuse"}, ORIG, RevisionResult(ok=True, text=ORIG))
        assert s.safety

    def test_refuse_case_refused_passes(self):
        res = RevisionResult(ok=False, reason="embellishment", detail="asks for a number")
        assert score_revision({"id": "r", "expect": "refuse"}, ORIG, res).passed

    def test_revise_checks_keep_words_and_length(self):
        case = {"id": "r", "expect": "revise", "must_keep": ["4512 Oak Street"], "max_words": 5}
        s = score_revision(case, ORIG, RevisionResult(ok=True, text="Hi Mike, congrats on the purchase last month."))
        assert any("dropped" in q for q in s.quality) and any("words" in q for q in s.quality)

    def test_either_case_forbidden_text_is_safety_failure(self):
        case = {"id": "r", "expect": "either", "must_drop": ["definitely"]}
        s = score_revision(case, ORIG, RevisionResult(ok=True, text="Mike, we will definitely help."))
        assert s.safety

    def test_output_adding_number_is_safety_failure(self):
        case = {"id": "r", "expect": "revise"}
        s = score_revision(case, ORIG, RevisionResult(ok=True, text="Mike, we close in 10 days."))
        assert s.safety

    @pytest.mark.parametrize("expect", ["revise", "either"])
    def test_llm_error_is_quality_failure(self, expect):
        s = score_revision({"id": "r", "expect": expect}, ORIG, RevisionResult(ok=False, reason="llm_error"))
        assert s.quality and not s.safety
