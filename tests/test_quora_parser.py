"""
Unit tests for quora_engine parser — no network, no browser, no Claude.

Covers:
  - flatten_quora_qtext (plain string, JSON string, dict, malformed, null)
  - _parse_unix_ts (seconds, microseconds, null, invalid)
  - _normalize_quora_url (relative, absolute, empty)
  - _get_first, _parse_topics, _parse_author_credentials
  - _parse_gql_response
  - _record_to_result (full, minimal, empty, alt fieldnames)
  - score_quora_result (high-priority, low-priority, blocked)
  - Cora graph wiring (compact input only, no raw GQL, conditional generation)
"""

import json
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from src.scrappers.quora.quora_engine import (
    QuoraResult,
    _get_first,
    _normalize_quora_url,
    _parse_author_credentials,
    _parse_gql_response,
    _parse_unix_ts,
    _parse_topics,
    _record_to_result,
    flatten_quora_qtext,
    score_quora_result,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FULL_RECORD = {
    "qid": 65537606,
    "title": "What are the best strategies for buying foreclosed homes?",
    "url": "/What-are-the-best-strategies-for-buying-foreclosed-homes",
    "answerCount": 23,
    "followerCount": 45,
    "decanonicalizedAnswerCount": 24,
    "numDisplayComments": 3,
    "viewCount": 12500,
    "creationTime": 1609459200000000,   # microseconds → 2021-01-01 UTC
    "lastFollowTime": 1700000000000000,  # microseconds → ~2023-11-14 UTC
    "isLocked": False,
    "isDeleted": False,
    "isSensitive": False,
    "viewerShouldShowWriteAnswer": True,
    "viewerCantAnswer": False,
    "isUserLimitedDistro": True,
    "questionDescription": "I'm looking to invest in foreclosed properties in Tampa FL.",
    "topics": {
        "edges": [
            {"node": {"name": "Foreclosures", "translatedName": "Foreclosures"}},
            {"node": {"name": "Real Estate Investing"}},
        ]
    },
    "topAnswer": {
        "content": "The best strategy is to start with courthouse steps auctions...",
        "numUpvotes": 156,
        "author": {
            "name": "Jane Investor",
            "profileUrl": "/profile/Jane-Investor-123",
            "credential": {
                "translatedCredential": "Real Estate Investor, 20+ years in Florida",
            },
        },
    },
}

MINIMAL_RECORD = {
    "title": "How do tax liens work in Florida?",
    "url": "/How-do-tax-liens-work-in-Florida",
}

EMPTY_RECORD: dict = {}

ALT_FIELDNAME_RECORD = {
    "qtext": "What is a short sale?",
    "canonicalUrl": "/What-is-a-short-sale",
    "numAnswers": 5,
    "numFollowers": 10,
    "numViews": 3000,
    "updatedAt": 1700000000,   # seconds (old-style)
    "questionTopics": [
        {"name": "Short Sales"},
        "Real Estate",
    ],
    "topAnswer": {
        "text": "A short sale is when a bank agrees...",
        "voteCount": 42,
        "author": {
            "displayName": "Bob Agent",
            "url": "/profile/Bob-Agent",
            "bestCredential": {"credentialText": "Licensed RE Agent"},
        },
    },
}

QTEXT_RECORD = {
    "title": json.dumps({
        "sections": [
            {"spans": [{"text": "How do I stop foreclosure in Florida?", "modifiers": {}}]}
        ]
    }),
    "url": "/How-do-I-stop-foreclosure-in-Florida",
    "answerCount": 0,
    "followerCount": 1,
}

GQL_PAYLOAD = {
    "data": {
        "searchConnection": {
            "edges": [
                {"node": {"question": FULL_RECORD}},
                {"node": {"question": MINIMAL_RECORD}},
            ]
        }
    }
}


# ---------------------------------------------------------------------------
# flatten_quora_qtext
# ---------------------------------------------------------------------------

def test_flatten_plain_string():
    assert flatten_quora_qtext("How do I stop foreclosure?") == "How do I stop foreclosure?"

def test_flatten_json_string():
    raw = json.dumps({
        "sections": [{"spans": [{"text": "How do I stop foreclosure in Florida?", "modifiers": {}}]}]
    })
    assert flatten_quora_qtext(raw) == "How do I stop foreclosure in Florida?"

def test_flatten_dict_object():
    raw = {
        "sections": [
            {"spans": [{"text": "What is a short sale?", "modifiers": {}}]}
        ]
    }
    assert flatten_quora_qtext(raw) == "What is a short sale?"

def test_flatten_malformed_json_returns_original():
    raw = "{not valid json"
    result = flatten_quora_qtext(raw)
    assert isinstance(result, str)
    assert len(result) > 0

def test_flatten_none_returns_empty():
    assert flatten_quora_qtext(None) == ""

def test_flatten_empty_string_returns_empty():
    assert flatten_quora_qtext("") == ""

def test_flatten_multi_span():
    raw = {
        "sections": [
            {"spans": [{"text": "Hello"}, {"text": " World"}]}
        ]
    }
    result = flatten_quora_qtext(raw)
    assert "Hello" in result
    assert "World" in result

def test_flatten_qtext_record():
    r = _record_to_result(1, QTEXT_RECORD)
    assert r is not None
    assert r.title == "How do I stop foreclosure in Florida?"


# ---------------------------------------------------------------------------
# _parse_unix_ts — seconds and microseconds
# ---------------------------------------------------------------------------

def test_parse_unix_ts_seconds():
    dt = _parse_unix_ts(1609459200)
    assert dt == datetime(2021, 1, 1, tzinfo=timezone.utc)

def test_parse_unix_ts_microseconds():
    dt = _parse_unix_ts(1609459200000000)
    assert dt is not None
    assert dt.year == 2021

def test_parse_unix_ts_float_string():
    dt = _parse_unix_ts("1609459200.0")
    assert dt is not None
    assert dt.year == 2021

def test_parse_unix_ts_none():
    assert _parse_unix_ts(None) is None

def test_parse_unix_ts_invalid():
    assert _parse_unix_ts("not-a-timestamp") is None

def test_parse_unix_ts_microseconds_gives_correct_year():
    # 1700000000 seconds ≈ 2023; as microseconds → 1700 seconds ≈ 1970
    # Verify the threshold works correctly
    dt_sec = _parse_unix_ts(1700000000)
    dt_us  = _parse_unix_ts(1700000000000000)
    assert dt_sec is not None and dt_sec.year == 2023
    assert dt_us  is not None and dt_us.year  == 2023


# ---------------------------------------------------------------------------
# _normalize_quora_url
# ---------------------------------------------------------------------------

def test_normalize_relative_url():
    assert _normalize_quora_url("/unanswered/How-do-I") == "https://www.quora.com/unanswered/How-do-I"

def test_normalize_absolute_url_unchanged():
    u = "https://www.quora.com/What-is-a-short-sale"
    assert _normalize_quora_url(u) == u

def test_normalize_empty_returns_empty():
    assert _normalize_quora_url("") == ""


# ---------------------------------------------------------------------------
# _get_first
# ---------------------------------------------------------------------------

def test_get_first_returns_first_match():
    assert _get_first({"a": 1, "b": 2}, ["a", "b"]) == 1

def test_get_first_skips_none_value():
    assert _get_first({"a": None, "b": 7}, ["a", "b"]) == 7

def test_get_first_returns_default_when_none_found():
    assert _get_first({"x": None}, ["a", "b"], default=99) == 99

def test_get_first_empty_dict():
    assert _get_first({}, ["a"], default=0) == 0


# ---------------------------------------------------------------------------
# _parse_topics
# ---------------------------------------------------------------------------

def test_parse_topics_edges_shape():
    topics = _parse_topics(FULL_RECORD)
    assert "Foreclosures" in topics
    assert "Real Estate Investing" in topics

def test_parse_topics_flat_list():
    topics = _parse_topics(ALT_FIELDNAME_RECORD)
    assert "Short Sales" in topics
    assert "Real Estate" in topics

def test_parse_topics_missing():
    assert _parse_topics({}) == []

def test_parse_topics_empty_edges():
    assert _parse_topics({"topics": {"edges": []}}) == []


# ---------------------------------------------------------------------------
# _parse_author_credentials
# ---------------------------------------------------------------------------

def test_parse_credentials_translated():
    author = FULL_RECORD["topAnswer"]["author"]
    assert _parse_author_credentials(author) == "Real Estate Investor, 20+ years in Florida"

def test_parse_credentials_credentialtext():
    author = ALT_FIELDNAME_RECORD["topAnswer"]["author"]
    assert _parse_author_credentials(author) == "Licensed RE Agent"

def test_parse_credentials_missing():
    assert _parse_author_credentials({}) is None

def test_parse_credentials_empty_cred_obj():
    assert _parse_author_credentials({"credential": {}}) is None


# ---------------------------------------------------------------------------
# _parse_gql_response
# ---------------------------------------------------------------------------

def test_parse_gql_response_extracts_records():
    records = _parse_gql_response(GQL_PAYLOAD)
    assert len(records) == 2

def test_parse_gql_response_empty_data():
    assert _parse_gql_response({}) == []

def test_parse_gql_response_no_edges():
    assert _parse_gql_response({"data": {"searchConnection": {"edges": []}}}) == []


# ---------------------------------------------------------------------------
# _record_to_result — full record with microsecond timestamps
# ---------------------------------------------------------------------------

def test_result_core_fields():
    r = _record_to_result(1, FULL_RECORD)
    assert r is not None
    assert r.position == 1
    assert "foreclosed homes" in r.title.lower()
    assert r.url.startswith("https://www.quora.com")
    assert r.answer_count == 23
    assert r.follower_count == 45

def test_result_qid():
    r = _record_to_result(1, FULL_RECORD)
    assert r.qid == 65537606

def test_result_decanonicalized_answer_count():
    r = _record_to_result(1, FULL_RECORD)
    assert r.decanonicalized_answer_count == 24

def test_result_comment_count():
    r = _record_to_result(1, FULL_RECORD)
    assert r.comment_count == 3

def test_result_view_count():
    r = _record_to_result(1, FULL_RECORD)
    assert r.view_count == 12500

def test_result_created_time_microseconds():
    r = _record_to_result(1, FULL_RECORD)
    assert r.created_time is not None
    assert r.created_time.year == 2021

def test_result_last_activity_time_microseconds():
    r = _record_to_result(1, FULL_RECORD)
    assert r.last_activity_time is not None
    assert r.last_activity_time.year == 2023

def test_result_flags():
    r = _record_to_result(1, FULL_RECORD)
    assert r.is_locked   is False
    assert r.is_deleted  is False
    assert r.is_sensitive is False
    assert r.viewer_should_show_write_answer is True
    assert r.viewer_cant_answer is False
    assert r.is_user_limited_distro is True

def test_result_description():
    r = _record_to_result(1, FULL_RECORD)
    assert r.question_description and "Tampa FL" in r.question_description

def test_result_topics():
    r = _record_to_result(1, FULL_RECORD)
    assert "Foreclosures" in r.topics

def test_result_top_answer_upvotes():
    r = _record_to_result(1, FULL_RECORD)
    assert r.top_answer_upvotes == 156

def test_result_author_credentials():
    r = _record_to_result(1, FULL_RECORD)
    assert r.top_answer_author_credentials and "Real Estate Investor" in r.top_answer_author_credentials

def test_result_raw_metadata_excluded_by_default():
    r = _record_to_result(1, FULL_RECORD, include_raw=False)
    assert r.raw_metadata is None

def test_result_raw_metadata_included_when_requested():
    r = _record_to_result(1, FULL_RECORD, include_raw=True)
    assert r.raw_metadata is not None
    assert r.raw_metadata["viewCount"] == 12500

def test_result_minimal_no_crash():
    r = _record_to_result(1, MINIMAL_RECORD)
    assert r is not None
    assert "tax liens" in r.title.lower()
    assert r.view_count is None
    assert r.created_time is None
    assert r.topics == []
    assert r.top_answer_upvotes is None
    assert r.is_locked is False

def test_result_empty_record_returns_none():
    assert _record_to_result(1, EMPTY_RECORD) is None

def test_result_alt_fieldnames():
    r = _record_to_result(2, ALT_FIELDNAME_RECORD)
    assert r is not None
    assert "short sale" in r.title.lower()
    assert r.url.startswith("https://www.quora.com")
    assert r.answer_count == 5
    assert r.view_count == 3000
    assert r.top_answer_upvotes == 42
    assert r.top_answer_author == "Bob Agent"
    assert r.top_answer_author_credentials and "Licensed RE Agent" in r.top_answer_author_credentials
    assert "Short Sales" in r.topics

def test_result_url_normalized_from_relative():
    r = _record_to_result(1, MINIMAL_RECORD)
    assert r.url.startswith("https://www.quora.com")

def test_result_optional_fields_default_none():
    r = _record_to_result(1, MINIMAL_RECORD)
    assert r.qid is None
    assert r.slug is None
    assert r.comment_count is None
    assert r.is_locked is False
    assert r.cora_classification is None
    assert r.cora_answer_draft is None
    assert r.deterministic_score is None
    assert r.deterministic_reasons == []


# ---------------------------------------------------------------------------
# score_quora_result
# ---------------------------------------------------------------------------

def _make_result(**kwargs) -> QuoraResult:
    defaults = dict(
        position=1, title="", url="", answer_count=0, follower_count=0,
        top_answer_snippet="", top_answer_author="", top_answer_author_url="",
    )
    defaults.update(kwargs)
    return QuoraResult(**defaults)

def test_score_high_priority_foreclosure():
    r = _make_result(
        title="How do I stop foreclosure in Florida?",
        answer_count=0, follower_count=1,
    )
    score, reasons = score_quora_result(r, "foreclosures florida")
    assert score >= 60
    assert any("Florida" in x or "foreclosure" in x or "how-do-I" in x for x in reasons)

def test_score_has_florida_location():
    r = _make_result(title="Foreclosures in Tampa Florida")
    score, reasons = score_quora_result(r, "foreclosures")
    assert any("Florida" in x for x in reasons)

def test_score_has_foreclosure_intent():
    r = _make_result(title="Foreclosure auction process in Hillsborough County")
    score, reasons = score_quora_result(r, "foreclosure")
    assert any("foreclosure" in x.lower() for x in reasons)

def test_score_lending_topic():
    r = _make_result(title="What is hard money lending and how does it work?")
    score, reasons = score_quora_result(r, "hard money lenders")
    assert any("lending" in x.lower() for x in reasons)

def test_score_engagement_bonus():
    r = _make_result(title="Tax liens Florida", answer_count=5, follower_count=3, comment_count=2)
    score, reasons = score_quora_result(r, "tax liens")
    assert any("answers" in x for x in reasons)
    assert any("followers" in x for x in reasons)
    assert any("comments" in x for x in reasons)

def test_score_low_generic_question():
    r = _make_result(title="What is foreclosure?")
    score, reasons = score_quora_result(r, "foreclosure")
    assert score < 50

def test_score_tenant_only_penalized():
    r = _make_result(title="What are tenant rights in Florida foreclosure?")
    score, reasons = score_quora_result(r, "foreclosure")
    assert any("tenant" in x.lower() for x in reasons)

def test_score_covid_penalized():
    r = _make_result(title="How did covid affect Florida foreclosure rates?")
    score, reasons = score_quora_result(r, "foreclosure")
    assert any("covid" in x.lower() for x in reasons)

def test_score_locked_question_penalized():
    r = _make_result(
        title="How do I stop foreclosure in Florida?",
        is_locked=True,
        follower_count=10,
    )
    score, reasons = score_quora_result(r, "foreclosure")
    assert score < 60
    assert any("blocked" in x.lower() or "locked" in x.lower() for x in reasons)

def test_score_deleted_question_penalized():
    r = _make_result(title="Hard money loans Florida", is_deleted=True)
    score, reasons = score_quora_result(r, "hard money")
    assert any("blocked" in x.lower() or "deleted" in x.lower() for x in reasons)

def test_score_clamped_to_zero():
    r = _make_result(title="covid pandemic market prediction tenant rights renter", is_locked=True)
    score, _ = score_quora_result(r, "anything")
    assert score == 0

def test_score_clamped_to_hundred():
    r = _make_result(
        title="How do I stop foreclosure in Florida Tampa hard money repair renovation?",
        answer_count=5, follower_count=3, comment_count=2,
    )
    score, _ = score_quora_result(r, "foreclosure")
    assert score <= 100


# ---------------------------------------------------------------------------
# Cora Quora graph — compact input, no raw GQL, conditional generation
# ---------------------------------------------------------------------------

def test_cora_classify_never_receives_raw_metadata():
    """The classify node must strip raw_metadata before calling Claude."""
    from src.agents.graphs.quora_channel import _node_classify_question

    captured_messages = []

    def fake_claude(**kwargs):
        captured_messages.append(kwargs.get("messages", []))
        return {"text": '{"qid":1,"is_relevant":false,"is_answerable":false,"priority_score":10,"intent_lane":"general_distress","target_reader":"","commercial_angle":"","recommended_action":"skip","risk_level":"low","risk_notes":[],"reason":"test"}', "input_tokens": 1, "output_tokens": 1, "cost_usd": 0.0}

    with patch("src.agents.graphs.quora_channel.load_prompt") as mock_load, \
         patch("src.services.claude_router.call_claude_with_usage", fake_claude):
        mock_load.return_value = {"system": "sys", "user": "{candidate_json}"}
        state = {
            "candidate": {"qid": 1, "title": "Test", "raw_metadata": {"huge": "payload"}},
            "matched_keyword": "foreclosure florida",
        }
        from src.agents.graphs import quora_channel as qc
        with patch.object(qc, "load_prompt", mock_load):
            with patch("src.agents.graphs.quora_channel.load_prompt", mock_load):
                # Use the node directly to check the candidate_json rendered
                original_call_claude = None
                calls = []

                def capture_and_fake(*a, **kw):
                    calls.append(kw.get("messages") or a)
                    return {"text": '{"qid":1,"is_relevant":false,"is_answerable":false,"priority_score":10,"intent_lane":"general_distress","target_reader":"","commercial_angle":"","recommended_action":"skip","risk_level":"low","risk_notes":[],"reason":"test"}', "input_tokens": 1, "output_tokens": 1, "cost_usd": 0.0}

                with patch("src.agents.graphs.quora_channel.load_prompt", return_value={"system": "sys", "user": "{candidate_json}"}):
                    with patch("src.services.claude_router.call_claude_with_usage", capture_and_fake):
                        result = _node_classify_question(state)
                        rendered_user = calls[0][0]["content"] if calls else ""
                        assert "raw_metadata" not in rendered_user
                        assert "huge" not in rendered_user


def test_cora_classify_compact_input_only():
    """Classify node sends only compact fields, not full GQL dump."""
    from src.agents.graphs.quora_channel import _node_classify_question

    sent_content = []

    def fake_claude(*a, **kw):
        msgs = kw.get("messages") or []
        if msgs:
            sent_content.append(msgs[0].get("content", ""))
        return {"text": '{"qid":1,"is_relevant":false,"is_answerable":false,"priority_score":5,"intent_lane":"general_distress","target_reader":"","commercial_angle":"","recommended_action":"skip","risk_level":"low","risk_notes":[],"reason":"test"}', "input_tokens": 1, "output_tokens": 1, "cost_usd": 0.0}

    state = {
        "candidate": {"qid": 1, "title": "Test question", "raw_metadata": {"gql_field": "should_not_appear"}},
        "matched_keyword": "foreclosure",
    }

    with patch("src.agents.graphs.quora_channel.load_prompt", return_value={"system": "sys", "user": "{candidate_json}"}):
        with patch("src.services.claude_router.call_claude_with_usage", fake_claude):
            _node_classify_question(state)

    assert sent_content
    assert "gql_field" not in sent_content[0]
    assert "should_not_appear" not in sent_content[0]


def test_cora_answer_generation_skipped_when_classification_rejects():
    """generate_answer node returns None when classification says skip."""
    from src.agents.graphs.quora_channel import _node_generate_answer

    state = {
        "candidate": {"qid": 1, "title": "Test"},
        "matched_keyword": "foreclosure",
        "generate_answer_drafts": True,
        "cora_classification": {
            "is_relevant": False,
            "is_answerable": False,
            "priority_score": 20,
            "recommended_action": "skip",
        },
    }
    result = _node_generate_answer(state)
    assert result.get("cora_answer_draft") is None


def test_cora_answer_generation_skipped_below_priority_threshold():
    """generate_answer node returns None when priority_score < 70."""
    from src.agents.graphs.quora_channel import _node_generate_answer

    state = {
        "candidate": {"qid": 1, "title": "Test"},
        "matched_keyword": "foreclosure",
        "generate_answer_drafts": True,
        "cora_classification": {
            "is_relevant": True,
            "is_answerable": True,
            "priority_score": 60,
            "recommended_action": "generate_answer",
        },
    }
    result = _node_generate_answer(state)
    assert result.get("cora_answer_draft") is None


def test_cora_answer_generation_skipped_when_flag_false():
    """generate_answer node returns None when generate_answer_drafts=False."""
    from src.agents.graphs.quora_channel import _should_generate, END

    state = {
        "generate_answer_drafts": False,
        "cora_classification": {
            "is_relevant": True,
            "is_answerable": True,
            "priority_score": 95,
            "recommended_action": "generate_answer",
        },
    }
    assert _should_generate(state) == END


def test_default_command_does_not_publish_cora_event(monkeypatch):
    """When --classify-with-cora is not set, publish_cora_event is never called."""
    import src.scrappers.quora.quora_miner as miner_mod

    published = []

    async def fake_scrape(queries, max_results, dump_raw):
        from src.scrappers.quora.quora_engine import QuoraSearchResponse
        return [QuoraSearchResponse(query=queries[0], results=[], status="ok")]

    monkeypatch.setattr("src.scrappers.quora.quora_engine.scrape_quora", fake_scrape)

    import asyncio
    asyncio.run(miner_mod.main(
        keyword="foreclosure florida",
        max_results=5,
        dump_raw=False,
        classify_with_cora=False,
        generate_answer_drafts=False,
    ))
    assert len(published) == 0


def test_classify_flag_publishes_event_with_correct_fields(monkeypatch):
    """--classify-with-cora publishes events with decision_id, correct flags, no raw_metadata."""
    import src.scrappers.quora.quora_miner as miner_mod

    published_events = []

    def fake_publish(event):
        published_events.append(event)

    async def fake_poll(decision_id_map, timeout=120, interval=3.0):
        return 0  # no agents process running in tests

    from src.scrappers.quora.quora_engine import QuoraResult, QuoraSearchResponse

    r = QuoraResult(
        position=1, title="Test foreclosure Florida", url="https://www.quora.com/Test",
        answer_count=0, follower_count=0,
        top_answer_snippet="", top_answer_author="", top_answer_author_url="",
        qid=12345,
    )

    async def fake_scrape(queries, max_results, dump_raw):
        return [QuoraSearchResponse(query=queries[0], results=[r], status="ok")]

    monkeypatch.setattr("src.scrappers.quora.quora_miner.scrape_quora", fake_scrape)
    monkeypatch.setattr("src.agents.events.ingestion.publish_cora_event", fake_publish)
    monkeypatch.setattr("src.scrappers.quora.quora_miner._poll_cora_results", fake_poll)

    import asyncio
    asyncio.run(miner_mod.main(
        keyword="foreclosure florida",
        max_results=5,
        dump_raw=False,
        classify_with_cora=True,
        generate_answer_drafts=False,
    ))

    assert len(published_events) == 1
    evt = published_events[0]
    assert evt["event_type"] == "quora_candidate_classify"
    assert "decision_id" in evt                        # carries ID for polling
    assert evt["payload"]["generate_answer_drafts"] is False
    assert evt["payload"]["matched_keyword"] == "foreclosure florida"
    assert "raw_metadata" not in evt["payload"]["candidate"]


def test_classify_flag_does_not_generate_answers_in_payload(monkeypatch):
    """--classify-with-cora without --generate-answer-drafts sets flag=False in payload."""
    import src.scrappers.quora.quora_miner as miner_mod

    published_events = []

    def fake_publish(event):
        published_events.append(event)

    async def fake_poll(decision_id_map, timeout=120, interval=3.0):
        return 0

    from src.scrappers.quora.quora_engine import QuoraResult, QuoraSearchResponse

    r = QuoraResult(
        position=1, title="Tax liens Florida", url="https://www.quora.com/Tax-liens",
        answer_count=0, follower_count=0,
        top_answer_snippet="", top_answer_author="", top_answer_author_url="",
    )

    async def fake_scrape(queries, max_results, dump_raw):
        return [QuoraSearchResponse(query=queries[0], results=[r], status="ok")]

    monkeypatch.setattr("src.scrappers.quora.quora_miner.scrape_quora", fake_scrape)
    monkeypatch.setattr("src.agents.events.ingestion.publish_cora_event", fake_publish)
    monkeypatch.setattr("src.scrappers.quora.quora_miner._poll_cora_results", fake_poll)

    import asyncio
    asyncio.run(miner_mod.main(
        keyword="tax liens florida",
        max_results=5,
        dump_raw=False,
        classify_with_cora=True,
        generate_answer_drafts=False,
    ))

    assert all(e["payload"]["generate_answer_drafts"] is False for e in published_events)


def test_poll_enriches_results_from_agent_decisions(monkeypatch):
    """_poll_cora_results fetches from agent_decisions and writes back to QuoraResult."""
    import src.scrappers.quora.quora_miner as miner_mod
    from src.scrappers.quora.quora_engine import QuoraResult

    r = QuoraResult(
        position=1, title="Foreclosure Florida", url="https://www.quora.com/Test",
        answer_count=0, follower_count=0,
        top_answer_snippet="", top_answer_author="", top_answer_author_url="",
        qid=99,
    )
    did = "aaaabbbb-0000-0000-0000-000000000001"
    decision_id_map = {did: r}

    fake_row = (did, "completed", {
        "cora_classification": {"recommended_action": "generate_answer", "priority_score": 88},
        "cora_answer_draft":   None,
    })

    class FakeSession:
        def execute(self, *a, **kw):
            class FakeResult:
                def fetchall(self_):
                    return [fake_row]
            return FakeResult()
        def __enter__(self): return self
        def __exit__(self, *a): pass

    class FakeCtx:
        def __enter__(self): return FakeSession()
        def __exit__(self, *a): pass

    monkeypatch.setattr("src.scrappers.quora.quora_miner.get_db_context", lambda: FakeCtx(),
                        raising=False)

    # patch get_db_context inside the miner module
    import src.scrappers.quora.quora_miner as _m
    original = getattr(_m, "get_db_context", None)

    import asyncio
    # Inject get_db_context into the miner's namespace temporarily
    import src.core.database as _db_mod
    original_gdc = _db_mod.get_db_context

    class _FakeCtx:
        def __enter__(self): return FakeSession()
        def __exit__(self, *a): pass

    _db_mod.get_db_context = lambda: _FakeCtx()
    try:
        count = asyncio.run(miner_mod._poll_cora_results(decision_id_map, timeout=5, interval=0.1))
    finally:
        _db_mod.get_db_context = original_gdc

    assert count == 1
    assert r.cora_classification is not None
    assert r.cora_classification["recommended_action"] == "generate_answer"
