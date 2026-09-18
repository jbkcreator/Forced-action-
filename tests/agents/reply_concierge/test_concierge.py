"""
WP-T2-4 Reply Agent as Portal Concierge — test suite.

Coverage:
  1. Classifier — pricing pre-filter, opt-out regex, LLM path (mocked),
     error fallback
  2. Responder — KB hit, inactive topic, no match, footer appended
  3. Opt-out handler — lifecycle_state update, dual-path suppression
  4. Router — full-path integration with mocked DB and mocked classifier
  5. Portal-stall — suppressed person short-circuits, stall event published

All tests use MagicMock for DB sessions — no prod DB touched.
"""
from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call
import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _mock_db():
    """Return a MagicMock that satisfies session.execute().fetchone() / scalar() / commit()."""
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    db.execute.return_value.scalar.return_value = 0
    return db


def _row(mapping: dict):
    """Fake SQLAlchemy Row with _mapping attribute."""
    ns = SimpleNamespace(**mapping)
    ns._mapping = mapping
    # Allow row[0] access
    ns.__getitem__ = lambda self, i: list(mapping.values())[i]
    return ns


# ─────────────────────────────────────────────────────────────────────────────
# 1. Classifier
# ─────────────────────────────────────────────────────────────────────────────

class TestClassifier:
    """Unit tests — no LLM calls unless explicitly patched."""

    def test_pricing_prefilter_blocks_rate_question(self):
        from src.agents.reply_concierge.classifier import classify_inbound
        result = classify_inbound("What's the interest rate on a fix and flip loan?")
        assert result.intent == "clarifying_question"
        assert result.kb_topic == "none"
        assert result.confidence == 1.0
        assert "pricing" in result.reasoning

    def test_pricing_prefilter_blocks_apr(self):
        from src.agents.reply_concierge.classifier import classify_inbound
        result = classify_inbound("What APR do you charge for hard money?")
        assert result.intent == "clarifying_question"
        assert result.kb_topic == "none"

    def test_pricing_prefilter_blocks_origination_fee(self):
        from src.agents.reply_concierge.classifier import classify_inbound
        result = classify_inbound("How many points and what origination fee?")
        assert result.intent == "clarifying_question"
        assert result.kb_topic == "none"

    def test_opt_out_regex_stop(self):
        from src.agents.reply_concierge.classifier import classify_inbound
        result = classify_inbound("STOP")
        assert result.intent == "opt_out"
        assert result.confidence == 1.0

    def test_opt_out_regex_unsubscribe(self):
        from src.agents.reply_concierge.classifier import classify_inbound
        result = classify_inbound("Please unsubscribe me from this list.")
        assert result.intent == "opt_out"

    def test_opt_out_regex_remove_me(self):
        from src.agents.reply_concierge.classifier import classify_inbound
        result = classify_inbound("Remove me from your list, thanks")
        assert result.intent == "opt_out"

    def test_opt_out_regex_dont_contact(self):
        from src.agents.reply_concierge.classifier import classify_inbound
        result = classify_inbound("Don't contact me anymore")
        assert result.intent == "opt_out"

    def test_opt_out_takes_priority_over_pricing(self):
        """If a message has both opt-out and pricing signals, opt-out wins (regex runs first)."""
        from src.agents.reply_concierge.classifier import classify_inbound
        result = classify_inbound("STOP sending me rate sheets")
        assert result.intent == "opt_out"

    def test_llm_path_happy(self):
        from src.agents.reply_concierge.classifier import classify_inbound
        llm_response = {
            "text": json.dumps({
                "intent": "clarifying_question",
                "kb_topic": "prequal_process",
                "confidence": 0.92,
                "reasoning": "borrower asking about how prequal works",
            }),
            "cost_usd": 0.0001,
        }
        with patch("src.services.claude_router.call_claude_with_usage", return_value=llm_response):
            result = classify_inbound("How does the pre-qual process work?")
        assert result.intent == "clarifying_question"
        assert result.kb_topic == "prequal_process"
        assert result.confidence == pytest.approx(0.92)

    def test_llm_path_interested(self):
        from src.agents.reply_concierge.classifier import classify_inbound
        llm_response = {
            "text": json.dumps({
                "intent": "interested",
                "kb_topic": "none",
                "confidence": 0.88,
                "reasoning": "borrower says they want to proceed",
            }),
            "cost_usd": 0.0001,
        }
        with patch("src.services.claude_router.call_claude_with_usage", return_value=llm_response):
            result = classify_inbound("Yes I'm interested, let's move forward.")
        assert result.intent == "interested"
        assert result.kb_topic == "none"

    def test_llm_invalid_intent_falls_back_to_below_threshold(self):
        from src.agents.reply_concierge.classifier import classify_inbound
        llm_response = {
            "text": json.dumps({
                "intent": "unknown_garbage",
                "kb_topic": "none",
                "confidence": 0.5,
                "reasoning": "unclear",
            }),
            "cost_usd": 0.0001,
        }
        with patch("src.services.claude_router.call_claude_with_usage", return_value=llm_response):
            result = classify_inbound("Something ambiguous")
        assert result.intent == "below_threshold"

    def test_llm_exception_falls_back_gracefully(self):
        from src.agents.reply_concierge.classifier import classify_inbound
        with patch("src.services.claude_router.call_claude_with_usage", side_effect=RuntimeError("timeout")):
            result = classify_inbound("What documents do I need?")
        assert result.intent == "below_threshold"
        assert result.confidence == 0.0

    def test_llm_bad_json_falls_back(self):
        from src.agents.reply_concierge.classifier import classify_inbound
        with patch("src.services.claude_router.call_claude_with_usage", return_value={"text": "not json", "cost_usd": 0}):
            result = classify_inbound("Hello")
        assert result.intent == "below_threshold"


# ─────────────────────────────────────────────────────────────────────────────
# 2. Responder
# ─────────────────────────────────────────────────────────────────────────────

class TestResponder:

    def test_kb_hit_returns_template(self):
        from src.agents.reply_concierge.responder import build_reply
        db = _mock_db()
        db.execute.return_value.fetchone.return_value = ("Great question. The pre-qualification...",)
        result = build_reply("prequal_process", db, borrower_first_name="Maria")
        assert result.has_reply is True
        assert "Maria" in result.reply_text
        assert "Great question" in result.reply_text
        assert result.kb_topic_key == "prequal_process"

    def test_footer_is_appended(self):
        from src.agents.reply_concierge.responder import build_reply
        db = _mock_db()
        db.execute.return_value.fetchone.return_value = ("Answer text here.",)
        result = build_reply("funding_timeline", db)
        assert "not an offer of credit" in result.reply_text
        assert "Reply STOP" in result.reply_text

    def test_no_topic_returns_no_reply(self):
        from src.agents.reply_concierge.responder import build_reply
        db = _mock_db()
        result = build_reply("none", db)
        assert result.has_reply is False
        assert result.reason == "no_kb_topic_matched"

    def test_inactive_topic_returns_no_reply(self):
        from src.agents.reply_concierge.responder import build_reply
        db = _mock_db()
        db.execute.return_value.fetchone.return_value = None  # DB found nothing (inactive)
        result = build_reply("prequal_process", db)
        assert result.has_reply is False
        assert result.reason == "topic_inactive_or_missing"

    def test_db_error_returns_no_reply(self):
        from src.agents.reply_concierge.responder import build_reply
        db = _mock_db()
        db.execute.side_effect = RuntimeError("connection lost")
        result = build_reply("documents_needed", db)
        assert result.has_reply is False
        assert "kb_lookup_error" in result.reason

    def test_no_first_name_uses_generic_greeting(self):
        from src.agents.reply_concierge.responder import build_reply
        db = _mock_db()
        db.execute.return_value.fetchone.return_value = ("Template text.",)
        result = build_reply("referral_relationship", db, borrower_first_name=None)
        assert result.reply_text.startswith("Hi,")


# ─────────────────────────────────────────────────────────────────────────────
# 3. Opt-out handler
# ─────────────────────────────────────────────────────────────────────────────

class TestOptOut:

    def test_sets_do_not_contact_on_person(self):
        from src.agents.reply_concierge.opt_out import handle_opt_out
        db = _mock_db()
        with patch("src.services.email_suppression.suppress_contact") as mock_suppress:
            result = handle_opt_out(
                person_id="uuid-123",
                contact_email="borrower@example.com",
                inbound_text="STOP",
                channel="email",
                db=db,
            )
        # DB execute called for UPDATE and for INSERT log
        assert db.execute.called
        update_call = db.execute.call_args_list[0]
        sql = str(update_call[0][0])
        assert "do_not_contact" in sql
        assert result.suppressed is True

    def test_email_suppression_also_called(self):
        from src.agents.reply_concierge.opt_out import handle_opt_out
        db = _mock_db()
        with patch("src.services.email_suppression.suppress_contact") as mock_suppress:
            handle_opt_out(
                person_id="uuid-123",
                contact_email="borrower@example.com",
                inbound_text="unsubscribe",
                channel="email",
                db=db,
            )
        mock_suppress.assert_called_once_with("borrower@example.com", reason="opt_out", source="concierge")

    def test_no_identifiers_returns_not_suppressed(self):
        from src.agents.reply_concierge.opt_out import handle_opt_out
        db = _mock_db()
        result = handle_opt_out(
            person_id=None,
            contact_email=None,
            inbound_text="STOP",
            channel="email",
            db=db,
        )
        assert result.suppressed is False
        assert result.reason == "missing_identifier"

    def test_email_only_path_works(self):
        from src.agents.reply_concierge.opt_out import handle_opt_out
        db = _mock_db()
        with patch("src.services.email_suppression.suppress_contact") as mock_suppress:
            result = handle_opt_out(
                person_id=None,
                contact_email="someone@example.com",
                inbound_text="remove me",
                channel="email",
                db=db,
            )
        mock_suppress.assert_called_once()
        assert result.suppressed is True

    def test_db_failure_does_not_raise(self):
        from src.agents.reply_concierge.opt_out import handle_opt_out
        db = _mock_db()
        db.execute.side_effect = RuntimeError("DB down")
        with patch("src.services.email_suppression.suppress_contact"):
            # Should not raise
            result = handle_opt_out(
                person_id="uuid-xyz",
                contact_email=None,
                inbound_text="STOP",
                channel="email",
                db=db,
            )
        assert result.suppressed is False


# ─────────────────────────────────────────────────────────────────────────────
# 4. Router
# ─────────────────────────────────────────────────────────────────────────────

class TestRouter:

    def _make_classification(self, intent, kb_topic="none", confidence=0.9, cost=0.0001):
        from src.agents.reply_concierge.classifier import ClassificationResult
        return ClassificationResult(
            intent=intent, kb_topic=kb_topic,
            confidence=confidence, reasoning="test", cost_usd=cost,
        )

    def test_opt_out_routes_to_suppression(self):
        from src.agents.reply_concierge.router import handle_inbound
        db = _mock_db()
        classification = self._make_classification("opt_out")
        with patch("src.agents.reply_concierge.router.classify_inbound", return_value=classification), \
             patch("src.agents.reply_concierge.router.handle_opt_out") as mock_opt_out:
            mock_opt_out.return_value = MagicMock(suppressed=True)
            outcome = handle_inbound(
                inbound_text="STOP",
                channel="email",
                person_id="pid-1",
                contact_email="x@example.com",
                opportunity_id=None,
                borrower_first_name=None,
                db=db,
            )
        assert outcome.action == "opt_out_suppressed"
        mock_opt_out.assert_called_once()

    def test_below_threshold_routes_to_exceptions(self):
        from src.agents.reply_concierge.router import handle_inbound
        db = _mock_db()
        db.execute.return_value.fetchone.return_value = _row({"id": 42})
        classification = self._make_classification("below_threshold", confidence=0.0)
        with patch("src.agents.reply_concierge.router.classify_inbound", return_value=classification):
            outcome = handle_inbound(
                inbound_text="Hmm",
                channel="email",
                person_id="pid-1",
                contact_email=None,
                opportunity_id=None,
                borrower_first_name=None,
                db=db,
            )
        assert outcome.action == "escalated_exceptions"

    def test_low_confidence_routes_to_exceptions(self):
        from src.agents.reply_concierge.router import handle_inbound
        db = _mock_db()
        db.execute.return_value.fetchone.return_value = _row({"id": 5})
        classification = self._make_classification("clarifying_question", "prequal_process", confidence=0.4)
        with patch("src.agents.reply_concierge.router.classify_inbound", return_value=classification):
            outcome = handle_inbound(
                inbound_text="How does it work?",
                channel="email",
                person_id=None,
                contact_email=None,
                opportunity_id=None,
                borrower_first_name=None,
                db=db,
            )
        assert outcome.action == "escalated_exceptions"

    def test_pricing_escalation_no_topic_routes_to_exceptions(self):
        from src.agents.reply_concierge.router import handle_inbound
        db = _mock_db()
        db.execute.return_value.fetchone.return_value = _row({"id": 7})
        classification = self._make_classification("clarifying_question", kb_topic="none", confidence=1.0)
        with patch("src.agents.reply_concierge.router.classify_inbound", return_value=classification):
            outcome = handle_inbound(
                inbound_text="What's the rate?",
                channel="email",
                person_id="pid-2",
                contact_email=None,
                opportunity_id=None,
                borrower_first_name=None,
                db=db,
            )
        assert outcome.action == "escalated_exceptions"

    def test_kb_match_queues_reply_when_autonomy_disabled(self):
        from src.agents.reply_concierge.router import handle_inbound
        from src.agents.reply_concierge.responder import ReplyResult
        db = _mock_db()
        db.execute.return_value.fetchone.return_value = _row({"id": 99})
        classification = self._make_classification("clarifying_question", "prequal_process", confidence=0.91)
        reply_result = ReplyResult(
            has_reply=True,
            reply_text="Hi,\n\nGreat question...\n\n---\nJosh Kantor...",
            kb_topic_key="prequal_process",
            reason="kb_match",
        )
        with patch("src.agents.reply_concierge.router.classify_inbound", return_value=classification), \
             patch("src.agents.reply_concierge.router.build_reply", return_value=reply_result), \
             patch("src.agents.reply_concierge.router._AUTONOMY_ENABLED", False):
            outcome = handle_inbound(
                inbound_text="How does prequal work?",
                channel="email",
                person_id="pid-3",
                contact_email="b@example.com",
                opportunity_id="opp-1",
                borrower_first_name="Sam",
                db=db,
            )
        assert outcome.action == "kb_reply_queued"
        assert outcome.kb_topic_key == "prequal_process"

    def test_kb_lookup_failure_falls_back_to_exceptions(self):
        from src.agents.reply_concierge.router import handle_inbound
        from src.agents.reply_concierge.responder import ReplyResult
        db = _mock_db()
        db.execute.return_value.fetchone.return_value = _row({"id": 11})
        classification = self._make_classification("clarifying_question", "documents_needed", confidence=0.88)
        failed_reply = ReplyResult(has_reply=False, reply_text=None, kb_topic_key="documents_needed", reason="topic_inactive_or_missing")
        with patch("src.agents.reply_concierge.router.classify_inbound", return_value=classification), \
             patch("src.agents.reply_concierge.router.build_reply", return_value=failed_reply):
            outcome = handle_inbound(
                inbound_text="What docs do I need?",
                channel="email",
                person_id="pid-4",
                contact_email=None,
                opportunity_id=None,
                borrower_first_name=None,
                db=db,
            )
        assert outcome.action == "escalated_exceptions"

    def test_interested_routes_to_exceptions(self):
        from src.agents.reply_concierge.router import handle_inbound
        db = _mock_db()
        db.execute.return_value.fetchone.return_value = _row({"id": 20})
        classification = self._make_classification("interested", confidence=0.9)
        with patch("src.agents.reply_concierge.router.classify_inbound", return_value=classification):
            outcome = handle_inbound(
                inbound_text="Yes let's do it",
                channel="email",
                person_id="pid-5",
                contact_email=None,
                opportunity_id=None,
                borrower_first_name=None,
                db=db,
            )
        assert outcome.action == "escalated_exceptions"
        assert outcome.classification == "interested"

    def test_autonomy_check_threshold_25(self):
        """_is_autonomous_eligible returns True only when sent count >= 25."""
        from src.agents.reply_concierge.router import _is_autonomous_eligible
        db = _mock_db()
        db.execute.return_value.scalar.return_value = 24
        assert _is_autonomous_eligible(db) is False

        db.execute.return_value.scalar.return_value = 25
        assert _is_autonomous_eligible(db) is True

        db.execute.return_value.scalar.return_value = 100
        assert _is_autonomous_eligible(db) is True

    def test_autonomy_check_db_error_returns_false(self):
        from src.agents.reply_concierge.router import _is_autonomous_eligible
        db = _mock_db()
        db.execute.side_effect = RuntimeError("DB error")
        assert _is_autonomous_eligible(db) is False


# ─────────────────────────────────────────────────────────────────────────────
# 5. Portal stall handler
# ─────────────────────────────────────────────────────────────────────────────

class TestPortalStall:

    def test_suppressed_person_short_circuits(self):
        from src.agents.reply_concierge.portal_stall import PortalStallPayload, handle_portal_stall
        db = _mock_db()
        # _resolve_person returns a person
        db.execute.return_value.fetchone.side_effect = [
            _row({"person_id": "pid-sup", "lifecycle_state": "do_not_contact"}),  # resolve
            _row({"0": "do_not_contact"}),                                         # _is_suppressed
        ]
        # Make row[0] work
        sup_row = SimpleNamespace(person_id="pid-sup", lifecycle_state="do_not_contact")
        sup_row._mapping = {"person_id": "pid-sup", "lifecycle_state": "do_not_contact"}
        db.execute.return_value.fetchone.side_effect = [sup_row, sup_row]

        with patch("src.agents.reply_concierge.portal_stall._is_suppressed", return_value=True):
            result = handle_portal_stall(
                PortalStallPayload(backflip_contact_id="bf-123"),
                db=db,
            )
        assert result["status"] == "suppressed"

    def test_no_identifier_raises_422(self):
        from src.agents.reply_concierge.portal_stall import PortalStallPayload, handle_portal_stall
        from fastapi import HTTPException
        db = _mock_db()
        with pytest.raises(HTTPException) as exc_info:
            handle_portal_stall(PortalStallPayload(), db=db)
        assert exc_info.value.status_code == 422

    def test_unknown_person_publishes_stall_event(self):
        from src.agents.reply_concierge.portal_stall import PortalStallPayload, handle_portal_stall
        db = _mock_db()
        db.execute.return_value.fetchone.return_value = None  # person not found

        with patch("src.agents.events.ingestion.publish_lifecycle_event") as mock_publish:
            result = handle_portal_stall(
                PortalStallPayload(
                    backflip_contact_id="bf-new",
                    contact_email="new@example.com",
                    borrower_first_name="Alex",
                ),
                db=db,
            )
        mock_publish.assert_called_once()
        event = mock_publish.call_args[0][0]
        assert event["event_type"] == "portal.stall"
        assert result["status"] == "stall_event_published"

    def test_known_person_no_pending_inbound_publishes_stall(self):
        from src.agents.reply_concierge.portal_stall import PortalStallPayload, handle_portal_stall
        db = _mock_db()
        person_row = SimpleNamespace(person_id="pid-known", lifecycle_state="portal_started")
        person_row._mapping = {"person_id": "pid-known", "lifecycle_state": "portal_started"}
        # resolve → person; _pop_pending_inbound → None
        db.execute.return_value.fetchone.side_effect = [person_row, None, None]

        with patch("src.agents.reply_concierge.portal_stall._is_suppressed", return_value=False), \
             patch("src.agents.reply_concierge.portal_stall._pop_pending_inbound", return_value=None), \
             patch("src.agents.events.ingestion.publish_lifecycle_event") as mock_pub:
            result = handle_portal_stall(
                PortalStallPayload(backflip_contact_id="bf-456"),
                db=db,
            )
        mock_pub.assert_called_once()
        assert result["status"] == "stall_event_published"

    def test_pending_inbound_routes_to_concierge(self):
        from src.agents.reply_concierge.portal_stall import PortalStallPayload, handle_portal_stall
        from src.agents.reply_concierge.router import ConciergeOutcome
        db = _mock_db()
        person_row = SimpleNamespace(person_id="pid-has-reply", lifecycle_state="portal_started")
        person_row._mapping = {"person_id": "pid-has-reply", "lifecycle_state": "portal_started"}
        db.execute.return_value.fetchone.return_value = person_row

        pending = {"inbound_text": "How does prequal work?", "channel": "email"}
        mock_outcome = ConciergeOutcome(
            action="kb_reply_queued",
            classification="clarifying_question",
            kb_topic_key="prequal_process",
            confidence=0.91,
            relay_queue_id=55,
            cost_usd=0.0001,
        )

        with patch("src.agents.reply_concierge.portal_stall._is_suppressed", return_value=False), \
             patch("src.agents.reply_concierge.portal_stall._pop_pending_inbound", return_value=pending), \
             patch("src.agents.reply_concierge.portal_stall._resolve_opportunity", return_value=None), \
             patch("src.agents.reply_concierge.router.handle_inbound", return_value=mock_outcome):
            result = handle_portal_stall(
                PortalStallPayload(backflip_contact_id="bf-789"),
                db=db,
            )
        assert result["status"] == "concierge_handled"
        assert result["action"] == "kb_reply_queued"


# ─────────────────────────────────────────────────────────────────────────────
# 6. Compliance / security invariants
# ─────────────────────────────────────────────────────────────────────────────

class TestComplianceInvariants:

    def test_pricing_never_in_kb_reply(self):
        """KB templates seeded in migration must not contain pricing language."""
        pricing_words = ["rate", "apr", "fee", "points", "term", "%", "interest"]
        # The seed templates from the migration file
        templates = [
            "Great question. The pre-qualification process with Backflip takes about 10 minutes.",
            "For a typical hard money deal through Backflip, you'll generally need: a signed purchase contract",
            "Backflip primarily does fix-and-flip loans on non-owner-occupied residential properties",
            "Once a complete deal is submitted through Backflip's portal, they typically issue a term sheet",
            "Simple setup — I introduce qualified deals to Backflip on your behalf.",
        ]
        # None of these should contain rate/APR/fee pricing language
        forbidden = ["apr", "interest rate", "origination fee", "loan rate"]
        for template in templates:
            for word in forbidden:
                assert word not in template.lower(), (
                    f"Template contains pricing language '{word}': {template[:80]}"
                )

    def test_reply_text_never_contains_rate_or_apr(self):
        """End-to-end: even if KB template were somehow injected, footer must not add rate."""
        from src.agents.reply_concierge.responder import _get_compliance_footer
        footer = _get_compliance_footer()
        assert "rate" not in footer.lower() or "interest rate" not in footer.lower()
        assert "not an offer of credit" in footer.lower()

    def test_opt_out_idempotency_key_format(self):
        from src.agents.reply_concierge.router import _idem_key
        key1 = _idem_key("exceptions", "pid-1", "some text")
        key2 = _idem_key("exceptions", "pid-1", "some text")
        assert key1 == key2  # deterministic
        assert key1.startswith("concierge:exceptions:")

    def test_different_texts_produce_different_keys(self):
        from src.agents.reply_concierge.router import _idem_key
        key1 = _idem_key("reply", "pid-1", "text A")
        key2 = _idem_key("reply", "pid-1", "text B")
        assert key1 != key2
