"""Tests for WP-9 dial-list in-place action handler and thread-reply parser."""
from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from src.services.dial_list.actions import (
    ActionResult,
    ThreadIntent,
    handle_action,
    handle_thread_reply,
    parse_thread_reply,
)
from src.services.dial_list.delivery import (
    ACTION_CALLED,
    ACTION_LOST,
    ACTION_SKIP,
    ACTION_WON,
    ACTIONS_BLOCK_PREFIX,
    _actions_block,
    format_dial_list_digest,
)
from src.services.dial_list.models import DialList, DialListEntry
from src.services.opportunity_outcome import LOSS_REASON_CODES

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_AS_OF = date(2026, 9, 16)
_THREAD = "OPP-XYZ"
_APPROVER = "U0APPROVER"


def _entry(property_id: int = 1, opportunity_id: str = _THREAD) -> DialListEntry:
    return DialListEntry(
        property_id=property_id,
        opportunity_id=opportunity_id,
        buyer_entity_id=None,
        triggers=["cash_purchase"],
        expected_revenue=Decimal("3000"),
        probability=Decimal("0.50"),
        expected_loan=Decimal("400000"),
        commission=Decimal("6000"),
        urgency=Decimal("1.0"),
        expected_loan_confidence="high",
        borrower_resolved=True,
        reason="Cash purchase.",
        talking_points=["Owns 2 properties"],
        rank=1,
    )


def _payload(action_id: str, value: str, user_id: str = _APPROVER,
             block_id: str = "dial_act:1") -> Dict[str, Any]:
    return {
        "user": {"id": user_id},
        "channel": {"id": "C123"},
        "message": {
            "ts": "111.222",
            "blocks": [
                {"type": "section", "block_id": "hdr"},
                {"type": "actions", "block_id": block_id},
            ],
        },
        "actions": [{"action_id": action_id, "value": value, "block_id": block_id}],
    }


def _value(**kwargs: Any) -> str:
    return json.dumps({"thread": _THREAD, "as_of": str(_AS_OF), **kwargs},
                      separators=(",", ":"))


# ---------------------------------------------------------------------------
# Card builder tests
# ---------------------------------------------------------------------------

class TestActionsBlock:
    def test_has_four_elements(self):
        block = _actions_block(_entry(), _AS_OF)
        assert len(block["elements"]) == 4

    def test_action_ids(self):
        block = _actions_block(_entry(), _AS_OF)
        ids = [e["action_id"] for e in block["elements"]]
        assert ACTION_CALLED in ids
        assert ACTION_WON in ids
        assert ACTION_LOST in ids
        assert ACTION_SKIP in ids

    def test_lost_select_has_all_loss_codes(self):
        block = _actions_block(_entry(), _AS_OF)
        lost_el = next(e for e in block["elements"] if e["action_id"] == ACTION_LOST)
        option_values_raw = [opt["value"] for opt in lost_el["options"]]
        codes_in_options = {json.loads(v)["loss_code"] for v in option_values_raw}
        assert codes_in_options == set(LOSS_REASON_CODES)

    def test_block_id_prefix(self):
        block = _actions_block(_entry(property_id=7), _AS_OF)
        assert block["block_id"].startswith(ACTIONS_BLOCK_PREFIX)

    def test_interactive_flag_adds_actions_blocks(self):
        dl = DialList(
            generated_for=_AS_OF,
            entries=[_entry()],
            candidate_count=1,
            config_version="test",
        )
        _, blocks_plain = format_dial_list_digest(dl, interactive=False)
        _, blocks_iact = format_dial_list_digest(dl, interactive=True)
        action_blocks = [b for b in blocks_iact if b.get("type") == "actions"]
        assert len(action_blocks) == 1
        assert len(blocks_iact) > len(blocks_plain)


# ---------------------------------------------------------------------------
# handle_action tests
# ---------------------------------------------------------------------------

class TestHandleAction:
    def _fake_session(self):
        sess = MagicMock()
        return sess

    def test_won_records_disposition(self):
        sess = self._fake_session()
        with patch("src.services.dial_list.actions.record_dial_disposition") as mock_rd:
            mock_rd.return_value = MagicMock()
            result = handle_action(
                _payload(ACTION_WON, _value()),
                sess,
                approver_id=_APPROVER,
                now=datetime(2026, 9, 16, 10, 0),
            )
        assert result.status == "recorded"
        assert result.kind == "won"
        mock_rd.assert_called_once()
        _, kwargs = mock_rd.call_args
        assert kwargs["outcome"] == "won"
        assert kwargs["loss_code"] is None

    def test_lost_records_with_loss_code(self):
        sess = self._fake_session()
        code = "price"
        val = _value(loss_code=code)
        payload = {
            "user": {"id": _APPROVER},
            "channel": {"id": "C123"},
            "message": {"ts": "111.222", "blocks": []},
            "actions": [{
                "action_id": ACTION_LOST,
                "block_id": "dial_act:1",
                "selected_option": {"value": val},
            }],
        }
        with patch("src.services.dial_list.actions.record_dial_disposition") as mock_rd:
            mock_rd.return_value = MagicMock()
            result = handle_action(payload, sess, approver_id=_APPROVER)
        assert result.status == "recorded"
        assert result.kind == "lost"
        assert result.loss_code == code

    def test_non_approver_ignored(self):
        sess = self._fake_session()
        result = handle_action(
            _payload(ACTION_WON, _value(), user_id="U_STRANGER"),
            sess,
            approver_id=_APPROVER,
        )
        assert result.status == "ignored"

    def test_called_is_non_terminal(self):
        sess = self._fake_session()
        with patch("src.services.dial_list.actions.record_dial_disposition") as mock_rd:
            result = handle_action(_payload(ACTION_CALLED, _value()), sess,
                                   approver_id=_APPROVER)
        assert result.status == "touched"
        assert result.kind == "called"
        mock_rd.assert_not_called()

    def test_skip_is_non_terminal(self):
        sess = self._fake_session()
        with patch("src.services.dial_list.actions.record_dial_disposition") as mock_rd:
            result = handle_action(_payload(ACTION_SKIP, _value()), sess,
                                   approver_id=_APPROVER)
        assert result.status == "touched"
        assert result.kind == "skipped"
        mock_rd.assert_not_called()

    def test_won_without_thread_is_error(self):
        sess = self._fake_session()
        val = json.dumps({"as_of": str(_AS_OF)}, separators=(",", ":"))
        result = handle_action(_payload(ACTION_WON, val), sess, approver_id=_APPROVER)
        assert result.status == "error"

    def test_idempotent_double_tap(self):
        sess = self._fake_session()
        with patch("src.services.dial_list.actions.record_dial_disposition") as mock_rd:
            mock_rd.return_value = MagicMock()
            handle_action(_payload(ACTION_WON, _value()), sess, approver_id=_APPROVER)
            handle_action(_payload(ACTION_WON, _value()), sess, approver_id=_APPROVER)
        assert mock_rd.call_count == 2  # write-path idempotency via DB UNIQUE

    def test_card_update_called_on_success(self):
        sess = self._fake_session()
        client = MagicMock()
        with patch("src.services.dial_list.actions.record_dial_disposition") as mock_rd:
            mock_rd.return_value = MagicMock()
            handle_action(_payload(ACTION_WON, _value()), sess,
                          approver_id=_APPROVER, client=client)
        client.chat_update.assert_called_once()


# ---------------------------------------------------------------------------
# parse_thread_reply tests
# ---------------------------------------------------------------------------

class TestParseThreadReply:
    def test_won(self):
        assert parse_thread_reply("won the deal").kind == "won"

    def test_win_synonym(self):
        assert parse_thread_reply("we win!").kind == "won"

    def test_lost_with_code(self):
        intent = parse_thread_reply("lost — price too high")
        assert intent.kind == "lost"
        assert intent.loss_code == "price"

    def test_lost_no_code(self):
        intent = parse_thread_reply("lost it")
        assert intent.kind == "lost"
        assert intent.loss_code is None

    def test_called(self):
        assert parse_thread_reply("called them today").kind == "called"

    def test_reached(self):
        assert parse_thread_reply("reached out").kind == "called"

    def test_skip(self):
        assert parse_thread_reply("skip this one").kind == "skipped"

    def test_unrecognised(self):
        assert parse_thread_reply("interesting property").kind is None

    def test_empty(self):
        assert parse_thread_reply("").kind is None


# ---------------------------------------------------------------------------
# handle_thread_reply tests
# ---------------------------------------------------------------------------

class TestHandleThreadReply:
    def test_won_calls_disposition(self):
        sess = MagicMock()
        with patch("src.services.dial_list.actions.record_dial_disposition") as mock_rd:
            mock_rd.return_value = MagicMock()
            result = handle_thread_reply("won", sess, opportunity_thread_id=_THREAD)
        assert result.status == "recorded"
        assert result.kind == "won"

    def test_lost_with_code(self):
        sess = MagicMock()
        with patch("src.services.dial_list.actions.record_dial_disposition") as mock_rd:
            mock_rd.return_value = MagicMock()
            result = handle_thread_reply("lost, competitor beat us",
                                         sess, opportunity_thread_id=_THREAD)
        assert result.status == "recorded"
        assert result.loss_code == "competitor"

    def test_lost_no_code_is_error(self):
        sess = MagicMock()
        result = handle_thread_reply("lost", sess, opportunity_thread_id=_THREAD)
        assert result.status == "error"

    def test_called_is_touched(self):
        sess = MagicMock()
        with patch("src.services.dial_list.actions.record_dial_disposition") as mock_rd:
            result = handle_thread_reply("called them", sess, opportunity_thread_id=_THREAD)
        assert result.status == "touched"
        mock_rd.assert_not_called()

    def test_unrecognised_is_ignored(self):
        sess = MagicMock()
        result = handle_thread_reply("nice property", sess, opportunity_thread_id=_THREAD)
        assert result.status == "ignored"


# ---------------------------------------------------------------------------
# Touch durability (#3a) — Called/Skip persist to dial_list_touch
# ---------------------------------------------------------------------------

class TestTouchDurability:
    def _real_session(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from src.core.models import DialListTouch
        engine = create_engine("sqlite:///:memory:")
        DialListTouch.__table__.create(bind=engine)
        return sessionmaker(bind=engine)(), DialListTouch

    def test_called_writes_touch_row(self):
        sess, DialListTouch = self._real_session()
        payload = _payload(ACTION_CALLED, _value(property_id=42))
        with patch("src.services.dial_list.actions.record_dial_disposition"):
            handle_action(payload, sess, approver_id=_APPROVER)
        rows = sess.query(DialListTouch).all()
        assert len(rows) == 1
        assert rows[0].property_id == 42
        assert rows[0].action == "called"
        assert rows[0].actor == _APPROVER

    def test_repeated_touches_are_all_retained(self):
        sess, DialListTouch = self._real_session()
        payload = _payload(ACTION_CALLED, _value(property_id=42))
        with patch("src.services.dial_list.actions.record_dial_disposition"):
            handle_action(payload, sess, approver_id=_APPROVER)
            handle_action(payload, sess, approver_id=_APPROVER)
        assert sess.query(DialListTouch).count() == 2

    def test_touch_write_failure_does_not_update_card(self):
        sess = MagicMock()
        sess.execute.side_effect = __import__("sqlalchemy").exc.SQLAlchemyError("db down")
        client = MagicMock()
        result = handle_action(
            _payload(ACTION_CALLED, _value(property_id=42)), sess,
            approver_id=_APPROVER, client=client,
        )
        assert result.status == "error"
        client.chat_update.assert_not_called()
