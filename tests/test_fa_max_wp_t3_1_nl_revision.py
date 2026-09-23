"""WP-T3-1 ticket 01 — button-driven NL revision (pending slot → rewrite in place)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.services import fa_max_pending_slot as slots
from src.services.fa_max_pending_slot import PendingSlot
from src.services.relay import nl_revision

NOW = datetime(2026, 9, 23, 12, 0, tzinfo=timezone.utc)


def _slot(**over) -> PendingSlot:
    base = dict(
        slack_user_id="U1", kind="revise", target_ref="7", channel_id="C1",
        thread_ts="111.1", set_at=NOW,
    )
    base.update(over)
    return PendingSlot(**base)


def _item(**over):
    base = dict(
        id=7, venture_key="fa_max_lending", status="pending", revision_count=0,
        slack_message_ts="111.1", original_draft="Hi Mike, saw your permit on 12 Oak St. We fund flips fast. Want to chat?",
        final_content=None, material_edit=False,
    )
    base.update(over)
    return MagicMock(**base)


# ── pending slot ─────────────────────────────────────────────────────────────

class TestPendingSlot:
    def test_open_slot_is_an_upsert_so_last_tap_wins(self):
        session = MagicMock()
        slots.open_slot(session, slack_user_id="U1", kind="revise", target_ref="7",
                        channel_id="C1", thread_ts="111.1")
        sql = str(session.execute.call_args.args[0])
        assert "ON CONFLICT (slack_user_id) DO UPDATE" in sql

    def test_open_slot_rejects_unknown_kind(self):
        with pytest.raises(ValueError):
            slots.open_slot(MagicMock(), slack_user_id="U1", kind="bogus", target_ref="7", channel_id="C1")

    def test_get_slot_returns_live_slot(self):
        session = MagicMock()
        session.execute.return_value.mappings.return_value.first.return_value = {
            "slack_user_id": "U1", "kind": "revise", "target_ref": "7", "channel_id": "C1",
            "thread_ts": "111.1", "set_at": NOW - timedelta(minutes=5),
        }
        slot = slots.get_slot(session, "U1", now=NOW, ttl_min=15)
        assert slot is not None and slot.target_ref == "7"

    def test_get_slot_expired_is_deleted_and_returns_none(self):
        session = MagicMock()
        session.execute.return_value.mappings.return_value.first.return_value = {
            "slack_user_id": "U1", "kind": "revise", "target_ref": "7", "channel_id": "C1",
            "thread_ts": "111.1", "set_at": NOW - timedelta(minutes=16),
        }
        assert slots.get_slot(session, "U1", now=NOW, ttl_min=15) is None
        assert "DELETE FROM fa_max_pending_slots" in str(session.execute.call_args.args[0])

    def test_slot_lookup_db_failure_degrades_to_no_slot(self):
        from sqlalchemy.exc import OperationalError
        from src.api import admin_router

        with patch.object(admin_router, "get_db_context", side_effect=OperationalError("x", {}, None)):
            assert admin_router._get_pending_slot("U1") is None

    def test_get_slot_missing_returns_none(self):
        session = MagicMock()
        session.execute.return_value.mappings.return_value.first.return_value = None
        assert slots.get_slot(session, "U1", now=NOW) is None


# ── rewrite core ─────────────────────────────────────────────────────────────

class TestReviseDraft:
    def test_returns_llm_text_on_success(self):
        llm = MagicMock(return_value="  Hi Mike, saw your permit on 12 Oak St. Want to chat?  ")
        orig = "Hi Mike, saw your permit on 12 Oak St. We fund flips fast. Want to chat?"
        res = nl_revision.revise_draft(instruction="shorter", original=orig, current=orig, llm=llm)
        assert res.ok and res.text == "Hi Mike, saw your permit on 12 Oak St. Want to chat?"

    def test_prompt_carries_instruction_original_and_current_as_data(self):
        llm = MagicMock(return_value="x")
        nl_revision.revise_draft(instruction="shorter", original="ORIG", current="CURR", llm=llm)
        system, user = llm.call_args.args
        assert "ORIGINAL" in system.upper()
        assert "shorter" in user and "ORIG" in user and "CURR" in user

    def test_llm_exception_is_llm_error(self):
        llm = MagicMock(side_effect=RuntimeError("boom"))
        res = nl_revision.revise_draft(instruction="shorter", original="o", current="o", llm=llm)
        assert not res.ok and res.reason == "llm_error"

    @pytest.mark.parametrize("out", ["", "   ", "[BLOCKED] Vendor cost pause active"])
    def test_empty_or_blocked_output_is_llm_error(self, out):
        res = nl_revision.revise_draft(
            instruction="shorter", original="o", current="o", llm=MagicMock(return_value=out),
        )
        assert not res.ok and res.reason == "llm_error"


ORIGINAL = "Hi Mike, saw your permit on 12 Oak St. Your $250,000 flip looks strong. Want to chat?"


class TestEmbellishmentGuard:
    @pytest.mark.parametrize("revised", [
        "Hi Mike, saw your permit on 12 Oak St. Want to chat?",
        "Mike — your $250,000 flip at 12 Oak St looks strong. Chat?",
        "Hi Mike, saw your 12 Oak St permit. Your 250,000 flip looks strong.",
    ])
    def test_clean_rewrites_pass(self, revised):
        assert nl_revision.embellishment_guard(revised, ORIGINAL) is None

    @pytest.mark.parametrize("revised,needle", [
        ("Hi Mike, we close in 10 days. Want to chat?", "10"),
        ("Hi Mike, we can lend $300,000 on 12 Oak St.", "$300,000"),
        ("Hi Mike, 12 Oak St could return 20%.", "20%"),
        ("Hi Mike, your 250,000 flip is worth $250,000 plus $1.", "$1"),
        ("Hi Mike, 12 Oak St. Rates from 9.5%!", "9.5%"),
    ])
    def test_new_number_money_or_percent_is_refused(self, revised, needle):
        flag = nl_revision.embellishment_guard(revised, ORIGINAL)
        assert flag is not None and needle in flag

    def test_bare_number_does_not_become_new_money_or_percent(self):
        assert nl_revision.embellishment_guard("12% off at Oak St", ORIGINAL) is not None

    @pytest.mark.parametrize("word", [
        "rate", "APR", "points", "terms", "approved", "pre-approved",
        "guarantee", "guaranteed", "commit", "commitment",
    ])
    def test_new_rate_or_term_word_is_refused(self, word):
        flag = nl_revision.embellishment_guard(f"Hi Mike, {word} for 12 Oak St. Want to chat?", ORIGINAL)
        assert flag is not None and word.lower() in flag.lower()

    def test_rate_word_already_in_original_is_allowed(self):
        original = "Hi Mike, our rate sheet is attached. Want to chat?"
        assert nl_revision.embellishment_guard("Mike — rate sheet attached. Chat?", original) is None

    def test_word_boundary_not_substring(self):
        assert nl_revision.embellishment_guard("Hi Mike, a pirate ship at 12 Oak St?", ORIGINAL) is None

    def test_revise_draft_refuses_embellished_output(self):
        llm = MagicMock(return_value="Hi Mike, we guarantee funding in 5 days.")
        res = nl_revision.revise_draft(instruction="stronger hook", original=ORIGINAL, current=ORIGINAL, llm=llm)
        assert not res.ok and res.reason == "embellishment" and res.detail


# ── thread routing (precedence) ──────────────────────────────────────────────

def _thread_event(text: str, thread_ts: str = "111.1") -> dict:
    return {"event": {"type": "message", "text": text, "thread_ts": thread_ts, "user": "U1", "channel": "C1"}}


class TestThreadRouting:
    def _run(self, text, *, slot, thread_ts="111.1", item=None):
        from src.api import admin_router

        item = item or _item()
        with patch("src.services.relay.queue.get_item_by_slack_message_ts", return_value=item), \
             patch.object(admin_router, "_relay_approver_authorized", return_value=True), \
             patch.object(admin_router, "_get_pending_slot", return_value=slot), \
             patch.object(admin_router, "_clear_pending_slot") as clear, \
             patch.object(admin_router, "_apply_nl_revision") as apply_nl, \
             patch.object(admin_router, "_post_relay_thread_note") as note, \
             patch.object(admin_router, "_handle_relay_decision") as decide, \
             patch("src.services.relay.thread_fallback_responder.handle_thread_fallback_reply") as fallback:
            admin_router._handle_relay_thread_action(_thread_event(text, thread_ts))
        return dict(clear=clear, apply_nl=apply_nl, note=note, decide=decide, fallback=fallback)

    def test_slot_same_thread_routes_to_revision(self):
        m = self._run("shorter", slot=_slot())
        m["apply_nl"].assert_called_once()
        m["fallback"].assert_not_called()

    def test_no_slot_still_reaches_fallback(self):
        m = self._run("shorter", slot=None)
        m["apply_nl"].assert_not_called()
        m["fallback"].assert_called_once()

    def test_slot_for_other_thread_is_not_consumed(self):
        m = self._run("shorter", slot=_slot(thread_ts="999.9"))
        m["apply_nl"].assert_not_called()
        m["clear"].assert_not_called()
        m["fallback"].assert_called_once()

    def test_voice_slot_does_not_capture_text_reply(self):
        m = self._run("shorter", slot=_slot(kind="voice"))
        m["apply_nl"].assert_not_called()

    def test_cancel_clears_slot_without_revising(self):
        m = self._run("cancel", slot=_slot())
        m["clear"].assert_called_once()
        m["apply_nl"].assert_not_called()
        m["fallback"].assert_not_called()
        m["note"].assert_called_once()

    def test_approve_with_slot_open_clears_and_goes_to_command_path(self):
        m = self._run("approve", slot=_slot())
        m["clear"].assert_called_once()
        m["apply_nl"].assert_not_called()
        m["decide"].assert_called_once()

    def test_revision_runs_while_relay_kill_switch_halted(self):
        red = {"color": "red", "feature": "relay_global"}
        with patch("src.services.kill_switch_service.get_kill_switch_status", return_value=red):
            m = self._run("shorter", slot=_slot())
        m["apply_nl"].assert_called_once()

    def test_non_fa_max_card_ignores_slot(self):
        m = self._run("shorter", slot=_slot(), item=_item(venture_key="hillsborough_distress"))
        m["apply_nl"].assert_not_called()


# ── apply NL revision ────────────────────────────────────────────────────────

class TestApplyNlRevision:
    def test_success_goes_through_shared_revision_helper(self):
        from src.api import admin_router

        item = _item()
        with patch.object(admin_router, "_clear_pending_slot") as clear, \
             patch.object(admin_router.nl_revision, "revise_draft",
                          return_value=nl_revision.RevisionResult(ok=True, text="Hi Mike. Want to chat?")), \
             patch.object(admin_router, "_apply_draft_revision", return_value=(item, True, True)) as apply, \
             patch.object(admin_router, "_post_relay_thread_note"):
            admin_router._apply_nl_revision(item, "shorter", "U1")
        clear.assert_called_once_with("U1")
        assert apply.call_args.args[1] == "Hi Mike. Want to chat?"
        assert apply.call_args.kwargs["revised_by"] == "slack_nl:U1"

    def test_failure_writes_nothing_and_posts_note(self):
        from src.api import admin_router

        item = _item()
        with patch.object(admin_router, "_clear_pending_slot") as clear, \
             patch.object(admin_router.nl_revision, "revise_draft",
                          return_value=nl_revision.RevisionResult(ok=False, reason="llm_error")), \
             patch.object(admin_router, "_apply_draft_revision") as apply, \
             patch.object(admin_router, "_post_relay_thread_note") as note:
            admin_router._apply_nl_revision(item, "shorter", "U1")
        clear.assert_called_once()
        apply.assert_not_called()
        note.assert_called_once()

    def test_embellishment_refusal_leaves_row_unchanged_and_explains(self):
        from src.api import admin_router

        item = _item()
        with patch.object(admin_router, "_clear_pending_slot"), \
             patch.object(admin_router.nl_revision, "revise_draft",
                          return_value=nl_revision.RevisionResult(
                              ok=False, reason="embellishment", detail="introduced 10")), \
             patch("src.services.relay.queue.record_revision") as rec, \
             patch.object(admin_router, "_post_relay_thread_note") as note:
            admin_router._apply_nl_revision(item, "stronger hook", "U1")
        rec.assert_not_called()
        text = note.call_args.args[1]
        assert "Skipped" in text and "Edit text" in text

    def test_not_pending_item_is_refused(self):
        from src.api import admin_router

        item = _item(status="approved")
        with patch.object(admin_router, "_clear_pending_slot"), \
             patch.object(admin_router.nl_revision, "revise_draft") as revise, \
             patch.object(admin_router, "_post_relay_thread_note") as note:
            admin_router._apply_nl_revision(item, "shorter", "U1")
        revise.assert_not_called()
        note.assert_called_once()

    def test_uses_current_text_and_original_draft(self):
        from src.api import admin_router

        item = _item(final_content="CURRENT", original_draft="ORIGINAL")
        with patch.object(admin_router, "_clear_pending_slot"), \
             patch.object(admin_router.nl_revision, "revise_draft",
                          return_value=nl_revision.RevisionResult(ok=False, reason="llm_error")) as revise, \
             patch.object(admin_router, "_post_relay_thread_note"):
            admin_router._apply_nl_revision(item, "shorter", "U1")
        assert revise.call_args.kwargs["original"] == "ORIGINAL"
        assert revise.call_args.kwargs["current"] == "CURRENT"


# ── shared revision helper (modal + NL) ──────────────────────────────────────

class TestApplyDraftRevision:
    def _run(self, existing, new_text):
        from src.api import admin_router

        saved = _item(revision_count=1)
        with patch("src.services.relay.queue.record_revision", return_value=saved) as rec, \
             patch.object(admin_router, "_post_relay_thread_note"), \
             patch("src.services.relay.slack_post.refresh_card_after_revision", return_value=True):
            out = admin_router._apply_draft_revision(existing, new_text, revised_by="slack_nl:U1")
        return out, rec

    def test_material_edit_baseline_is_original_draft(self):
        existing = _item(original_draft="a b c d e f g h i j", final_content="totally different words here")
        (_, material, _), rec = self._run(existing, "a b c d e f g h i j")
        assert material is False
        assert rec.call_args.kwargs["material_edit"] is False

    def test_material_edit_is_sticky(self):
        existing = _item(original_draft="a b c d e f g h i j", material_edit=True)
        (_, material, _), _ = self._run(existing, "a b c d e f g h i j")
        assert material is True

    def test_not_pending_returns_none(self):
        from src.api import admin_router

        with patch("src.services.relay.queue.record_revision", return_value=None), \
             patch.object(admin_router, "_post_relay_thread_note") as note:
            item, _, _ = admin_router._apply_draft_revision(_item(), "x", revised_by="slack:U1")
        assert item is None
        note.assert_not_called()


# ── fallback responder hint ──────────────────────────────────────────────────

class TestFallbackReviseHint:
    def test_card_thread_other_ack_hints_revise(self):
        from src.services.relay.thread_fallback_responder import build_other_ack_text

        assert "tap Revise" in build_other_ack_text(on_card=True)

    def test_channel_other_ack_has_no_revise_hint(self):
        from src.services.relay.thread_fallback_responder import build_other_ack_text

        assert "Revise" not in build_other_ack_text()


# ── buttons ──────────────────────────────────────────────────────────────────

def _action_payload(action_id: str) -> dict:
    return {
        "type": "block_actions", "user": {"id": "U1"}, "trigger_id": "T1",
        "channel": {"id": "C1"},
        "actions": [{"action_id": action_id, "value": json.dumps({"item_id": 7, "action": "revise"})}],
    }


class TestButtons:
    def test_revise_button_opens_slot_not_modal(self):
        from src.api import admin_router

        with patch("src.services.relay.queue.get_item", return_value=_item()), \
             patch.object(admin_router, "_relay_approver_authorized", return_value=True), \
             patch.object(admin_router, "_open_pending_slot") as open_slot, \
             patch.object(admin_router, "_post_relay_thread_note") as note, \
             patch("src.services.relay.slack_post.open_revise_modal") as modal:
            admin_router._handle_relay_revise_open(_action_payload("fa_max_revise"))
        open_slot.assert_called_once()
        assert open_slot.call_args.kwargs["kind"] == "revise"
        assert open_slot.call_args.kwargs["thread_ts"] == "111.1"
        note.assert_called_once()
        modal.assert_not_called()

    def test_revise_button_unauthorized_opens_nothing(self):
        from src.api import admin_router

        with patch("src.services.relay.queue.get_item", return_value=_item()), \
             patch.object(admin_router, "_relay_approver_authorized", return_value=False), \
             patch.object(admin_router, "_open_pending_slot") as open_slot:
            admin_router._handle_relay_revise_open(_action_payload("fa_max_revise"))
        open_slot.assert_not_called()

    def test_edit_text_button_opens_modal(self):
        from src.api import admin_router

        with patch("src.services.relay.queue.get_item", return_value=_item()), \
             patch.object(admin_router, "_relay_approver_authorized", return_value=True), \
             patch("src.services.relay.slack_post.open_revise_modal") as modal:
            admin_router._handle_relay_edit_text_open(_action_payload("fa_max_edit_text"))
        modal.assert_called_once()

    def test_card_has_revise_and_edit_text_buttons(self):
        from src.services.relay.slack_post import _build_approval_blocks

        item = _item(recipient="x@example.com", channel="email", payload={"body": "b"})
        with patch("src.services.relay.slack_post._summary_text", return_value="s"):
            blocks = _build_approval_blocks(item)
        ids = {e["action_id"] for e in blocks[1]["elements"]}
        assert {"fa_max_revise", "fa_max_edit_text"} <= ids

    @pytest.mark.parametrize("action_id,handler", [
        ("fa_max_revise", "_handle_relay_revise_open"),
        ("fa_max_edit_text", "_handle_relay_edit_text_open"),
    ])
    def test_socket_mode_dispatches_revise_buttons(self, action_id, handler):
        from src.api import admin_router
        from src.services.relay import socket_listener

        request = MagicMock(type="interactive", envelope_id="E1", payload=_action_payload(action_id))
        with patch.object(admin_router, handler, return_value={}) as h:
            assert socket_listener.handle_socket_request(MagicMock(), request) is True
        h.assert_called_once()


# ── ticket 03: revision log (working memory) ─────────────────────────────────

def _queue_row(**over) -> dict:
    row = {
        "id": 7, "idempotency_key": "k", "channel": "email", "recipient": "a@b.com",
        "payload": {"body": "new"}, "thread_id": None, "status": "pending", "batch_id": None,
        "decided_by": None, "error": None, "dispatched_at": None, "created_at": None,
        "venture_key": "fa_max_lending", "lane": "MONEY", "agent_name": "cora",
        "autonomy_tier_at_send": "A", "person_id": "p1", "autonomy_gate_reason": None,
        "decision_interaction_id": None, "send_interaction_id": None,
        "slack_post_attempted_at": None, "slack_post_lease_until": None, "eligible_at": None,
        "original_draft": "orig", "final_content": "new", "revision_count": 3,
        "last_revised_by": "slack_nl:U1", "last_revised_at": None, "material_edit": True,
        "slack_message_ts": None, "decided_at": None,
    }
    row.update(over)
    return row


def _fake_ctx(session):
    ctx = MagicMock()
    ctx.return_value.__enter__ = MagicMock(return_value=session)
    ctx.return_value.__exit__ = MagicMock(return_value=False)
    return ctx


class TestRecordRevisionLog:
    def test_log_row_written_in_same_session_with_revision_no(self):
        from src.services.relay import queue as rq

        session = MagicMock()
        session.execute.return_value.mappings.return_value.first.return_value = _queue_row()
        with patch("src.services.relay.queue.get_db_context", _fake_ctx(session)):
            rq.record_revision(
                7, final_content="new", revised_by="slack_nl:U1", material_edit=True,
                log=rq.RevisionLogEntry(source="nl", before_text="old", instruction="shorter"),
            )
        assert session.execute.call_count == 2
        sql, params = session.execute.call_args.args
        assert "INSERT INTO fa_max_draft_revisions" in str(sql)
        assert params["revision_no"] == 3 and params["source"] == "nl"
        assert params["before_text"] == "old" and params["after_text"] == "new"
        assert params["instruction"] == "shorter" and params["material_edit"] is True

    def test_not_pending_writes_no_log(self):
        from src.services.relay import queue as rq

        session = MagicMock()
        session.execute.return_value.mappings.return_value.first.return_value = None
        with patch("src.services.relay.queue.get_db_context", _fake_ctx(session)):
            out = rq.record_revision(
                7, final_content="new", revised_by="slack:U1", material_edit=False,
                log=rq.RevisionLogEntry(source="modal", before_text="old"),
            )
        assert out is None and session.execute.call_count == 1

    def test_rejects_unknown_source(self):
        from src.services.relay import queue as rq

        with pytest.raises(ValueError):
            rq.RevisionLogEntry(source="bogus", before_text="x")


class TestRevisionHistory:
    def test_history_labels_nl_and_modal_in_order(self):
        from src.services.relay import queue as rq

        session = MagicMock()
        session.execute.return_value.mappings.return_value.all.return_value = [
            {"source": "nl", "instruction": "drop the address"},
            {"source": "modal", "instruction": None},
        ]
        with patch("src.services.relay.queue.get_db_context", _fake_ctx(session)):
            assert rq.get_revision_history(7) == ["drop the address", "(manual text edit)"]
        assert "ORDER BY revision_no" in str(session.execute.call_args.args[0])

    def test_prompt_includes_history(self):
        llm = MagicMock(return_value="x")
        nl_revision.revise_draft(
            instruction="put the address back", original="ORIG", current="CURR",
            history=["drop the address"], llm=llm,
        )
        assert "drop the address" in llm.call_args.args[1]


class TestApplyPassesLogAndHistory:
    def _apply(self, existing, **kwargs):
        from src.api import admin_router

        with patch("src.services.relay.queue.record_revision", return_value=_item(revision_count=1)) as rec, \
             patch.object(admin_router, "_post_relay_thread_note"), \
             patch("src.services.relay.slack_post.refresh_card_after_revision", return_value=True):
            admin_router._apply_draft_revision(existing, "NEW", **kwargs)
        return rec.call_args.kwargs["log"]

    def test_nl_revision_logs_instruction_and_before_text(self):
        log = self._apply(_item(final_content="CURRENT", original_draft="ORIGINAL"),
                          revised_by="slack_nl:U1", source="nl", instruction="shorter")
        assert (log.source, log.before_text, log.instruction) == ("nl", "CURRENT", "shorter")

    def test_modal_revision_logs_modal_source(self):
        log = self._apply(_item(final_content=None, original_draft="ORIGINAL"), revised_by="slack:U1")
        assert (log.source, log.before_text, log.instruction) == ("modal", "ORIGINAL", None)

    def _nl_history_seen(self, history_patch):
        from src.api import admin_router

        with patch.object(admin_router, "_clear_pending_slot"), \
             patch("src.services.relay.queue.get_revision_history", **history_patch), \
             patch.object(admin_router.nl_revision, "revise_draft",
                          return_value=nl_revision.RevisionResult(ok=False, reason="llm_error")) as revise, \
             patch.object(admin_router, "_post_relay_thread_note"):
            admin_router._apply_nl_revision(_item(), "put it back", "U1")
        return revise.call_args.kwargs["history"]

    def test_nl_revision_feeds_history_into_rewrite(self):
        assert self._nl_history_seen({"return_value": ["drop the address"]}) == ["drop the address"]

    def test_history_lookup_failure_degrades_to_empty(self):
        from sqlalchemy.exc import OperationalError

        assert self._nl_history_seen({"side_effect": OperationalError("x", {}, None)}) == []


# ── ticket 03: DB-backed (shared DB, self-cleaning) ──────────────────────────

class TestRevisionLogDb:
    def test_modal_then_nl_writes_two_ordered_log_rows(self):
        from sqlalchemy import text as sql
        from src.core.database import get_db_context
        from src.services.relay import queue as rq

        key = f"wp-t3-1-test-{datetime.now(timezone.utc).timestamp()}"
        with get_db_context() as s:
            item_id = s.execute(sql(
                "INSERT INTO relay_approval_queue "
                "(idempotency_key, channel, recipient, payload, original_draft) "
                "VALUES (:k, 'email', 'test@example.com', CAST(:p AS jsonb), 'orig') RETURNING id"
            ), {"k": key, "p": json.dumps({"body": "orig"})}).scalar_one()
        try:
            rq.record_revision(item_id, final_content="edited by hand", revised_by="slack:U1",
                               material_edit=True, log=rq.RevisionLogEntry(source="modal", before_text="orig"))
            rq.record_revision(item_id, final_content="shorter", revised_by="slack_nl:U1", material_edit=True,
                               log=rq.RevisionLogEntry(source="nl", before_text="edited by hand",
                                                       instruction="shorter"))
            with get_db_context() as s:
                rows = s.execute(sql(
                    "SELECT revision_no, source, instruction, before_text, after_text "
                    "FROM fa_max_draft_revisions WHERE relay_item_id = :id ORDER BY revision_no"
                ), {"id": item_id}).all()
            assert [tuple(r) for r in rows] == [
                (1, "modal", None, "orig", "edited by hand"),
                (2, "nl", "shorter", "edited by hand", "shorter"),
            ]
            assert rq.get_revision_history(item_id) == ["(manual text edit)", "shorter"]
        finally:
            with get_db_context() as s:
                s.execute(sql("DELETE FROM fa_max_draft_revisions WHERE relay_item_id = :id"), {"id": item_id})
                s.execute(sql("DELETE FROM relay_approval_queue WHERE id = :id"), {"id": item_id})


# ── instruction pre-check (live test: model silently declined to add facts) ──

_ORIG = "Hi Mike, I saw the cash purchase on 4512 Oak Street closed last month. Want to chat?"


class TestInstructionPreCheck:
    @pytest.mark.parametrize("instruction,needle", [
        ("add that we can close in 10 days at 80% LTV", "10"),
        ("mention the $5,000 fee", "$5,000"),
        ("say he's pre-approved", "pre-approved"),
        ("tell him our rate is great", "rate"),
        ("mention we charge 2 points", "2"),
    ])
    def test_new_fact_in_instruction_is_refused_before_llm(self, instruction, needle):
        llm = MagicMock()
        r = nl_revision.revise_draft(instruction=instruction, original=_ORIG, current=_ORIG, llm=llm)
        assert r.reason == "embellishment" and needle in r.detail and "asks for" in r.detail
        llm.assert_not_called()

    @pytest.mark.parametrize("instruction", [
        "shorter, drop the second sentence",
        "keep it under 50 words",
        "make it 2 sentences",
        "drop paragraph 2",
        "remove the 2nd sentence",
        "use 3 bullet points",
        "mention 4512 Oak Street first",
        "friendlier tone",
    ])
    def test_formatting_numbers_and_existing_facts_pass(self, instruction):
        llm = MagicMock(return_value="Hi Mike, saw your Oak Street purchase. Want to chat?")
        r = nl_revision.revise_draft(instruction=instruction, original=_ORIG, current=_ORIG, llm=llm)
        assert r.ok, r.detail
        llm.assert_called_once()


# ── Edit text modal save over Socket Mode ────────────────────────────────────

def _view_request(callback_id="fa_max_revise_submit"):
    payload = {"type": "view_submission", "user": {"id": "U1"},
               "view": {"callback_id": callback_id, "private_metadata": '{"item_id": 7}'}}
    return MagicMock(type="interactive", envelope_id="env-1", payload=payload)


class TestSocketModalSubmission:
    def _run(self, request, **handler_kw):
        from src.services.relay import socket_listener as sl

        client = MagicMock()
        with patch("src.api.admin_router._handle_relay_revise_submission", **handler_kw) as handler:
            handled = sl.handle_socket_request(client, request)
        acks = client.send_socket_mode_response.call_args_list
        return handled, handler, acks

    def test_submission_dispatched_and_result_sent_in_ack(self):
        handled, handler, acks = self._run(_view_request(), return_value={"response_action": "clear"})
        assert handled is True
        handler.assert_called_once()
        assert len(acks) == 1
        resp = acks[0].args[0]
        assert resp.envelope_id == "env-1" and resp.payload == {"response_action": "clear"}

    def test_validation_errors_reach_the_modal(self):
        errors = {"response_action": "errors", "errors": {"revised_content_block": "Item is no longer pending."}}
        _, _, acks = self._run(_view_request(), return_value=errors)
        assert acks[0].args[0].payload == errors

    def test_handler_crash_shows_error_instead_of_silent_close(self):
        _, _, acks = self._run(_view_request(), side_effect=RuntimeError("db down"))
        assert len(acks) == 1
        assert acks[0].args[0].payload["response_action"] == "errors"

    def test_other_modals_are_acked_empty_without_revise_handler(self):
        handled, handler, acks = self._run(_view_request(callback_id="something_else"))
        assert handled is True  # dev's view_submission block acks and owns every modal envelope
        handler.assert_not_called()
        assert len(acks) == 1 and not acks[0].args[0].payload
