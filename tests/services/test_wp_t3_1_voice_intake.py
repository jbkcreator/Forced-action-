"""Unit tests for WP-T3-1 voice intake (ticket 04 + 05).

Tests:
  - FakeTranscriber returns seeded text
  - get_transcriber() returns None when openai_api_key unset
  - extract_disposition() with mocked call_claude_with_usage
  - strip_financial() word-boundary guard and multi-sentence behaviour
  - handle_voice_intake() happy path (DB test — writes interaction + disposition)
  - handle_voice_intake() no-slot path (writes nothing)
  - handle_voice_intake() no-person path (writes nothing)
  - handle_voice_intake() oversized file (refused)
  - handle_voice_intake() non-audio mimetype (refused)
  - handle_voice_intake() transcription error (slot NOT cleared)
  - handle_voice_intake() unclear outcome (row still written, receipt flags it)
"""
from __future__ import annotations

import uuid
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from src.services.fa_max_voice_intake import (
    Disposition,
    FakeTranscriber,
    extract_disposition,
    get_transcriber,
    handle_voice_intake,
    strip_financial,
)


# Due dates must be within [today, today+366d] or extraction drops them.
_DUE = date.today() + timedelta(days=6)
_DUE_ISO = _DUE.isoformat()


# ── Transcriber ───────────────────────────────────────────────────────────────

class TestFakeTranscriber:
    def test_returns_seeded_text(self):
        t = FakeTranscriber("talked to Mike, callback Tuesday")
        result = t.transcribe(b"audio", "clip.m4a", "audio/mp4")
        assert result == "talked to Mike, callback Tuesday"

    def test_ignores_audio_bytes_and_filename(self):
        t = FakeTranscriber("hello")
        assert t.transcribe(b"\x00\x01", "other.ogg", "audio/ogg") == "hello"


class TestGetTranscriber:
    def test_openai_mode_returns_none_when_no_openai_key(self):
        with patch("src.services.fa_max_voice_intake.get_settings") as mock_gs:
            mock_gs.return_value.fa_max_transcriber = "openai"
            mock_gs.return_value.openai_api_key = None
            result = get_transcriber()
        assert result is None

    def test_openai_mode_returns_whisper_when_key_set(self):
        from src.services.fa_max_voice_intake import WhisperTranscriber

        with patch("src.services.fa_max_voice_intake.get_settings") as mock_gs:
            mock_gs.return_value.fa_max_transcriber = "openai"
            mock_gs.return_value.openai_api_key = MagicMock()
            result = get_transcriber()
        assert isinstance(result, WhisperTranscriber)

    def test_local_mode_needs_no_api_key(self):
        from src.services.fa_max_voice_intake import FasterWhisperTranscriber

        with patch("src.services.fa_max_voice_intake.get_settings") as mock_gs,              patch("src.services.fa_max_voice_intake.importlib.util.find_spec", return_value=object()):
            mock_gs.return_value.fa_max_transcriber = "local"
            mock_gs.return_value.openai_api_key = None
            mock_gs.return_value.fa_max_local_whisper_model = "small.en"
            mock_gs.return_value.fa_max_local_whisper_threads = 2
            result = get_transcriber()
        assert isinstance(result, FasterWhisperTranscriber)

    def test_local_mode_without_package_is_not_configured(self):
        with patch("src.services.fa_max_voice_intake.get_settings") as mock_gs,              patch("src.services.fa_max_voice_intake.importlib.util.find_spec", return_value=None):
            mock_gs.return_value.fa_max_transcriber = "local"
            assert get_transcriber() is None

    def test_default_setting_is_local_small_en(self):
        from config.settings import AppSettings

        fields = AppSettings.model_fields
        assert fields["fa_max_transcriber"].default == "local"
        assert fields["fa_max_local_whisper_model"].default == "small.en"
        assert fields["fa_max_local_whisper_threads"].default == 2


# ── extract_disposition ───────────────────────────────────────────────────────

class TestExtractDisposition:
    def _call(self, tool_input: dict) -> Disposition:
        mock_result = {"tool_input": tool_input, "text": ""}
        with patch(
            "src.services.claude_router.call_claude_with_usage",
            return_value=mock_result,
        ):
            return extract_disposition("talked to Mike, he's interested, call back Tuesday")

    def test_connected_interested(self):
        d = self._call({"outcome": "connected_interested", "summary": "Spoke with Mike."})
        assert d.outcome == "connected_interested"
        assert d.summary == "Spoke with Mike."
        assert d.next_action is None
        assert d.next_action_due is None

    def test_callback_with_date(self):
        d = self._call({
            "outcome": "callback_requested",
            "summary": "Wants callback.",
            "next_action": "Call back Tuesday",
            "next_action_due": _DUE_ISO,
        })
        assert d.outcome == "callback_requested"
        assert d.next_action == "Call back Tuesday"
        assert d.next_action_due == _DUE

    def test_invalid_outcome_defaults_to_unclear(self):
        d = self._call({"outcome": "bogus_outcome", "summary": "Some text."})
        assert d.outcome == "unclear"

    def test_summary_truncated_to_280(self):
        long = "x" * 400
        d = self._call({"outcome": "voicemail", "summary": long})
        assert len(d.summary) == 280

    def test_bad_date_ignored(self):
        d = self._call({
            "outcome": "voicemail",
            "summary": "Left voicemail.",
            "next_action_due": "not-a-date",
        })
        assert d.next_action_due is None

    @pytest.mark.parametrize("text", ["", "[BLOCKED] Vendor cost pause active"])
    def test_no_tool_input_raises_not_unclear(self, text):
        with patch(
            "src.services.claude_router.call_claude_with_usage",
            return_value={"tool_input": None, "text": text},
        ), pytest.raises(RuntimeError):
            extract_disposition("transcript")


# ── strip_financial ───────────────────────────────────────────────────────────

class TestStripFinancial:
    def test_removes_credit_score_sentence(self):
        result = strip_financial("He has a credit score of 720. He wants to close fast.")
        assert "credit score" not in result
        assert "close fast" in result

    def test_removes_fico_sentence(self):
        result = strip_financial("His FICO is 680. Great candidate.")
        assert "FICO" not in result.upper()
        assert "Great candidate" in result

    def test_removes_rate_sentence(self):
        result = strip_financial("Discussed rate options. Very interested.")
        assert "rate" not in result.lower()
        assert "Very interested" in result

    def test_keeps_clean_sentence(self):
        result = strip_financial("Has two flips in March. Wants to close by April.")
        assert result == "Has two flips in March. Wants to close by April."

    def test_pirate_not_matched_by_rate(self):
        result = strip_financial("The pirate left a voicemail. Discuss rate next call.")
        # "pirate" should NOT be stripped; "rate" sentence should be
        assert "pirate" in result
        assert "rate next call" not in result

    def test_empty_string(self):
        assert strip_financial("") == ""

    def test_removes_apr_sentence(self):
        result = strip_financial("He asked about APR. Good lead.")
        assert "APR" not in result
        assert "Good lead" in result

    def test_spec_example_credit_score_720_removed(self):
        assert strip_financial("credit score 720") == ""

    @pytest.mark.parametrize("sentence", [
        "His credit is shaky.", "Score came back fine.", "Sent his W-2 over.",
        "Asked for the bank statement.", "Salary is steady.",
    ])
    def test_each_spec_term_is_stripped(self, sentence):
        assert strip_financial(f"{sentence} Call Tuesday.") == "Call Tuesday."

    def test_mixed_keeps_only_safe(self):
        text = "Good lead. Income is $80k. Wants to close soon."
        result = strip_financial(text)
        assert "Income" not in result
        assert "Good lead" in result
        assert "close soon" in result


# ── handle_voice_intake integration tests ─────────────────────────────────────

def _mock_client(notes: list, file_meta: dict | None = None) -> MagicMock:
    client = MagicMock()
    client.chat_postMessage.side_effect = lambda **kw: notes.append(kw["text"])
    client.files_info.return_value = {"file": file_meta or {}}
    return client


def _fake_download(audio_bytes: bytes = b"fake_audio"):
    """Patch requests.get so file download returns audio_bytes."""
    mock_resp = MagicMock()
    mock_resp.content = audio_bytes
    mock_resp.raise_for_status.return_value = None
    return mock_resp


_OPP_ID = str(uuid.uuid4())
_PERSON_ID = str(uuid.uuid4())
_USER_ID = "U_JOSH"


def _base_file_info() -> dict:
    return {
        "id": "F123",
        "mimetype": "audio/mp4",
        "size": 1024,
        "url_private_download": "https://files.slack.com/fake.m4a",
        "name": "voice.m4a",
    }


def _run_pipeline(session, *, file_info=None, transcriber_text="Spoke with Mike, interested.",
                  outcome="connected_interested", summary="Good call.", next_action=None,
                  next_action_due=None, notes=None):
    """Helper — wires FakeTranscriber + mock LLM + mock download, runs handle_voice_intake."""
    if file_info is None:
        file_info = _base_file_info()
    if notes is None:
        notes = []
    client = _mock_client(notes)

    fake_disp = {"outcome": outcome, "summary": summary}
    if next_action:
        fake_disp["next_action"] = next_action
    if next_action_due:
        fake_disp["next_action_due"] = next_action_due

    with (
        patch("src.services.fa_max_voice_intake.get_transcriber",
              return_value=FakeTranscriber(transcriber_text)),
        patch("src.services.claude_router.call_claude_with_usage",
              return_value={"tool_input": fake_disp, "text": ""}),
        patch("src.services.fa_max_voice_intake.requests.get",
              return_value=_fake_download()),
        patch("src.services.fa_max_voice_intake._resolve_person_id",
              return_value=_PERSON_ID),
    ):
        handle_voice_intake(
            session=session,
            slack_user_id=_USER_ID,
            opportunity_id=_OPP_ID,
            file_info=file_info,
            bot_token="xoxb-fake",
            slack_client=client,
            channel_id="C_MONEY",
        )
    return notes


class TestHandleVoiceIntakeUnit:
    """Pure-unit tests: no real DB (use mock_db fixture)."""

    def test_non_audio_mimetype_writes_nothing(self, mock_db):
        notes = []
        client = _mock_client(notes)
        file_info = {"id": "F1", "mimetype": "image/png", "url_private_download": "https://x", "name": "img.png"}
        with patch("src.services.fa_max_voice_intake.get_transcriber",
                   return_value=FakeTranscriber("x")):
            handle_voice_intake(
                session=mock_db,
                slack_user_id=_USER_ID,
                opportunity_id=_OPP_ID,
                file_info=file_info,
                bot_token="tok",
                slack_client=client,
                channel_id="C",
            )
        assert any("audio file" in n for n in notes)

    def test_oversized_file_refused_before_download(self, mock_db):
        notes = []
        client = _mock_client(notes, file_meta={"size": 26_000_000})

        with (
            patch("src.services.fa_max_voice_intake.get_transcriber",
                  return_value=FakeTranscriber("hi")),
            patch("src.services.fa_max_voice_intake.requests.get") as mock_get,
            patch("src.services.state_engine.write_interaction") as mock_wi,
        ):
            handle_voice_intake(
                session=mock_db,
                slack_user_id=_USER_ID,
                opportunity_id=_OPP_ID,
                file_info=_base_file_info(),
                bot_token="tok",
                slack_client=client,
                channel_id="C",
            )
        assert any("too large" in n for n in notes)
        mock_get.assert_not_called()
        mock_wi.assert_not_called()

    def test_no_openai_key_writes_nothing(self, mock_db):
        notes = []
        client = _mock_client(notes)

        with (
            patch("src.services.fa_max_voice_intake.get_transcriber", return_value=None),
            patch("src.services.fa_max_voice_intake.requests.get",
                  return_value=_fake_download()),
        ):
            handle_voice_intake(
                session=mock_db,
                slack_user_id=_USER_ID,
                opportunity_id=_OPP_ID,
                file_info=_base_file_info(),
                bot_token="tok",
                slack_client=client,
                channel_id="C",
            )
        assert any("not configured" in n for n in notes)

    def test_transcription_error_slot_not_cleared(self, mock_db):
        notes = []
        client = _mock_client(notes)

        failing_transcriber = MagicMock()
        failing_transcriber.transcribe.side_effect = RuntimeError("Whisper down")

        with (
            patch("src.services.fa_max_voice_intake.get_transcriber",
                  return_value=failing_transcriber),
            patch("src.services.fa_max_voice_intake.requests.get",
                  return_value=_fake_download()),
            patch("src.services.fa_max_pending_slot.clear_slot") as mock_clear,
        ):
            handle_voice_intake(
                session=mock_db,
                slack_user_id=_USER_ID,
                opportunity_id=_OPP_ID,
                file_info=_base_file_info(),
                bot_token="tok",
                slack_client=client,
                channel_id="C",
            )
        # Slot must NOT be cleared on transcription error (SPEC §4.3)
        mock_clear.assert_not_called()
        assert any("transcribe" in n.lower() for n in notes)

    def test_no_person_writes_nothing(self, mock_db):
        notes = []
        client = _mock_client(notes)

        with (
            patch("src.services.fa_max_voice_intake.get_transcriber",
                  return_value=FakeTranscriber("transcript")),
            patch("src.services.claude_router.call_claude_with_usage",
                  return_value={"tool_input": {"outcome": "voicemail", "summary": "VM."}, "text": ""}),
            patch("src.services.fa_max_voice_intake.requests.get",
                  return_value=_fake_download()),
            patch("src.services.fa_max_voice_intake._resolve_person_id", return_value=None),
        ):
            handle_voice_intake(
                session=mock_db,
                slack_user_id=_USER_ID,
                opportunity_id=_OPP_ID,
                file_info=_base_file_info(),
                bot_token="tok",
                slack_client=client,
                channel_id="C",
            )
        assert any("No person" in n for n in notes)

    def test_unclear_outcome_receipt_flags_it(self, mock_db):
        notes = []
        client = _mock_client(notes)
        mock_db.execute.return_value = MagicMock()

        with (
            patch("src.services.fa_max_voice_intake.get_transcriber",
                  return_value=FakeTranscriber("unclear call")),
            patch("src.services.claude_router.call_claude_with_usage",
                  return_value={"tool_input": {"outcome": "unclear", "summary": "Couldn't tell."}, "text": ""}),
            patch("src.services.fa_max_voice_intake.requests.get",
                  return_value=_fake_download()),
            patch("src.services.fa_max_voice_intake._resolve_person_id",
                  return_value=_PERSON_ID),
            patch("src.services.state_engine.write_interaction",
                  return_value=str(uuid.uuid4())),
        ):
            handle_voice_intake(
                session=mock_db,
                slack_user_id=_USER_ID,
                opportunity_id=_OPP_ID,
                file_info=_base_file_info(),
                bot_token="tok",
                slack_client=client,
                channel_id="C",
            )
        receipt = "\n".join(notes)
        assert "couldn't tell how the call went" in receipt.lower()


class TestHandleVoiceIntakeHappyPath:
    """Unit-level happy-path test verifying both write_interaction and the INSERT are called."""

    def test_writes_interaction_and_disposition(self, mock_db):
        """FakeTranscriber + mock LLM → write_interaction called + INSERT executed."""
        notes: list = []
        client = _mock_client(notes)
        fake_interaction_id = str(uuid.uuid4())
        inserted_params: list = []

        # Capture the INSERT params
        def _fake_execute(stmt, params=None, *args, **kwargs):
            if params and "interaction_id" in params:
                inserted_params.append(params)
            return MagicMock()

        mock_db.execute.side_effect = _fake_execute

        with (
            patch("src.services.fa_max_voice_intake.get_transcriber",
                  return_value=FakeTranscriber("Talked to Mike, very interested, call Thursday.")),
            patch("src.services.claude_router.call_claude_with_usage",
                  return_value={"tool_input": {
                      "outcome": "connected_interested",
                      "summary": "Good lead, interested.",
                      "next_action": "Call Thursday",
                      "next_action_due": _DUE_ISO,
                  }, "text": ""}),
            patch("src.services.fa_max_voice_intake.requests.get",
                  return_value=_fake_download()),
            patch("src.services.fa_max_voice_intake._resolve_person_id",
                  return_value=_PERSON_ID),
            patch("src.services.state_engine.write_interaction",
                  return_value=fake_interaction_id) as mock_wi,
        ):
            handle_voice_intake(
                session=mock_db,
                slack_user_id=_USER_ID,
                opportunity_id=_OPP_ID,
                file_info=_base_file_info(),
                bot_token="xoxb-fake",
                slack_client=client,
                channel_id="C_MONEY",
            )

        # write_interaction was called with voice channel
        mock_wi.assert_called_once()
        call_kwargs = mock_wi.call_args.kwargs
        assert call_kwargs["channel"] == "voice"
        assert call_kwargs["actor"] == "josh"
        assert "connected_interested" in call_kwargs["body_redacted"]

        # INSERT into fa_max_call_dispositions was executed
        assert len(inserted_params) == 1
        p = inserted_params[0]
        assert p["outcome"] == "connected_interested"
        assert p["summary"] == "Good lead, interested."
        assert p["next_action"] == "Call Thursday"
        assert p["next_action_due"] == _DUE
        assert p["slack_user_id"] == _USER_ID
        assert p["interaction_id"] == fake_interaction_id

        # Receipt was posted
        assert any("connected interested" in n.lower() for n in notes)


# ── review fixes: clip types, files.info, blocked LLM, logs, kill switch ─────

_TRANSCRIPT = "Talked to Mike Rivera, interested, call back Tuesday."
_AUDIO = b"SECRET_AUDIO_BYTES"
_DISP = {"outcome": "callback_requested", "summary": "Mike wants a callback.",
         "next_action": "Call Mike", "next_action_due": _DUE_ISO}


def _patched_pipeline(session, *, client, file_info=None, llm=None, person=_PERSON_ID, extra=()):
    from contextlib import ExitStack

    with ExitStack() as st:
        st.enter_context(patch("src.services.fa_max_voice_intake.get_transcriber",
                               return_value=FakeTranscriber(_TRANSCRIPT)))
        st.enter_context(patch("src.services.claude_router.call_claude_with_usage",
                               return_value=llm or {"tool_input": _DISP, "text": ""}))
        st.enter_context(patch("src.services.fa_max_voice_intake.requests.get",
                               return_value=_fake_download(_AUDIO)))
        st.enter_context(patch("src.services.fa_max_voice_intake._resolve_person_id",
                               return_value=person))
        mocks = [st.enter_context(p) for p in extra]
        handle_voice_intake(
            session=session, slack_user_id=_USER_ID, opportunity_id=_OPP_ID,
            file_info=file_info or _base_file_info(), bot_token="xoxb-fake",
            slack_client=client, channel_id="C_MONEY",
        )
        return mocks


class TestVoiceFileDetection:
    @pytest.mark.parametrize("info,ok", [
        ({"mimetype": "audio/webm"}, True),
        ({"mimetype": "video/mp4", "subtype": "slack_audio"}, True),
        ({"mimetype": "video/mp4"}, False),
        ({"mimetype": "image/png"}, False),
        ({}, False),
    ])
    def test_is_voice_file(self, info, ok):
        from src.services.fa_max_voice_intake import is_voice_file
        assert is_voice_file(info) is ok

    def test_slack_voice_clip_reported_as_video_mp4_is_processed(self, mock_db):
        notes: list = []
        client = _mock_client(notes, file_meta={"mimetype": "video/mp4", "subtype": "slack_audio"})
        (wi,) = _patched_pipeline(mock_db, client=client, extra=[
            patch("src.services.state_engine.write_interaction", return_value=str(uuid.uuid4()))])
        wi.assert_called_once()
        assert not any("Voice clips only" in n for n in notes)


class TestFailureBranchesWriteNothing:
    def test_files_info_failure_refuses(self, mock_db):
        notes: list = []
        client = _mock_client(notes)
        client.files_info.side_effect = RuntimeError("missing_scope")
        (wi, get) = _patched_pipeline(mock_db, client=client, extra=[
            patch("src.services.state_engine.write_interaction"),
            patch("src.services.fa_max_voice_intake.requests.get")])
        wi.assert_not_called()
        get.assert_not_called()
        assert any("Couldn't retrieve" in n for n in notes)

    def test_blocked_llm_writes_no_unclear_row(self, mock_db):
        notes: list = []
        (wi,) = _patched_pipeline(
            mock_db, client=_mock_client(notes),
            llm={"tool_input": None, "text": "[BLOCKED] Vendor cost pause active"},
            extra=[patch("src.services.state_engine.write_interaction")])
        wi.assert_not_called()
        assert any("Couldn't process" in n for n in notes)

    def test_no_person_does_not_call_write_interaction(self, mock_db):
        notes: list = []
        (wi,) = _patched_pipeline(mock_db, client=_mock_client(notes), person=None,
                                  extra=[patch("src.services.state_engine.write_interaction")])
        wi.assert_not_called()


class TestVoiceNeverTouchesOutcomes:
    def test_record_dial_disposition_not_called(self, mock_db):
        (_, rdd) = _patched_pipeline(mock_db, client=_mock_client([]), extra=[
            patch("src.services.state_engine.write_interaction", return_value=str(uuid.uuid4())),
            patch("src.services.dial_list.disposition.record_dial_disposition")])
        rdd.assert_not_called()


class TestNoAudioOrTranscriptInLogs:
    def test_logs_carry_lengths_only(self, mock_db, caplog):
        import logging
        caplog.set_level(logging.DEBUG)
        _patched_pipeline(mock_db, client=_mock_client([]), extra=[
            patch("src.services.state_engine.write_interaction", return_value=str(uuid.uuid4()))])
        assert "Mike Rivera" not in caplog.text
        assert "SECRET_AUDIO_BYTES" not in caplog.text
        assert f"downloaded {len(_AUDIO)} bytes" in caplog.text
        assert f"transcript length={len(_TRANSCRIPT)}" in caplog.text

    def test_transcription_error_does_not_log_exception_text(self, mock_db, caplog):
        import logging
        caplog.set_level(logging.DEBUG)
        bad = MagicMock()
        bad.transcribe.side_effect = RuntimeError("upstream echoed: Mike Rivera")
        with patch("src.services.fa_max_voice_intake.get_transcriber", return_value=bad), \
             patch("src.services.fa_max_voice_intake.requests.get", return_value=_fake_download()):
            handle_voice_intake(session=mock_db, slack_user_id=_USER_ID, opportunity_id=_OPP_ID,
                                file_info=_base_file_info(), bot_token="t",
                                slack_client=_mock_client([]), channel_id="C")
        assert "Mike Rivera" not in caplog.text


class TestKillSwitchHalted:
    def test_voice_intake_runs_while_relay_halted(self, mock_db):
        red = {"color": "red", "feature": "relay_global"}
        (wi, _) = _patched_pipeline(mock_db, client=_mock_client([]), extra=[
            patch("src.services.state_engine.write_interaction", return_value=str(uuid.uuid4())),
            patch("src.services.kill_switch_service.get_kill_switch_status", return_value=red)])
        wi.assert_called_once()


# ── Log call button (dial-list actions) ──────────────────────────────────────

def _log_call_payload(user="U_JOSH") -> dict:
    import json
    return {
        "user": {"id": user},
        "channel": {"id": "C_DIAL"},
        "actions": [{"action_id": "dial_log_call", "block_id": "dial_act:42",
                     "value": json.dumps({"thread": _OPP_ID, "property_id": 42, "as_of": "2026-09-23"})}],
        "message": {"ts": "1.1", "blocks": []},
    }


class TestLogCallButton:
    def test_button_on_card(self):
        from src.services.dial_list.delivery import ACTION_LOG_CALL, _actions_block
        from tests.services.test_dial_list_actions import _AS_OF, _entry
        ids = [e["action_id"] for e in _actions_block(_entry(), _AS_OF)["elements"]]
        assert ACTION_LOG_CALL in ids

    def test_approver_opens_voice_slot_and_gets_ephemeral(self):
        from src.services.dial_list.actions import handle_action
        session, client = MagicMock(), MagicMock()
        with patch("src.services.fa_max_pending_slot.open_slot") as open_slot:
            res = handle_action(_log_call_payload(), session, approver_id="U_JOSH", client=client)
        assert res.status == "touched" and res.kind == "voice"
        kw = open_slot.call_args.kwargs
        assert (kw["kind"], kw["target_ref"], kw["slack_user_id"], kw["channel_id"]) == \
            ("voice", _OPP_ID, "U_JOSH", "C_DIAL")
        session.commit.assert_called_once()
        assert "voice note" in client.chat_postEphemeral.call_args.kwargs["text"]

    def test_non_approver_opens_nothing(self):
        from src.services.dial_list.actions import handle_action
        with patch("src.services.fa_max_pending_slot.open_slot") as open_slot:
            res = handle_action(_log_call_payload(user="U_OTHER"), MagicMock(), approver_id="U_JOSH")
        assert res.status == "ignored"
        open_slot.assert_not_called()

    def test_log_call_writes_no_outcome(self):
        from src.services.dial_list.actions import handle_action
        with patch("src.services.fa_max_pending_slot.open_slot"), \
             patch("src.services.dial_list.actions.record_dial_disposition") as rdd:
            handle_action(_log_call_payload(), MagicMock(), approver_id="U_JOSH")
        rdd.assert_not_called()


# ── socket routing (audio event) ─────────────────────────────────────────────

def _file_share_payload(mimetype="audio/webm", channel="C_DIAL", user="U_JOSH", **file_extra) -> dict:
    return {"type": "event_callback", "event": {
        "type": "message", "subtype": "file_share", "user": user, "channel": channel,
        "files": [{"id": "F1", "mimetype": mimetype, **file_extra}]}}


class TestSocketVoiceRouting:
    def _run(self, payload, *, slot=None, channel_ok=True, approver=True):
        from contextlib import contextmanager
        from src.services.relay import socket_listener as sl

        @contextmanager
        def _ctx():
            yield MagicMock()

        client = MagicMock()
        with patch.object(sl, "_is_fa_max_voice_channel", return_value=channel_ok), \
             patch.object(sl, "_relay_approver_authorized_for_voice", return_value=approver), \
             patch("src.core.database.get_db_context", _ctx), \
             patch("src.services.fa_max_pending_slot.get_slot", return_value=slot), \
             patch("src.services.fa_max_voice_intake.handle_voice_intake") as pipeline:
            sl._handle_voice_file_share(client, payload)
        return client, pipeline

    def _slot(self, kind="voice"):
        from datetime import datetime, timezone
        from src.services.fa_max_pending_slot import PendingSlot
        return PendingSlot(slack_user_id="U_JOSH", kind=kind, target_ref=_OPP_ID,
                           channel_id="C_DIAL", thread_ts=None, set_at=datetime.now(timezone.utc))

    def test_live_voice_slot_runs_pipeline_on_its_opportunity(self):
        _, pipeline = self._run(_file_share_payload(), slot=self._slot())
        assert pipeline.call_args.kwargs["opportunity_id"] == _OPP_ID

    def test_no_slot_posts_hint_and_skips_pipeline(self):
        client, pipeline = self._run(_file_share_payload(), slot=None)
        pipeline.assert_not_called()
        assert "Log call" in client.web_client.chat_postMessage.call_args.kwargs["text"]

    def test_revise_slot_does_not_capture_audio(self):
        client, pipeline = self._run(_file_share_payload(), slot=self._slot(kind="revise"))
        pipeline.assert_not_called()
        client.web_client.chat_postMessage.assert_called_once()

    @pytest.mark.parametrize("kw", [{"channel_ok": False}, {"approver": False}])
    def test_non_fa_channel_or_non_approver_is_silent(self, kw):
        client, pipeline = self._run(_file_share_payload(), slot=self._slot(), **kw)
        pipeline.assert_not_called()
        client.web_client.chat_postMessage.assert_not_called()

    def test_non_audio_file_is_silent(self):
        client, pipeline = self._run(_file_share_payload(mimetype="image/png"), slot=self._slot())
        pipeline.assert_not_called()
        client.web_client.chat_postMessage.assert_not_called()

    def test_file_share_bypasses_thread_action_handler(self):
        from src.services.relay import socket_listener as sl
        req = MagicMock(type="events_api", envelope_id="e1", payload=_file_share_payload())
        with patch.object(sl, "_handle_voice_file_share") as voice, \
             patch("src.api.admin_router._handle_relay_thread_action") as thread_action:
            sl.handle_socket_request(MagicMock(), req)
        voice.assert_called_once()
        thread_action.assert_not_called()


# ── shared-DB test (outer transaction always rolled back) ────────────────────

class TestVoiceIntakeDb:
    """fa_max_interactions is immutable (no DELETE), so this test never commits:
    the pipeline's commits become savepoints inside a rolled-back transaction."""

    def _session(self):
        from sqlalchemy.orm import Session
        from src.core.database import db
        conn = db._engine.connect()
        outer = conn.begin()
        return conn, outer, Session(bind=conn, join_transaction_mode="create_savepoint")

    def _seed(self, s):
        from sqlalchemy import text as sql
        person = s.execute(sql("INSERT INTO fa_max_persons (source) VALUES ('wp_t3_1_test') "
                               "RETURNING person_id::text")).scalar_one()
        opp = s.execute(sql("INSERT INTO fa_max_opportunities (person_id, opportunity_type, source) "
                            "VALUES (CAST(:p AS uuid), 'acquisition', 'wp_t3_1_test') "
                            "RETURNING opportunity_id::text"), {"p": person}).scalar_one()
        return person, opp

    def test_voice_note_writes_interaction_and_disposition_only(self):
        from sqlalchemy import text as sql
        conn, outer, s = self._session()
        try:
            person, opp = self._seed(s)
            notes: list = []
            with patch("src.services.fa_max_voice_intake.get_transcriber",
                       return_value=FakeTranscriber(_TRANSCRIPT)), \
                 patch("src.services.claude_router.call_claude_with_usage",
                       return_value={"tool_input": _DISP, "text": ""}), \
                 patch("src.services.fa_max_voice_intake.requests.get",
                       return_value=_fake_download(_AUDIO)), \
                 patch("src.services.dial_list.disposition.record_dial_disposition") as rdd:
                handle_voice_intake(session=s, slack_user_id="U_WP_T3_1_TEST", opportunity_id=opp,
                                    file_info=_base_file_info(), bot_token="t",
                                    slack_client=_mock_client(notes), channel_id="C")
            inter = s.execute(sql("SELECT interaction_id::text, channel, direction, body_redacted "
                                  "FROM fa_max_interactions WHERE person_id = CAST(:p AS uuid)"),
                              {"p": person}).all()
            disp = s.execute(sql("SELECT interaction_id::text, opportunity_id::text, outcome, summary, "
                                 "next_action, next_action_due FROM fa_max_call_dispositions "
                                 "WHERE person_id = CAST(:p AS uuid)"), {"p": person}).all()
            assert len(inter) == 1 and len(disp) == 1
            assert tuple(inter[0][1:]) == ("voice", "outbound", "call disposition: callback_requested")
            assert disp[0][0] == inter[0][0] and disp[0][1] == opp
            assert disp[0][2] == "callback_requested" and disp[0][5] == _DUE
            stored = " ".join(str(v) for row in inter + disp for v in row)
            assert "Mike Rivera" not in stored and "SECRET_AUDIO" not in stored
            rdd.assert_not_called()
            assert any("Callback Requested" in n for n in notes)
        finally:
            s.close()
            outer.rollback()
            conn.close()

    def test_no_slot_audio_writes_zero_rows(self):
        from contextlib import contextmanager
        from sqlalchemy import text as sql
        from src.services.relay import socket_listener as sl
        conn, outer, s = self._session()
        try:
            def counts():
                return tuple(s.execute(sql(f"SELECT count(*) FROM {t}")).scalar_one()
                             for t in ("fa_max_interactions", "fa_max_call_dispositions"))
            before = counts()

            @contextmanager
            def _ctx():
                yield s

            client = MagicMock()
            with patch.object(sl, "_is_fa_max_voice_channel", return_value=True), \
                 patch.object(sl, "_relay_approver_authorized_for_voice", return_value=True), \
                 patch("src.core.database.get_db_context", _ctx):
                sl._handle_voice_file_share(client, _file_share_payload(user="U_WP_T3_1_NOSLOT"))
            assert counts() == before
            assert "Log call" in client.web_client.chat_postMessage.call_args.kwargs["text"]
        finally:
            s.close()
            outer.rollback()
            conn.close()


# ── self-hosted faster-whisper transcriber ───────────────────────────────────

class _FakeSegment:
    def __init__(self, text: str) -> None:
        self.text = text


class _FakeWhisperModel:
    instances = 0
    calls: list = []

    def __init__(self, name, device, compute_type, cpu_threads):
        type(self).instances += 1
        self.args = (name, device, compute_type, cpu_threads)

    def transcribe(self, audio, **kwargs):
        type(self).calls.append((audio, kwargs))
        return iter([_FakeSegment(" Talked to Mike. "), _FakeSegment(" Call back Tuesday. ")]), None


@pytest.fixture
def fake_faster_whisper(monkeypatch):
    import sys
    import types
    from src.services.fa_max_voice_intake import FasterWhisperTranscriber

    _FakeWhisperModel.instances = 0
    _FakeWhisperModel.calls = []
    monkeypatch.setitem(sys.modules, "faster_whisper",
                        types.SimpleNamespace(WhisperModel=_FakeWhisperModel))
    monkeypatch.setattr(FasterWhisperTranscriber, "_model", None)
    yield _FakeWhisperModel


class TestFasterWhisperTranscriber:
    def test_decodes_bytes_in_memory_with_domain_hint(self, fake_faster_whisper):
        import io
        from src.services.fa_max_voice_intake import FasterWhisperTranscriber

        text = FasterWhisperTranscriber("small.en", 2).transcribe(b"AUDIO", "v.m4a", "audio/mp4")
        assert text == "Talked to Mike. Call back Tuesday."
        audio, kwargs = fake_faster_whisper.calls[0]
        assert isinstance(audio, io.BytesIO) and audio.getvalue() == b"AUDIO"
        assert kwargs["beam_size"] == 1
        assert "rehab" in kwargs["initial_prompt"]

    def test_model_loaded_once_on_cpu_int8(self, fake_faster_whisper):
        from src.services.fa_max_voice_intake import FasterWhisperTranscriber

        FasterWhisperTranscriber("small.en", 2).transcribe(b"a", "a.m4a", "audio/mp4")
        FasterWhisperTranscriber("small.en", 2).transcribe(b"b", "b.m4a", "audio/mp4")
        assert fake_faster_whisper.instances == 1
        assert FasterWhisperTranscriber._model.args == ("small.en", "cpu", "int8", 2)

    def test_concurrent_calls_are_serialized(self, fake_faster_whisper, monkeypatch):
        import threading
        import time
        from src.services.fa_max_voice_intake import FasterWhisperTranscriber

        active, peak = [0], [0]
        guard = threading.Lock()

        def slow_transcribe(self, audio, **kwargs):
            with guard:
                active[0] += 1
                peak[0] = max(peak[0], active[0])
            time.sleep(0.05)
            with guard:
                active[0] -= 1
            return iter([_FakeSegment("ok")]), None

        monkeypatch.setattr(_FakeWhisperModel, "transcribe", slow_transcribe)
        t = FasterWhisperTranscriber("small.en", 2)
        threads = [threading.Thread(target=t.transcribe, args=(b"x", "x.m4a", "audio/mp4"))
                   for _ in range(4)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        assert peak[0] == 1

    def test_real_model_opt_in(self):
        """Loads the real model. Opt in: FA_MAX_REAL_WHISPER_SAMPLE=<path to audio>."""
        import importlib.util
        import os
        from src.services.fa_max_voice_intake import FasterWhisperTranscriber

        sample = os.environ.get("FA_MAX_REAL_WHISPER_SAMPLE")
        if not sample or importlib.util.find_spec("faster_whisper") is None:
            pytest.skip("set FA_MAX_REAL_WHISPER_SAMPLE and install faster-whisper")
        with open(sample, "rb") as fh:
            text = FasterWhisperTranscriber("small.en", 2).transcribe(fh.read(), sample, "audio/mpeg")
        assert len(text.split()) > 5


class TestDialListTapsOnFaMaxSocket:
    """deliver_dial_list posts cards with the FA Max bot, so Slack sends their
    button taps to the FA Max socket, which must dispatch them."""

    @pytest.mark.parametrize("action_id", ["dial_log_call", "dial_called", "dial_won", "dial_lost", "dial_skip"])
    def test_dial_actions_reach_dial_list_handler(self, action_id):
        from contextlib import contextmanager
        from src.services.relay import socket_listener as sl

        @contextmanager
        def _ctx():
            yield MagicMock()

        payload = {"type": "block_actions", "user": {"id": "U_JOSH"},
                   "actions": [{"action_id": action_id, "value": "{}"}]}
        req = MagicMock(type="interactive", envelope_id="e1", payload=payload)
        client = MagicMock()
        with patch("src.core.database.get_db_context", _ctx), \
             patch("src.services.dial_list.actions.handle_action") as handle, \
             patch("src.api.admin_router._handle_relay_decision") as relay_decision:
            assert sl.handle_socket_request(client, req) is True
        handle.assert_called_once()
        assert handle.call_args.kwargs["client"] is client.web_client
        relay_decision.assert_not_called()


class TestDueDateResolution:
    def _extract(self, due, today=date(2026, 9, 23)):
        with patch("src.services.claude_router.call_claude_with_usage",
                   return_value={"tool_input": {"outcome": "callback_requested", "summary": "x",
                                                "next_action": "Call", "next_action_due": due},
                                 "text": ""}) as llm:
            return extract_disposition("call back next Tuesday", today=today), llm

    def test_prompt_carries_today(self):
        _, llm = self._extract("2026-09-29")
        assert "Today is Wednesday, 2026-09-23" in llm.call_args.kwargs["system"]

    def test_next_tuesday_kept(self):
        d, _ = self._extract("2026-09-29")
        assert d.next_action_due == date(2026, 9, 29)

    def test_past_date_dropped(self):
        d, _ = self._extract("2025-07-22")
        assert d.next_action_due is None and d.next_action == "Call"

    def test_more_than_a_year_out_dropped(self):
        d, _ = self._extract("2027-12-01")
        assert d.next_action_due is None
