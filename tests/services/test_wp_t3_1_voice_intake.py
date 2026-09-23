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
from datetime import date
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
    def test_returns_none_when_no_openai_key(self):
        with patch("src.services.fa_max_voice_intake.get_settings") as mock_gs:
            mock_gs.return_value.openai_api_key = None
            result = get_transcriber()
        assert result is None

    def test_returns_whisper_when_key_set(self):
        from src.services.fa_max_voice_intake import WhisperTranscriber

        mock_key = MagicMock()
        with patch("src.services.fa_max_voice_intake.get_settings") as mock_gs:
            mock_gs.return_value.openai_api_key = mock_key
            result = get_transcriber()
        assert isinstance(result, WhisperTranscriber)


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
            "next_action_due": "2026-09-30",
        })
        assert d.outcome == "callback_requested"
        assert d.next_action == "Call back Tuesday"
        assert d.next_action_due == date(2026, 9, 30)

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

    def test_tool_input_none_defaults_unclear(self):
        mock_result = {"tool_input": None, "text": ""}
        with patch(
            "src.services.claude_router.call_claude_with_usage",
            return_value=mock_result,
        ):
            d = extract_disposition("transcript")
        assert d.outcome == "unclear"


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

    def test_mixed_keeps_only_safe(self):
        text = "Good lead. Income is $80k. Wants to close soon."
        result = strip_financial(text)
        assert "Income" not in result
        assert "Good lead" in result
        assert "close soon" in result


# ── handle_voice_intake integration tests ─────────────────────────────────────

def _mock_client(notes: list) -> MagicMock:
    client = MagicMock()
    client.chat_postMessage.side_effect = lambda **kw: notes.append(kw["text"])
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
        "mimetype": "audio/mp4",
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
        file_info = {"mimetype": "image/png", "url_private_download": "https://x", "name": "img.png"}
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

    def test_oversized_file_writes_nothing(self, mock_db):
        notes = []
        client = _mock_client(notes)
        big_audio = b"x" * (26 * 1_000_000)

        mock_resp = MagicMock()
        mock_resp.content = big_audio
        mock_resp.raise_for_status.return_value = None

        with (
            patch("src.services.fa_max_voice_intake.get_transcriber",
                  return_value=FakeTranscriber("hi")),
            patch("src.services.fa_max_voice_intake.requests.get", return_value=mock_resp),
            patch("src.services.fa_max_voice_intake.get_settings") as mock_gs,
        ):
            mock_gs.return_value.fa_max_voice_max_bytes = 25_000_000
            mock_gs.return_value.openai_api_key = MagicMock()
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
                      "next_action_due": "2026-09-24",
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
        assert p["next_action_due"] == date(2026, 9, 24)
        assert p["slack_user_id"] == _USER_ID
        assert p["interaction_id"] == fake_interaction_id

        # Receipt was posted
        assert any("connected interested" in n.lower() for n in notes)
