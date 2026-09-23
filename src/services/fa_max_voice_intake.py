"""FA Max voice-note intake (WP-T3-1 §4).

Josh taps 🎙 Log call on a dial-list card, then sends a Slack voice clip.
The pipeline: download → transcribe → extract disposition → guard → persist → receipt.
Audio bytes and transcripts are never stored; only their lengths are logged.
"""
from __future__ import annotations

import importlib.util
import io
import logging
import re
import threading
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Protocol, runtime_checkable

import requests
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.settings import get_settings

logger = logging.getLogger(__name__)

VOICE_OUTCOMES = frozenset({
    "connected_interested",
    "connected_not_interested",
    "callback_requested",
    "voicemail",
    "no_answer",
    "wrong_number",
    "unclear",
})

# Sentences matching these terms are stripped from summary/next_action.
# Word boundaries prevent partial matches (e.g. "pirate" does not match "rate").
_FINANCIAL_TERMS = re.compile(
    r"\b(credit|score|fico|income|salary|bank\s+statement|tax\s+return|ssn"
    r"|social\s+security|w-?2|rate|apr)\b",
    re.IGNORECASE,
)


def is_voice_file(file_info: dict) -> bool:
    """Slack voice clips carry subtype 'slack_audio' and may report video/mp4."""
    return (
        str(file_info.get("mimetype") or "").startswith("audio/")
        or file_info.get("subtype") == "slack_audio"
    )


# ── Transcriber protocol ──────────────────────────────────────────────────────

@runtime_checkable
class Transcriber(Protocol):
    def transcribe(self, audio: bytes, filename: str, mimetype: str) -> str: ...


class FakeTranscriber:
    def __init__(self, fixed_text: str) -> None:
        self._text = fixed_text

    def transcribe(self, audio: bytes, filename: str, mimetype: str) -> str:
        return self._text


class WhisperTranscriber:
    def transcribe(self, audio: bytes, filename: str, mimetype: str) -> str:
        settings = get_settings()
        key = settings.openai_api_key
        if key is None:
            raise RuntimeError("openai_api_key unset")
        model = settings.fa_max_whisper_model
        resp = requests.post(
            "https://api.openai.com/v1/audio/transcriptions",
            headers={"Authorization": f"Bearer {key.get_secret_value()}"},
            files={"file": (filename, audio, mimetype)},
            data={"model": model},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json().get("text", "")


# Biases decoding toward lending vocabulary (small.en heard "rehab" as "Rahab").
_LOCAL_HINT_PROMPT = (
    "Call notes from a private lender: rehab, flip, fix and flip, DSCR, bridge loan, "
    "Backflip, cash purchase, permit, contractor, draw, closing, callback."
)


class FasterWhisperTranscriber:
    """Self-hosted Whisper via faster-whisper (CPU, int8). Audio is decoded in
    memory and never written to disk.

    One model per process, loaded on first use. Calls are serialized: the
    Socket Mode listener runs up to 25 worker threads, and parallel decodes on
    a 4-vCPU prod box would each run slower and multiply RAM.
    """

    _model = None
    _lock = threading.Lock()

    def __init__(self, model_name: str, cpu_threads: int) -> None:
        self._model_name = model_name
        self._cpu_threads = cpu_threads

    def _get_model(self):
        cls = type(self)
        if cls._model is None:
            from faster_whisper import WhisperModel

            logger.info("[VoiceIntake] loading local whisper model %s", self._model_name)
            cls._model = WhisperModel(
                self._model_name, device="cpu", compute_type="int8",
                cpu_threads=self._cpu_threads,
            )
        return cls._model

    def transcribe(self, audio: bytes, filename: str, mimetype: str) -> str:
        with type(self)._lock:
            model = self._get_model()
            segments, _info = model.transcribe(
                io.BytesIO(audio), beam_size=1, initial_prompt=_LOCAL_HINT_PROMPT,
            )
            return " ".join(s.text.strip() for s in segments).strip()


def get_transcriber() -> Optional[Transcriber]:
    """Return the configured transcriber, or None when it can't run here."""
    settings = get_settings()
    if settings.fa_max_transcriber == "local":
        if importlib.util.find_spec("faster_whisper") is None:
            logger.warning("[VoiceIntake] fa_max_transcriber=local but faster-whisper not installed")
            return None
        return FasterWhisperTranscriber(
            settings.fa_max_local_whisper_model, settings.fa_max_local_whisper_threads,
        )
    if settings.openai_api_key is None:
        return None
    return WhisperTranscriber()


# ── Disposition extraction ────────────────────────────────────────────────────

@dataclass(frozen=True)
class Disposition:
    outcome: str
    summary: str
    next_action: Optional[str]
    next_action_due: Optional[date]


_LOCAL_TZ = ZoneInfo("America/Detroit")
_MAX_DUE_AHEAD = timedelta(days=366)


def extract_disposition(
    transcript: str, db: Optional[Session] = None, *, today: Optional[date] = None,
) -> Disposition:
    """Extract a structured call disposition from a transcript via Claude tool_use."""
    from src.services.claude_router import call_claude_with_usage

    # The model has no clock: without today's date it resolves "next Tuesday"
    # against its training data (a live test produced a date a year in the past).
    today = today or datetime.now(_LOCAL_TZ).date()

    tool = {
        "name": "record_disposition",
        "description": "Record the outcome of a sales call.",
        "input_schema": {
            "type": "object",
            "properties": {
                "outcome": {
                    "type": "string",
                    "enum": sorted(VOICE_OUTCOMES),
                    "description": "Call outcome.",
                },
                "summary": {
                    "type": "string",
                    "description": "Call summary, max 280 chars.",
                },
                "next_action": {
                    "type": "string",
                    "description": "Next step, max 140 chars. Omit if none.",
                },
                "next_action_due": {
                    "type": "string",
                    "description": (
                        "ISO date (YYYY-MM-DD) the next action is due, resolved against "
                        "today's date. Omit if the caller gave no day or date."
                    ),
                },
            },
            "required": ["outcome", "summary"],
        },
    }
    result = call_claude_with_usage(
        task_type="fa_max_voice_disposition",
        messages=[{"role": "user", "content": f"DATA:\n{transcript}"}],
        system=(
            "You extract call dispositions from sales call transcripts. "
            "Never infer borrower financial data (rates, income, credit). "
            "Use only what the caller said about the conversation outcome. "
            f"Today is {today:%A, %Y-%m-%d}; resolve relative days like 'next Tuesday' "
            "against today."
        ),
        tools=[tool],
        db=db,
    )
    inp = result.get("tool_input")
    if not inp:
        # Forced tool_choice always yields tool_input; None means the call was
        # blocked (vendor-cost pause) — never record that as an 'unclear' call.
        raise RuntimeError("disposition extraction returned no tool_input")
    outcome = inp.get("outcome", "unclear")
    if outcome not in VOICE_OUTCOMES:
        outcome = "unclear"
    summary = str(inp.get("summary") or "")[:280]
    next_action_raw = inp.get("next_action")
    next_action = str(next_action_raw)[:140] if next_action_raw else None
    due_raw = inp.get("next_action_due")
    next_action_due: Optional[date] = None
    if due_raw:
        try:
            next_action_due = date.fromisoformat(str(due_raw))
        except ValueError:
            pass
    if next_action_due and not (today <= next_action_due <= today + _MAX_DUE_AHEAD):
        logger.warning("[VoiceIntake] dropped implausible next_action_due (%s days from today)",
                       (next_action_due - today).days)
        next_action_due = None
    return Disposition(
        outcome=outcome,
        summary=summary,
        next_action=next_action,
        next_action_due=next_action_due,
    )


# ── Financial guard ───────────────────────────────────────────────────────────

def strip_financial(txt: str) -> str:
    """Remove sentences containing financial data terms.

    Word-boundary matching prevents false positives (e.g. "pirate" ≠ "rate").
    """
    sentences = re.split(r"(?<=[.!?])\s+", txt.strip())
    clean = [s for s in sentences if not _FINANCIAL_TERMS.search(s)]
    return " ".join(clean)


# ── Full pipeline ─────────────────────────────────────────────────────────────

def handle_voice_intake(
    *,
    session: Session,
    slack_user_id: str,
    opportunity_id: str,
    file_info: dict,
    bot_token: str,
    slack_client: object,
    channel_id: str,
    thread_ts: Optional[str] = None,
) -> None:
    """Run the full voice intake pipeline for one audio file.

    Downloads, transcribes, extracts disposition, guards, persists, receipts.
    Never raises — all failures post a thread note and return.
    """
    from src.services.fa_max_pending_slot import clear_slot
    from src.services.state_engine import write_interaction

    settings = get_settings()

    def _note(msg: str) -> None:
        _post_note(slack_client, channel_id=channel_id, thread_ts=thread_ts, text=msg)

    def _refuse(msg: str) -> None:
        _note(msg)
        clear_slot(session, slack_user_id)
        session.commit()

    # 1. Download (files.info for the authoritative url/size/mimetype)
    try:
        file_info = {**file_info, **slack_client.files_info(file=file_info["id"])["file"]}  # type: ignore[attr-defined]
    except Exception as exc:
        logger.warning("[VoiceIntake] files.info failed: %s", type(exc).__name__)
        _refuse("Couldn't retrieve the audio file — try again.")
        return

    mimetype = str(file_info.get("mimetype") or "")
    if not is_voice_file(file_info):
        _refuse("Voice clips only — please send an audio file.")
        return

    max_mb = settings.fa_max_voice_max_bytes // 1_000_000
    if int(file_info.get("size") or 0) > settings.fa_max_voice_max_bytes:
        _refuse(f"Audio file is too large (max {max_mb} MB).")
        return

    url = file_info.get("url_private_download") or file_info.get("url_private", "")
    if not url:
        _refuse("Couldn't retrieve the audio file.")
        return

    try:
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {bot_token}"},
            timeout=60,
        )
        resp.raise_for_status()
        audio = resp.content
    except Exception as exc:
        logger.warning("[VoiceIntake] download failed: %s", type(exc).__name__)
        _refuse("Couldn't download the audio — try again.")
        return

    if len(audio) > settings.fa_max_voice_max_bytes:
        _refuse(f"Audio file is too large (max {max_mb} MB).")
        return

    logger.info("[VoiceIntake] downloaded %d bytes, mimetype=%s", len(audio), mimetype)

    # 2. Transcribe
    transcriber = get_transcriber()
    if transcriber is None:
        _refuse("Voice intake not configured.")
        return

    filename = file_info.get("name") or "audio.m4a"
    try:
        transcript = transcriber.transcribe(audio, filename, mimetype)
    except Exception as exc:
        logger.warning("[VoiceIntake] transcription failed: %s", type(exc).__name__)
        # Slot kept (SPEC §4.3) so Josh can resend within the TTL.
        _note("Couldn't transcribe — try again.")
        return

    logger.info("[VoiceIntake] transcript length=%d", len(transcript))

    # 3. Extract disposition
    try:
        disposition = extract_disposition(transcript, db=session)
    except Exception as exc:
        logger.warning("[VoiceIntake] extract_disposition failed: %s", type(exc).__name__)
        _refuse("Couldn't process the call — try again.")
        return

    # 4. Guard — strip any sentences with financial data terms
    summary = strip_financial(disposition.summary)
    next_action = strip_financial(disposition.next_action) if disposition.next_action else None

    # 5. Persist
    person_id = _resolve_person_id(session, opportunity_id)
    if person_id is None:
        _refuse("No person linked to this opportunity — can't record the call.")
        return

    try:
        interaction_id = write_interaction(
            session=session,
            person_id=person_id,
            channel="voice",
            direction="outbound",
            actor="josh",
            body_redacted=f"call disposition: {disposition.outcome}",
            agent_name="voice_intake",
        )
        session.execute(
            text("""
                INSERT INTO fa_max_call_dispositions
                    (interaction_id, person_id, opportunity_id, outcome,
                     summary, next_action, next_action_due, slack_user_id)
                VALUES
                    (:interaction_id, :person_id, :opportunity_id, :outcome,
                     :summary, :next_action, :next_action_due, :slack_user_id)
            """),
            {
                "interaction_id": interaction_id,
                "person_id": person_id,
                "opportunity_id": opportunity_id,
                "outcome": disposition.outcome,
                "summary": summary,
                "next_action": next_action,
                "next_action_due": disposition.next_action_due,
                "slack_user_id": slack_user_id,
            },
        )
        clear_slot(session, slack_user_id)
        session.commit()
    except Exception as exc:
        logger.error("[VoiceIntake] persist failed: %s", type(exc).__name__)
        session.rollback()
        _note("Couldn't save the call record — try again.")
        return

    # 6. Receipt
    outcome_label = disposition.outcome.replace("_", " ").title()
    lines = [f":white_check_mark: *{outcome_label}*", summary]
    if next_action:
        due = f" (due {disposition.next_action_due})" if disposition.next_action_due else ""
        lines.append(f"_Next: {next_action}{due}_")
    if disposition.outcome == "unclear":
        lines.append("_I couldn't tell how the call went._")
    _note("\n".join(lines))


def _resolve_person_id(session: Session, opportunity_id: str) -> Optional[str]:
    row = session.execute(
        text("SELECT person_id FROM fa_max_opportunities WHERE opportunity_id = :oid"),
        {"oid": opportunity_id},
    ).mappings().first()
    return str(row["person_id"]) if row else None


def _post_note(
    client: object, *, channel_id: str, thread_ts: Optional[str], text: str
) -> None:
    try:
        kwargs: dict = {"channel": channel_id, "text": text}
        if thread_ts:
            kwargs["thread_ts"] = thread_ts
        client.chat_postMessage(**kwargs)  # type: ignore[attr-defined]
    except Exception as exc:
        logger.warning("[VoiceIntake] note post failed: %s", exc)
