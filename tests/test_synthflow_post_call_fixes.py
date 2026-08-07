"""
Tests for two post-call webhook fixes:

Fix 3: outcome tags applied to newly-created GHL contacts
  - Previously _create_prospect_contact ran but _apply_tags_to_contact
    was never called for new contacts — outcome tags were silently lost.

Fix 4: outbound webhook dedup
  - Synthflow retries on network errors; duplicate call_ids must be
    rejected before process_call_outcome fires a second time.
"""
from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

# ── stub out modules that need real env to import ───────────────────────────
for _mod in ["config.agents", "src.services.ghl_webhook", "stripe"]:
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()


# ────────────────────────────────────────────────────────────────────────────
# Fix 3 — outcome tags on new contacts
# ────────────────────────────────────────────────────────────────────────────

def _make_db_ctx_session():
    mock_session = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = lambda s: mock_session
    mock_ctx.__exit__ = MagicMock(return_value=False)
    return mock_ctx, mock_session


class TestOutcomeTagsOnNewContact:
    """process_call_outcome must call _apply_tags_to_contact for new contacts."""

    def _run(self, outcome: str, contact_id_returned="ghl-new-001"):
        mock_ctx, _ = _make_db_ctx_session()

        with (
            patch("src.services.synthflow_service._find_ghl_contact_by_phone", return_value=None),
            patch("src.services.synthflow_service._create_prospect_contact", return_value=contact_id_returned),
            patch("src.services.synthflow_service._apply_tags_to_contact") as mock_apply,
            patch("src.services.synthflow_service.get_settings"),
            # local import inside process_call_outcome: patch at source
            patch("src.core.database.get_db_context", return_value=mock_ctx),
        ):
            from src.services.synthflow_service import process_call_outcome
            result = process_call_outcome(
                prospect_phone="+18135550101",
                outcome=outcome,
                vertical="roofing",
                zip_code="33602",
                prospect_name="Test User",
            )
            return result, mock_apply

    def test_sample_requested_tags_applied_to_new_contact(self):
        result, mock_apply = self._run("sample_requested")

        assert result["created"] is True
        mock_apply.assert_called_once()
        _, tags_arg = mock_apply.call_args[0]
        assert "sample_leads_requested" in tags_arg
        assert "synthflow-called" in tags_arg

    def test_demo_requested_tags_applied_to_new_contact(self):
        result, mock_apply = self._run("demo_requested")

        assert result["created"] is True
        mock_apply.assert_called_once()
        _, tags_arg = mock_apply.call_args[0]
        assert "demo_requested" in tags_arg

    def test_voicemail_tags_applied_to_new_contact(self):
        result, mock_apply = self._run("voicemail")

        assert result["created"] is True
        mock_apply.assert_called_once()
        _, tags_arg = mock_apply.call_args[0]
        assert "synthflow-voicemail" in tags_arg

    def test_tags_not_applied_when_create_returns_none(self):
        """If GHL contact creation fails (returns None), don't call apply_tags."""
        result, mock_apply = self._run("sample_requested", contact_id_returned=None)

        assert result["created"] is True
        mock_apply.assert_not_called()

    def test_existing_contact_still_gets_tags(self):
        """Control: existing contact path unchanged."""
        mock_ctx, _ = _make_db_ctx_session()

        with (
            patch("src.services.synthflow_service._find_ghl_contact_by_phone", return_value="ghl-existing-999"),
            patch("src.services.synthflow_service._create_prospect_contact") as mock_create,
            patch("src.services.synthflow_service._apply_tags_to_contact") as mock_apply,
            patch("src.services.synthflow_service.get_settings"),
            patch("src.core.database.get_db_context", return_value=mock_ctx),
        ):
            from src.services.synthflow_service import process_call_outcome
            result = process_call_outcome(
                prospect_phone="+18135550101",
                outcome="not_interested",
                vertical="roofing",
                zip_code="33602",
            )

        assert result["created"] is False
        mock_create.assert_not_called()
        mock_apply.assert_called_once()
        _, tags_arg = mock_apply.call_args[0]
        assert "not_interested" in tags_arg


# ────────────────────────────────────────────────────────────────────────────
# Fix 4 — outbound webhook dedup
# ────────────────────────────────────────────────────────────────────────────

class TestOutboundWebhookDedup:
    """POST /webhooks/synthflow must return duplicate and skip processing on repeated call_id."""

    def _webhook_payload(self, call_id="call-abc-123", outcome="voicemail"):
        return {
            "call_id": call_id,
            "phone": "+18135550101",
            "outcome": outcome,
            "vertical": "roofing",
            "zip_code": "33602",
        }

    def _mock_process_outcome(self):
        m = MagicMock(return_value={
            "contact_id": "ghl-001",
            "tags_applied": ["synthflow-called", "synthflow-voicemail"],
            "created": False,
            "sms_sent": None,
        })
        return m

    def test_first_call_processes_normally(self):
        """First occurrence of a call_id goes through process_call_outcome."""
        dedup_session = MagicMock()
        dedup_session.execute.return_value.fetchone.return_value = None  # not seen before
        dedup_ctx = MagicMock()
        dedup_ctx.__enter__ = lambda s: dedup_session
        dedup_ctx.__exit__ = MagicMock(return_value=False)

        mock_process = self._mock_process_outcome()

        with (
            patch("src.core.database.get_db_context", return_value=dedup_ctx),
            patch("src.services.synthflow_service.process_call_outcome", mock_process),
            patch("src.services.webhook_log.log_webhook_event"),
        ):
            from fastapi.testclient import TestClient
            from src.api.main import app
            client = TestClient(app)
            resp = client.post("/webhooks/synthflow", json=self._webhook_payload())

        assert resp.status_code == 200
        assert resp.json().get("status") != "duplicate"
        mock_process.assert_called_once()

    def test_duplicate_call_id_returns_duplicate_status(self):
        """Second call with same call_id returns {"status":"duplicate"} without processing."""
        dedup_session = MagicMock()
        dedup_session.execute.return_value.fetchone.return_value = MagicMock()  # already seen
        dedup_ctx = MagicMock()
        dedup_ctx.__enter__ = lambda s: dedup_session
        dedup_ctx.__exit__ = MagicMock(return_value=False)

        mock_process = self._mock_process_outcome()

        with (
            patch("src.core.database.get_db_context", return_value=dedup_ctx),
            patch("src.services.synthflow_service.process_call_outcome", mock_process),
            patch("src.services.webhook_log.log_webhook_event"),
        ):
            from fastapi.testclient import TestClient
            from src.api.main import app
            client = TestClient(app)
            resp = client.post("/webhooks/synthflow", json=self._webhook_payload())

        assert resp.status_code == 200
        assert resp.json() == {"status": "duplicate"}
        mock_process.assert_not_called()

    def test_no_call_id_skips_dedup_and_processes(self):
        """Payloads without call_id skip dedup check — still processed."""
        payload = self._webhook_payload()
        del payload["call_id"]

        mock_process = self._mock_process_outcome()

        with (
            patch("src.core.database.get_db_context") as mock_ctx_fn,
            patch("src.services.synthflow_service.process_call_outcome", mock_process),
            patch("src.services.webhook_log.log_webhook_event"),
        ):
            from fastapi.testclient import TestClient
            from src.api.main import app
            client = TestClient(app)
            resp = client.post("/webhooks/synthflow", json=payload)

        assert resp.status_code == 200
        # dedup DB ctx should NOT have been opened (no call_id)
        mock_ctx_fn.assert_not_called()
        mock_process.assert_called_once()


# ────────────────────────────────────────────────────────────────────────────
# Fix 5 — transcript / recording / duration persistence
# ────────────────────────────────────────────────────────────────────────────

class TestTranscriptPersistence:
    """process_call_outcome must write transcript_text, recording_url and
    duration_seconds onto the SynthflowCall row when provided."""

    def test_transcript_fields_saved_on_call_row(self):
        mock_ctx, mock_session = _make_db_ctx_session()

        with (
            patch("src.services.synthflow_service._find_ghl_contact_by_phone", return_value="ghl-1"),
            patch("src.services.synthflow_service._apply_tags_to_contact"),
            patch("src.services.synthflow_service.get_settings"),
            patch("src.core.database.get_db_context", return_value=mock_ctx),
        ):
            from src.services.synthflow_service import process_call_outcome
            process_call_outcome(
                prospect_phone="+18135550101",
                outcome="voicemail",
                vertical="roofing",
                zip_code="33602",
                call_id="call-tx-001",
                transcript_text="Agent: Hi... Prospect: not interested.",
                recording_url="https://synthflow.example/rec/abc.mp3",
                duration_seconds=113,
            )

        # Find the SynthflowCall object added to the session
        added = [c.args[0] for c in mock_session.add.call_args_list]
        synth_rows = [o for o in added if type(o).__name__ == "SynthflowCall"]
        assert synth_rows, "no SynthflowCall row added"
        row = synth_rows[0]
        assert row.transcript_text == "Agent: Hi... Prospect: not interested."
        assert row.recording_url == "https://synthflow.example/rec/abc.mp3"
        assert row.duration_seconds == 113

    def test_transcript_fields_null_when_absent(self):
        """Backward compat — omitting the fields saves NULLs, no error."""
        mock_ctx, mock_session = _make_db_ctx_session()

        with (
            patch("src.services.synthflow_service._find_ghl_contact_by_phone", return_value="ghl-1"),
            patch("src.services.synthflow_service._apply_tags_to_contact"),
            patch("src.services.synthflow_service.get_settings"),
            patch("src.core.database.get_db_context", return_value=mock_ctx),
        ):
            from src.services.synthflow_service import process_call_outcome
            process_call_outcome(
                prospect_phone="+18135550101",
                outcome="no_answer",
                vertical="roofing",
                zip_code="33602",
                call_id="call-tx-002",
            )

        added = [c.args[0] for c in mock_session.add.call_args_list]
        synth_rows = [o for o in added if type(o).__name__ == "SynthflowCall"]
        assert synth_rows
        row = synth_rows[0]
        assert row.transcript_text is None
        assert row.recording_url is None
        assert row.duration_seconds is None


class TestWebhookPayloadResolvers:
    """SynthflowWebhookPayload must extract transcript/recording/duration from
    both the nested Finetuner `call` object and flat legacy fields."""

    def test_nested_finetuner_shape(self):
        from src.api.main import SynthflowWebhookPayload
        p = SynthflowWebhookPayload(
            call={
                "call_id": "c1",
                "transcript": "full convo text",
                "recording_url": "https://rec/x.mp3",
                "duration": 87,
            }
        )
        assert p.resolved_transcript == "full convo text"
        assert p.resolved_recording_url == "https://rec/x.mp3"
        assert p.resolved_duration == 87

    def test_flat_legacy_shape(self):
        from src.api.main import SynthflowWebhookPayload
        p = SynthflowWebhookPayload(
            recording_url="https://rec/y.mp3",
            duration=42,
            notes="short transcript snippet",
        )
        assert p.resolved_recording_url == "https://rec/y.mp3"
        assert p.resolved_duration == 42
        assert p.resolved_transcript == "short transcript snippet"

    def test_recording_short_url_fallback(self):
        from src.api.main import SynthflowWebhookPayload
        p = SynthflowWebhookPayload(call={"recording_short_url": "https://rec/short"})
        assert p.resolved_recording_url == "https://rec/short"

    def test_duration_non_numeric_returns_none(self):
        from src.api.main import SynthflowWebhookPayload
        p = SynthflowWebhookPayload(call={"duration": "not-a-number"})
        assert p.resolved_duration is None
