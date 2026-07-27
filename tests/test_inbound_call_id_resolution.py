"""TDD for the code-review fix on Block 11 PR #165, finding #3:

call_id/event_id/id are optional on the Synthflow inbound payload. Without a
fallback, a missing id meant (a) inbound_response.decision_id was NULL and
could never reconcile (SQL never joins NULL to NULL), and (b) the webhook
idempotency check was skipped entirely, letting retries enqueue duplicate
hot-inbound callbacks. _resolve_inbound_call_id fixes both: it derives a
deterministic id from the raw request body when the provider omits one.
"""

from src.api.main import SynthflowInboundPayload, _resolve_inbound_call_id


class TestResolveInboundCallId:
    def test_uses_call_id_when_present(self):
        payload = SynthflowInboundPayload(call_id="synthflow-call-123", phone="+15551234567")
        assert _resolve_inbound_call_id(payload, b'{"call_id":"synthflow-call-123"}') == "synthflow-call-123"

    def test_falls_back_to_deterministic_id_when_call_id_missing(self):
        raw_body = b'{"phone":"+15551234567","zip_code":"33604"}'
        payload = SynthflowInboundPayload(phone="+15551234567", zip_code="33604")
        resolved = _resolve_inbound_call_id(payload, raw_body)
        assert resolved is not None
        assert len(resolved) == 36  # matches decision_id VARCHAR(36) columns

    def test_identical_retry_body_produces_identical_fallback_id(self):
        raw_body = b'{"phone":"+15551234567","zip_code":"33604"}'
        payload = SynthflowInboundPayload(phone="+15551234567", zip_code="33604")
        first = _resolve_inbound_call_id(payload, raw_body)
        second = _resolve_inbound_call_id(payload, raw_body)
        assert first == second  # a true retry must dedupe via the idempotency check

    def test_distinct_bodies_produce_distinct_fallback_ids(self):
        payload_a = SynthflowInboundPayload(phone="+15551234567")
        payload_b = SynthflowInboundPayload(phone="+15559876543")
        id_a = _resolve_inbound_call_id(payload_a, b'{"phone":"+15551234567"}')
        id_b = _resolve_inbound_call_id(payload_b, b'{"phone":"+15559876543"}')
        assert id_a != id_b
