import pytest

from src.lending import prequal_ghl
from src.lending.prequal_ghl import EMAIL_SUBJECT, GhlPrequalAttachmentSink
from src.lending.web_leads import DeliveryError

FILE_URL = "https://files.example/conv/abc.pdf"


class _Settings:
    lending_prequal_email_from = None


class _Resp:
    def __init__(self, status=201, payload=None):
        self.status_code = status
        self._payload = payload if payload is not None else {"uploadedFiles": {"Pre-Qualification-Estimate.pdf": FILE_URL}}

    def json(self):
        return self._payload


class _Account:
    api_key = "k"
    location_id = "loc"


class RecordingSink(GhlPrequalAttachmentSink):
    def __init__(self, upload_resp=None):
        super().__init__(_Account())
        self.calls = []
        self._upload_resp = upload_resp

    def _upload(self, contact_id, pdf):
        if self._upload_resp is not None:
            raise self._upload_resp
        self.calls.append(("upload", contact_id, pdf))
        return FILE_URL

    def _send_email(self, body):
        self.calls.append(("prequal email send", "POST", "/conversations/messages", body))


@pytest.fixture
def no_sender(monkeypatch):
    monkeypatch.setattr(prequal_ghl, "get_settings", lambda: _Settings())


def test_deliver_uploads_then_sends_email_with_attachment(no_sender):
    sink = RecordingSink()
    sink.deliver(1, "cid1", b"%PDF")
    assert sink.calls[0] == ("upload", "cid1", b"%PDF")
    step, method, path, body = sink.calls[1]
    assert (step, method, path) == ("prequal email send", "POST", "/conversations/messages")
    assert body["type"] == "Email" and body["contactId"] == "cid1"
    assert body["subject"] == EMAIL_SUBJECT
    assert body["attachments"] == [FILE_URL]
    assert "emailFrom" not in body


def test_email_from_set_when_configured(monkeypatch):
    class S:
        lending_prequal_email_from = "hello@example.com"
    monkeypatch.setattr(prequal_ghl, "get_settings", lambda: S())
    sink = RecordingSink()
    sink.deliver(1, "cid1", b"%PDF")
    assert sink.calls[1][3]["emailFrom"] == "hello@example.com"


def test_upload_failure_sends_no_email(no_sender):
    sink = RecordingSink(upload_resp=DeliveryError("prequal upload: HTTP 500"))
    with pytest.raises(DeliveryError):
        sink.deliver(1, "cid1", b"%PDF")
    assert sink.calls == []


def test_email_copy_has_no_rate_or_term_language():
    text = (prequal_ghl.EMAIL_SUBJECT + prequal_ghl.EMAIL_HTML).lower()
    for word in ("interest rate", "apr", "points", "per annum", "months"):
        assert word not in text


def _real_upload(monkeypatch, resp):
    from src.services import ghl_webhook
    monkeypatch.setattr(ghl_webhook, "ghl_post_multipart", lambda *a, **k: resp)
    return GhlPrequalAttachmentSink(_Account())


def test_upload_returns_url(monkeypatch):
    assert _real_upload(monkeypatch, _Resp())._upload("cid1", b"%PDF") == FILE_URL


def test_upload_http_error_raises(monkeypatch):
    with pytest.raises(DeliveryError):
        _real_upload(monkeypatch, _Resp(status=500, payload={}))._upload("cid1", b"%PDF")


def test_upload_without_url_raises(monkeypatch):
    with pytest.raises(DeliveryError):
        _real_upload(monkeypatch, _Resp(payload={"uploadedFiles": {}}))._upload("cid1", b"%PDF")


def _send_with(monkeypatch, outcome):
    """Real _send_email against a stubbed single-attempt POST; returns the number of POSTs made."""
    from src.services import ghl_webhook

    calls = []

    def fake_post_once(path, **kwargs):
        calls.append(path)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(ghl_webhook, "ghl_post_once", fake_post_once)
    sink = GhlPrequalAttachmentSink(_Account())
    try:
        sink._send_email({"type": "Email"})
        return calls, None
    except DeliveryError as exc:
        return calls, exc


def test_send_read_timeout_is_unknown_and_not_retried(monkeypatch):
    from requests.exceptions import ReadTimeout

    calls, exc = _send_with(monkeypatch, ReadTimeout("slow"))
    assert calls == ["/conversations/messages"]
    assert isinstance(exc, prequal_ghl.SendOutcomeUnknown)


def test_send_dropped_connection_is_unknown(monkeypatch):
    from requests.exceptions import ConnectionError as RequestsConnectionError

    calls, exc = _send_with(monkeypatch, RequestsConnectionError("reset"))
    assert len(calls) == 1 and isinstance(exc, prequal_ghl.SendOutcomeUnknown)


def test_send_server_error_is_unknown(monkeypatch):
    calls, exc = _send_with(monkeypatch, _Resp(status=502, payload={}))
    assert len(calls) == 1 and isinstance(exc, prequal_ghl.SendOutcomeUnknown)


def test_send_connect_timeout_is_a_plain_failure(monkeypatch):
    from requests.exceptions import ConnectTimeout

    calls, exc = _send_with(monkeypatch, ConnectTimeout("no route"))
    assert len(calls) == 1
    assert isinstance(exc, DeliveryError) and not isinstance(exc, prequal_ghl.SendOutcomeUnknown)


@pytest.mark.parametrize("status,config", [(429, False), (422, False), (401, True)])
def test_send_rejected_is_a_plain_failure(monkeypatch, status, config):
    calls, exc = _send_with(monkeypatch, _Resp(status=status, payload={}))
    assert len(calls) == 1
    assert not isinstance(exc, prequal_ghl.SendOutcomeUnknown) and exc.config_error is config


def test_send_accepted(monkeypatch):
    calls, exc = _send_with(monkeypatch, _Resp(status=201, payload={"messageId": "m"}))
    assert len(calls) == 1 and exc is None
