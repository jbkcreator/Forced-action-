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

    def _call(self, step, method, path, **kwargs):
        self.calls.append((step, method, path, kwargs.get("json")))
        return {}


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
    monkeypatch.setattr(ghl_webhook, "_ghl_request", lambda *a, **k: resp)
    return GhlPrequalAttachmentSink(_Account())


def test_upload_returns_url(monkeypatch):
    assert _real_upload(monkeypatch, _Resp())._upload("cid1", b"%PDF") == FILE_URL


def test_upload_http_error_raises(monkeypatch):
    with pytest.raises(DeliveryError):
        _real_upload(monkeypatch, _Resp(status=500, payload={}))._upload("cid1", b"%PDF")


def test_upload_without_url_raises(monkeypatch):
    with pytest.raises(DeliveryError):
        _real_upload(monkeypatch, _Resp(payload={"uploadedFiles": {}}))._upload("cid1", b"%PDF")
