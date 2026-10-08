import pytest

from src.lending import prequal_ghl
from src.lending.prequal import LinkDeliverySink
from src.lending.prequal_ghl import GHL_TAG_PREQUAL_READY, GhlPrequalLinkPublisher
from src.lending.web_leads import DeliveryError

FIELD_ID = "Lm1uhxnpWjj8iUobfSAk"


class _Settings:
    lending_ghl_cf_prequal_pdf_link = FIELD_ID


class RecordingPublisher(GhlPrequalLinkPublisher):
    def __init__(self, fail_delete=False):
        self.calls = []
        self._fail_delete = fail_delete

    def _call(self, step, method, path, **kwargs):
        self.calls.append((step, method, path, kwargs.get("json")))
        if self._fail_delete and method == "DELETE":
            raise DeliveryError("boom")
        return {}


@pytest.fixture
def with_field(monkeypatch):
    monkeypatch.setattr(prequal_ghl, "get_settings", lambda: _Settings())


def test_publish_writes_field_then_resets_and_adds_tag(with_field):
    pub = RecordingPublisher()
    pub.publish("cid1", "https://x/p/abc")
    assert pub.calls == [
        ("prequal link field", "PUT", "/contacts/cid1", {"customFields": [{"id": FIELD_ID, "value": "https://x/p/abc"}]}),
        ("prequal tag reset", "DELETE", "/contacts/cid1/tags", {"tags": [GHL_TAG_PREQUAL_READY]}),
        ("prequal tag add", "POST", "/contacts/cid1/tags", {"tags": [GHL_TAG_PREQUAL_READY]}),
    ]


def test_stale_tag_reset_failure_does_not_block_tag_add(with_field):
    pub = RecordingPublisher(fail_delete=True)
    pub.publish("cid1", "https://x/p/abc")
    assert pub.calls[-1][0] == "prequal tag add"


def test_missing_field_setting_is_config_error(monkeypatch):
    class _Empty:
        lending_ghl_cf_prequal_pdf_link = None
    monkeypatch.setattr(prequal_ghl, "get_settings", lambda: _Empty())
    pub = RecordingPublisher()
    with pytest.raises(DeliveryError) as exc:
        pub.publish("cid1", "https://x/p/abc")
    assert exc.value.config_error is True
    assert pub.calls == []


def test_link_delivery_sink_stores_then_publishes():
    saved, published = [], []

    class Store:
        def save(self, lead_id, pdf):
            saved.append((lead_id, pdf))
            return f"https://x/p/{lead_id}"

    class Pub:
        def publish(self, contact_id, url):
            published.append((contact_id, url))

    LinkDeliverySink(Store(), Pub()).deliver(5, "cid5", b"%PDF")
    assert saved == [(5, b"%PDF")]
    assert published == [("cid5", "https://x/p/5")]
