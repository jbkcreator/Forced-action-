"""
Unit tests for src/services/meta_capi_service.py (S2 — Meta Conversions API).

DB-free. Patches get_settings + requests.post so no network or env is needed.
Covers: feature gating, PII hashing, payload shape, test_event_code, failure
handling (never raises), and token/URL safety.
"""
from __future__ import annotations

import hashlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import requests

from src.services import meta_capi_service
from src.services.meta_capi_service import fire_purchase_event


# ── Helpers ──────────────────────────────────────────────────────────────────

def _settings(*, enabled=True, pixel="111222333", token="SECRET_TOKEN",
              test_mode=False, test_code=None, version="v17.0"):
    return SimpleNamespace(
        meta_capi_enabled=enabled,
        meta_pixel_id=pixel,
        meta_access_token=SimpleNamespace(get_secret_value=lambda: token) if token else None,
        meta_capi_test_mode=test_mode,
        meta_test_event_code=test_code,
        meta_graph_api_version=version,
    )


def _subscriber(email="Test@Example.COM ", phone="+18135550123", sid=42):
    return SimpleNamespace(id=sid, email=email, phone=phone)


def _resp(status=200, body=None):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = body if body is not None else {"events_received": 1, "fbtrace_id": "tr_ok"}
    r.text = "" if body is None else str(body)
    return r


def _patch_settings(settings):
    return patch.object(meta_capi_service, "get_settings", return_value=settings)


def _patch_post(resp=None, side_effect=None):
    kw = {}
    if side_effect is not None:
        kw["side_effect"] = side_effect
    else:
        kw["return_value"] = resp if resp is not None else _resp()
    return patch.object(meta_capi_service.requests, "post", MagicMock(**kw))


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


# ── Gating ───────────────────────────────────────────────────────────────────

class TestGating:
    def test_disabled_skips_without_network(self):
        with _patch_settings(_settings(enabled=False)), \
             patch.object(meta_capi_service.requests, "post") as post:
            out = fire_purchase_event(_subscriber(), 99.0, "subscription", {}, event_id="sub_cs_1")
        assert out == {"status": "skipped", "event_id": "sub_cs_1", "reason": "disabled"}
        post.assert_not_called()

    def test_missing_pixel_skips(self):
        with _patch_settings(_settings(pixel=None)), \
             patch.object(meta_capi_service.requests, "post") as post:
            out = fire_purchase_event(_subscriber(), 99.0, "subscription", {})
        assert out["status"] == "skipped"
        assert out["reason"] == "missing_config"
        post.assert_not_called()

    def test_missing_token_skips(self):
        with _patch_settings(_settings(token=None)), \
             patch.object(meta_capi_service.requests, "post") as post:
            out = fire_purchase_event(_subscriber(), 99.0, "subscription", {})
        assert out["status"] == "skipped"
        assert out["reason"] == "missing_config"
        post.assert_not_called()


# ── Hashing + payload ──────────────────────────────────────────────────────

class TestPayload:
    def _fire(self, settings, subscriber, amount, ctx, event_id="sub_cs_9"):
        post = MagicMock(return_value=_resp())
        with _patch_settings(settings), \
             patch.object(meta_capi_service.requests, "post", post), \
             patch("src.services.phone_utils.normalize", return_value="+18135550123"):
            out = fire_purchase_event(subscriber, amount, "subscription", ctx, event_id=event_id)
        return out, post

    def test_email_and_phone_hashed(self):
        out, post = self._fire(_settings(), _subscriber(), 99.0, {})
        assert out["status"] == "sent"
        payload = post.call_args.kwargs["json"]
        ud = payload["data"][0]["user_data"]
        assert ud["em"] == [_sha("test@example.com")]
        assert ud["ph"] == [_sha("18135550123")]

    def test_phone_missing_omits_ph(self):
        out, post = self._fire(_settings(), _subscriber(phone=None), 99.0, {})
        ud = post.call_args.kwargs["json"]["data"][0]["user_data"]
        assert "ph" not in ud
        assert "em" in ud

    def test_value_currency_and_event_fields(self):
        ctx = {"currency": "usd", "campaign_id": "fa_test_001", "utm_campaign": "fa_test_campaign",
               "buyer_ip": "1.2.3.4", "buyer_user_agent": "UA/1.0", "fbclid": "abc"}
        out, post = self._fire(_settings(), _subscriber(), 99.0, ctx, event_id="sub_cs_42")
        event = post.call_args.kwargs["json"]["data"][0]
        assert event["event_name"] == "Purchase"
        assert event["action_source"] == "website"
        assert event["event_id"] == "sub_cs_42"
        assert isinstance(event["event_time"], int)
        cd = event["custom_data"]
        assert cd["value"] == 99.0 and isinstance(cd["value"], float)
        assert cd["currency"] == "USD"
        assert cd["campaign_id"] == "fa_test_001"
        assert cd["utm_campaign"] == "fa_test_campaign"
        ud = event["user_data"]
        assert ud["client_ip_address"] == "1.2.3.4"
        assert ud["client_user_agent"] == "UA/1.0"
        assert ud["fbc"].startswith("fb.1.") and ud["fbc"].endswith(".abc")

    def test_test_event_code_only_in_test_mode(self):
        # test_mode + code → included
        _, post = self._fire(_settings(test_mode=True, test_code="TEST123"), _subscriber(), 10.0, {})
        assert post.call_args.kwargs["json"]["test_event_code"] == "TEST123"
        # not test mode → omitted even if code present
        _, post2 = self._fire(_settings(test_mode=False, test_code="TEST123"), _subscriber(), 10.0, {})
        assert "test_event_code" not in post2.call_args.kwargs["json"]


# ── Token / URL safety ───────────────────────────────────────────────────────

class TestTokenSafety:
    def test_token_in_body_not_url(self):
        post = MagicMock(return_value=_resp())
        with _patch_settings(_settings(token="SUPER_SECRET", pixel="999", version="v17.0")), \
             patch.object(meta_capi_service.requests, "post", post), \
             patch("src.services.phone_utils.normalize", return_value="+18135550123"):
            fire_purchase_event(_subscriber(), 5.0, "lead_pack", {}, event_id="leadpack_pi_1")
        url = post.call_args.args[0]
        assert "SUPER_SECRET" not in url
        assert "/v17.0/999/events" in url
        assert post.call_args.kwargs["json"]["access_token"] == "SUPER_SECRET"


# ── Failure handling — never raises ─────────────────────────────────────────

class TestFailureHandling:
    def test_non_2xx_returns_failed(self):
        body = {"error": {"message": "bad", "fbtrace_id": "tr_err"}}
        with _patch_settings(_settings()), \
             patch.object(meta_capi_service.requests, "post", MagicMock(return_value=_resp(400, body))), \
             patch("src.services.phone_utils.normalize", return_value="+18135550123"):
            out = fire_purchase_event(_subscriber(), 99.0, "subscription", {}, event_id="sub_cs_5")
        assert out == {"status": "failed", "event_id": "sub_cs_5", "reason": "meta_error"}

    def test_network_exception_returns_failed_not_raise(self):
        with _patch_settings(_settings()), \
             patch.object(meta_capi_service.requests, "post",
                          MagicMock(side_effect=requests.Timeout("timed out"))), \
             patch("src.services.phone_utils.normalize", return_value="+18135550123"):
            out = fire_purchase_event(_subscriber(), 99.0, "subscription", {}, event_id="sub_cs_6")
        assert out["status"] == "failed"
        assert out["reason"] == "meta_error"
