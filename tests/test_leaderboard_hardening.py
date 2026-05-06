"""
Phase A.2 (2026-05-04): /api/leaderboard hardening.

Asserts the three protections we added:

  1. Response payload no longer contains subscriber_id (was leaking the
     numeric id of every top-5 referrer).
  2. Per-IP rate limit fires at 60 req/min, returning 429 with a
     Retry-After header.
  3. Cache-Control header is set so a CDN / browser absorbs scraper
     traffic.

Also confirms the leaderboard builder skips nameless subscribers — the
old "Member <id>" fallback used to leak ids into the snapshot.

Run:
    pytest tests/test_leaderboard_hardening.py -v
"""
from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from src.core.models import ReferralEvent, Subscriber


@pytest.fixture
def client():
    from src.api.main import app
    return TestClient(app)


@pytest.fixture(autouse=True)
def reset_rate_limit_buckets():
    """In-memory rate-limit state must be cleared between tests so one
    test's traffic doesn't poison the next."""
    from src.services.rate_limit import reset_local_buckets
    reset_local_buckets()
    yield
    reset_local_buckets()


def _seed_leaderboard_snapshot(tmp_path, payload):
    """Drop a snapshot at the on-disk path the endpoint reads from."""
    from src.tasks import leaderboard as lb
    lb._OUTPUT_DIR = tmp_path  # type: ignore[attr-defined]
    out = tmp_path / "latest.json"
    out.write_text(json.dumps(payload), encoding="utf-8")


# ── 1. subscriber_id must not leak ──────────────────────────────────────────


class TestSubscriberIdNotLeaked:
    def test_response_strips_subscriber_id(self, client, tmp_path, monkeypatch):
        from src.tasks import leaderboard as lb
        monkeypatch.setattr(lb, "_OUTPUT_DIR", tmp_path)
        snapshot = {
            "as_of": "2026-05-04",
            "leaderboards": [
                {
                    "county_id": "hillsborough",
                    "vertical": "roofing",
                    "leaderboard": [
                        {
                            "rank": 1,
                            "subscriber_id": 12345,    # the leak we're stripping
                            "handle": "Mike J.",
                            "refs_this_week": 4,
                            "refs_total": 12,
                            "badge": "rising_star",
                        },
                    ],
                },
            ],
        }
        (tmp_path / "latest.json").write_text(json.dumps(snapshot), encoding="utf-8")

        resp = client.get("/api/leaderboard")
        assert resp.status_code == 200
        body = resp.json()
        # subscriber_id must NOT appear anywhere in the public payload
        assert "12345" not in resp.text
        for board in body["leaderboards"]:
            for row in board["leaderboard"]:
                assert "subscriber_id" not in row, \
                    f"subscriber_id leaked: {row}"
                # Public fields are unchanged
                assert "handle" in row
                assert "refs_this_week" in row
                assert "rank" in row


# ── 2. Per-IP rate limit at 60/min ──────────────────────────────────────────


class TestRateLimit:
    def test_61st_request_in_a_minute_is_rate_limited(self, client, tmp_path, monkeypatch):
        from src.tasks import leaderboard as lb
        monkeypatch.setattr(lb, "_OUTPUT_DIR", tmp_path)
        (tmp_path / "latest.json").write_text(
            json.dumps({"as_of": "2026-05-04", "leaderboards": []}),
            encoding="utf-8",
        )

        # First 60 must succeed
        for i in range(60):
            resp = client.get("/api/leaderboard")
            assert resp.status_code == 200, f"req {i+1} unexpectedly limited"

        # 61st should be rate-limited
        resp = client.get("/api/leaderboard")
        assert resp.status_code == 429, \
            f"expected 429 on req 61, got {resp.status_code}"
        body = resp.json()
        assert body["detail"]["error"] == "rate_limited"
        assert body["detail"]["scope"] == "leaderboard"
        assert "Retry-After" in resp.headers

    def test_different_ips_have_independent_buckets(self, client, tmp_path, monkeypatch):
        """X-Forwarded-For varying = different rate-limit buckets."""
        from src.tasks import leaderboard as lb
        monkeypatch.setattr(lb, "_OUTPUT_DIR", tmp_path)
        (tmp_path / "latest.json").write_text(
            json.dumps({"as_of": "2026-05-04", "leaderboards": []}),
            encoding="utf-8",
        )

        # Burn the bucket for IP A
        for _ in range(60):
            client.get("/api/leaderboard", headers={"X-Forwarded-For": "10.0.0.1"})
        resp = client.get("/api/leaderboard", headers={"X-Forwarded-For": "10.0.0.1"})
        assert resp.status_code == 429

        # IP B is fresh — should still succeed
        resp = client.get("/api/leaderboard", headers={"X-Forwarded-For": "10.0.0.2"})
        assert resp.status_code == 200


# ── 3. Cache-Control header ─────────────────────────────────────────────────


class TestCacheControl:
    def test_response_sets_cache_control_header(self, client, tmp_path, monkeypatch):
        from src.tasks import leaderboard as lb
        monkeypatch.setattr(lb, "_OUTPUT_DIR", tmp_path)
        (tmp_path / "latest.json").write_text(
            json.dumps({"as_of": "2026-05-04", "leaderboards": []}),
            encoding="utf-8",
        )

        resp = client.get("/api/leaderboard")
        assert resp.status_code == 200
        cc = resp.headers.get("Cache-Control", "")
        assert "max-age" in cc, f"missing max-age in Cache-Control: {cc!r}"
        assert "public" in cc.lower()


# ── 4. Builder skips nameless subscribers (no Member-id leak in snapshot) ──


class TestBuilderSkipsNameless:
    def test_subscriber_without_name_excluded_from_snapshot(self, fresh_db):
        """The on-disk snapshot used to fall back to f'Member {sub.id}' for
        nameless subscribers. We now skip them entirely — no id leak."""
        from src.tasks.leaderboard import build

        uid = uuid.uuid4().hex[:8]
        nameless = Subscriber(
            stripe_customer_id=f"cus_nameless_{uid}",
            tier="starter", vertical="roofing", county_id="hillsborough",
            event_feed_uuid=f"nameless-{uid}",
            name=None,
            status="active",
        )
        named = Subscriber(
            stripe_customer_id=f"cus_named_{uid}",
            tier="starter", vertical="roofing", county_id="hillsborough",
            event_feed_uuid=f"named-{uid}",
            name="Test Person",
            status="active",
        )
        fresh_db.add_all([nameless, named])
        fresh_db.flush()

        # Each gets a confirmed referral so they qualify for the leaderboard.
        # (Use distinct dummy referees — we just need ReferralEvent rows.)
        ref1 = Subscriber(
            stripe_customer_id=f"cus_r1_{uid}",
            tier="starter", vertical="roofing", county_id="hillsborough",
            event_feed_uuid=f"r1-{uid}", status="active",
        )
        ref2 = Subscriber(
            stripe_customer_id=f"cus_r2_{uid}",
            tier="starter", vertical="roofing", county_id="hillsborough",
            event_feed_uuid=f"r2-{uid}", status="active",
        )
        fresh_db.add_all([ref1, ref2])
        fresh_db.flush()

        now = datetime.now(timezone.utc)
        for referrer, referee in [(nameless, ref1), (named, ref2)]:
            ev = ReferralEvent(
                referrer_subscriber_id=referrer.id,
                referee_subscriber_id=referee.id,
                referral_code=f"X{referrer.id}",
                status="confirmed",
                confirmed_at=now,
            )
            fresh_db.add(ev)
        fresh_db.commit()

        try:
            payload = build(fresh_db)
            flat = json.dumps(payload)
            assert f"Member {nameless.id}" not in flat, \
                "nameless subscriber id leaked via 'Member <id>' fallback"
            # Named subscriber still appears
            assert "Test P." in flat
        finally:
            for ev in fresh_db.query(ReferralEvent).filter(
                ReferralEvent.referrer_subscriber_id.in_([nameless.id, named.id])
            ).all():
                fresh_db.delete(ev)
            for s in [nameless, named, ref1, ref2]:
                fresh_db.delete(s)
            fresh_db.commit()
