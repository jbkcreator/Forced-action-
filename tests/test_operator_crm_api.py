"""fa045 — HTTP-layer tests for the operator CRM router.

Covers every route mounted by src.api.operator_crm_router and the
sms_compliance.send_sms `do_not_text` behavioral side-effect.

Endpoint coverage (14 routes):
  GET    /api/admin/subscribers
  GET    /api/admin/subscribers/hot
  GET    /api/admin/subscribers/{id}
  GET    /api/admin/subscribers/{id}/conversation
  GET    /api/admin/subscribers/{id}/deals
  PATCH  /api/admin/deals/{deal_id}
  GET    /api/admin/subscribers/{id}/notes
  POST   /api/admin/subscribers/{id}/notes
  PATCH  /api/admin/notes/{note_id}
  DELETE /api/admin/notes/{note_id}
  GET    /api/admin/subscribers/{id}/tags
  POST   /api/admin/subscribers/{id}/tags
  DELETE /api/admin/subscribers/{id}/tags/{tag}
  GET    /api/admin/subscriber-tag-suggestions

Strategy:
  - Use FastAPI dependency_overrides to swap the `get_db` session for a
    MagicMock, matching the pattern in test_lifecycle_incidents_api.py.
  - The hot-queue endpoint and the do_not_text suppression test need real
    SQL (Postgres INTERVAL, real SubscriberTag table joins). Those are
    gated on the `fresh_db` fixture and skip when DATABASE_URL is not
    configured.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


# ════════════════════════════════════════════════════════════════════════════
# Fixtures
# ════════════════════════════════════════════════════════════════════════════


@pytest.fixture
def client():
    from src.api.main import app
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def admin_token(monkeypatch):
    from pydantic import SecretStr
    from config.settings import settings
    monkeypatch.setattr(settings, "admin_jwt_secret", SecretStr("test-jwt-secret"))
    monkeypatch.setattr(settings, "admin_password", SecretStr("test-admin-pass"))
    from src.api.admin_router import create_access_token
    return create_access_token({"sub": "ops@heu.ai", "scope": "admin"})


@pytest.fixture
def auth(admin_token):
    return {"Authorization": f"Bearer {admin_token}"}


@pytest.fixture
def mock_session():
    """A MagicMock session installed as the `get_db` dependency override.

    Yields the bare mock so each test can program its query/execute chains.
    Auto-cleans the override on teardown.
    """
    from src.api.main import app
    from src.api.admin_router import get_db

    sess = MagicMock()

    def _override():
        yield sess

    app.dependency_overrides[get_db] = _override
    yield sess
    app.dependency_overrides.pop(get_db, None)


def _row(**kw):
    """SimpleNamespace shortcut for fake SQLAlchemy result rows."""
    return SimpleNamespace(**kw)


def _make_subscriber(**overrides):
    """Minimal Subscriber-shaped object for `_subscriber_row` serialization."""
    defaults = dict(
        id=1, email="user@example.com", name="Test User", phone="+15551234567",
        tier="pro", vertical="roofing", county_id="hillsborough",
        status="active",
        revenue_signal_score=72, revenue_signal_band="high",
        revenue_signal_updated_at=datetime(2026, 5, 28, tzinfo=timezone.utc),
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    defaults.update(overrides)
    return _row(**defaults)


# ════════════════════════════════════════════════════════════════════════════
# Auth — every route under this router must reject missing/invalid tokens
# ════════════════════════════════════════════════════════════════════════════


class TestAuth:

    @pytest.mark.parametrize("method,path", [
        ("get",    "/api/admin/subscribers"),
        ("get",    "/api/admin/subscribers/hot"),
        ("get",    "/api/admin/subscribers/1"),
        ("get",    "/api/admin/subscribers/1/conversation"),
        ("get",    "/api/admin/subscribers/1/deals"),
        ("patch",  "/api/admin/deals/1"),
        ("get",    "/api/admin/subscribers/1/notes"),
        ("post",   "/api/admin/subscribers/1/notes"),
        ("patch",  "/api/admin/notes/1"),
        ("delete", "/api/admin/notes/1"),
        ("get",    "/api/admin/subscribers/1/tags"),
        ("post",   "/api/admin/subscribers/1/tags"),
        ("delete", "/api/admin/subscribers/1/tags/vip"),
        ("get",    "/api/admin/subscriber-tag-suggestions"),
    ])
    def test_no_token_rejected(self, client, method, path):
        kwargs = {"json": {}} if method in ("post", "patch") else {}
        r = getattr(client, method)(path, **kwargs)
        assert r.status_code in (401, 403), f"{method.upper()} {path} should require auth"


# ════════════════════════════════════════════════════════════════════════════
# GET /subscribers — list
# ════════════════════════════════════════════════════════════════════════════


class TestSubscribersList:

    def test_returns_paginated_items_with_total(self, client, auth, mock_session):
        # COUNT(*) is the first execute().scalar() call.
        mock_session.execute.return_value.scalar.return_value = 42
        # The id-only query returns ranked subscriber ids.
        mock_session.execute.return_value.fetchall.return_value = [_row(id=11), _row(id=12)]
        # Final ORM .all() returns the hydrated Subscriber rows.
        subs = [
            _make_subscriber(id=11, email="a@x.com", revenue_signal_score=80),
            _make_subscriber(id=12, email="b@x.com", revenue_signal_score=60),
        ]
        mock_session.query.return_value.filter.return_value.all.return_value = subs

        r = client.get("/api/admin/subscribers?limit=25&offset=0", headers=auth)
        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 42
        assert body["limit"] == 25
        assert body["offset"] == 0
        assert [it["id"] for it in body["items"]] == [11, 12]
        assert body["items"][0]["revenue_signal_score"] == 80
        assert body["items"][0]["grace"] is False

    def test_grace_flag_set_when_status_grace(self, client, auth, mock_session):
        mock_session.execute.return_value.scalar.return_value = 1
        mock_session.execute.return_value.fetchall.return_value = [_row(id=7)]
        mock_session.query.return_value.filter.return_value.all.return_value = [
            _make_subscriber(id=7, status="grace"),
        ]
        r = client.get("/api/admin/subscribers", headers=auth)
        assert r.status_code == 200
        assert r.json()["items"][0]["grace"] is True

    def test_limit_above_max_rejected(self, client, auth, mock_session):
        r = client.get("/api/admin/subscribers?limit=999", headers=auth)
        assert r.status_code == 422

    def test_filters_forwarded_to_sql(self, client, auth, mock_session):
        mock_session.execute.return_value.scalar.return_value = 0
        mock_session.execute.return_value.fetchall.return_value = []
        r = client.get(
            "/api/admin/subscribers?q=acme&vertical=roofing&band=high&status=active"
            "&tier=pro&county_id=hillsborough&tag=vip",
            headers=auth,
        )
        assert r.status_code == 200
        # The first execute() carries the WHERE-bound params.
        params = mock_session.execute.call_args_list[0].args[1]
        assert params["q"] == "%acme%"
        assert params["vertical"] == "roofing"
        assert params["band"] == "high"
        assert params["status"] == "active"
        assert params["tier"] == "pro"
        assert params["county_id"] == "hillsborough"
        assert params["tag"] == "vip"


# ════════════════════════════════════════════════════════════════════════════
# GET /subscribers/hot — at-risk queue
# ════════════════════════════════════════════════════════════════════════════


class TestHotQueue:

    def test_empty_queue_returns_empty_items(self, client, auth, mock_session):
        mock_session.execute.return_value.fetchall.return_value = []
        r = client.get("/api/admin/subscribers/hot", headers=auth)
        assert r.status_code == 200
        assert r.json() == {"items": []}

    def test_queue_delifecycletes_with_cool_down_days(self, client, auth, mock_session):
        old = datetime.now(timezone.utc) - timedelta(days=12)
        mock_session.execute.return_value.fetchall.return_value = [
            _row(id=101, last_significant_action_at=old),
        ]
        mock_session.query.return_value.filter.return_value.all.return_value = [
            _make_subscriber(id=101, revenue_signal_score=70, status="active"),
        ]
        r = client.get("/api/admin/subscribers/hot", headers=auth)
        assert r.status_code == 200
        items = r.json()["items"]
        assert len(items) == 1
        assert items[0]["id"] == 101
        assert items[0]["days_since_action"] >= 11
        assert items[0]["last_significant_action_at"] is not None


# ════════════════════════════════════════════════════════════════════════════
# Real-Postgres hot-queue spec
# ════════════════════════════════════════════════════════════════════════════


@pytest.mark.integration
class TestHotQueueSqlSpec:
    """Verify the SQL filter against a real Postgres DB.

    Builds three subscribers:
      - hot (RSS=80, last_action 14d ago, active)        — should appear
      - too-recent (RSS=80, last_action 3d ago, active)  — excluded (cool < 7d)
      - cancelled (RSS=80, last_action 30d ago, status=cancelled) — excluded
    """

    def test_hot_queue_filters(self, client, auth, fresh_db):
        from sqlalchemy import text as _t
        from src.api.main import app
        from src.api.admin_router import get_db
        from src.core.models import Subscriber

        def _seed(stripe_id, **kw):
            s = Subscriber(
                stripe_customer_id=stripe_id,
                tier="pro", vertical="roofing", county_id="hillsborough",
                **kw,
            )
            fresh_db.add(s)
            fresh_db.flush()
            return s

        hot = _seed("cus_hot", email="hot@x.com",
                    status="active", revenue_signal_score=80,
                    revenue_signal_band="very_high")
        recent = _seed("cus_recent", email="recent@x.com",
                       status="active", revenue_signal_score=80,
                       revenue_signal_band="very_high")
        cancelled = _seed("cus_cancel", email="cancel@x.com",
                          status="cancelled", revenue_signal_score=80,
                          revenue_signal_band="very_high")
        fresh_db.flush()

        # Seed user_segments rows with the right last_significant_action_at.
        now = datetime.now(timezone.utc)
        fresh_db.execute(_t("""
            INSERT INTO user_segments
                (subscriber_id, segment, last_significant_action_at,
                 last_classified_at, created_at, updated_at)
            VALUES (:hot, 'browsing', :old, :now, :now, :now),
                   (:recent, 'browsing', :fresh, :now, :now, :now),
                   (:cancel, 'browsing', :old, :now, :now, :now)
        """), {
            "hot": hot.id, "recent": recent.id, "cancel": cancelled.id,
            "old":  now - timedelta(days=14),
            "fresh": now - timedelta(days=3),
            "now":  now,
        })
        fresh_db.flush()

        # Route via the in-flight transaction so we see the seed data.
        def _override():
            yield fresh_db
        app.dependency_overrides[get_db] = _override
        try:
            r = client.get("/api/admin/subscribers/hot", headers=auth)
            assert r.status_code == 200
            ids = {it["id"] for it in r.json()["items"]}
            assert hot.id in ids
            assert recent.id not in ids
            assert cancelled.id not in ids
        finally:
            app.dependency_overrides.pop(get_db, None)


# ════════════════════════════════════════════════════════════════════════════
# GET /subscribers/{id} — detail bundle
# ════════════════════════════════════════════════════════════════════════════


class TestSubscriberDetail:

    def test_detail_returns_bundle(self, client, auth, mock_session):
        mock_session.get.return_value = _make_subscriber(id=42)
        # tags
        tag_q = MagicMock()
        tag_q.filter.return_value.order_by.return_value.all.return_value = [
            _row(tag="vip"), _row(tag="coach_weekly"),
        ]
        # notes
        note_q = MagicMock()
        note_q.filter.return_value.order_by.return_value.limit.return_value.all.return_value = [
            _row(id=1, author_email="ops@heu.ai", body="hello", pinned=True,
                 created_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
                 updated_at=datetime(2026, 5, 1, tzinfo=timezone.utc)),
        ]
        # open deals count
        deal_q = MagicMock()
        deal_q.filter.return_value.count.return_value = 3

        # query() dispatches by model class
        from src.core.models import DealOutcome, SubscriberNote, SubscriberTag
        mock_session.query.side_effect = lambda model: {
            SubscriberTag:  tag_q,
            SubscriberNote: note_q,
            DealOutcome:    deal_q,
        }[model]

        with patch(
            "src.api.operator_crm_router.get_revenue_signal_score",
            return_value={
                "score": 72, "band": "high",
                "breakdown": {"spend_velocity": 20},
                "reasons": ["wallet tier: power"],
                "updated_at": None, "last_significant_action_at": None,
                "last_action": None,
            },
        ):
            r = client.get("/api/admin/subscribers/42", headers=auth)

        assert r.status_code == 200
        body = r.json()
        assert body["subscriber"]["id"] == 42
        assert body["rss"]["score"] == 72
        assert body["tags"] == ["vip", "coach_weekly"]
        assert len(body["recent_notes"]) == 1
        assert body["recent_notes"][0]["pinned"] is True
        assert body["open_deals_count"] == 3

    def test_unknown_subscriber_returns_404(self, client, auth, mock_session):
        mock_session.get.return_value = None
        r = client.get("/api/admin/subscribers/99999", headers=auth)
        assert r.status_code == 404


# ════════════════════════════════════════════════════════════════════════════
# GET /subscribers/{id}/conversation — two-sided history
# ════════════════════════════════════════════════════════════════════════════


class TestConversation:

    def test_unknown_subscriber_returns_404(self, client, auth, mock_session):
        mock_session.get.return_value = None
        r = client.get("/api/admin/subscribers/9/conversation", headers=auth)
        assert r.status_code == 404

    def test_merges_outbound_sms_chat_and_opt_out_keyword(
        self, client, auth, mock_session,
    ):
        mock_session.get.return_value = _make_subscriber(id=42, phone="+15551112222")
        t_out  = datetime(2026, 5, 28, 12, 0, tzinfo=timezone.utc)
        t_in   = datetime(2026, 5, 28, 12, 5, tzinfo=timezone.utc)
        t_chat = datetime(2026, 5, 28, 12, 10, tzinfo=timezone.utc)

        # The endpoint runs three execute(...).fetchall() calls in order:
        #   1) sms_send_logs (outbound)
        #   2) sms_opt_outs  (inbound keyword)
        #   3) chat_messages
        mock_session.execute.return_value.fetchall.side_effect = [
            [_row(
                id=1, body_preview="Hello there", message_type="marketing",
                outcome="sent", vendor="telnyx", campaign="fomo",
                decision_id="d1", created_at=t_out,
            )],
            [_row(id=10, keyword_used="STOP", opted_out_at=t_in)],
            [_row(
                id=99, role="user", content="hi from chat",
                intent_label="greeting", created_at=t_chat, session_id="sess-1",
            )],
        ]

        r = client.get("/api/admin/subscribers/42/conversation", headers=auth)
        assert r.status_code == 200
        items = r.json()["items"]
        assert {it["id"] for it in items} == {"sms_out:1", "sms_in:10", "chat:99"}
        # Sorted newest-first.
        assert items[0]["id"] == "chat:99"
        assert items[-1]["id"] == "sms_out:1"
        # Direction inference for chat: role=user → inbound.
        chat_item = next(it for it in items if it["id"] == "chat:99")
        assert chat_item["direction"] == "inbound"
        assert chat_item["channel"] == "chat"


# ════════════════════════════════════════════════════════════════════════════
# Deals
# ════════════════════════════════════════════════════════════════════════════


class TestDeals:

    def _build_deal(self, **kw):
        d = SimpleNamespace(
            id=1, subscriber_id=42, property_id=None,
            pipeline_stage="lead", deal_size_bucket="10_25k",
            deal_amount=15000, deal_date=None, lead_source="foreclosure",
            days_to_close=None, created_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        )
        for k, v in kw.items():
            setattr(d, k, v)
        return d

    def test_list_excludes_closed_by_default(self, client, auth, mock_session):
        q = MagicMock()
        q.filter.return_value = q
        q.order_by.return_value.all.return_value = [self._build_deal()]
        mock_session.query.return_value = q

        r = client.get("/api/admin/subscribers/42/deals", headers=auth)
        assert r.status_code == 200
        items = r.json()["items"]
        assert len(items) == 1
        # Two .filter() chains applied: by subscriber, then exclude closed.
        assert q.filter.call_count == 2

    def test_list_includes_closed_when_requested(self, client, auth, mock_session):
        q = MagicMock()
        q.filter.return_value = q
        q.order_by.return_value.all.return_value = []
        mock_session.query.return_value = q
        r = client.get("/api/admin/subscribers/42/deals?include_closed=true", headers=auth)
        assert r.status_code == 200
        # Only the subscriber-id filter is applied.
        assert q.filter.call_count == 1

    def test_patch_invalid_stage_returns_400(self, client, auth, mock_session):
        r = client.patch(
            "/api/admin/deals/1",
            json={"pipeline_stage": "bogus"},
            headers=auth,
        )
        assert r.status_code == 400

    def test_patch_unknown_deal_returns_404(self, client, auth, mock_session):
        mock_session.get.return_value = None
        r = client.patch(
            "/api/admin/deals/9999",
            json={"pipeline_stage": "qualified"},
            headers=auth,
        )
        assert r.status_code == 404

    def test_patch_writes_audit_event_and_updates_stage(self, client, auth, mock_session):
        deal = self._build_deal(pipeline_stage="contacted")
        mock_session.get.return_value = deal

        r = client.patch(
            "/api/admin/deals/1",
            json={"pipeline_stage": "qualified", "note": "called him back"},
            headers=auth,
        )
        assert r.status_code == 200
        body = r.json()
        assert body["from_stage"] == "contacted"
        assert body["to_stage"]   == "qualified"
        assert deal.pipeline_stage == "qualified"
        # An audit row was added.
        assert mock_session.add.called
        audit = mock_session.add.call_args.args[0]
        assert audit.deal_id    == 1
        assert audit.from_stage == "contacted"
        assert audit.to_stage   == "qualified"
        assert audit.note       == "called him back"
        assert audit.changed_by == "ops@heu.ai"

    def test_patch_no_change_is_idempotent(self, client, auth, mock_session):
        deal = self._build_deal(pipeline_stage="qualified")
        mock_session.get.return_value = deal
        r = client.patch(
            "/api/admin/deals/1",
            json={"pipeline_stage": "qualified"},
            headers=auth,
        )
        assert r.status_code == 200
        assert r.json()["no_change"] is True
        # No audit row written.
        assert mock_session.add.called is False


# ════════════════════════════════════════════════════════════════════════════
# Notes
# ════════════════════════════════════════════════════════════════════════════


class TestNotes:

    def _note(self, **kw):
        n = SimpleNamespace(
            id=1, subscriber_id=42, author_email="ops@heu.ai",
            body="hello", pinned=False,
            created_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
            updated_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        )
        for k, v in kw.items():
            setattr(n, k, v)
        return n

    def test_list_orders_pinned_first(self, client, auth, mock_session):
        q = MagicMock()
        # Real ordering happens in SQL; we only verify the route returns
        # whatever the query yields, in order.
        q.filter.return_value.order_by.return_value.all.return_value = [
            self._note(id=2, pinned=True,  body="pin"),
            self._note(id=1, pinned=False, body="plain"),
        ]
        mock_session.query.return_value = q

        r = client.get("/api/admin/subscribers/42/notes", headers=auth)
        assert r.status_code == 200
        items = r.json()["items"]
        assert items[0]["pinned"] is True
        assert items[1]["pinned"] is False

    def test_create_sets_author_and_returns_201(self, client, auth, mock_session):
        mock_session.get.return_value = _make_subscriber(id=42)
        # The route calls db.flush() and reads back; emulate the persisted row.
        captured: list = []

        def _capture_add(obj):
            obj.id = 99
            captured.append(obj)

        mock_session.add.side_effect = _capture_add

        r = client.post(
            "/api/admin/subscribers/42/notes",
            json={"body": "watch this customer", "pinned": True},
            headers=auth,
        )
        assert r.status_code == 201
        body = r.json()
        assert body["body"] == "watch this customer"
        assert body["pinned"] is True
        assert body["author_email"] == "ops@heu.ai"
        assert captured and captured[0].subscriber_id == 42

    def test_create_on_unknown_subscriber_returns_404(self, client, auth, mock_session):
        mock_session.get.return_value = None
        r = client.post(
            "/api/admin/subscribers/9999/notes",
            json={"body": "x"},
            headers=auth,
        )
        assert r.status_code == 404

    def test_create_empty_body_rejected(self, client, auth, mock_session):
        r = client.post(
            "/api/admin/subscribers/42/notes",
            json={"body": ""},
            headers=auth,
        )
        assert r.status_code == 422

    def test_patch_updates_pin_only(self, client, auth, mock_session):
        n = self._note(pinned=False)
        mock_session.get.return_value = n
        r = client.patch(
            "/api/admin/notes/1",
            json={"pinned": True},
            headers=auth,
        )
        assert r.status_code == 200
        assert n.pinned is True
        # Body untouched.
        assert n.body == "hello"

    def test_patch_unknown_note_returns_404(self, client, auth, mock_session):
        mock_session.get.return_value = None
        r = client.patch("/api/admin/notes/999", json={"pinned": True}, headers=auth)
        assert r.status_code == 404

    def test_delete_returns_204(self, client, auth, mock_session):
        mock_session.get.return_value = self._note()
        r = client.delete("/api/admin/notes/1", headers=auth)
        assert r.status_code == 204
        assert mock_session.delete.called

    def test_delete_unknown_note_returns_404(self, client, auth, mock_session):
        mock_session.get.return_value = None
        r = client.delete("/api/admin/notes/999", headers=auth)
        assert r.status_code == 404


# ════════════════════════════════════════════════════════════════════════════
# Tags
# ════════════════════════════════════════════════════════════════════════════


class TestTags:

    def test_list_returns_items(self, client, auth, mock_session):
        q = MagicMock()
        q.filter.return_value.order_by.return_value.all.return_value = [
            _row(id=1, tag="vip", created_at=datetime(2026, 5, 1, tzinfo=timezone.utc)),
            _row(id=2, tag="at_risk", created_at=datetime(2026, 5, 2, tzinfo=timezone.utc)),
        ]
        mock_session.query.return_value = q
        r = client.get("/api/admin/subscribers/42/tags", headers=auth)
        assert r.status_code == 200
        assert [it["tag"] for it in r.json()["items"]] == ["vip", "at_risk"]

    def test_add_tag_normalizes_to_lowercase_snake(self, client, auth, mock_session):
        mock_session.get.return_value = _make_subscriber(id=42)
        # No existing tag.
        mock_session.query.return_value.filter.return_value.first.return_value = None

        def _stamp(obj):
            obj.id = 7
            obj.created_at = datetime(2026, 5, 28, tzinfo=timezone.utc)
        mock_session.add.side_effect = _stamp

        r = client.post(
            "/api/admin/subscribers/42/tags",
            json={"tag": "  Coach Weekly  "},
            headers=auth,
        )
        assert r.status_code == 201
        body = r.json()
        assert body["tag"] == "coach_weekly"

    def test_add_existing_tag_is_idempotent(self, client, auth, mock_session):
        mock_session.get.return_value = _make_subscriber(id=42)
        existing = _row(id=5, tag="vip")
        mock_session.query.return_value.filter.return_value.first.return_value = existing
        r = client.post(
            "/api/admin/subscribers/42/tags",
            json={"tag": "vip"},
            headers=auth,
        )
        assert r.status_code == 201
        body = r.json()
        assert body["existing"] is True
        assert body["id"] == 5

    def test_add_tag_on_unknown_subscriber_returns_404(self, client, auth, mock_session):
        mock_session.get.return_value = None
        r = client.post(
            "/api/admin/subscribers/9999/tags",
            json={"tag": "vip"},
            headers=auth,
        )
        assert r.status_code == 404

    def test_add_tag_too_long_rejected(self, client, auth, mock_session):
        r = client.post(
            "/api/admin/subscribers/42/tags",
            json={"tag": "x" * 60},
            headers=auth,
        )
        assert r.status_code == 422

    def test_remove_tag_returns_204(self, client, auth, mock_session):
        existing = _row(id=5, tag="vip")
        mock_session.query.return_value.filter.return_value.first.return_value = existing
        r = client.delete("/api/admin/subscribers/42/tags/vip", headers=auth)
        assert r.status_code == 204
        assert mock_session.delete.called

    def test_remove_unknown_tag_returns_404(self, client, auth, mock_session):
        mock_session.query.return_value.filter.return_value.first.return_value = None
        r = client.delete("/api/admin/subscribers/42/tags/ghost", headers=auth)
        assert r.status_code == 404


# ════════════════════════════════════════════════════════════════════════════
# Tag suggestions
# ════════════════════════════════════════════════════════════════════════════


class TestTagSuggestions:

    def test_returns_curated_list_including_do_not_text(self, client, auth, mock_session):
        r = client.get("/api/admin/subscriber-tag-suggestions", headers=auth)
        assert r.status_code == 200
        tags = {it["tag"] for it in r.json()["items"]}
        # Anchor on the contract: do_not_text must be present and labeled.
        assert "do_not_text" in tags
        dnt = next(it for it in r.json()["items"] if it["tag"] == "do_not_text")
        assert "marketing" in dnt["effect"].lower()


# ════════════════════════════════════════════════════════════════════════════
# do_not_text — behavioral side-effect in sms_compliance.send_sms
# ════════════════════════════════════════════════════════════════════════════


@pytest.mark.integration
class TestDoNotTextSuppression:
    """The do_not_text tag must block marketing SMS but not transactional."""

    def _make_subscriber_row(self, db):
        from src.core.models import Subscriber
        s = Subscriber(
            stripe_customer_id="cus_dnt_test",
            email="dnt@example.com", phone="+15557654321",
            tier="pro", vertical="roofing", county_id="hillsborough",
            status="active",
        )
        db.add(s)
        db.flush()
        return s

    def _opt_in(self, db, phone):
        from src.core.models import SmsOptIn
        db.add(SmsOptIn(
            phone=phone, source="manual",
            opted_in_at=datetime.now(timezone.utc),
        ))
        db.flush()

    def test_do_not_text_blocks_marketing(self, fresh_db):
        from src.core.models import SubscriberTag
        from src.services.sms_compliance import send_sms

        sub = self._make_subscriber_row(fresh_db)
        self._opt_in(fresh_db, sub.phone)
        fresh_db.add(SubscriberTag(subscriber_id=sub.id, tag="do_not_text"))
        fresh_db.flush()

        # Dispatch dispatched via dispatcher patch so we don't hit Telnyx.
        with patch("src.services.sms_compliance.telnyx_send_message",
                   return_value={"message_id": "m1", "status": "queued", "vendor": "telnyx", "cost_cents": 0, "sent_at": "2026-01-01T00:00:00"}):
            sent = send_sms(
                to=sub.phone, body="marketing blast",
                db=fresh_db, message_type="marketing",
                subscriber_id=sub.id,
            )
        assert sent is False

    def test_do_not_text_does_not_block_transactional(self, fresh_db):
        from src.core.models import SubscriberTag
        from src.services.sms_compliance import send_sms

        sub = self._make_subscriber_row(fresh_db)
        fresh_db.add(SubscriberTag(subscriber_id=sub.id, tag="do_not_text"))
        fresh_db.flush()

        with patch("src.services.sms_compliance.telnyx_send_message",
                   return_value={"message_id": "m2", "status": "queued", "vendor": "telnyx", "cost_cents": 0, "sent_at": "2026-01-01T00:00:00"}):
            sent = send_sms(
                to=sub.phone, body="your payment failed",
                db=fresh_db, message_type="transactional",
                subscriber_id=sub.id,
            )
        # Transactional bypasses the do_not_text + opt-in + frequency gates.
        # It may still be False if quiet hours / dispatcher creds are not set,
        # so the strict assertion is "no do_not_text suppression":
        from src.core.models import SmsSendLog
        log = fresh_db.query(SmsSendLog).filter(
            SmsSendLog.subscriber_id == sub.id,
        ).order_by(SmsSendLog.id.desc()).first()
        assert log is not None
        assert log.suppress_reason != "do_not_text_tag"
