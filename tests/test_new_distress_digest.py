"""
T-B12-03 — New Distress Digest job tests.

Unit/integration only (read-path query functions) — avoids exercising the
commit-per-send loops against the shared dev DB (those touch sent_leads /
waitlist_entries state and are covered by manual --dry-run verification per
project convention: DB-backed suite is slow, run narrow subsets).
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from src.core.models import DistressScore, Property, SentLead, Subscriber, WaitlistEntry
from src.tasks.new_distress_digest import (
    _new_lead_count_for_waitlist,
    _new_leads_for_subscriber,
    _render_waitlist_teaser,
)


def _rand_zip() -> str:
    return "9" + f"{uuid.uuid4().int % 10000:04d}"


def _property(db, *, zip_code, county_id="hillsborough"):
    tag = uuid.uuid4().hex[:12]
    p = Property(parcel_id=f"ndd-{tag}", zip=zip_code, county_id=county_id)
    db.add(p)
    db.flush()
    return p


def _qualified_score(db, prop, *, vertical="roofing", score_date=None):
    ds = DistressScore(
        property_id=prop.id, qualified=True, lead_tier="Gold",
        vertical_scores={vertical: 90},
        score_date=score_date or datetime.now(timezone.utc),
    )
    db.add(ds)
    db.flush()
    return ds


def _subscriber(db, *, vertical="roofing", county_id="hillsborough"):
    tag = uuid.uuid4().hex[:8]
    s = Subscriber(
        stripe_customer_id=f"cus_ndd_{tag}", tier="starter", vertical=vertical,
        county_id=county_id, status="active", email=f"ndd_{tag}@example.com",
    )
    db.add(s)
    db.flush()
    return s


class TestNewLeadsForSubscriber:
    def test_excludes_already_sent_leads(self, fresh_db):
        db = fresh_db
        z = _rand_zip()
        sub = _subscriber(db)

        already_sent = _property(db, zip_code=z)
        _qualified_score(db, already_sent, vertical=sub.vertical)
        db.add(SentLead(subscriber_id=sub.id, property_id=already_sent.id, source="daily_email"))

        new_lead = _property(db, zip_code=z)
        _qualified_score(db, new_lead, vertical=sub.vertical)
        db.flush()

        leads = _new_leads_for_subscriber(db, sub, [z])
        lead_ids = {l["property_id"] for l in leads}

        assert new_lead.id in lead_ids
        assert already_sent.id not in lead_ids

    def test_empty_zip_list_returns_no_leads(self, fresh_db):
        db = fresh_db
        sub = _subscriber(db)
        assert _new_leads_for_subscriber(db, sub, []) == []

    def test_only_matches_subscriber_vertical(self, fresh_db):
        db = fresh_db
        z = _rand_zip()
        sub = _subscriber(db, vertical="roofing")

        wrong_vertical = _property(db, zip_code=z)
        _qualified_score(db, wrong_vertical, vertical="wholesalers")
        db.flush()

        leads = _new_leads_for_subscriber(db, sub, [z])
        assert not any(l["property_id"] == wrong_vertical.id for l in leads)


class TestNewLeadCountForWaitlist:
    def test_counts_only_leads_scored_after_last_notify(self, fresh_db):
        db = fresh_db
        z = _rand_zip()
        entry = WaitlistEntry(
            zip_code=z, vertical="roofing", county_id="hillsborough",
            name="Test", email=f"wl_{uuid.uuid4().hex[:8]}@example.com",
            notified_email_at=datetime.now(timezone.utc) - timedelta(days=1),
        )
        db.add(entry)
        db.flush()

        stale = _property(db, zip_code=z)
        _qualified_score(db, stale, score_date=datetime.now(timezone.utc) - timedelta(days=2))

        fresh = _property(db, zip_code=z)
        _qualified_score(db, fresh, score_date=datetime.now(timezone.utc))
        db.flush()

        count, example = _new_lead_count_for_waitlist(db, entry)
        assert count == 1
        assert example is not None

    def test_zero_when_no_new_leads(self, fresh_db):
        db = fresh_db
        z = _rand_zip()
        entry = WaitlistEntry(
            zip_code=z, vertical="roofing", county_id="hillsborough",
            name="Test", email=f"wl_{uuid.uuid4().hex[:8]}@example.com",
        )
        db.add(entry)
        db.flush()

        count, example = _new_lead_count_for_waitlist(db, entry)
        assert count == 0
        assert example is None


class TestRenderWaitlistTeaser:
    def test_masks_example_address(self):
        entry = type("E", (), {"vertical": "roofing", "zip_code": "33629"})()
        subject, html, plain = _render_waitlist_teaser(entry, 3, {"address": "1234 Oak Street"})
        assert "1234" in html
        assert "Oak" not in html
        assert "3 new" in subject
        assert "unlock to see" in subject.lower()
