"""
Density-only HTTP shape-lock test for the team-view endpoint.

The referral-team mechanic gives 3 cross-referenced subscribers a SHARED
view of ZIP density for the team's vertical. The platform invariant is
that no PII or lead-detail field ever crosses team-member boundaries.

This test pins the response shape so the next person who adds a field to
`/api/feed/{feed_uuid}/team-view` has to deliberately update the allow
list — they cannot silently leak owner names, addresses, phones, or any
other per-lead detail through the team channel.

Run:
    pytest tests/test_team_view_shape.py -v
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from src.core.models import (
    DistressScore,
    Property,
    ReferralTeam,
    Subscriber,
)


# Allowed top-level response keys across every shape the endpoint can return
# (active team, no team, broken team). Anything outside this set is treated
# as a potential PII regression and fails the test.
ALLOWED_TOP_KEYS = {
    "unlocked",
    "team_id",
    "county_id",
    "vertical",
    "shared_zips",
    "density",
    "status",
    "broken_at",
    "broken_reason",
}

# Each density row may ONLY carry the ZIP + count. No address, owner, score,
# property_id, etc. — they would re-identify a lead through team channels.
ALLOWED_DENSITY_KEYS = {"zip", "leads"}

# Substrings that, if present anywhere in the serialized JSON, strongly
# suggest a PII / lead-detail leak. Kept generous on purpose.
FORBIDDEN_SUBSTRINGS = (
    "owner_name",
    "phone_1",
    "phone",
    "email",
    "address",
    "street",
    "first_name",
    "last_name",
    "cds_score",
    "score",
    "property_id",
    "lat",
    "lon",
    "incidents",
    "distress_types",
)


@pytest.fixture(scope="module")
def client():
    from src.api.main import app
    return TestClient(app)


def _mk_sub(db, *, suffix: str, county: str = "hillsborough",
            vertical: str = "roofing") -> Subscriber:
    sub = Subscriber(
        stripe_customer_id=f"cus_tv_{suffix}",
        tier="starter",
        vertical=vertical,
        county_id=county,
        event_feed_uuid=f"tv-{suffix}",
        name="Test User",
        status="active",
    )
    db.add(sub)
    db.flush()
    return sub


def _mk_property_with_score(db, *, zip_code: str, county: str,
                            vertical: str, score: float) -> Property:
    """Create a property + qualified DistressScore so it counts toward density."""
    prop = Property(
        parcel_id=f"P{uuid.uuid4().hex[:10]}",
        zip=zip_code,
        county_id=county,
        address=f"123 Test St #{uuid.uuid4().hex[:4]}",
        city="Tampa",
        state="FL",
    )
    db.add(prop)
    db.flush()
    db.add(DistressScore(
        property_id=prop.id,
        county_id=county,
        score_date=datetime.now(timezone.utc),
        cds_score=score,
        lead_tier="Gold",
        qualified=True,
        vertical_scores={vertical: score},
        factor_scores={},
    ))
    db.flush()
    return prop


def _build_team(db, *, county: str = "hillsborough",
                vertical: str = "roofing",
                shared_zips=("33601", "33602")) -> tuple[Subscriber, ReferralTeam]:
    suffix = uuid.uuid4().hex[:8]
    lead = _mk_sub(db, suffix=f"lead_{suffix}", county=county, vertical=vertical)
    m1 = _mk_sub(db, suffix=f"m1_{suffix}", county=county, vertical=vertical)
    m2 = _mk_sub(db, suffix=f"m2_{suffix}", county=county, vertical=vertical)

    # Seed each shared ZIP with at least one qualified Gold lead so density>0
    for z in shared_zips:
        _mk_property_with_score(
            db, zip_code=z, county=county, vertical=vertical, score=78.0,
        )

    team = ReferralTeam(
        lead_subscriber_id=lead.id,
        county_id=county,
        vertical=vertical,
        member_subscriber_ids=[lead.id, m1.id, m2.id],
        shared_zips=list(shared_zips),
        status="active",
    )
    db.add(team)
    db.flush()
    return lead, team


def _serialized_lower(payload) -> str:
    import json
    return json.dumps(payload, default=str).lower()


# ─────────────────────────────────────────────────────────────────────────────
# Shape-lock assertions
# ─────────────────────────────────────────────────────────────────────────────


class TestTeamViewShapeLock:
    def test_active_team_response_is_density_only(self, client, fresh_db):
        lead, team = _build_team(fresh_db, shared_zips=("33601", "33602"))
        fresh_db.commit()
        try:
            resp = client.get(f"/api/feed/{lead.event_feed_uuid}/team-view")
            assert resp.status_code == 200, resp.text
            body = resp.json()

            # Top-level keys are pinned to the documented allow-list
            unknown = set(body.keys()) - ALLOWED_TOP_KEYS
            assert not unknown, (
                f"team-view introduced un-vetted top-level field(s) {unknown}. "
                "If this is intentional, audit for PII / lead-detail leak "
                "across team-member boundaries first, then update ALLOWED_TOP_KEYS."
            )

            assert body["unlocked"] is True
            assert body["county_id"] == team.county_id
            assert body["vertical"] == team.vertical
            assert sorted(body["shared_zips"]) == sorted(team.shared_zips)

            # Density rows are density-only
            assert isinstance(body["density"], list)
            for row in body["density"]:
                row_unknown = set(row.keys()) - ALLOWED_DENSITY_KEYS
                assert not row_unknown, (
                    f"team-view density row leaked field(s) {row_unknown}. "
                    "Density must be ONLY {zip, leads} — never address, owner, "
                    "score, property_id, etc."
                )
                assert isinstance(row["zip"], str)
                assert isinstance(row["leads"], int)
                assert row["leads"] >= 0

            # Generic PII grep on the full payload — catches anything we forgot
            # to enumerate explicitly
            serialized = _serialized_lower(body)
            for needle in FORBIDDEN_SUBSTRINGS:
                assert needle not in serialized, (
                    f"team-view payload contains forbidden token '{needle}'. "
                    "PII or lead-detail leak suspected."
                )
        finally:
            fresh_db.rollback()

    def test_no_team_response_is_still_density_only(self, client, fresh_db):
        """Subscriber with no team must still return the safe density-only shape."""
        suffix = uuid.uuid4().hex[:8]
        loner = _mk_sub(fresh_db, suffix=f"loner_{suffix}")
        fresh_db.commit()
        try:
            resp = client.get(f"/api/feed/{loner.event_feed_uuid}/team-view")
            assert resp.status_code == 200, resp.text
            body = resp.json()

            unknown = set(body.keys()) - ALLOWED_TOP_KEYS
            assert not unknown, f"no-team path introduced un-vetted field(s): {unknown}"
            assert body["unlocked"] is False
            assert body["density"] == []
            assert body["shared_zips"] == []
        finally:
            fresh_db.rollback()

    def test_invalid_feed_uuid_does_not_leak_shape(self, client):
        """Bad feed_uuid must 403, never echo a partial team payload."""
        resp = client.get("/api/feed/definitely-not-a-real-uuid/team-view")
        assert resp.status_code == 403
        # 403 body should NOT contain density or team data — it's an error envelope
        body = resp.json()
        assert "density" not in body
        assert "shared_zips" not in body
