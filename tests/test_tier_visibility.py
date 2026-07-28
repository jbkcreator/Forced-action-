"""
Stage A — tier_visibility stopgap.

Pinellas tier labels are hidden from subscriber-facing surfaces (feed API,
GHL contact custom fields/tags, Lifecycle SMS prompts) until the cross-county
calibration retune lands. These tests pin the suppression contract so a
future config edit can't silently re-expose the broken labels.
"""

from unittest.mock import MagicMock, patch

from config.scoring import SCORING_CONFIG, for_county


# ---------------------------------------------------------------------------
# Config — tier_visibility resolution
# ---------------------------------------------------------------------------

class TestTierVisibilityConfig:

    def test_hillsborough_is_public(self):
        assert for_county("hillsborough").tier_visibility == "public"

    def test_pinellas_is_internal(self):
        assert for_county("pinellas").tier_visibility == "internal"

    def test_unknown_county_defaults_to_public(self):
        # No override -> falls back to the global ScoringConfig default.
        assert for_county("orange").tier_visibility == "public"

    def test_none_county_defaults_to_public(self):
        assert for_county(None).tier_visibility == "public"

    def test_global_default_is_public(self):
        # Belt-and-suspenders: the default in the dataclass must be "public"
        # so a newly added county without overrides exposes tiers normally.
        assert SCORING_CONFIG.tier_visibility == "public"


# ---------------------------------------------------------------------------
# Feed API helper — _visible_tier_fields
# ---------------------------------------------------------------------------

class TestVisibleTierFields:

    def _mk_prop(self, county_id):
        prop = MagicMock()
        prop.county_id = county_id
        return prop

    def _mk_score(self, tier="Ultra Platinum", urgency="immediate"):
        score = MagicMock()
        score.lead_tier = tier
        score.urgency_level = urgency
        return score

    def test_hillsborough_lead_exposes_tier(self):
        from src.api.main import _visible_tier_fields
        tier, urgency = _visible_tier_fields(
            self._mk_prop("hillsborough"), self._mk_score("Gold", "daily")
        )
        assert tier == "Gold"
        assert urgency == "daily"

    def test_pinellas_lead_suppresses_tier_and_urgency(self):
        from src.api.main import _visible_tier_fields
        tier, urgency = _visible_tier_fields(
            self._mk_prop("pinellas"), self._mk_score("Ultra Platinum", "immediate")
        )
        assert tier is None
        assert urgency is None

    def test_unknown_county_exposes_tier(self):
        from src.api.main import _visible_tier_fields
        tier, urgency = _visible_tier_fields(
            self._mk_prop("orange"), self._mk_score("Platinum", "daily")
        )
        assert tier == "Platinum"
        assert urgency == "daily"


# ---------------------------------------------------------------------------
# GHL contact upsert — tier custom field + cds-* tag
# ---------------------------------------------------------------------------

def _ghl_score_data(county_id, lead_tier="Ultra Platinum"):
    """Build the minimal score_data dict that _upsert_contact reads."""
    return {
        "parcel_id":         "U-123",
        "county_id":         county_id,
        "owner_name":        "Jane Doe",
        "address":           "123 Main St",
        "city":              "Tampa",
        "state":             "FL",
        "zip":               "33647",
        "final_cds_score":   95,
        "lead_tier":         lead_tier,
        "urgency_level":     "immediate",
        "vertical_scores":   {"wholesalers": 95.0},
        "distress_types":    ["foreclosures"],
        "signal_count":      1,
        "owner_phone":       None,
        "owner_email":       None,
        "owner_type":        "individual",
        "absentee_status":   "In-County",
        "mailing_address":   "123 Main St",
        "ownership_years":   5,
        "assessed_value_mkt": 250000,
        "homestead_exempt":  False,
        "est_equity":        100000,
        "equity_pct":        40.0,
        "last_sale_price":   200000,
        "last_sale_date":    "2020-01-15",
        "sq_ft":             1500,
        "beds":              3,
        "baths":             2,
        "year_built":        1990,
        "lot_size":          0.25,
        "signal_summaries":  {},
        "ghl_contact_id":    None,
    }


def _build_ghl_payload(score_data):
    """Run _upsert_contact up to the GHL HTTP call, capture the payload."""
    from src.services import ghl_webhook

    captured = {}

    def _fake_request(method, url, **kwargs):
        captured["payload"] = kwargs.get("json")
        # Return a "not found" so _upsert_contact takes the create path and
        # returns a fresh ID — captured payload is the create body.
        resp = MagicMock()
        resp.status_code = 201
        resp.json.return_value = {"contact": {"id": "fake-contact-id"}}
        resp.raise_for_status.return_value = None
        return resp

    # Bypass the search-by-parcel branch so we go straight to the create call.
    with patch.object(ghl_webhook, "_ghl_request", side_effect=_fake_request), \
         patch.object(ghl_webhook, "_find_contact_by_parcel", return_value=None), \
         patch.object(ghl_webhook, "_is_configured", return_value=True):
        ghl_webhook._upsert_contact(score_data)

    return captured.get("payload") or {}


class TestGHLTierSuppression:

    TIER_CUSTOM_FIELD_ID = "x2gdIlD8v1mMTt1kZKEI"
    URGENCY_CUSTOM_FIELD_ID = "3AHU9KWEyXaDNKFy3azC"

    def _custom_field(self, payload, field_id):
        for cf in payload.get("customFields", []):
            if cf.get("id") == field_id:
                return cf.get("value")
        return None

    def test_hillsborough_sends_tier_field(self):
        payload = _build_ghl_payload(_ghl_score_data("hillsborough", "Gold"))
        assert self._custom_field(payload, self.TIER_CUSTOM_FIELD_ID) == "Gold"
        assert self._custom_field(payload, self.URGENCY_CUSTOM_FIELD_ID) == "immediate"
        assert "cds-gold" in payload["tags"]
        assert "synthflow-eligible" in payload["tags"]

    def test_pinellas_suppresses_tier_field(self):
        payload = _build_ghl_payload(_ghl_score_data("pinellas", "Ultra Platinum"))
        # Tier and urgency custom fields are emptied — the field still appears
        # (GHL doesn't have a per-call "omit field" API) but carries no value.
        assert self._custom_field(payload, self.TIER_CUSTOM_FIELD_ID) == ""
        assert self._custom_field(payload, self.URGENCY_CUSTOM_FIELD_ID) == ""
        # No cds-* tag, no synthflow-eligible/synthflow-suppress decision.
        assert not any(t.startswith("cds-") for t in payload["tags"])
        assert "synthflow-eligible" not in payload["tags"]
        assert "synthflow-suppress" not in payload["tags"]
        # The non-tier tags are still emitted.
        assert "distressed-property" in payload["tags"]


# ---------------------------------------------------------------------------
# Lifecycle prompt context — tier_visibility suppression
# ---------------------------------------------------------------------------

class TestLifecycleFOMOTierSuppression:

    def _fomo_state(self, county_id, lead_tier="Ultra Platinum"):
        return {
            "subscriber_id": 1,
            "decision_id": "d1",
            "event_payload": {
                "zip_code": "33647",
                "vertical": "wholesalers",
                "lead_tier": lead_tier,
            },
            "subscriber_profile": {
                "id": 1,
                "name": "Jane Doe",
                "vertical": "wholesalers",
                "county_id": county_id,
            },
            "zip_activity": {"active_viewers": 3},
        }

    def test_hillsborough_keeps_tier_in_competitor_signal(self):
        from src.agents.graphs.fomo import _node_build_compose_context

        with patch("src.agents.graphs.fomo.render_for_subscriber_auto",
                   return_value=("sys", "usr", None, None)), \
             patch("src.agents.graphs.fomo.render_fallback_body",
                   return_value="fb"):
            out = _node_build_compose_context(
                self._fomo_state("hillsborough", "Gold")
            )

        ctx = out["_render_context"]
        assert ctx["lead_tier"] == "Gold"
        assert "Gold lead" in ctx["competitor_signal"]

    def test_pinellas_replaces_tier_with_neutral_phrase(self):
        from src.agents.graphs.fomo import _node_build_compose_context, _TIER_SUPPRESSED_PHRASE

        with patch("src.agents.graphs.fomo.render_for_subscriber_auto",
                   return_value=("sys", "usr", None, None)), \
             patch("src.agents.graphs.fomo.render_fallback_body",
                   return_value="fb"):
            out = _node_build_compose_context(
                self._fomo_state("pinellas", "Ultra Platinum")
            )

        ctx = out["_render_context"]
        assert ctx["lead_tier"] == _TIER_SUPPRESSED_PHRASE
        assert "Ultra Platinum" not in ctx["competitor_signal"]
        assert _TIER_SUPPRESSED_PHRASE in ctx["competitor_signal"]


class TestLifecycleAbandonmentTierSuppression:

    def _w2_state(self, county_id, lead_tier_viewed="Gold"):
        return {
            "subscriber_id": 1,
            "decision_id": "d2",
            "event_payload": {
                "lead_tier_viewed": lead_tier_viewed,
                "wall_countdown_minutes": 2,
            },
            "subscriber_profile": {
                "id": 1,
                "name": "Jane Doe",
                "county_id": county_id,
            },
            "zip_activity": {"active_viewers": 3},
        }

    def test_hillsborough_keeps_viewed_tier(self):
        from src.agents.graphs.abandonment import _wave2_build_context

        with patch("src.agents.graphs.abandonment.render_for_subscriber_auto",
                   return_value=("sys", "usr", None, None)) as rfs, \
             patch("src.agents.graphs.abandonment.render_fallback_body",
                   return_value="fb"):
            _wave2_build_context(self._w2_state("hillsborough", "Gold"))
            args, _ = rfs.call_args
            # render_for_subscriber_auto(graph_name, subscriber_id, ctx)
            ctx = args[2]
            assert ctx["lead_tier_viewed"] == "Gold"

    def test_pinellas_replaces_viewed_tier_with_neutral_phrase(self):
        from src.agents.graphs.abandonment import (
            _wave2_build_context, _TIER_SUPPRESSED_PHRASE,
        )

        with patch("src.agents.graphs.abandonment.render_for_subscriber_auto",
                   return_value=("sys", "usr", None, None)) as rfs, \
             patch("src.agents.graphs.abandonment.render_fallback_body",
                   return_value="fb"):
            _wave2_build_context(self._w2_state("pinellas", "Ultra Platinum"))
            args, _ = rfs.call_args
            ctx = args[2]
            assert ctx["lead_tier_viewed"] == _TIER_SUPPRESSED_PHRASE
