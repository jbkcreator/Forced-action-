"""
Branch-complete unit tests for the triangulation classifier (ADR 0015) and
the corroboration-aware freshness changes. Pure functions, no DB.
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from src.services.contact_freshness import compute_contact_freshness
from src.services.contact_triangulation import (
    PersonEvidence,
    _extract_emails,
    _extract_phones,
    _natural_name,
    compute_corroboration,
)

PHONE = "+18135551234"
OTHER = "+18135559999"


def person(name=None, phones=None, emails=None, mailing=None):
    return PersonEvidence(
        name=name,
        phones=phones or [],
        emails=emails or [],
        mailing_address=mailing,
    )


def voter(name="Gonzalez, Ana R", current=PHONE, history=None, email=None, active=True):
    return {
        "name": name,
        "phone_current": current,
        "phones_history": history or [],
        "email": email,
        "active": active,
    }


OWNER_NAMES = ["ANA R GONZALEZ"]


class TestPhoneCorroboration:

    def test_voter_match_with_name_agreement_is_strong(self):
        level, detail = compute_corroboration(
            [person(phones=[{"phone": PHONE, "source": "tracerfy", "kind": "mobile"}])],
            [voter()],
            OWNER_NAMES,
            [],
        )
        assert level == "strong"
        assert detail["rule_fired"] == "cross_source_name_match"
        assert detail["matched_phone"] == PHONE
        assert "voter_current" in detail["sources"]

    def test_phone_match_without_name_agreement_is_weak(self):
        """The tenant trap — voter Gorzen, owner Almenarez (real prod pair)."""
        level, detail = compute_corroboration(
            [person(phones=[{"phone": PHONE, "source": "tracerfy", "kind": "mobile"}])],
            [voter(name="Gorzen, Miguel")],
            ["LORENZO ALMENAREZ"],
            [],
        )
        assert level == "weak"
        assert detail["rule_fired"] == "phone_match_no_name_agreement"

    def test_historical_voter_phone_is_weak(self):
        level, detail = compute_corroboration(
            [person(phones=[{"phone": PHONE, "source": "tracerfy", "kind": "mobile"}])],
            [voter(current=OTHER, history=[PHONE])],
            OWNER_NAMES,
            [],
        )
        assert level == "weak"
        assert detail["rule_fired"] == "historical_voter_phone"

    def test_inactive_voter_is_weak(self):
        level, detail = compute_corroboration(
            [person(phones=[{"phone": PHONE, "source": "tracerfy", "kind": "mobile"}])],
            [voter(active=False)],
            OWNER_NAMES,
            [],
        )
        assert level == "weak"
        assert detail["rule_fired"] == "inactive_voter"

    def test_cross_provider_match_is_strong(self):
        level, detail = compute_corroboration(
            [person(phones=[
                {"phone": PHONE, "source": "tracerfy", "kind": "mobile"},
                {"phone": PHONE, "source": "batch_skip_tracing", "kind": "mobile"},
            ])],
            [],
            OWNER_NAMES,
            [],
        )
        assert level == "strong"
        assert detail["rule_fired"] == "cross_provider_match"

    def test_single_source_no_evidence_is_none(self):
        level, detail = compute_corroboration(
            [person(phones=[{"phone": PHONE, "source": "tracerfy", "kind": "mobile"}])],
            [],
            OWNER_NAMES,
            [],
        )
        assert level == "none"
        assert detail["rule_fired"] == "no_corroboration"


class TestIdentityAnchor:

    def test_mailing_agreement_lifts_single_source_mobile(self):
        level, detail = compute_corroboration(
            [person(
                phones=[{"phone": PHONE, "source": "batch_skip_tracing", "kind": "mobile"}],
                mailing="456 Pine Ave, Orlando, FL",
            )],
            [],
            OWNER_NAMES,
            ["456 PINE AVE ORLANDO FL"],
            {PHONE: {"reachable": True, "type": "mobile"}},
        )
        assert level == "strong"
        assert detail["rule_fired"] == "identity_anchor_match"

    def test_known_unreachable_mobile_not_lifted(self):
        level, _ = compute_corroboration(
            [person(
                phones=[{"phone": PHONE, "source": "batch_skip_tracing", "kind": "mobile"}],
                mailing="456 Pine Ave, Orlando, FL",
            )],
            [],
            OWNER_NAMES,
            ["456 PINE AVE ORLANDO FL"],
            {PHONE: {"reachable": False, "type": "mobile"}},
        )
        assert level == "none"

    def test_landline_not_lifted_by_anchor(self):
        level, _ = compute_corroboration(
            [person(
                phones=[{"phone": PHONE, "source": "tracerfy", "kind": "landline"}],
                mailing="456 Pine Ave, Orlando, FL",
            )],
            [],
            OWNER_NAMES,
            ["456 PINE AVE ORLANDO FL"],
        )
        assert level == "none"


class TestEmailCorroboration:

    def test_email_match_with_name_agreement_lifts_mobile(self):
        level, detail = compute_corroboration(
            [person(
                phones=[{"phone": PHONE, "source": "tracerfy", "kind": "mobile"}],
                emails=[{"email": "ana.g@example.com", "source": "tracerfy"}],
            )],
            [voter(current=None, email="ana.g@example.com")],
            OWNER_NAMES,
            [],
        )
        assert level == "strong"
        assert detail["rule_fired"] == "identity_anchor_match"
        assert detail["email_corroboration"] == "strong"
        assert detail["matched_email"] == "ana.g@example.com"

    def test_role_localpart_email_is_weak_only(self):
        level, detail = compute_corroboration(
            [person(
                phones=[{"phone": PHONE, "source": "tracerfy", "kind": "mobile"}],
                emails=[{"email": "info@example.com", "source": "tracerfy"}],
            )],
            [voter(current=None, email="info@example.com")],
            OWNER_NAMES,
            [],
        )
        assert level == "weak"
        assert detail["email_corroboration"] == "weak"

    def test_email_only_owner_records_email_axis(self):
        level, detail = compute_corroboration(
            [person(emails=[{"email": "ana.g@example.com", "source": "tracerfy"}])],
            [voter(current=None, email="ana.g@example.com")],
            OWNER_NAMES,
            [],
        )
        assert level == "weak"
        assert detail["rule_fired"] == "email_agreement_only"
        assert detail["email_corroboration"] == "strong"
        assert detail["matched_phone"] is None


class TestPerPersonGrouping:

    def test_heir_a_evidence_never_credits_heir_b(self):
        heir_a = person(
            name="Maria Lopez",
            phones=[{"phone": PHONE, "source": "tracerfy", "kind": "mobile"}],
        )
        heir_b = person(
            name="Carlos Lopez",
            phones=[{"phone": OTHER, "source": "tracerfy", "kind": "mobile"}],
        )
        level, detail = compute_corroboration(
            [heir_a, heir_b],
            [voter(name="Lopez, Maria", current=PHONE)],
            ["MARIA LOPEZ", "CARLOS LOPEZ"],
            [],
        )
        assert level == "strong"
        assert detail["person"] == "Maria Lopez"
        assert detail["matched_phone"] == PHONE

    def test_llc_owner_matches_managing_member_name(self):
        level, detail = compute_corroboration(
            [person(phones=[{"phone": PHONE, "source": "tracerfy", "kind": "mobile"}])],
            [voter(name="Lopez, Maria")],
            ["SUNSHINE HOLDINGS LLC", "Maria Lopez"],   # owner + managing member
            [],
        )
        assert level == "strong"
        assert detail["rule_fired"] == "cross_source_name_match"


class TestExtraction:

    def _row(self, **kw):
        defaults = dict(source="tracerfy", mobile_phone=None, landline=None,
                        email=None, raw_response=None)
        defaults.update(kw)
        return SimpleNamespace(**defaults)

    def test_junk_phone_excluded(self):
        row = self._row(mobile_phone="111111111")
        assert _extract_phones(row) == []

    def test_raw_response_extra_phones_and_emails_used(self):
        row = self._row(
            mobile_phone="8135551234",
            raw_response={
                "mobile_2": "813-555-9999",
                "landline_1": "(813) 555-7777",
                "email_2": "Second@Example.com",
            },
        )
        phones = _extract_phones(row)
        numbers = {p["phone"] for p in phones}
        assert numbers == {"+18135551234", "+18135559999", "+18135557777"}
        kinds = {p["phone"]: p["kind"] for p in phones}
        assert kinds["+18135559999"] == "mobile"
        assert kinds["+18135557777"] == "landline"
        assert _extract_emails(row) == [
            {"email": "second@example.com", "source": "tracerfy"}
        ]

    def test_voter_file_name_flip(self):
        assert _natural_name("Gonzalez, Ana R") == "Ana R Gonzalez"
        assert _natural_name("ANA R GONZALEZ") == "ANA R GONZALEZ"


class TestFreshnessCorroboration:
    """compute_contact_freshness with the new corroboration input."""

    NOW = datetime(2026, 6, 11, tzinfo=timezone.utc)

    def _owner(self, score=90, reachable=True, line="mobile"):
        return SimpleNamespace(
            phone_1=PHONE, phone_2=None, phone_3=None,
            phone_metadata={"phone_1": {"score": score, "reachable": reachable,
                                        "type": line}},
        )

    def _contact(self, age_days=10, confidence=0.0):
        return SimpleNamespace(
            confidence=confidence,
            enriched_at=self.NOW - timedelta(days=age_days),
            verification_status=None,
        )

    def test_none_corroboration_identical_to_legacy(self):
        legacy = compute_contact_freshness(self._owner(), self._contact(), now=self.NOW)
        explicit = compute_contact_freshness(
            self._owner(), self._contact(), now=self.NOW, corroboration=None
        )
        assert legacy == explicit

    def test_strong_relaxes_age_decay(self):
        """120 'real' days: uncorroborated drops to medium, strong stays high."""
        plain = compute_contact_freshness(
            self._owner(), self._contact(age_days=120), now=self.NOW
        )
        strong = compute_contact_freshness(
            self._owner(), self._contact(age_days=120), now=self.NOW,
            corroboration="strong",
        )
        assert plain.level == "medium"
        assert strong.level == "high"

    def test_weak_caps_at_medium(self):
        weak = compute_contact_freshness(
            self._owner(), self._contact(age_days=10), now=self.NOW,
            corroboration="weak",
        )
        assert weak.level == "medium"

    def test_sms_failure_override_beats_strong_corroboration(self):
        result = compute_contact_freshness(
            self._owner(), self._contact(age_days=10), now=self.NOW,
            last_sms_failed_at=self.NOW - timedelta(days=5),
            corroboration="strong",
        )
        assert result.level == "stale"
        assert result.reason == "recent_sms_failure"
