"""
CLONE-v2.2 CL3 — config/venture_template.py.

validate_venture_config() is pure, so these need no DB and no fixtures.
"""

from __future__ import annotations

from config.venture_template import (
    COUNTY_TEMPLATE,
    DEFAULT_KILL_SWITCH_FEATURE,
    DEFAULT_VENTURE_KEY,
    REQUIRED_SIGNAL_TYPES,
    VENTURE_TEMPLATE,
    new_venture_config,
    validate_venture_config,
)


def test_template_itself_validates():
    """The shipped template must be valid as-is, so `--emit-template` never
    hands anyone a config that fails before they have changed a thing."""
    assert validate_venture_config(VENTURE_TEMPLATE) == []


def test_default_venture_key_is_venture_one():
    assert DEFAULT_VENTURE_KEY == "hillsborough_distress"
    assert DEFAULT_KILL_SWITCH_FEATURE == "relay_global"
    assert VENTURE_TEMPLATE["kill_switch_feature"] == DEFAULT_KILL_SWITCH_FEATURE


def test_required_signal_types_non_empty_and_unique():
    assert REQUIRED_SIGNAL_TYPES
    assert len(set(REQUIRED_SIGNAL_TYPES)) == len(REQUIRED_SIGNAL_TYPES)


def test_missing_required_field_is_reported():
    cfg = new_venture_config(venture_key="")
    problems = validate_venture_config(cfg)
    assert any("venture_key is required" in p for p in problems)


def test_every_missing_field_is_reported_in_one_pass():
    """One message naming all the gaps, not one round trip per gap."""
    problems = validate_venture_config({})
    for field in ("venture_key", "display_name", "brand_name", "state"):
        assert any(p.startswith(field) for p in problems), field


def test_unknown_field_is_reported():
    cfg = new_venture_config()
    cfg["relay_slak_channel"] = "#typo"
    problems = validate_venture_config(cfg)
    assert any("unknown field" in p and "relay_slak_channel" in p for p in problems)


def test_uppercase_or_spaced_venture_key_is_reported():
    assert any(
        "lowercase" in p
        for p in validate_venture_config(new_venture_config(venture_key="Venture Two"))
    )


def test_non_two_letter_state_is_reported():
    assert any(
        "two-letter" in p
        for p in validate_venture_config(new_venture_config(state="Florida"))
    )


def test_inverted_send_window_is_reported():
    problems = validate_venture_config(
        new_venture_config(relay_send_window_start=18, relay_send_window_end=11)
    )
    assert any("must be less than" in p for p in problems)


def test_equal_send_window_bounds_is_reported():
    """start == end is an empty window — it sends nothing, so it is a
    configuration mistake rather than a way to disable sending (start=0/end=24
    is the documented off switch)."""
    problems = validate_venture_config(
        new_venture_config(relay_send_window_start=12, relay_send_window_end=12)
    )
    assert any("must be less than" in p for p in problems)


def test_out_of_range_send_window_is_reported():
    assert any(
        "0..24" in p
        for p in validate_venture_config(new_venture_config(relay_send_window_start=25))
    )
    assert any(
        "0..24" in p
        for p in validate_venture_config(new_venture_config(relay_send_window_end=-1))
    )


def test_full_day_window_is_valid():
    """start=0/end=24 is the documented way to disable the window."""
    cfg = new_venture_config(relay_send_window_start=0, relay_send_window_end=24)
    assert validate_venture_config(cfg) == []


def test_non_positive_ceiling_is_reported():
    for ceiling in (0, -5):
        assert any(
            "positive int" in p
            for p in validate_venture_config(new_venture_config(relay_daily_ceiling=ceiling))
        )


def test_boolean_is_not_accepted_as_a_ceiling():
    """bool is an int subclass in Python, so a config carrying `true` would
    silently become a ceiling of 1 without an explicit type check."""
    problems = validate_venture_config(new_venture_config(relay_daily_ceiling=True))
    assert any("positive int" in p for p in problems)


def test_campaign_without_sender_email_is_reported():
    cfg = new_venture_config(
        relay_instantly_campaign_id="camp_123", relay_instantly_sender_email=None
    )
    assert any("relay_instantly_sender_email is required" in p for p in validate_venture_config(cfg))


def test_campaign_with_sender_email_is_valid():
    cfg = new_venture_config(
        relay_instantly_campaign_id="camp_123",
        relay_instantly_sender_email="hello@venture2.example",
    )
    assert validate_venture_config(cfg) == []


def test_non_list_approvers_is_reported():
    assert any(
        "must be a list" in p
        for p in validate_venture_config(new_venture_config(relay_approvers="U123"))
    )


def test_new_venture_config_does_not_share_mutable_state_with_the_template():
    """A caller mutating its own copy's lists must not corrupt the template
    for the next caller."""
    first = new_venture_config()
    first["relay_approvers"].append("U_LEAK")
    second = new_venture_config()
    assert second["relay_approvers"] == []
    assert VENTURE_TEMPLATE["relay_approvers"] == []


def test_county_template_has_the_keys_provisioning_writes():
    for key in ("county_id", "display_name", "zip_prefixes", "source_url_overrides"):
        assert key in COUNTY_TEMPLATE
