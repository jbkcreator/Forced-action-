"""
CLONE-v2.2 CL3 — src/utils/venture_config.py.

These tests deliberately NEVER commit. get_venture_config() accepts the
session it should read on, so every row here lives inside fresh_db's nested
transaction and disappears on rollback — no teardown helper, and no chance of
a `ventures` row leaking into later runs (the failure mode CL2's rollup test
hit when it committed inside the test body).

The cache is module-level state shared across tests, so `clean_cache`
autouse-flushes it before and after each one.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from config.settings import get_settings
from config.venture_template import DEFAULT_KILL_SWITCH_FEATURE, DEFAULT_VENTURE_KEY
from src.utils import venture_config
from src.utils.venture_config import get_venture_config, invalidate_cache


@pytest.fixture(autouse=True)
def clean_cache():
    invalidate_cache()
    yield
    invalidate_cache()


def _insert_venture(db, venture_key: str, **overrides) -> None:
    params = {
        "venture_key": venture_key,
        "display_name": f"Display {venture_key}",
        "brand_name": f"Brand {venture_key}",
        "postal_address": None,
        "state": "TX",
        "bankruptcy_court_code": "txnb",
        "default_bankruptcy_division": "4:",
        "template_county_id": None,
        "relay_slack_channel": "#venture-approvals",
        "relay_instantly_campaign_id": "camp_abc",
        "relay_instantly_sender_email": "hello@venture.example",
        "relay_send_window_start": 9,
        "relay_send_window_end": 17,
        "relay_send_window_timezone": "America/Chicago",
        "relay_daily_ceiling": 42,
        "kill_switch_feature": "relay_venture_two",
        "is_active": True,
    }
    params.update(overrides)
    db.execute(text("""
        INSERT INTO ventures (
            venture_key, display_name, brand_name, postal_address, state,
            bankruptcy_court_code, default_bankruptcy_division, template_county_id,
            relay_slack_channel, relay_approvers, relay_instantly_campaign_id,
            relay_instantly_sender_email, relay_send_window_start,
            relay_send_window_end, relay_send_window_timezone, relay_daily_ceiling,
            kill_switch_feature, is_active
        )
        VALUES (
            :venture_key, :display_name, :brand_name, :postal_address, :state,
            :bankruptcy_court_code, :default_bankruptcy_division, :template_county_id,
            :relay_slack_channel, '["U_ONE"]'::jsonb, :relay_instantly_campaign_id,
            :relay_instantly_sender_email, :relay_send_window_start,
            :relay_send_window_end, :relay_send_window_timezone, :relay_daily_ceiling,
            :kill_switch_feature, :is_active
        )
    """), params)


def test_db_row_wins_over_env(fresh_db):
    key = f"vc_{uuid.uuid4().hex[:8]}"
    _insert_venture(fresh_db, key)

    cfg = get_venture_config(key, session=fresh_db)

    assert cfg.venture_key == key
    assert cfg.brand_name == f"Brand {key}"
    assert cfg.state == "TX"
    assert cfg.bankruptcy_court_code == "txnb"
    assert cfg.default_bankruptcy_division == "4:"
    assert cfg.relay_slack_channel == "#venture-approvals"
    assert cfg.relay_approvers == ("U_ONE",)
    assert cfg.relay_instantly_campaign_id == "camp_abc"
    assert cfg.relay_send_window_start == 9
    assert cfg.relay_send_window_end == 17
    assert cfg.relay_send_window_timezone == "America/Chicago"
    assert cfg.relay_daily_ceiling == 42
    assert cfg.kill_switch_feature == "relay_venture_two"


def test_unknown_venture_falls_back_to_env(fresh_db):
    """A key with no row must resolve, not raise — Relay has to keep sending
    through a config-resolution problem."""
    settings = get_settings()

    cfg = get_venture_config(f"missing_{uuid.uuid4().hex[:8]}", session=fresh_db)

    assert cfg.state == "FL"
    assert cfg.bankruptcy_court_code == "flmb"
    assert cfg.kill_switch_feature == DEFAULT_KILL_SWITCH_FEATURE
    assert cfg.relay_send_window_start == settings.relay_send_window_start
    assert cfg.relay_send_window_end == settings.relay_send_window_end
    assert cfg.relay_send_window_timezone == settings.relay_send_window_timezone
    assert cfg.relay_daily_ceiling == settings.relay_daily_ceiling
    assert cfg.relay_slack_channel == settings.relay_slack_channel
    assert cfg.postal_address == settings.company_postal_address


def test_inactive_venture_resolves_to_a_disabled_config(fresh_db):
    """A deactivated venture must not keep governing sends -- and, per the
    PR #195 review finding, must NOT silently fall back to venture #1's
    env-backed Instantly identity either (the old bug: is_active=false
    rows missed _SELECT_VENTURE's `AND is_active = true` filter and were
    treated exactly like a missing row). Geography/branding are kept from
    the row -- they carry no send-dispatch risk and an operator
    re-activating the venture later still needs them intact."""
    key = f"vc_{uuid.uuid4().hex[:8]}"
    _insert_venture(fresh_db, key, is_active=False, state="TX")

    cfg = get_venture_config(key, session=fresh_db)

    assert cfg.is_active is False
    assert cfg.state == "TX"  # kept from the row, not env-fallback 'FL'
    assert cfg.relay_instantly_campaign_id is None
    assert cfg.relay_instantly_sender_email == ""


def test_deactivating_the_default_venture_also_clears_its_instantly_identity(fresh_db):
    """Even venture #1 must stop sending when deactivated -- is_default
    only controls the MISSING-value fallback in _from_row(); a deactivated
    row skips _from_row() entirely and never reaches that fallback."""
    fresh_db.execute(
        text("UPDATE ventures SET is_active = false WHERE venture_key = :vk"),
        {"vk": DEFAULT_VENTURE_KEY},
    )

    cfg = get_venture_config(DEFAULT_VENTURE_KEY, session=fresh_db)

    assert cfg.is_active is False
    assert cfg.relay_instantly_campaign_id is None
    assert cfg.relay_instantly_sender_email == ""


def test_missing_row_still_resolves_active_via_env_fallback(fresh_db):
    """A venture_key with NO row at all (unmigrated env, unknown key) is a
    different case from a deactivated row -- it must keep the pre-CL3
    env-fallback behavior, is_active=True included."""
    cfg = get_venture_config(f"missing_{uuid.uuid4().hex[:8]}", session=fresh_db)

    assert cfg.is_active is True


def test_null_row_columns_fall_back_to_env_per_field(fresh_db):
    """A venture that has not provisioned its Slack channel yet still
    resolves to a usable config from settings."""
    key = f"vc_{uuid.uuid4().hex[:8]}"
    settings = get_settings()
    _insert_venture(
        fresh_db, key,
        relay_slack_channel=None,
        relay_instantly_campaign_id=None,
        relay_instantly_sender_email=None,
        postal_address=None,
    )

    cfg = get_venture_config(key, session=fresh_db)

    assert cfg.state == "TX"  # the row still wins where it has values
    assert cfg.relay_slack_channel == settings.relay_slack_channel
    assert cfg.postal_address == settings.company_postal_address


def test_non_default_venture_never_falls_back_to_env_instantly_identity(monkeypatch, fresh_db):
    """A non-default venture with no Instantly campaign/sender configured
    must NOT inherit venture #1's RELAY_INSTANTLY_* env values — those are
    venture #1's outbound identity specifically. Sharing them would route
    this venture's email through venture #1's campaign (cross-venture sends,
    false duplicate-contact failures) instead of failing closed.

    Settings is monkeypatched with definitely-truthy env values (rather than
    relying on whatever the real .env happens to set) so this assertion is
    deterministic regardless of local dev configuration."""
    import config.settings as settings_module

    fake_settings = SimpleNamespace(
        relay_instantly_campaign_id="camp-venture-one-env",
        relay_instantly_sender_email="one@venture-one.example",
        company_postal_address="1 Env St",
        relay_slack_channel="#env-approvals",
        relay_approvers=["U_ENV"],
        relay_send_window_start=8,
        relay_send_window_end=20,
        relay_send_window_timezone="America/New_York",
        relay_daily_ceiling=99,
    )
    monkeypatch.setattr(settings_module, "get_settings", lambda: fake_settings)

    key = f"vc_{uuid.uuid4().hex[:8]}"
    _insert_venture(
        fresh_db, key,
        relay_instantly_campaign_id=None,
        relay_instantly_sender_email=None,
    )

    cfg = get_venture_config(key, session=fresh_db)

    assert cfg.relay_instantly_campaign_id is None
    assert cfg.relay_instantly_sender_email == ""


def test_default_venture_still_falls_back_to_env_instantly_identity(fresh_db):
    """Venture #1 keeps the pre-CL3 env-fallback behavior — this is what
    makes CL3 a no-op on day one for the existing venture. Updates the
    migration-seeded venture #1 row in place (it already exists) rather than
    inserting a second one, which would violate the venture_key uniqueness
    constraint."""
    settings = get_settings()
    fresh_db.execute(
        text("""
            UPDATE ventures
            SET relay_instantly_campaign_id = NULL, relay_instantly_sender_email = NULL
            WHERE venture_key = :vk
        """),
        {"vk": DEFAULT_VENTURE_KEY},
    )

    cfg = get_venture_config(DEFAULT_VENTURE_KEY, session=fresh_db)

    assert cfg.relay_instantly_campaign_id == settings.relay_instantly_campaign_id
    assert cfg.relay_instantly_sender_email == settings.relay_instantly_sender_email


def test_seeded_venture_one_matches_env(fresh_db):
    """The migration's venture #1 row must resolve to the same send window,
    ceiling and court the pre-CL3 code read straight off settings — this is
    the day-one no-op guarantee."""
    settings = get_settings()

    cfg = get_venture_config(DEFAULT_VENTURE_KEY, session=fresh_db)

    assert cfg.relay_send_window_start == settings.relay_send_window_start
    assert cfg.relay_send_window_end == settings.relay_send_window_end
    assert cfg.relay_send_window_timezone == settings.relay_send_window_timezone
    assert cfg.relay_daily_ceiling == settings.relay_daily_ceiling
    assert cfg.state == "FL"
    assert cfg.bankruptcy_court_code == "flmb"
    assert cfg.default_bankruptcy_division == "8:"
    assert cfg.kill_switch_feature == DEFAULT_KILL_SWITCH_FEATURE


def test_second_call_is_served_from_cache(fresh_db):
    key = f"vc_{uuid.uuid4().hex[:8]}"
    _insert_venture(fresh_db, key)
    first = get_venture_config(key, session=fresh_db)

    calls: list[str] = []
    original = venture_config._load_from_db
    venture_config._load_from_db = lambda k, session=None: calls.append(k) or original(k, session=session)
    try:
        second = get_venture_config(key, session=fresh_db)
    finally:
        venture_config._load_from_db = original

    assert calls == []          # never re-read
    assert second is first      # same cached object


def test_invalidate_cache_forces_a_reread(fresh_db):
    key = f"vc_{uuid.uuid4().hex[:8]}"
    _insert_venture(fresh_db, key, relay_daily_ceiling=42)
    assert get_venture_config(key, session=fresh_db).relay_daily_ceiling == 42

    fresh_db.execute(
        text("UPDATE ventures SET relay_daily_ceiling = 7 WHERE venture_key = :k"),
        {"k": key},
    )
    assert get_venture_config(key, session=fresh_db).relay_daily_ceiling == 42  # still cached

    invalidate_cache(key)
    assert get_venture_config(key, session=fresh_db).relay_daily_ceiling == 7


def test_invalidate_one_venture_leaves_the_others_cached(fresh_db):
    first, second = f"vc_a_{uuid.uuid4().hex[:6]}", f"vc_b_{uuid.uuid4().hex[:6]}"
    _insert_venture(fresh_db, first)
    _insert_venture(fresh_db, second)
    get_venture_config(first, session=fresh_db)
    get_venture_config(second, session=fresh_db)

    invalidate_cache(first)

    assert first not in venture_config._config_cache
    assert second in venture_config._config_cache


def test_two_ventures_resolve_independently(fresh_db):
    first, second = f"vc_a_{uuid.uuid4().hex[:6]}", f"vc_b_{uuid.uuid4().hex[:6]}"
    _insert_venture(fresh_db, first, state="TX", relay_daily_ceiling=42)
    _insert_venture(fresh_db, second, state="GA", relay_daily_ceiling=5)

    cfg_a = get_venture_config(first, session=fresh_db)
    cfg_b = get_venture_config(second, session=fresh_db)

    assert (cfg_a.state, cfg_a.relay_daily_ceiling) == ("TX", 42)
    assert (cfg_b.state, cfg_b.relay_daily_ceiling) == ("GA", 5)


def test_config_is_immutable():
    """The cached object is shared across callers, so nothing may mutate it."""
    from dataclasses import FrozenInstanceError

    cfg = get_venture_config("no_such_venture_for_immutability_check")
    with pytest.raises(FrozenInstanceError):
        cfg.relay_daily_ceiling = 999  # type: ignore[misc]


def test_db_failure_falls_back_to_env(monkeypatch, fresh_db):
    """A broken query must not take Relay down with it."""
    class _Boom:
        def execute(self, *args, **kwargs):
            raise RuntimeError("connection reset")

    cfg = venture_config._load_from_db("anything", session=_Boom())

    assert cfg.state == "FL"
    assert cfg.relay_daily_ceiling == get_settings().relay_daily_ceiling


def test_list_ventures_includes_the_seeded_venture_one():
    assert DEFAULT_VENTURE_KEY in venture_config.list_ventures()
