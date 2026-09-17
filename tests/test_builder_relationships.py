"""WP-T2-8 Stage F — RELATIONSHIPS Slack queue output tests."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest import mock

from sqlalchemy import text

from src.services.builder_patterns import BuilderHit
from src.services.builder_relationships import (
    build_relationships_blocks,
    build_relationships_text,
    emit_relationships_alert,
    is_relationships_candidate,
    surface_relationship_hits,
)
from src.services.builder_sizing import BuilderSizingResult


def _hit(pattern: str, **kwargs) -> BuilderHit:
    defaults = dict(
        buyer_entity_id=42,
        principal_name="ACME BUILDERS LLC",
        evidence_permit_ids=[1, 2, 3],
        staging_permit_ids=[],
        county_id="hillsborough",
        latest_permit_date=date(2026, 6, 1),
        total_job_value=Decimal("300000"),
        property_id=99,
    )
    defaults.update(kwargs)
    return BuilderHit(pattern=pattern, **defaults)


def _sizing(loan: Decimal = Decimal("255000"), source: str = "job_value") -> BuilderSizingResult:
    return BuilderSizingResult(
        buyer_entity_id=42,
        pattern="repeat_builder",
        estimated_loan=loan,
        confidence="medium",
        sizing_source=source,
    )


# ── is_relationships_candidate ────────────────────────────────────────────────

def test_repeat_builder_is_candidate():
    assert is_relationships_candidate(_hit("repeat_builder"))


def test_spec_cadence_is_candidate():
    assert is_relationships_candidate(_hit("spec_cadence"))


def test_concurrent_builder_not_candidate():
    assert not is_relationships_candidate(_hit("concurrent_builder"))


def test_townhome_infill_not_candidate():
    assert not is_relationships_candidate(_hit("townhome_infill"))


def test_land_to_permit_not_candidate():
    assert not is_relationships_candidate(_hit("land_to_permit"))


# ── build_relationships_blocks ────────────────────────────────────────────────

def test_blocks_header_contains_name_and_pattern():
    blocks = build_relationships_blocks(_hit("repeat_builder"), _sizing())
    section = blocks[0]
    assert section["type"] == "section"
    assert "*ACME BUILDERS LLC*" in section["text"]["text"]
    assert "Repeat Builder" in section["text"]["text"]


def test_blocks_fields_contain_permit_count_and_loan():
    blocks = build_relationships_blocks(_hit("repeat_builder"), _sizing(Decimal("255000")))
    field_block = next(b for b in blocks if b["type"] == "section" and "fields" in b)
    field_texts = [f["text"] for f in field_block["fields"]]
    assert any("3 permits" in t for t in field_texts)
    assert any("$255,000" in t for t in field_texts)


def test_blocks_loan_formats_millions():
    blocks = build_relationships_blocks(_hit("repeat_builder"), _sizing(Decimal("1_400_000")))
    field_block = next(b for b in blocks if b["type"] == "section" and "fields" in b)
    assert any("$1.40M" in f["text"] for f in field_block["fields"])


def test_blocks_unknown_loan_when_no_sizing():
    blocks = build_relationships_blocks(_hit("repeat_builder"), sizing=None)
    field_block = next(b for b in blocks if b["type"] == "section" and "fields" in b)
    assert any("Unknown" in f["text"] for f in field_block["fields"])


def test_blocks_actions_present_with_primary_and_danger():
    blocks = build_relationships_blocks(_hit("repeat_builder"), _sizing())
    action_block = next(b for b in blocks if b["type"] == "actions")
    styles = [e.get("style") for e in action_block["elements"]]
    assert "primary" in styles
    assert "danger" in styles


def test_blocks_footer_contains_lane_and_source():
    blocks = build_relationships_blocks(_hit("repeat_builder"), _sizing())
    footer = next(b for b in blocks if b["type"] == "context")
    assert "RELATIONSHIPS" in footer["elements"][0]["text"]
    assert "WP-T2-8" in footer["elements"][0]["text"]


# ── build_relationships_text ──────────────────────────────────────────────────

def test_plain_text_contains_name_and_loan():
    text = build_relationships_text(_hit("repeat_builder"), _sizing(Decimal("255000")))
    assert "ACME BUILDERS LLC" in text
    assert "$255,000" in text
    assert "Repeat Builder" in text


# ── emit_relationships_alert ──────────────────────────────────────────────────

def test_emit_noops_when_unconfigured():
    fake = mock.MagicMock()
    fake.slack_bot_token = None
    fake.fa_max_slack_channel_relationships = ""
    with mock.patch("src.services.builder_relationships.get_settings", return_value=fake):
        with mock.patch("slack_sdk.WebClient") as web:
            emit_relationships_alert(_hit("repeat_builder"))
            web.assert_not_called()


def test_emit_posts_when_configured():
    fake = mock.MagicMock()
    fake.slack_bot_token.get_secret_value.return_value = "xoxb-test"
    fake.fa_max_slack_channel_relationships = "C0C2BRYKU4C"
    with mock.patch("src.services.builder_relationships.get_settings", return_value=fake):
        with mock.patch("slack_sdk.WebClient") as web_cls:
            client = web_cls.return_value
            emit_relationships_alert(_hit("repeat_builder"), _sizing())
            client.chat_postMessage.assert_called_once()
            call_kwargs = client.chat_postMessage.call_args.kwargs
            assert call_kwargs["channel"] == "C0C2BRYKU4C"
            assert "blocks" in call_kwargs
            assert "text" in call_kwargs


def test_surface_relationship_hits_posts_each_event_once(fresh_db, monkeypatch):
    fresh_db.execute(text("""
        CREATE TABLE builder_relationship_alerts (
            buyer_entity_id BIGINT NOT NULL,
            pattern VARCHAR(32) NOT NULL,
            latest_permit_date DATE NOT NULL,
            surfaced_at TIMESTAMP,
            PRIMARY KEY (buyer_entity_id, pattern, latest_permit_date)
        )
    """))
    fresh_db.commit()
    posted = []
    monkeypatch.setattr(
        "src.services.builder_relationships.emit_relationships_alert",
        lambda hit, sizing=None: posted.append(hit) or True,
    )
    hit = _hit("repeat_builder")

    assert surface_relationship_hits(fresh_db, [hit]) == 1
    assert surface_relationship_hits(fresh_db, [hit]) == 0
    assert posted == [hit]


def test_surface_relationship_hits_retries_when_delivery_fails(fresh_db, monkeypatch):
    fresh_db.execute(text("""
        CREATE TABLE builder_relationship_alerts (
            buyer_entity_id BIGINT NOT NULL,
            pattern VARCHAR(32) NOT NULL,
            latest_permit_date DATE NOT NULL,
            surfaced_at TIMESTAMP,
            PRIMARY KEY (buyer_entity_id, pattern, latest_permit_date)
        )
    """))
    fresh_db.commit()
    monkeypatch.setattr(
        "src.services.builder_relationships.emit_relationships_alert",
        lambda hit, sizing=None: False,
    )
    hit = _hit("repeat_builder")

    assert surface_relationship_hits(fresh_db, [hit]) == 0
    assert surface_relationship_hits(fresh_db, [hit]) == 0
    count = fresh_db.execute(text("SELECT COUNT(*) FROM builder_relationship_alerts")).scalar_one()
    assert count == 0
