"""WP-T3-4 — sequence content loader/validation. No DB required (plan
Section 6.8): parse_sequence_csv() is pure validation over the CSV rows.
"""
from __future__ import annotations

import os

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-stub")
os.environ.setdefault("FIRECRAWL_API_KEY", "test-key-stub")
os.environ.setdefault("COURT_LISTENER_API_KEY", "test-key-stub")

import csv

from config import fa_max_campaigns as cfg
from src.services.fa_max_campaigns import content


def _write_csv(tmp_path, rows, filename="sequence.csv"):
    path = tmp_path / filename
    fieldnames = ["campaign", "step", "days_after_previous", "channel", "subject", "body", "notes"]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return str(path)


def _valid_capital_desk_loop_rows():
    return [
        {
            "campaign": "capital_desk_loop", "step": "1", "days_after_previous": "0",
            "channel": "email", "subject": "About {{property_street}}",
            "body": "Hi {{first_name|there}}, saw your purchase of {{property_street}} in {{county}}.",
            "notes": "",
        },
        {
            "campaign": "capital_desk_loop", "step": "2", "days_after_previous": "3",
            "channel": "sms", "subject": "",
            "body": "Following up on {{property_street}}. Reply STOP to opt out.",
            "notes": "",
        },
    ]


def test_config_validates_cleanly():
    cfg.validate_campaign_config()


def test_valid_file_has_no_errors(tmp_path):
    path = _write_csv(tmp_path, _valid_capital_desk_loop_rows())
    rows, errors = content.parse_sequence_csv(path, "capital_desk_loop")
    assert errors == []
    assert len(rows) == 2


def test_unknown_merge_field_rejected(tmp_path):
    rows = _valid_capital_desk_loop_rows()
    rows[0]["body"] = "Hi {{first_name|there}}, {{fake_field}} was great."
    path = _write_csv(tmp_path, rows)
    _, errors = content.parse_sequence_csv(path, "capital_desk_loop")
    assert any("fake_field" in e.message for e in errors)


def test_sms_without_stop_language_rejected(tmp_path):
    rows = _valid_capital_desk_loop_rows()
    rows[1]["body"] = "Following up on {{property_street}}."
    path = _write_csv(tmp_path, rows)
    _, errors = content.parse_sequence_csv(path, "capital_desk_loop")
    assert any("STOP" in e.message for e in errors)


def test_step_gap_rejected(tmp_path):
    rows = _valid_capital_desk_loop_rows()
    rows[1]["step"] = "3"  # skips step 2
    path = _write_csv(tmp_path, rows)
    _, errors = content.parse_sequence_csv(path, "capital_desk_loop")
    assert any("step numbers" in e.message for e in errors)


def test_prohibited_financial_wording_rejected(tmp_path):
    rows = _valid_capital_desk_loop_rows()
    rows[0]["body"] = "Hi {{first_name|there}}, our interest rate is unbeatable."
    path = _write_csv(tmp_path, rows)
    _, errors = content.parse_sequence_csv(path, "capital_desk_loop")
    assert any("prohibited" in e.message for e in errors)


def test_email_step_requires_subject(tmp_path):
    rows = _valid_capital_desk_loop_rows()
    rows[0]["subject"] = ""
    path = _write_csv(tmp_path, rows)
    _, errors = content.parse_sequence_csv(path, "capital_desk_loop")
    assert any("subject" in e.message for e in errors)


def test_wrong_campaign_column_rejected(tmp_path):
    rows = _valid_capital_desk_loop_rows()
    rows[0]["campaign"] = "exit_desk"
    path = _write_csv(tmp_path, rows)
    _, errors = content.parse_sequence_csv(path, "capital_desk_loop")
    assert any("does not match" in e.message for e in errors)


def test_render_template_fills_fields_and_fallback():
    rendered = content.render_template(
        "Hi {{first_name|there}}, re: {{property_street}}.",
        {"property_street": "123 Main St"},
    )
    assert rendered == "Hi there, re: 123 Main St."


def test_allowed_merge_fields_include_common_and_campaign_specific():
    allowed = content.allowed_merge_fields("exit_desk")
    assert "sender_name" in allowed  # common
    assert "loan_age_months" in allowed  # exit_desk-specific
    assert "buy_box_zips" not in allowed  # belongs to capital_desk_loop only


def test_lender_name_not_yet_in_exit_desk_merge_fields():
    """Q-C3 is open (plan Section 11) — the client hasn't confirmed whether
    Exit Desk emails may name the person's current lender. Until answered,
    the field must not be offered to the writer or accepted by the loader."""
    assert "lender_name" not in content.allowed_merge_fields("exit_desk")


def test_render_merge_field_doc_lists_every_campaign():
    doc = content.render_merge_field_doc()
    for campaign_key in cfg.CAMPAIGNS:
        assert campaign_key in doc
