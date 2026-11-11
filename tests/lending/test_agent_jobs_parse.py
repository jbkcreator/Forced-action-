"""P1.1: an agent job file is parsed strictly, and a bad file's error never carries the file's content."""
from __future__ import annotations

import json
from uuid import UUID

import pytest

from config.lending_agent_jobs import MAX_FILE_BYTES
from src.lending.agent_jobs.job import AgentJob, JobFormatError, parse_job

JOB_ID = "7f1c2e9a-3b4d-4c5e-8f60-718293a4b5c6"
FILE_NAME = f"2026-11-09T13:52:00-05:00_lf_{JOB_ID}.json"


def _spec_example(**overrides) -> dict:
    job = {
        "job_id": JOB_ID,
        "lane": "NDL",
        "created_by": "LendingFlow follow up",
        "created_at": "2026-11-09T13:52:00-05:00",
        "type": "send_sms",
        "ghl_contact_id": "abc123",
        "contact_label": "Mark T., Tampa",
        "template_version": "LF-D2-SMS-v1",
        "body": "Draft text exactly as it should send",
        "reason": "Day 2 touch, no reply yet",
        "requires_approval": True,
        "not_before": "2026-11-09T09:00:00-05:00",
        "expires_at": "2026-11-10T20:00:00-05:00",
    }
    job.update(overrides)
    return job


def _raw(job: dict) -> bytes:
    return json.dumps(job).encode()


def test_parses_spec_example() -> None:
    job = parse_job(_raw(_spec_example()), FILE_NAME)
    assert isinstance(job, AgentJob)
    assert job.job_id == UUID(JOB_ID)
    assert job.type == "send_sms"
    assert job.ghl_contact_id == "abc123"
    assert job.requires_approval is True
    assert job.expires_at.utcoffset() is not None


def test_rejects_oversized_file() -> None:
    with pytest.raises(JobFormatError) as err:
        parse_job(b" " * (MAX_FILE_BYTES + 1), FILE_NAME)
    assert err.value.job_id == JOB_ID


def test_rejects_unknown_type() -> None:
    with pytest.raises(JobFormatError) as err:
        parse_job(_raw(_spec_example(type="send_fax")), FILE_NAME)
    assert "type" in err.value.reason
    assert err.value.job_id == JOB_ID


def test_rejects_naive_datetime() -> None:
    with pytest.raises(JobFormatError) as err:
        parse_job(_raw(_spec_example(expires_at="2026-11-10T20:00:00")), FILE_NAME)
    assert "expires_at" in err.value.reason


def test_expires_before_not_before_rejected() -> None:
    with pytest.raises(JobFormatError):
        parse_job(_raw(_spec_example(not_before="2026-11-11T09:00:00-05:00")), FILE_NAME)


def test_email_requires_subject() -> None:
    with pytest.raises(JobFormatError) as err:
        parse_job(_raw(_spec_example(type="send_email")), FILE_NAME)
    assert "subject" in err.value.reason
    assert parse_job(_raw(_spec_example(type="send_email", subject="Your loan")), FILE_NAME).subject == "Your loan"


def test_bad_json_recovers_job_id_from_filename() -> None:
    with pytest.raises(JobFormatError) as err:
        parse_job(b"{not json", FILE_NAME)
    assert err.value.job_id == JOB_ID


def test_bad_file_without_job_id_anywhere() -> None:
    with pytest.raises(JobFormatError) as err:
        parse_job(b"{not json", "random-name.json")
    assert err.value.job_id is None


def test_error_reason_has_no_body_text() -> None:
    raw = _raw(_spec_example(type="send_fax", body="Call me at 555-867-5309"))
    with pytest.raises(JobFormatError) as err:
        parse_job(raw, FILE_NAME)
    assert "555" not in err.value.reason
    assert "send_fax" not in err.value.reason
