"""
Tests for claude_savings_report module.
"""

import pytest
from datetime import datetime, timedelta, timezone
from sqlalchemy import select, delete

from src.core.models import ApiUsageLog
from src.services.claude_savings_report import compute_savings_report, format_savings_sms


class TestComputeSavingsReport:
    def test_returns_zeros_when_no_data(self, fresh_db):
        fresh_db.execute(delete(ApiUsageLog))
        fresh_db.flush()
        report = compute_savings_report(fresh_db, since_hours=24)
        assert report["actual_cost"] == 0.0
        assert report["all_sonnet_cost"] == 0.0
        assert report["savings_pct"] == 0.0
        assert report["haiku_share"] == 0.0
        assert report["calls"] == 0

    def test_computes_correct_savings(self, fresh_db):
        fresh_db.execute(delete(ApiUsageLog))
        fresh_db.flush()
        
        now = datetime.now(timezone.utc)
        rows = [
            ApiUsageLog(
                service="claude",
                model="haiku",
                input_tokens=1000,
                output_tokens=500,
                cost_usd=0.002,
                task_type="sms_copy",
                blocked_by_pause=False,
                created_at=now - timedelta(hours=1),
            ),
            ApiUsageLog(
                service="claude",
                model="haiku",
                input_tokens=1000,
                output_tokens=500,
                cost_usd=0.002,
                task_type="classification",
                blocked_by_pause=False,
                created_at=now - timedelta(hours=2),
            ),
        ]
        for r in rows:
            fresh_db.add(r)
        fresh_db.flush()

        report = compute_savings_report(fresh_db, since_hours=24)
        assert report["calls"] == 2
        assert report["actual_cost"] == 0.004
        all_sonnet = 2 * (1000 * 3.00 + 500 * 15.00) / 1_000_000
        assert report["all_sonnet_cost"] == pytest.approx(all_sonnet, rel=1e-4)
        assert report["haiku_share"] == 1.0
        assert report["savings_pct"] > 0.75

    def test_groups_by_task_type(self, fresh_db):
        fresh_db.execute(delete(ApiUsageLog))
        fresh_db.flush()
        
        now = datetime.now(timezone.utc)
        fresh_db.add(ApiUsageLog(
            service="claude",
            model="haiku",
            input_tokens=1000,
            output_tokens=500,
            cost_usd=0.002,
            task_type="sms_copy",
            blocked_by_pause=False,
            created_at=now,
        ))
        fresh_db.add(ApiUsageLog(
            service="claude",
            model="sonnet",
            input_tokens=1000,
            output_tokens=500,
            cost_usd=0.0075,
            task_type="conversational_close",
            blocked_by_pause=False,
            created_at=now,
        ))
        fresh_db.flush()

        report = compute_savings_report(fresh_db, since_hours=24)
        assert len(report["by_task"]) == 2
        tasks = {t["task_type"]: t for t in report["by_task"]}
        assert "sms_copy" in tasks
        assert "conversational_close" in tasks
        assert tasks["sms_copy"]["calls"] == 1
        assert tasks["conversational_close"]["calls"] == 1

    def test_excludes_blocked_calls(self, fresh_db):
        fresh_db.execute(delete(ApiUsageLog))
        fresh_db.flush()
        
        now = datetime.now(timezone.utc)
        fresh_db.add(ApiUsageLog(
            service="claude",
            model="haiku",
            input_tokens=1000,
            output_tokens=500,
            cost_usd=0.002,
            task_type="sms_copy",
            blocked_by_pause=False,
            created_at=now,
        ))
        fresh_db.add(ApiUsageLog(
            service="claude",
            model="haiku",
            input_tokens=1000,
            output_tokens=500,
            cost_usd=0.0,
            task_type="sms_copy",
            blocked_by_pause=True,
            created_at=now,
        ))
        fresh_db.flush()

        report = compute_savings_report(fresh_db, since_hours=24)
        assert report["calls"] == 1


class TestFormatSavingsSms:
    def test_handles_zero_calls(self):
        msg = format_savings_sms({"calls": 0})
        assert "no data" in msg.lower()

    def test_formats_message(self):
        report = {
            "calls": 100,
            "actual_cost": 1.50,
            "all_sonnet_cost": 15.0,
            "savings_pct": 0.9,
            "haiku_share": 0.8,
        }
        msg = format_savings_sms(report)
        assert "$1.50" in msg
        assert "Claude:" in msg