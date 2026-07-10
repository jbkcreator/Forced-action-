"""funnel_analytics.compute_funnel_counts — stage-count query shape."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from src.services.funnel_analytics import compute_funnel_counts, _STAGE_EVENT_TYPES


class TestComputeFunnelCounts:
    def test_query_filters_by_source(self):
        """webhook_events is a shared audit table across Stripe/GHL/NWS/etc —
        the query must scope to business/frontend-emitted rows only, per the
        safe pattern documented in business_events.py's own module docstring."""
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = []

        frm = datetime(2026, 1, 1, tzinfo=timezone.utc)
        to = datetime(2026, 2, 1, tzinfo=timezone.utc)
        compute_funnel_counts(db, frm, to)

        sql_text = str(db.execute.call_args[0][0])
        assert "source IN ('business', 'frontend')" in sql_text

    def test_returns_zero_for_stages_with_no_rows(self):
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = []

        result = compute_funnel_counts(
            db,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 2, 1, tzinfo=timezone.utc),
        )

        assert result == {stage: 0 for stage in _STAGE_EVENT_TYPES}

    def test_counts_mapped_to_correct_stage(self):
        db = MagicMock()
        row = MagicMock(event_type="PAYMENT_SUCCEEDED", n=7)
        db.execute.return_value.fetchall.return_value = [row]

        result = compute_funnel_counts(
            db,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 2, 1, tzinfo=timezone.utc),
        )

        assert result["paid"] == 7
        assert result["visits"] == 0
