"""Stream self-diagnosis config (Sprint 4.2).

STREAM_METRICS: registry of observable stream metrics.
All targets are fractions 0–1 (same convention as the stored column values).
Targets are placeholders — refine as real baselines emerge.
"""
from __future__ import annotations

STREAM_METRICS: dict[str, dict] = {
    "enrichment_rate": {
        "stream": "lead_pipeline",
        "target": 0.80,
        "direction": "higher_is_better",
        "compute": "compute_enrichment_rate",
        "column": "enrichment_rate",
    },
    "dialable_rate": {
        "stream": "lead_pipeline",
        "target": 0.50,
        "direction": "higher_is_better",
        "compute": "compute_dialable_rate",
        "column": "dialable_rate",
    },
    "sms_delivery": {
        "stream": "outreach",
        "target": 0.95,
        "direction": "higher_is_better",
        "compute": "compute_sms_delivery_rate",
        "column": "sms_delivery_rate",
    },
    "closer_conv": {
        "stream": "closer",
        "target": 0.15,
        "direction": "higher_is_better",
        "compute": "compute_closer_conv_rate",
        "column": "closer_conv_rate",
    },
}

CONSECUTIVE_DAYS_TO_DIAGNOSE = 3
RED_AFTER_DAYS = 7
RED_IF_BELOW_FRACTION_OF_TARGET = 0.50

# Per-(metric, category) recommendation playbook.
# Each entry is a list of concrete action strings shown in the diagnostic row.
RECOMMENDATION_PLAYBOOK: dict[str, dict[str, list[str]]] = {
    "enrichment_rate": {
        "skip_trace_degraded": [
            "Check Tracerfy API quota — review last 24h error rate",
            "Verify BatchData fallback is healthy",
            "Check contact_info_confidence distribution — drop in 'high' signals enrichment failure",
        ],
        "data_freshness": [
            "Verify CDS scoring ran today (check platform_daily_stats.properties_scored)",
            "Check distress_scores for stale rows (last scored > 2 days ago)",
        ],
        "default": [
            "Review enrichment pipeline logs for the last 3 days",
            "Check skip-trace vendor response rates",
        ],
    },
    "dialable_rate": {
        "skip_trace_degraded": [
            "Check phone_metadata type distribution — increase in NULL/unknown signals vendor issue",
            "Review Tracerfy mobile/landline classification accuracy",
        ],
        "default": [
            "Check phone_metadata for recent batch — compare mobile_pct to phone_deliverability_snapshots",
            "Review owners with null phone_metadata — may need re-enrichment",
        ],
    },
    "sms_delivery": {
        "carrier_filtering": [
            "Check Telnyx delivery receipts for 30x/50x status codes",
            "Review A2P 10DLC registration status",
            "Check if specific carrier is filtering — segment by carrier in message_outcomes",
        ],
        "opt_out_spike": [
            "Review recent message content for compliance issues",
            "Check opt-out rate trend — spike may indicate list hygiene problem",
        ],
        "default": [
            "Review Telnyx dashboard for delivery errors",
            "Check message_outcomes delivered_at null rate for today",
        ],
    },
    "closer_conv": {
        "call_quality": [
            "Review closer_calls transcripts for objection patterns",
            "Check average call duration — drop may indicate early hang-ups",
            "Verify escalation routing — are the right leads reaching closers?",
        ],
        "lead_quality": [
            "Check CDS tier distribution of escalated leads",
            "Review distress_scores for escalated properties — score floor may need raising",
        ],
        "default": [
            "Review closer call outcomes for last 30 days",
            "Check closer capacity — low volume may skew the rate",
        ],
    },
}
