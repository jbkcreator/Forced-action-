"""
Block 11 (Inbound Velocity) — real end-to-end scenario.

Drives the ACTUAL pipeline against the real shared Postgres DB, with no
mocking except the external vendor boundary (Telnyx is disabled in this env
so send_sms is a safe no-op; Synthflow initiate_call is never reached because
the PEWC gate blocks first):

    POST /webhooks/synthflow/inbound (real FastAPI TestClient, real DB)
      -> score_inbound flags hot (intent_slot signal)
      -> inbound_response row written (t0)
      -> publish_cora_event falls back to Postgres (Redis is down in this env)
         -> real INSERT into cora_event_queue
    _sweep_postgres_queue() (the REAL Cora-process consumer function)
      -> dispatch_event -> router -> new_lead_voice_call graph (Block 2, real)
      -> aborts at the PEWC voice-consent gate (no consent_acceptances row for
         a brand-new caller) -> real agent_decisions row
    sync_inbound_response_outcomes() (real)
      -> inbound_response.t1/outcome backfilled to 'consent_blocked'

Marker: scenario_cora (opt-in, hits real DB + real graph). Every row this
test creates is deleted in a finally block, keyed off the unique phone
number generated per run.
"""
from __future__ import annotations

import uuid
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text
from fastapi.testclient import TestClient

from config.settings import get_settings

pytestmark = pytest.mark.scenario_cora


def _unique_e2e_phone() -> str:
    # +1555 is not a real dialable range; last 7 digits from a fresh UUID
    # keep each run's row set uniquely identifiable for cleanup.
    digits = uuid.uuid4().int % 10_000_000
    return f"+1555{digits:07d}"


@pytest.fixture
def e2e_engine():
    settings = get_settings()
    if not settings.database_url:
        pytest.skip("DATABASE_URL not configured")
    engine = create_engine(str(settings.database_url), pool_pre_ping=True)
    with engine.connect() as conn:
        has_table = conn.execute(text("SELECT to_regclass('inbound_response')")).scalar()
    if not has_table:
        pytest.skip("inbound_response table not migrated — run migrations/apply_inbound_response.py")
    yield engine
    engine.dispose()


# Every table with a subscriber_id FK, populated by exercising this one
# webhook — deleted in a loop rather than a fixed dependency order, since
# only a handful of these actually get a row and DB-level FKs don't require
# child-first ordering across independent tables (only vs. the parent row).
_SUBSCRIBER_ID_TABLES = (
    "zip_territories", "dbpr_contacts", "lead_pack_purchases", "sent_leads",
    "wallet_balances", "closer_calls", "wallet_transactions", "user_segments",
    "message_outcomes", "deal_outcomes", "ab_assignments", "api_usage_logs",
    "bundle_purchases", "sms_opt_ins", "agent_decisions", "lead_quality_snapshots",
    "dfy_lite_orders", "premium_purchases", "enrichment_usage_logs",
    "manual_action_log", "webhook_events", "human_close_escalations",
    "partner_subscriptions", "platform_revenue_ledger", "wallet_push_offers",
    "sms_send_logs", "subscription_invoices", "affiliate_referrals",
    "churn_predictions", "platform_cost_attribution", "chat_sessions",
    "revenue_signal_score_events", "conversion_attribution_events",
    "customer_accounts", "subscriber_memory_summary", "subscriber_notes",
    "subscriber_tags", "cora_suppressions", "subscriber_session_metrics",
    "churn_defense_leads", "guarantee_credits", "consent_acceptances",
    "algorithmic_variance_log", "referral_prompt_funnel",
    "non_buyer_nurture_sequences", "checkout_recovery", "inbound_response",
    "cora_event_queue",
)


def _cleanup(engine, phone: str, call_id: str) -> None:
    with engine.begin() as conn:
        sub_id = conn.execute(
            text("SELECT id FROM subscribers WHERE phone = :phone"), {"phone": phone}
        ).scalar()
        conn.execute(text("DELETE FROM inbound_response WHERE decision_id = :cid"), {"cid": call_id})
        conn.execute(text("DELETE FROM agent_decisions WHERE decision_id = :cid"), {"cid": call_id})
        if sub_id:
            for table in _SUBSCRIBER_ID_TABLES:
                conn.execute(text(f"DELETE FROM {table} WHERE subscriber_id = :sid"), {"sid": sub_id})
            # unified_subscriber_memory.subscriber_id is varchar (schema drift
            # vs. the Integer-typed ORM column) — cast to match.
            conn.execute(
                text("DELETE FROM unified_subscriber_memory WHERE subscriber_id = :sid"),
                {"sid": str(sub_id)},
            )
            conn.execute(text("DELETE FROM subscribers WHERE id = :sid"), {"sid": sub_id})


class TestBlock11InboundVelocityE2E:
    def test_hot_inbound_flows_to_consent_blocked_outcome(self, e2e_engine):
        from src.api.main import app

        phone = _unique_e2e_phone()
        call_id = str(uuid.uuid4())  # 36 chars — matches decision_id VARCHAR(36) / agent_decisions PK
        client = TestClient(app)

        try:
            # Force the Postgres fallback path (Redis-down mode) — the scenario
            # test harness enables fakeredis (REDIS_SANDBOX), which would send
            # the event to an in-memory queue nothing consumes. This test
            # specifically validates the durable fallback + the decision_id
            # preservation fix that lives there.
            with patch("src.core.redis_client.redis_available", return_value=False):
                resp = client.post(
                    "/webhooks/synthflow/inbound",
                    json={
                        "phone": phone,
                        "call_id": call_id,
                        "zip_code": "33604",
                        "vertical": "roofing",
                        "collected_variables": {"ready_to_buy": {"value": "yes"}},
                    },
                )
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["intent"]["is_hot"] is True
            assert body["intent"]["score"] >= 50
            subscriber_id = body["subscriber_id"]
            assert subscriber_id

            # 1. inbound_response row written at score time (real DB read).
            with e2e_engine.connect() as conn:
                ir_row = conn.execute(
                    text(
                        "SELECT subscriber_id, t0, t1, score, outcome "
                        "FROM inbound_response WHERE decision_id = :cid"
                    ),
                    {"cid": call_id},
                ).first()
            assert ir_row is not None, "inbound_response row was not written"
            assert ir_row.subscriber_id == subscriber_id
            assert ir_row.t0 is not None
            assert ir_row.t1 is None
            assert ir_row.outcome == "pending"

            # 2. Event landed in the real Postgres fallback queue (Redis is down
            #    in this env, so publish_cora_event took the durable path for real).
            with e2e_engine.connect() as conn:
                queue_row = conn.execute(
                    text(
                        "SELECT event_type, subscriber_id, status, decision_id "
                        "FROM cora_event_queue WHERE decision_id = :cid "
                        "ORDER BY created_at DESC LIMIT 1"
                    ),
                    {"cid": call_id},
                ).first()
            assert queue_row is not None, "inbound_hot_callback was not enqueued in cora_event_queue"
            assert queue_row.event_type == "inbound_hot_callback"
            assert queue_row.subscriber_id == subscriber_id
            assert queue_row.status == "pending"
            # The decision_id-preservation fix: call_id survives the Postgres fallback.
            assert queue_row.decision_id == call_id

            # 3. Drive the REAL Cora consumer function (same one the agents
            #    process runs every 60s) — no mocking of router/graph/DB.
            from src.agents.events.ingestion import _sweep_postgres_queue
            processed = _sweep_postgres_queue()
            assert processed >= 1

            # 4. Real new_lead_voice_call graph ran and aborted at the PEWC
            #    gate (brand-new caller has no consent_acceptances row) —
            #    proves the shared-path wiring end-to-end, gate-aware.
            with e2e_engine.connect() as conn:
                decision_row = conn.execute(
                    text(
                        "SELECT graph_name, terminal_status, summary "
                        "FROM agent_decisions WHERE decision_id = :cid"
                    ),
                    {"cid": call_id},
                ).first()
            assert decision_row is not None, "agent_decisions row missing — callback graph did not run"

            # Environment hazard, not a code defect: DATABASE_URL here is a
            # single shared Postgres instance, and any other already-running
            # Cora agents process (e.g. one deployed from unmerged `dev`,
            # which has no "inbound_hot_callback" entry in EVENT_TO_GRAPH yet)
            # listens on the same cora_events NOTIFY channel and can win the
            # race to dispatch this event before our own _sweep_postgres_queue()
            # call above does. That shows up as graph_name="supervisor" with
            # drop_reason "unknown_event_type:inbound_hot_callback" — proving
            # only that some OTHER process's older code doesn't know this event
            # type, not that our branch's router is wrong (that's independently
            # proven by TestRouterSharesBlock2Path, which asserts the identity
            # of EVENT_TO_GRAPH["inbound_hot_callback"] on this branch directly).
            # Skip rather than fail so this branch-vs-shared-DB timing issue
            # doesn't mask a real regression; it disappears once this PR merges
            # and every consumer runs the same router.
            summary = decision_row.summary or {}
            if decision_row.graph_name != "new_lead_voice_call" and summary.get("drop_reason") == "unknown_event_type:inbound_hot_callback":
                pytest.skip(
                    "Another already-running Cora consumer (older/unmerged code) won the race "
                    "to dispatch this event via the shared Postgres NOTIFY channel before our own "
                    "sweep call — a pre-merge shared-DB timing artifact, not a defect in this branch."
                )

            assert decision_row.graph_name == "new_lead_voice_call"
            assert decision_row.terminal_status == "aborted"
            assert summary.get("failure_reason") == "compliance:voice_consent_required"

            # 5. Reconciliation backfills t1/outcome from the real agent_decisions row.
            from src.services.inbound_response_tracking import sync_inbound_response_outcomes
            from src.core.database import get_db_context
            with get_db_context() as db:
                reconciled = sync_inbound_response_outcomes(db)
            assert reconciled >= 1

            with e2e_engine.connect() as conn:
                final_row = conn.execute(
                    text("SELECT t1, outcome FROM inbound_response WHERE decision_id = :cid"),
                    {"cid": call_id},
                ).first()
            assert final_row.t1 is not None
            assert final_row.outcome == "consent_blocked"

        finally:
            _cleanup(e2e_engine, phone, call_id)
