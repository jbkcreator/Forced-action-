"""Auto-converted from alembic migration `fa036_cora_autonomy_scorecard` (revision fa036_cora_autonomy_scorecard).

DDL rendered verbatim from the migration's upgrade() via alembic offline
(as_sql) mode. Historical record — the live DB already reflects this; kept so
every schema change lives in scripts/. Idempotency NOT guaranteed (verbatim).

Usage:
    PYTHONPATH=. python scripts/apply_fa036_cora_autonomy_scorecard.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SQL = r"""
CREATE TABLE cora_playbook (
    id BIGSERIAL NOT NULL, 
    name VARCHAR(120) NOT NULL, 
    description TEXT, 
    pattern_json JSONB NOT NULL, 
    authored_by VARCHAR(80) NOT NULL, 
    authored_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    status VARCHAR(20) DEFAULT 'recommended' NOT NULL, 
    adopted_at TIMESTAMP WITH TIME ZONE, 
    adopted_by VARCHAR(80), 
    rejected_at TIMESTAMP WITH TIME ZONE, 
    rejected_by VARCHAR(80), 
    rejection_reason TEXT, 
    retired_at TIMESTAMP WITH TIME ZONE, 
    retired_by VARCHAR(80), 
    decision_id VARCHAR(36), 
    source_type VARCHAR(40), 
    source_id VARCHAR(80), 
    source_key VARCHAR(160), 
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_cora_playbook_status CHECK (status IN ('recommended','adopted','rejected','retired')), 
    CONSTRAINT fk_cora_playbook_decision FOREIGN KEY(decision_id) REFERENCES agent_decisions (decision_id) ON DELETE SET NULL
);

CREATE INDEX idx_cora_playbook_status ON cora_playbook (status);

CREATE INDEX idx_cora_playbook_authored ON cora_playbook (authored_by, authored_at);

CREATE INDEX idx_cora_playbook_adopted
            ON cora_playbook(adopted_at)
            WHERE adopted_at IS NOT NULL;

CREATE UNIQUE INDEX idx_cora_playbook_source_key_unique
            ON cora_playbook(source_key)
            WHERE source_key IS NOT NULL;

ALTER TABLE agent_decisions ADD COLUMN autonomy_class VARCHAR(32);

ALTER TABLE agent_decisions ADD COLUMN was_autonomous BOOLEAN DEFAULT FALSE NOT NULL;

ALTER TABLE agent_decisions ADD COLUMN requires_approval BOOLEAN DEFAULT FALSE NOT NULL;

ALTER TABLE agent_decisions ADD COLUMN approved_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE agent_decisions ADD COLUMN approved_by VARCHAR(80);

ALTER TABLE agent_decisions ADD COLUMN overridden_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE agent_decisions ADD COLUMN overridden_by VARCHAR(80);

ALTER TABLE agent_decisions ADD COLUMN override_reason TEXT;

ALTER TABLE agent_decisions ADD COLUMN playbook_id BIGINT;

ALTER TABLE agent_decisions ADD CONSTRAINT fk_agent_decisions_playbook FOREIGN KEY(playbook_id) REFERENCES cora_playbook (id) ON DELETE SET NULL;

ALTER TABLE agent_decisions ADD CONSTRAINT check_agent_autonomy_class CHECK (autonomy_class IS NULL OR autonomy_class IN ('autonomous','approval_required','approved','rejected','overridden','recommendation_only'));

CREATE INDEX idx_agent_decisions_autonomy_class
            ON agent_decisions(autonomy_class)
            WHERE autonomy_class IS NOT NULL;

CREATE INDEX idx_agent_decisions_overridden
            ON agent_decisions(overridden_at)
            WHERE overridden_at IS NOT NULL;

CREATE INDEX idx_agent_decisions_was_autonomous
            ON agent_decisions(was_autonomous, started_at)
            WHERE was_autonomous = TRUE;

CREATE INDEX idx_agent_decisions_playbook
            ON agent_decisions(playbook_id)
            WHERE playbook_id IS NOT NULL;

ALTER TABLE learning_cards DROP CONSTRAINT check_card_type;

ALTER TABLE learning_cards ADD CONSTRAINT check_card_type CHECK (card_type IN ('message_perf','deal_pattern','ab_result','churn_signal','pricing_test','general','autonomy_summary'));
"""


def main() -> None:
    engine = create_engine(get_settings().database_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    logger.info("applied fa036_cora_autonomy_scorecard")


if __name__ == "__main__":
    main()
