"""Apply vertical autopilot DDL — REVINT-v2.2 (I4).

Creates vertical_candidate_packets, vertical_probes, and vertical_verdicts.
Idempotent — all statements use IF NOT EXISTS guards.

Usage:
    PYTHONPATH=. python migrations/apply_vertical_autopilot.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    # ── vertical_candidate_packets ────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS vertical_candidate_packets (
        id                   SERIAL PRIMARY KEY,
        vertical_name        VARCHAR(100)  NOT NULL,
        dim1_score           INTEGER       NOT NULL DEFAULT 0,
        dim2_score           INTEGER       NOT NULL DEFAULT 0,
        dim3_score           INTEGER       NOT NULL DEFAULT 0,
        dim4_score           INTEGER       NOT NULL DEFAULT 0,
        dim5_score           INTEGER       NOT NULL DEFAULT 0,
        dim6_score           INTEGER       NOT NULL DEFAULT 0,
        total_score          INTEGER       NOT NULL DEFAULT 0,
        legal_status         VARCHAR(30)   NOT NULL
                             CONSTRAINT ck_vcp_legal_status
                             CHECK (legal_status IN ('approved','blocked','pending_review')),
        eligible_for_probe   BOOLEAN       NOT NULL DEFAULT FALSE,
        evidence             JSONB,
        status               VARCHAR(30)   NOT NULL DEFAULT 'candidate'
                             CONSTRAINT ck_vcp_status
                             CHECK (status IN ('candidate','probing','won','killed','pending_legal')),
        created_at           TIMESTAMPTZ   NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_vcp_status
        ON vertical_candidate_packets (status);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_vcp_vertical_name
        ON vertical_candidate_packets (vertical_name);
    """,

    # ── vertical_probes ───────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS vertical_probes (
        id                              SERIAL PRIMARY KEY,
        vertical_candidate_packet_id    INTEGER       NOT NULL
                                        REFERENCES vertical_candidate_packets (id),
        vertical_name                   VARCHAR(100)  NOT NULL,
        idempotency_key                 VARCHAR(100)  NOT NULL UNIQUE,
        sends_count                     INTEGER       NOT NULL DEFAULT 0,
        reply_count                     INTEGER       NOT NULL DEFAULT 0,
        reply_rate                      NUMERIC(6,4)  NOT NULL DEFAULT 0.0,
        tcpa_preflight_passed           BOOLEAN       NOT NULL DEFAULT FALSE,
        suppression_checked             BOOLEAN       NOT NULL DEFAULT FALSE,
        touch_collision_checked         BOOLEAN       NOT NULL DEFAULT FALSE,
        frequency_cap_checked           BOOLEAN       NOT NULL DEFAULT FALSE,
        quiet_hours_checked             BOOLEAN       NOT NULL DEFAULT FALSE,
        channel_limits_checked          BOOLEAN       NOT NULL DEFAULT FALSE,
        kill_switch_active              BOOLEAN       NOT NULL DEFAULT FALSE,
        completion_receipt              BOOLEAN       NOT NULL DEFAULT FALSE,
        started_at                      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        completed_at                    TIMESTAMPTZ,
        status                          VARCHAR(20)   NOT NULL DEFAULT 'running'
                                        CONSTRAINT ck_vprobe_status
                                        CHECK (status IN ('running','completed','killed','aborted'))
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_vprobe_packet_id
        ON vertical_probes (vertical_candidate_packet_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_vprobe_status
        ON vertical_probes (status);
    """,

    # ── vertical_verdicts ─────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS vertical_verdicts (
        id                              SERIAL PRIMARY KEY,
        vertical_probe_id               INTEGER       NOT NULL
                                        REFERENCES vertical_probes (id),
        vertical_candidate_packet_id    INTEGER       NOT NULL,
        vertical_name                   VARCHAR(100)  NOT NULL,
        verdict                         VARCHAR(20)   NOT NULL
                                        CONSTRAINT ck_vverdict_verdict
                                        CHECK (verdict IN ('won','killed','running')),
        verdict_at                      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        rule_fired                      VARCHAR(60)   NOT NULL,
        reply_rate_at_verdict           NUMERIC(6,4)  NOT NULL DEFAULT 0.0,
        presell_confirmed               BOOLEAN       NOT NULL DEFAULT FALSE,
        package_generated               BOOLEAN       NOT NULL DEFAULT FALSE,
        package_id                      VARCHAR(100),
        clone_status                    VARCHAR(60),
        source_county                   VARCHAR(50),
        handoff_payload                 JSONB
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_vverdict_probe_id
        ON vertical_verdicts (vertical_probe_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_vverdict_verdict
        ON vertical_verdicts (verdict);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_vverdict_vertical_name
        ON vertical_verdicts (vertical_name);
    """,
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt.strip()))

    logger.info(
        "apply_vertical_autopilot complete — "
        "vertical_candidate_packets, vertical_probes, vertical_verdicts applied."
    )


if __name__ == "__main__":
    main()
