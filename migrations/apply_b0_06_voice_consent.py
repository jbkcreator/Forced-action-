"""Apply B0-06 — voice-call PEWC consent columns on consent_acceptances.

Automated AI voice calls (Synthflow) require Prior Express Written Consent
under 47 CFR 64.1200(f)(9), distinct from the existing generic marketing
consent (`consent_scope='marketing'`). See docs/adr/0030.

Idempotent — IF NOT EXISTS guard.

Usage:
    PYTHONPATH=. python migrations/apply_b0_06_voice_consent.py
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL = [
    "ALTER TABLE consent_acceptances ADD COLUMN IF NOT EXISTS voice_consent_at TIMESTAMPTZ;",
    "ALTER TABLE consent_acceptances ADD COLUMN IF NOT EXISTS voice_consent_text TEXT;",
    "ALTER TABLE consent_acceptances ADD COLUMN IF NOT EXISTS voice_consent_version VARCHAR(30);",
]


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            logger.info("DDL step %d/%d", i, len(DDL))
            conn.execute(text(stmt))

    logger.info("b0_06_voice_consent complete.")


if __name__ == "__main__":
    main()
