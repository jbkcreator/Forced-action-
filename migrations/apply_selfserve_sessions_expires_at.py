"""Add expires_at to selfserve_sessions.

get_session_by_token() had no TTL check -- only status='handed_off' blocked
access, so a forwarded/cached/leaked session URL (owner name, address,
distress data) stayed fully functional indefinitely. Token is unguessable
(UUID4, not IDOR), but this closes the unbounded exposure window. Existing
rows get a 30-day window from their own started_at rather than from
migration-apply time, so already-old sessions don't all suddenly share one
expiry moment.

Usage: PYTHONPATH=. python migrations/apply_selfserve_sessions_expires_at.py
"""
import logging
from sqlalchemy import create_engine, text
from config.settings import get_settings

logger = logging.getLogger(__name__)


def apply(engine=None):
    if engine is None:
        engine = create_engine(get_settings().database_url)

    with engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE selfserve_sessions
                ADD COLUMN IF NOT EXISTS expires_at timestamptz
        """))
        conn.execute(text("""
            UPDATE selfserve_sessions
               SET expires_at = started_at + interval '30 days'
             WHERE expires_at IS NULL
        """))
        conn.execute(text("""
            ALTER TABLE selfserve_sessions
                ALTER COLUMN expires_at SET DEFAULT now() + interval '30 days',
                ALTER COLUMN expires_at SET NOT NULL
        """))
        logger.info("selfserve_sessions.expires_at applied")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    apply()
    print("Done.")
