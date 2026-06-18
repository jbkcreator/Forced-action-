"""Nightly re-tag sweep for closer_calls (Closer Cockpit, Sprint S1b).

Backstop for the event-driven tagging path: re-runs Call Tagging on rows that
have a transcript but were left untagged (tagged_at IS NULL) — e.g. an LLM parse
failure, or the transcription.created event was dropped before tagging ran.

Schedule OUTSIDE the 04:00–07:00 UTC scraper/CDS crunch to avoid contention.
Usage: python -m src.tasks.closer_call_retag
"""
import logging
import sys

from sqlalchemy import text

from src.core.database import Database
from src.services.closer_call_tagging import tag_closer_call

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_DEFAULT_BATCH = 200


def run(limit: int = _DEFAULT_BATCH) -> int:
    """Re-tag up to `limit` untagged-but-transcribed calls. Returns count tagged."""
    db = Database()
    with db.session_scope() as session:
        ids = [
            r[0]
            for r in session.execute(
                text(
                    "SELECT aircall_call_id FROM closer_calls "
                    "WHERE tagged_at IS NULL AND transcript_text IS NOT NULL "
                    "ORDER BY id LIMIT :lim"
                ),
                {"lim": limit},
            )
        ]

    tagged = 0
    for call_id in ids:
        try:
            if tag_closer_call(call_id):
                tagged += 1
        except Exception as exc:
            logger.warning("[retag] failed call_id=%s: %s", call_id, exc)

    logger.info("[retag] closer_call sweep complete: %s/%s tagged", tagged, len(ids))
    return tagged


if __name__ == "__main__":
    run()
    sys.exit(0)
