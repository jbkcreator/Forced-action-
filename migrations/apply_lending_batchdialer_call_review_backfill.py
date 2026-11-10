"""Bring stored BatchDialer call rows in line with the call-review fixes (2026-11-10). Dry run unless ``--apply``.

Steps (BatchDialer rows only, each idempotent):

1. ``recording_view_url`` = the BatchDialer contact page, where empty
2. ``recording_status`` = ``recorded`` (talk time and ``recordingenabled`` not 0) or ``none``
3. "Unknown" with no talk time -> ``NO_ANSWER``
4. "Answering Machine" stored as ``LEFT_VOICEMAIL`` with no talk time -> ``NO_ANSWER`` (no voicemail was left)
5. extractions marked ``failed`` -> ``deferred`` so the transcript worker extracts them again (an empty answer is
   now a done extraction)
6. connected "Unknown" calls not yet alerted are marked alerted, so neither the new missing-result alert nor the
   "No result yet" post (which only posts a call not yet alerted) ever posts about calls from before the fix

Nothing is posted: steps 3-4 also move ``slack_posted_disposition`` / ``sheet_synced_disposition`` to the new value
where they held the old one, so the delivery retry job finds nothing behind. Old Slack posts, Sheet rows and GHL
notes keep their text. Run after ``apply_lending_recording_view_url.py`` and BEFORE deploying the code.

Usage:
    PYTHONPATH=. python migrations/apply_lending_batchdialer_call_review_backfill.py           # dry run
    PYTHONPATH=. python migrations/apply_lending_batchdialer_call_review_backfill.py --apply
"""
from __future__ import annotations

import argparse
import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from config.lending_dispositions import DEFAULT_CAUSE, DIALER_CONTACT_VIEW_URL, NO_ANSWER_CODE
from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_BD = "call_source = 'batchdialer'"
_NO_TALK = "coalesce(talk_duration_sec, 0) = 0"


def _relabel(where: str) -> str:
    """Set NO_ANSWER and move the delivery markers that held the old value, so nothing is re-sent."""
    return (
        "UPDATE lending.call_dispositions SET "
        "slack_posted_disposition = CASE WHEN slack_posted_disposition IS NOT DISTINCT FROM disposition "
        "  THEN :no_answer ELSE slack_posted_disposition END, "
        "sheet_synced_disposition = CASE WHEN sheet_synced_disposition IS NOT DISTINCT FROM disposition "
        "  THEN :no_answer ELSE sheet_synced_disposition END, "
        "unfunded_cause_provisional = CASE WHEN unfunded_cause IS NULL THEN true ELSE unfunded_cause_provisional END, "
        "unfunded_cause = COALESCE(unfunded_cause, :no_answer_cause), "
        "disposition = :no_answer, updated_at = now() "
        f"WHERE {_BD} AND {where}"
    )


STEPS: tuple[tuple[str, str], ...] = (
    ("recording_view_url", f"UPDATE lending.call_dispositions SET recording_view_url = :view_prefix || dialer_contact_id "
                           f"WHERE {_BD} AND recording_view_url IS NULL AND coalesce(dialer_contact_id, '') <> ''"),
    ("recording_status", "UPDATE lending.call_dispositions SET recording_status = s.v FROM ("
                         "  SELECT id, CASE WHEN coalesce(talk_duration_sec, 0) > 0 "
                         "    AND lower(coalesce(raw_event->>'recordingenabled', '1')) NOT IN ('0', 'false', 'no', 'n') "
                         "  THEN 'recorded' ELSE 'none' END AS v "
                         f"  FROM lending.call_dispositions WHERE {_BD}) s "
                         "WHERE call_dispositions.id = s.id AND call_dispositions.recording_status IS DISTINCT FROM s.v"),
    ("unknown_no_talk", _relabel(f"disposition IS NULL AND upper(btrim(disposition_raw)) = 'UNKNOWN' AND {_NO_TALK}")),
    ("answering_machine", _relabel(f"disposition = 'LEFT_VOICEMAIL' "
                                   f"AND upper(btrim(disposition_raw)) = 'ANSWERING MACHINE' AND {_NO_TALK}")),
    ("failed_extractions", f"UPDATE lending.call_dispositions SET transcript_status = 'deferred', transcript_attempts = 0 "
                           f"WHERE {_BD} AND transcript_status = 'failed'"),
    ("old_unknown_alerts", "UPDATE lending.call_dispositions SET disposition_missing_alerted_at = now() "
                           f"WHERE {_BD} AND disposition IS NULL AND upper(btrim(disposition_raw)) = 'UNKNOWN' "
                           "AND coalesce(talk_duration_sec, 0) > 0 AND disposition_missing_alerted_at IS NULL"),
)


def run_steps(conn: Connection) -> dict[str, int]:
    """Run every step on ``conn`` (the caller commits or rolls back); returns rows changed per step."""
    conn.execute(text("SET LOCAL lock_timeout = '5s'"))
    params = {"view_prefix": DIALER_CONTACT_VIEW_URL.format(contact_id=""), "no_answer": NO_ANSWER_CODE,
              "no_answer_cause": DEFAULT_CAUSE[NO_ANSWER_CODE]}
    return {name: int(conn.execute(text(sql), params).rowcount or 0) for name, sql in STEPS}


def apply(engine: Engine | None = None, *, commit: bool = False) -> dict[str, int]:
    engine = engine or create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.connect() as conn:
        with conn.begin() as tx:
            counts = run_steps(conn)
            if commit:
                tx.commit()
            else:
                tx.rollback()
    for name, count in counts.items():
        logger.info("%s %-20s %d row(s)", "applied" if commit else "would change", name, count)
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="commit the changes (default: dry run, rolled back)")
    apply(commit=parser.parse_args().apply)
