"""Flag internal/test subscribers whose is_test was never set (idempotent).

Root cause (fix A): `is_test` is stamped at checkout from the *Stripe session*
email only (`stripe_webhooks._on_checkout_completed`). When a subscriber's
internal-domain email (e.g. @heu.ai) is populated on the row *after* checkout —
or the checkout session carried a different/blank email — the internal account
is never flagged and leaks into MRR on both the DB and Stripe sides (this is how
`hari@heu.ai` showed a live $299 charge as real revenue).

This reconciles the stored email against the same internal-domain rule used at
creation (`src.utils.test_account`) and sets is_test=true for any active/paying
row that should have been flagged. is_test is a REVENUE-REPORTING filter only —
flagging does not change billing, access, or lead delivery (see CLAUDE.md).

Idempotent: only rows where the email matches an internal domain AND is_test is
not already true are touched; re-running is a no-op.

Usage:
    PYTHONPATH=. python migrations/apply_flag_internal_test_accounts.py          # dry-run
    PYTHONPATH=. python migrations/apply_flag_internal_test_accounts.py --apply   # write
"""
from __future__ import annotations

import argparse
import logging

from sqlalchemy import text

from src.core.database import get_db_session
from src.utils.test_account import _INTERNAL_EMAIL_DOMAINS

logger = logging.getLogger("flag_internal_test_accounts")


def _like_clauses() -> tuple[str, dict]:
    """Build a parameterized OR of `email ILIKE '%@domain'` clauses."""
    clauses, params = [], {}
    for i, domain in enumerate(_INTERNAL_EMAIL_DOMAINS):
        key = f"dom{i}"
        clauses.append(f"lower(email) LIKE :{key}")
        params[key] = f"%{domain}"
    return "(" + " OR ".join(clauses) + ")", params


def main(apply: bool) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    where_domains, params = _like_clauses()

    select_sql = text(
        f"""
        SELECT id, email, plan_price, status, is_test
        FROM subscribers
        WHERE {where_domains}
          AND is_test IS NOT TRUE
        ORDER BY id
        """
    )

    session = get_db_session()
    try:
        rows = session.execute(select_sql, params).mappings().all()
        if not rows:
            logger.info("No unflagged internal-domain subscribers found — nothing to do.")
            return

        logger.info("Found %d internal-domain row(s) to flag is_test=true:", len(rows))
        for r in rows:
            logger.info(
                "  id=%s email=%s status=%s plan_price=%s -> is_test=true",
                r["id"], r["email"], r["status"], r["plan_price"],
            )

        if not apply:
            logger.info("Dry-run — no changes written. Re-run with --apply to persist.")
            return

        update_sql = text(
            f"""
            UPDATE subscribers
            SET is_test = TRUE, updated_at = now()
            WHERE {where_domains}
              AND is_test IS NOT TRUE
            """
        )
        result = session.execute(update_sql, params)
        session.commit()
        logger.info("Flagged %d subscriber(s) is_test=true.", result.rowcount)
    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="write changes (default: dry-run)")
    main(parser.parse_args().apply)
