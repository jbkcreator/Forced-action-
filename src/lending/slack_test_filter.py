"""Keep posts about test records out of the lending Slack channels on the live server.

``LENDING_SLACK_TEST_ENABLED`` (default false): while false, a post about a test record is skipped and logged by id;
set it true while testing so test records post as usual. Posts about real leads are never affected by the switch.

A record is a test record by the rules in config/lending_test_data.py: ``is_test`` is set, its email is on a test
domain, its name starts with the word "test", (a website lead) it came from an office IP in ``LENDING_TEST_IPS``, or (a
loan file) it came from a test partner. The rules are checked here as well as the flag, because the tagging job runs
every 10 minutes and the first alerts for a new lead fire before it is tagged.

A phone or GHL contact counts as test only when EVERY lead record holding it is a test record, so a real borrower who
shares a number with a test entry still gets their posts. A failed lookup posts as usual: it never hides a real alert.
"""
from __future__ import annotations

import logging
from typing import Any, Iterable, Optional

from sqlalchemy import text

from config.settings import get_settings
from src.lending.test_data import rule_params

logger = logging.getLogger(__name__)


def _test(email: str, name: Optional[str] = None, ip: Optional[str] = None) -> str:
    rules = ["is_test", f"split_part(lower(COALESCE({email}, '')), '@', 2) = ANY(:domains)"]
    if name:
        rules.append(f"COALESCE({name}, '') ~* :name_pattern")
    if ip:
        rules.append(f"COALESCE({ip}, '') = ANY(:test_ips)")
    return "(" + " OR ".join(rules) + ")"


_LF = _test("email", "concat_ws(' ', first_name, last_name)")
_WEB = _test("email", "name", "ip_address")
_FILE = (_test("borrower_email", "concat_ws(' ', borrower_first_name, borrower_last_name)")[:-1]
         + " OR partner_id IN (SELECT id FROM lending.partners WHERE is_test OR lower(name) LIKE :partner_prefix))")

_TEST_PHONES = text(f"""
    SELECT phone FROM (
        SELECT phone, {_LF} AS test FROM lending.lendingflow_leads WHERE phone = ANY(:keys)
        UNION ALL
        SELECT phone, {_WEB} FROM lending.web_leads WHERE phone = ANY(:keys)
        UNION ALL
        SELECT phone, {_test("email")} FROM lending.contacts WHERE phone = ANY(:keys)
        UNION ALL
        SELECT borrower_phone, {_FILE} FROM lending.loan_files WHERE borrower_phone = ANY(:keys)
    ) s
     GROUP BY phone HAVING bool_and(test)
""")

_TEST_GHL_CONTACTS = text(f"""
    SELECT ghl_contact_id FROM (
        SELECT ghl_contact_id, {_LF} AS test FROM lending.lendingflow_leads WHERE ghl_contact_id = ANY(:keys)
        UNION ALL
        SELECT ghl_contact_id, {_WEB} FROM lending.web_leads WHERE ghl_contact_id = ANY(:keys)
    ) s
     GROUP BY ghl_contact_id HAVING bool_and(test)
""")

_TEST_LENDINGFLOW_UUIDS = text(f"""
    SELECT lead_uuid::text FROM lending.lendingflow_leads WHERE lead_uuid::text = ANY(:keys) AND {_LF}
""")

_TEST_LENDINGFLOW_IDS = text(f"SELECT id FROM lending.lendingflow_leads WHERE id = ANY(:keys) AND {_LF}")

_TEST_WEB_LEAD_IDS = text(f"SELECT id FROM lending.web_leads WHERE id = ANY(:keys) AND {_WEB}")

_TEST_LOAN_FILES = text(f"SELECT id FROM lending.loan_files WHERE id = ANY(:keys) AND {_FILE}")


def slack_test_posts_enabled() -> bool:
    return get_settings().lending_slack_test_enabled


def _hidden(db, query, keys: Iterable[Any]) -> set:
    """The keys whose posts are skipped: none while test posts are on, or when the lookup fails."""
    wanted = sorted({key for key in keys if key not in (None, "")})
    if not wanted or slack_test_posts_enabled():
        return set()
    try:
        with db.begin_nested():
            return {row[0] for row in db.execute(query, {**rule_params(), "keys": wanted})}
    except Exception as exc:  # class only
        logger.warning("[slack-test-filter] test-record lookup failed (%s); posting as usual", type(exc).__name__)
        return set()


def hidden_phones(db, phones: Iterable[Optional[str]]) -> set[str]:
    return _hidden(db, _TEST_PHONES, phones)


def hidden_ghl_contacts(db, contact_ids: Iterable[Optional[str]]) -> set[str]:
    return _hidden(db, _TEST_GHL_CONTACTS, contact_ids)


def is_hidden_lendingflow_uuid(db, lead_uuid: Any) -> bool:
    return bool(_hidden(db, _TEST_LENDINGFLOW_UUIDS, [str(lead_uuid)] if lead_uuid else []))


def hidden_lendingflow_ids(db, lead_ids: Iterable[Optional[int]]) -> set[int]:
    return _hidden(db, _TEST_LENDINGFLOW_IDS, lead_ids)


def hidden_web_lead_ids(db, lead_ids: Iterable[Optional[int]]) -> set[int]:
    return _hidden(db, _TEST_WEB_LEAD_IDS, lead_ids)


def hidden_loan_file_ids(db, file_ids: Iterable[Optional[int]]) -> set[int]:
    return _hidden(db, _TEST_LOAN_FILES, file_ids)


def log_skipped(post: str, ref: Any) -> None:
    """One line per skipped post, by id only (never a name, phone or email)."""
    logger.info("[slack-test-filter] %s skipped for test record %s (LENDING_SLACK_TEST_ENABLED is off)", post, ref)
