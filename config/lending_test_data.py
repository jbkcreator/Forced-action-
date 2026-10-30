"""What counts as test traffic in the lending tables (Q35, Q42).

``is_test`` is a REPORTING flag: scoreboards, attribution, cohort reports, audience exports and monthly seat caps leave
flagged rows out. It never changes suppression, consent, opt-outs or routing, so a test number that opted out stays
suppressed. Rows are tagged, never deleted. Rules: the ones in the Q42 answer (22 Oct 2026), a test email domain plus
the test partner by name, and two more for tests entered with a real email: a name starting with the word "test" and,
for website leads, an office IP (``LENDING_TEST_IPS``). The same rules decide which posts stay out of Slack on live
(src/lending/slack_test_filter.py).
"""
from __future__ import annotations

# Lower-case domains of the addresses test traffic uses (Q42).
TEST_EMAIL_DOMAINS: tuple[str, ...] = ("example.com", "example.org", "test.com")

# A partner whose name starts with this (case-insensitive) is a test partner ("TEST R1 Partner", Q33).
TEST_PARTNER_NAME_PREFIX = "test "

# A person whose name starts with the WORD "test" ("TEST Sonetel", "test lead"); "Testa" and "Tester" do not match.
# PostgreSQL case-insensitive regex (~*); \M is the end of a word.
TEST_NAME_PATTERN = r"^\s*test\M"
