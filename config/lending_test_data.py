"""What counts as test traffic in the lending tables (Q35, Q42).

``is_test`` is a REPORTING flag: scoreboards, attribution, cohort reports, audience exports and monthly seat caps leave
flagged rows out. It never changes suppression, consent, opt-outs or routing, so a test number that opted out stays
suppressed. Rows are tagged, never deleted. Rules are the ones in the Q42 answer (22 Oct 2026): a test email domain,
plus the test partner by name.
"""
from __future__ import annotations

# Lower-case domains of the addresses test traffic uses (Q42).
TEST_EMAIL_DOMAINS: tuple[str, ...] = ("example.com", "example.org", "test.com")

# A partner whose name starts with this (case-insensitive) is a test partner ("TEST R1 Partner", Q33).
TEST_PARTNER_NAME_PREFIX = "test "

# A booking whose reference starts with this (case-insensitive) is a test booking ("TEST-E2E-BOOK-1"). The bookings and
# confirmation tables have no is_test column, so Cora's morning digest leaves these out by prefix.
TEST_BOOKING_REF_PREFIX = "TEST-"
