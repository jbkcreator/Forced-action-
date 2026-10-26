"""Borrower-facing copy: always "Next Deal Lending", never a personal name, and one callback number.

Client instruction (Engineering Pod Action List section 2, Q18 / Q60): Josh's name appears on nothing the borrower
sees, and (656) 221-1100 replaces (727) 436-9951. The 10DLC sample messages must reflect the texts actually sent
(GHL rejection 30893), so this keeps the code's wording from drifting away from them again.
"""
from __future__ import annotations

import pytest

from config import lending_missed_call, lending_prequal, lending_reminders, lending_text_back, lending_web

BRAND = "Next Deal Lending"
BANNED = ("Josh", "Kantor", "(727)", "436-9951", "4369951")

_REMINDER_TEXTS = {
    name: value for name, value in vars(lending_reminders).items()
    if name.isupper() and isinstance(value, str)
    and name.startswith(("CONFIRMATION_", "NIGHT_BEFORE_", "NINETY_MIN_", "GATE_FAIL_", "EMAIL_"))
}
_SMS_TEXTS = {
    **{name: value for name, value in _REMINDER_TEXTS.items()
       if not name.startswith("EMAIL_") and "RESCHEDULE" not in name},  # a reschedule link is a fragment, not a message
    **{f"TEXT_BACK_{key}": value for key, value in lending_text_back.TEMPLATES.items()},
    "MISSED_CALL_WITH_PROPERTY": lending_missed_call.TEMPLATE_WITH_PROPERTY,
    "MISSED_CALL_NO_PROPERTY": lending_missed_call.TEMPLATE_NO_PROPERTY,
}
_ALL_COPY = {
    **_REMINDER_TEXTS,
    **_SMS_TEXTS,
    "SMS_CONSENT_TEXT": lending_web.SMS_CONSENT_TEXT,
    "CALL_CONSENT_TEXT": lending_web.CALL_CONSENT_TEXT,
    "EMAIL_FROM_NAME": lending_reminders.EMAIL_FROM_NAME,
    "BORROWER_SIGNER": lending_prequal.BORROWER_SIGNER,
}


def test_the_guard_actually_covers_the_copy():
    assert len(_REMINDER_TEXTS) >= 20 and len(_SMS_TEXTS) >= 12


@pytest.mark.parametrize("name", sorted(_ALL_COPY))
def test_no_borrower_facing_copy_names_a_person_or_the_old_number(name):
    for banned in BANNED:
        assert banned not in _ALL_COPY[name], f"{name} contains {banned!r}"


@pytest.mark.parametrize("name", sorted(_SMS_TEXTS))
def test_every_text_names_the_business(name):
    assert BRAND in _SMS_TEXTS[name], name


def test_the_sender_and_signer_are_the_business():
    assert lending_reminders.EMAIL_FROM_NAME == BRAND
    assert lending_prequal.BORROWER_SIGNER == BRAND
    assert all(BRAND in subject for subject in (
        lending_reminders.EMAIL_SUBJECT_CONFIRMATION, lending_reminders.EMAIL_SUBJECT_NIGHT_BEFORE,
        lending_reminders.EMAIL_SUBJECT_NINETY_MIN, lending_reminders.EMAIL_SUBJECT_GATE_FAIL))


def test_one_callback_number_everywhere():
    assert lending_prequal.BORROWER_CALLBACK_PHONE == "(656) 221-1100"


def test_the_two_consent_boxes_are_separate():
    sms, call = lending_web.SMS_CONSENT_TEXT, lending_web.CALL_CONSENT_TEXT
    assert BRAND in sms and BRAND in call
    assert "text messages" in sms and "may call me" not in sms  # the text box does not also grant calls
    assert "may call me" in call and "text message" not in call.lower()  # the call box does not also grant texts
    assert "To stop calls" in call and "optout@nextdeallending.com" in call  # a call box needs its own way to stop
    assert "STOP" in sms and "HELP" in sms and "Message and data rates may apply" in sms
    assert "not a condition of any financing" in sms and "not a condition of any financing" in call
