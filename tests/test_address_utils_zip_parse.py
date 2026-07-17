"""split_address ZIP parsing — regression for the 5-digit-house-number bug.

Pinellas beach-city house numbers are themselves 5 digits (14339, 13413, ...).
A first-match 5-digit scan grabbed the house number as the ZIP, poisoning the
property-match filter and producing a silent 0% Pinellas violation match rate.
The ZIP must anchor to the state/tail, never the leading house number.
"""
from src.loaders._address_utils import split_address


def test_five_digit_house_number_not_stolen_as_zip():
    street, city, zc = split_address("14339 110TH TER N, LARGO FL 33774")
    assert zc == "33774"           # not "14339"
    assert street == "14339 110TH TER N"
    assert city == "LARGO"


def test_five_digit_house_number_no_comma():
    _, _, zc = split_address("13413 CORONADO DR LARGO FL 33774")
    assert zc == "33774"


def test_zip_plus_four_returns_prefix():
    _, _, zc = split_address("2617 BUCKHORN PRESERVE BLVD, VALRICO, FL 33594-6511")
    assert zc == "33594"


def test_standard_short_house_number_still_parses():
    # Hillsborough (3-4 digit house numbers) must be unaffected.
    _, _, zc = split_address("123 MAIN ST APT 5 TAMPA FL 33602")
    assert zc == "33602"


def test_no_zip_present_does_not_return_house_number():
    # Was the core bug: with no real ZIP, the 5-digit house number must NOT be
    # returned as the ZIP — better to return None and let the cascade skip the
    # ZIP filter than to poison it with a bogus ZIP.
    _, _, zc = split_address("14339 110TH TER N")
    assert zc is None
