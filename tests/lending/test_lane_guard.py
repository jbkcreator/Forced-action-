"""Brief section 7 / P1.5: Backflip names, numbers and domains never leave through Cora."""
from __future__ import annotations

import pytest

from src.lending.lane_guard import backflip_hit


@pytest.mark.parametrize("value, label", [
    ("BackFlip has a better rate", "word"),
    ("call 656.209.6700 today", "phone"),
    ("(656) 207 6500", "phone"),
    ("+1 656-209-6700", "phone"),
    ("x@BACKFLIP.com", "word"),
    ("see backflip.com/apply", "word"),
])
def test_backflip_any_case_and_phone_format(value: str, label: str) -> None:
    assert backflip_hit(value) == label


def test_domain_alone_is_caught() -> None:
    assert backflip_hit("mail@sub.BACKFLIP.COM") is not None


def test_clean_text_passes() -> None:
    assert backflip_hit("Next Deal Lending, (656) 221-1100", "mark@example.com") is None


def test_none_values_ignored() -> None:
    assert backflip_hit(None, "", None) is None


def test_hit_in_any_of_several_values() -> None:
    assert backflip_hit("clean body", None, "16562076500") == "phone"
