"""WP-T2-6 abandonment — borrower-facing copy must never price or commit.

Spec §Compliance: "never sends a rate, term, or commitment to a borrower."
"""
from __future__ import annotations

import re

import pytest

from src.agents.reply_concierge.abandonment_agent import _TOUCH_TEMPLATES

_FORBIDDEN = re.compile(r"\b(rates?|lock\w*|apr|interest|term sheet)\b|%", re.IGNORECASE)


@pytest.mark.parametrize("touch_number", sorted(_TOUCH_TEMPLATES))
def test_touch_copy_has_no_pricing_or_commitment_language(touch_number):
    template = _TOUCH_TEMPLATES[touch_number]
    assert not _FORBIDDEN.search(template), f"touch {touch_number}: {template!r}"
