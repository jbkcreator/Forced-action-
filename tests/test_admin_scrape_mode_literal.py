"""
src.api.admin_router.ScrapeMode — the Pydantic Literal gating
CountySourceCreateRequest/CountySourceUpdateRequest.scrape_mode.

Regression for a High finding in PR review: this PR added nodriver_only/
nodriver_then_ai to the county_sources.scrape_mode DB CheckConstraint
(src/core/models.py) and to migrations/apply_nodriver_scrape_modes.py, but
never updated this Pydantic Literal — so an admin trying to set either new
mode through the admin API got a 422 validation error even though the DB
now permits it, leaving those modes manageable only by hand-editing the DB
or via the one-time migration.
"""
import pytest
from pydantic import ValidationError

from src.api.admin_router import CountySourceCreateRequest, CountySourceUpdateRequest


@pytest.mark.parametrize("mode", ["nodriver_only", "nodriver_then_ai"])
def test_create_request_accepts_new_nodriver_modes(mode):
    req = CountySourceCreateRequest(
        signal_type="liens", url="https://example.com", scrape_mode=mode,
    )
    assert req.scrape_mode == mode


@pytest.mark.parametrize("mode", ["nodriver_only", "nodriver_then_ai"])
def test_update_request_accepts_new_nodriver_modes(mode):
    req = CountySourceUpdateRequest(scrape_mode=mode)
    assert req.scrape_mode == mode


def test_create_request_still_rejects_an_unknown_mode():
    with pytest.raises(ValidationError):
        CountySourceCreateRequest(
            signal_type="liens", url="https://example.com", scrape_mode="not_a_real_mode",
        )
