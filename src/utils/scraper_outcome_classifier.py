"""
The one place that decides which ScraperOutcome category a failure belongs
to. Every scraper/connector should call classify_exception()/
classify_http_status() instead of hand-picking an error_type string at each
call site — that per-site guessing is exactly what produced two undocumented
error_type values in production (evictions_engine.py's 'export_unavailable',
connectors/runner.py's 'connector_error') and the same FEMA timeout being
labeled 'no_data' on one day and 'scraper_error' on another.

Both functions are pure (no I/O, no DB) — testable standalone.
"""
from __future__ import annotations

import logging
from typing import Optional

import requests

from config.scraper_outcomes import ScraperOutcome
from src.utils.http_helpers import RETRYABLE_STATUS_CODES
from src.utils.scraper_exceptions import ScraperNoDataError

logger = logging.getLogger(__name__)


def classify_http_status(status_code: int) -> str:
    """429/5xx = vendor misbehaved (SOURCE_ERROR) — reuses
    http_helpers.RETRYABLE_STATUS_CODES exactly, so this can never disagree
    with requests_get_with_retry's own retry/no-retry split. Other 4xx = we
    sent a bad request (INTERNAL_ERROR) — stale key, wrong URL/params, our
    bug to fix, not the vendor's."""
    if status_code in RETRYABLE_STATUS_CODES:
        return ScraperOutcome.SOURCE_ERROR.value
    if 400 <= status_code < 500:
        return ScraperOutcome.INTERNAL_ERROR.value
    return ScraperOutcome.UNKNOWN.value


def classify_exception(exc: BaseException) -> str:
    """Map a caught exception to a ScraperOutcome value. Anything not
    explicitly recognized returns UNKNOWN and is logged — a classifier gap
    should be visible and fixable, never silently folded into whichever
    category happened to be the default."""
    if isinstance(exc, ScraperNoDataError):
        return ScraperOutcome.NO_DATA.value

    if isinstance(exc, requests.HTTPError):
        status = exc.response.status_code if exc.response is not None else 0
        return classify_http_status(status)

    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        # ConnectionError (refused/DNS failure/reset) never got a response
        # either — bucketed with Timeout under "request didn't complete."
        # There is no separate "unreachable" category; both mean the same
        # thing for freshness purposes: we don't know if data exists.
        return ScraperOutcome.TIMEOUT.value

    try:
        from playwright._impl._errors import TimeoutError as PlaywrightTimeoutError
        if isinstance(exc, PlaywrightTimeoutError):
            return ScraperOutcome.TIMEOUT.value
    except ImportError:
        pass

    logger.warning(
        "[classify_exception] Unrecognized exception type %s — returning UNKNOWN "
        "(add a rule here if this recurs): %s",
        type(exc).__name__, exc,
    )
    return ScraperOutcome.UNKNOWN.value
