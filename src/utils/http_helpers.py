"""
HTTP request helpers with built-in retry logic.
"""

import logging
import time

import requests

logger = logging.getLogger(__name__)

# Retry on network-level failures and server errors; never retry on client errors (4xx)
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def requests_get_with_retry(
    url: str,
    max_retries: int = 5,
    retry_delay: int = 5,
    **kwargs,
) -> requests.Response:
    """
    requests.get wrapper with automatic retry on transient failures.

    Retries on:
      - requests.Timeout / requests.ConnectionError (network issues)
      - HTTP 429 (rate-limited) and 5xx (server errors)

    Does NOT retry on:
      - 4xx client errors (except 429)

    Args:
        url:         Target URL.
        max_retries: Number of attempts before giving up (default 5).
        retry_delay: Seconds to wait between attempts (default 5).
        **kwargs:    Forwarded to requests.get (headers, params, timeout, etc.).

    Returns:
        requests.Response with status verified via raise_for_status().

    Raises:
        requests.HTTPError, requests.Timeout, requests.ConnectionError, etc.
    """
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, **kwargs)
            response.raise_for_status()
            return response
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt < max_retries:
                logger.warning(
                    f"Request attempt {attempt}/{max_retries} failed ({type(e).__name__}): {e}"
                    f" — retrying in {retry_delay}s..."
                )
                time.sleep(retry_delay)
                continue
            logger.error(f"All {max_retries} request attempts exhausted: {e}")
            raise
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status in _RETRYABLE_STATUS_CODES and attempt < max_retries:
                logger.warning(
                    f"Request attempt {attempt}/{max_retries} got HTTP {status}: {e}"
                    f" — retrying in {retry_delay}s..."
                )
                time.sleep(retry_delay)
                continue
            logger.error(f"HTTP error (not retried): {e}")
            raise
