"""
HTTP request helpers with built-in retry logic.
"""

import logging
import time
from typing import Optional
from uuid import uuid4

import requests

logger = logging.getLogger(__name__)


def get_playwright_proxy() -> Optional[dict]:
    """
    Return a Playwright proxy config dict if OXYLABS_PROXY_URL is set in .env.
    Returns None if not configured (scrapers run without proxy).

    Usage in playwright:
        browser = await pw.chromium.launch(proxy=get_playwright_proxy(), ...)
    """
    from config.settings import settings
    if not settings.oxylabs_username or not settings.oxylabs_password:
        logger.info("[Proxy] OXYLABS_USERNAME/PASSWORD not set — running without proxy")
        return None
    username = settings.oxylabs_username
    if settings.oxylabs_rotate:
        username = f"{username}-sessid-{uuid4().hex[:8]}"
    password = settings.oxylabs_password.get_secret_value()
    logger.info(f"[Proxy] Using proxy")
    return {
        "server": "http://pr.oxylabs.io:7777",
        "username": username,
        "password": password,
    }


def get_browser_use_proxy():
    """
    Return a browser_use ProxySettings if OXYLABS_PROXY_URL is set in .env.
    Returns None if not configured.

    Usage in browser_use:
        browser = Browser(proxy=get_browser_use_proxy(), ...)
    """
    from config.settings import settings
    if not settings.oxylabs_username or not settings.oxylabs_password:
        return None
    username = settings.oxylabs_username
    if settings.oxylabs_rotate:
        username = f"{username}-sessid-{uuid4().hex[:8]}"
    from browser_use.browser.profile import ProxySettings
    return ProxySettings(
        server="http://pr.oxylabs.io:7777",
        username=username,
        password=settings.oxylabs_password.get_secret_value(),
    )


def get_requests_proxies(rotate: bool = False) -> Optional[dict]:
    """
    Return a requests-compatible proxies dict for Oxylabs, or None if not configured.

    Usage:
        resp = requests.get(url, proxies=get_requests_proxies(rotate=True), ...)
    """
    from config.settings import settings
    if not settings.oxylabs_username or not settings.oxylabs_password:
        logger.warning("[Proxy] OXYLABS_USERNAME/PASSWORD not found — requests will be sent directly")
        return None
    username = settings.oxylabs_username
    if rotate or settings.oxylabs_rotate:
        username = f"{username}-sessid-{uuid4().hex[:8]}"
    password = settings.oxylabs_password.get_secret_value()
    proxy_url = f"http://{username}:{password}@pr.oxylabs.io:7777"
    return {"http": proxy_url, "https": proxy_url}


# Retry on network-level failures and server errors; never retry on client errors (4xx)
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def requests_get_with_retry(
    url: str,
    max_retries: int = 5,
    retry_delay: int = 5,
    use_proxy: bool = False,
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
        use_proxy:   Route through Oxylabs proxy if credentials are configured.
        **kwargs:    Forwarded to requests.get (headers, params, timeout, etc.).

    Returns:
        requests.Response

    Raises:
        requests.HTTPError, requests.Timeout, requests.ConnectionError, etc.
    """
    _proxy_warned = False
    for attempt in range(1, max_retries + 1):
        request_kwargs = kwargs
        if use_proxy:
            proxies = get_requests_proxies()
            if proxies:
                request_kwargs = {**kwargs, "proxies": proxies}
            elif not _proxy_warned:
                logger.warning("[Proxy] use_proxy=True but OXYLABS_USERNAME/PASSWORD not set — sending direct")
                _proxy_warned = True
        try:
            response = requests.get(url, **request_kwargs)  # type: ignore[arg-type]
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
