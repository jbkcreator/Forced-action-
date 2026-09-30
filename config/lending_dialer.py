"""Lending dialer load: pool-to-campaign mapping and provider request limits.

Aircall allows 120 Public API requests per minute per company and returns
HTTP 429 beyond that. The account sends no rate-limit headers, so the client
paces itself to the documented limit rather than reacting to headers.
"""

AIRCALL_REQUESTS_PER_MINUTE: int = 120
AIRCALL_MAX_ATTEMPTS: int = 5
AIRCALL_RETRY_BASE_SECONDS: float = 2.0
AIRCALL_MAX_RETRY_WAIT_SECONDS: float = 60.0
AIRCALL_REQUEST_TIMEOUT_SECONDS: float = 20.0

# Pool -> dialer campaign. Empty until the client decides which pool goes to
# which campaign; a live load refuses to run for any pool without an entry here.
POOL_CAMPAIGN_TAGS: dict[str, str] = {}
