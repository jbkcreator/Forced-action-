"""Lending dialer load: Aircall write limits and retry policy.

Aircall allows 120 Public API requests per minute per company and returns
HTTP 429 beyond that. The account sends no rate-limit headers, so the client
paces itself to the documented limit rather than reacting to headers.
"""

AIRCALL_REQUESTS_PER_MINUTE: int = 120
AIRCALL_MAX_ATTEMPTS: int = 5
AIRCALL_RETRY_BASE_SECONDS: float = 2.0
AIRCALL_MAX_RETRY_WAIT_SECONDS: float = 60.0
AIRCALL_REQUEST_TIMEOUT_SECONDS: float = 20.0

# Pool -> Aircall campaign tag. Empty until the client decides which pool gets
# DESK_CAPITAL_LOOP / DESK_CONSTRUCTION / DESK_RESCUE; a live load refuses to
# run for any pool without an entry here.
POOL_CAMPAIGN_TAGS: dict[str, str] = {}


# BatchDialer (client decision, Go Live Brief 2.4). Auth header X-ApiKey. The base URL
# and each endpoint stay unset until confirmed against the client's account (E1);
# an unset endpoint raises UnconfirmedCapability and removals stay pending.
BATCHDIALER_BASE_URL: str = ""
BATCHDIALER_TIMEOUT_SECONDS: float = 20.0
BATCHDIALER_ENDPOINTS: dict[str, "tuple[str, str] | None"] = {
    "contact_upsert": None,
    "campaign_remove": None,
    "campaign_restore": None,
    "dnc_add": None,
}
