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

# Pool (launch queue) -> dialer campaign name (Go Live Brief 2.5). The campaigns must
# exist in BatchDialer with these exact names; a live load refuses any unmapped pool.
POOL_CAMPAIGN_TAGS: dict[str, str] = {
    "verified_maturity": "Verified maturity",
    "transaction_ready": "Transaction ready",
    "builders": "Builders",
}


# BatchDialer (client decision, Go Live Brief 2.4). Auth header X-ApiKey. Each endpoint
# stays unset until confirmed with a write test on a test campaign (E1);
# an unset endpoint raises UnconfirmedCapability and removals stay pending.
# Confirmed 2026-09-30 with the client key (GET /campaigns, /contacts, /cdrs, /lists -> 200).
BATCHDIALER_BASE_URL: str = "https://app.batchdialer.com/api"
BATCHDIALER_TIMEOUT_SECONDS: float = 20.0
BATCHDIALER_ENDPOINTS: dict[str, "tuple[str, str] | None"] = {
    "contact_upsert": None,
    "contact_update": None,
    "campaign_remove": None,
    "campaign_restore": None,
    "dnc_add": None,
}
# Path discovery (read-only, 2026-09-30): GET-405 (exists, other method) on /contact,
# /dnclist, /campaigns/search; GET-200 on /cdrs (call records, paged) and /lists.
# The API lists no remove-from-campaign action: holds use DNC add/delete (Option A).
