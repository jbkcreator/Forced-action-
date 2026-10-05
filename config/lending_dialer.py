"""Lending dialer load: queue -> BatchDialer campaign names, hook lines and endpoints."""

# Pool (launch queue) -> dialer campaign name (Go Live Brief 2.5). The campaigns must
# exist in BatchDialer with these exact names; a live load refuses any unmapped pool.
POOL_CAMPAIGN_TAGS: dict[str, str] = {
    "verified_maturity": "Verified maturity",
    "transaction_ready": "Transaction ready",  # List 2 (cash buyers) rides along here too (Oct 4 §2)
    "builders": "Builders",
    "partners": "Partners",  # List 4: dialed as a real queue, partner script only, never bookable
    "nurture": "Nurture",    # fallback for any future list with no rank of its own
}

# Hook line per campaign, shown on the caller's card. Seeded from the client's voicemail
# scripts (2026-10-01) until the playbook's per-campaign hooks arrive.
CAMPAIGN_HOOKS: dict[str, str] = {
    "Verified maturity": "It looks like the loan on it is coming up; we help investors get ahead of that.",
    "Transaction ready": "Saw you picked up the property; we can help fund your next deal.",
    "Builders": "Saw your project; we fund builders and investors in your county.",
    "Partners": "You move deals, we fund buyers; let's connect our pipelines.",
}


# BatchDialer (client decision, Go Live Brief 2.4). Auth header X-ApiKey. Each endpoint
# stays unset until confirmed with a write test on a test campaign (E1);
# an unset endpoint raises UnconfirmedCapability and removals stay pending.
# Confirmed 2026-09-30 with the client key (GET /campaigns, /contacts, /cdrs, /lists -> 200).
BATCHDIALER_BASE_URL: str = "https://app.batchdialer.com/api"
BATCHDIALER_TIMEOUT_SECONDS: float = 20.0
BATCHDIALER_ENDPOINTS: dict[str, "tuple[str, str] | None"] = {
    # Confirmed live 2026-09-30: create/read/update/delete on a test contact.
    "contact_upsert": ("POST", "/contact"),
    "contact_update": ("PUT", "/contact/{id}"),
    # Public API docs, "Add contacts": imports straight into the given campaign ids.
    # Exercised live with the first campaign.
    "contacts_add_to_campaign": ("POST", "/contacts"),
    "campaign_remove": None,
    "campaign_restore": None,
    "dnc_add": None,
    # Opt-outs delete the contact (no DNC endpoint in the public API). Confirmed live 2026-09-30.
    "contact_delete": ("DELETE", "/contact/{id}"),
}
# Path discovery (read-only, 2026-09-30): GET-405 (exists, other method) on /contact,
# /dnclist, /campaigns/search; GET-200 on /cdrs (call records, paged) and /lists.
# DNC safety: temporary holds (window/cap) only leave and rejoin the campaign; they never
# use the DNC list, and there is deliberately no DNC-delete endpoint, so a restore can
# never remove a real DNC entry. The API lists no remove-from-campaign action, so holds
# stay pending until campaign_remove / campaign_restore are confirmed with a write test.
