# Durable inbound webhooks (R1 0.5)

Apply the inbound-events migration before enabling LENDING_INBOUND_JOURNAL_ENABLED.
The application authenticates requests before saving them. The five-minute
webhook-failure job reclaims interrupted journal work and retries saved handler
failures. Use the DLQ CLI to inspect or replay saved failures:

    python -m src.lending.inbound.dlq_handler --list
    python -m src.lending.inbound.dlq_handler --replay ID

Do not capture LendingFlow bodies in nginx logs. If the previous example was
installed, remove its body logging directive and validate the nginx configuration
before reloading. For LendingFlow deliveries that never reached the authenticated
journal, request a resend from LendingFlow. The nightly reconciliation job
queries GHL for missing stage changes when LENDING_RECONCILE_ENABLED is enabled.
