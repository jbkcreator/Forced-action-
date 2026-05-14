Final selected option list — Compliance Baseline Q&A
Q1 — Inbound YES disambiguation:
Selected: B — Use opt_in_pending:{phone} Redis sentinel. Only record opt-in if user was actually sent an opt-in prompt.
Q2 — Fix quiet_hours DLQ constraint:
Selected: A — Add quiet_hours to allowed sms_dead_letters.reason values through Alembic migration.
Q3 — Marketing vs transactional classification:
Selected: A — Add explicit message_type parameter to send_sms(), defaulting to marketing.
Q4 — Opt-in prompt bypass:
Selected: B — Add special message_type="opt_in_prompt" so opt-in prompt can bypass opt-in gate but still check opt-out and quiet hours.
Q5 — _find_subscriber() phone mapping:
Selected: B — Use existing SmsOptIn table to map phone → subscriber_id → subscriber.
Q6 — Per-send audit logging:
Selected: B — Add a new lightweight SmsSendLog table for every SMS attempt.
Q7 — Phone number standard format:
Selected: A — Use strict E.164 format everywhere, example: +18135550100.
Q8 — Phone normalization rollout:
Selected: A — Run dry-run collision report first, then Alembic data migration to normalize old rows.
Q9 — Redis STOP/DLQ:
Selected: A — Defer Redis for now. Keep Postgres and document/code-comment the decision.
Q10 — Twilio → Telnyx cleanup:
Selected: Codebase-only check — No doc cleanup needed. But if any runtime/code implementation still uses Twilio, mention it clearly.
Q11 — Test strategy:
Selected: B — Mixed test strategy: webhook flow as scenario tests, internal logic as unit tests.
Q12 — has_opted_in() gate order:
Selected: B — Use this order inside send_sms():
opt-out → opt-in → quiet hours → credentials → send