# Alembic → scripts conversion: coverage report

Every alembic migration's schema is represented in `scripts/apply_*.py`:
160 auto-converted (verbatim SQL, offline render), 23 already had a script, and
7 that couldn't auto-render (mixed schema+data migrations) were **hand-backfilled**.
The remaining 20 are merge revisions / pure data backfills with no schema to script.

## Hand-backfilled (7) — schema DDL extracted manually, idempotent

These use `upgrade(conn)` or read the DB (`fetchone`/`scalar`) so they can't
render offline. DDL extracted by hand, made idempotent, SQL-parse validated.

- `apply_fa062_email_campaigns.py` — email_campaigns / campaign_contacts / campaign_daily_analytics / email_sequence_templates + dbpr_contacts columns
- `apply_fa063_campaign_instantly_settings.py` — email_campaigns.instantly_settings
- `apply_b1c2d3e4f5a6_add_trgm_indexes_for_matching.py` — pg_trgm GIN indexes (guarded on extension)
- `apply_fa029_add_normalized_address.py` — properties.normalized_address + indexes
- `apply_fa_s0_reactivation_foundation.py` — subscribers.last_reactivation_attempt_at + gold_plus_zip_snapshots
- `apply_z0a1b2c3d4e5_add_chat_sessions_and_messages.py` — chat_sessions + chat_messages
- `apply_p6q7r8s9t0u1_add_bundle_purchases.py` — bundle_purchases

## No schema to script (20) — merge revisions / data-only backfills

Nothing to convert (merge heads, or pure `UPDATE`/data migrations):

- `13847230a4d6_merge_fa013_referral_team_broken_audit_`
- `2750ffc7086f_merge_vendor_cost_monitor_with_main`
- `5b3cff139150_merge_fa017_signup_source_owner_manager_`
- `a1b2c3_add_agent_decisions_subscriber_started_idx`
- `b4c8ae3057fa_merge_sunbiz_skiptrace_with_retune_`
- `fa017_sms_send_logs`
- `fa018_phone_normalize_backfill` — data backfill (phone normalization)
- `fa020_dlq_reason_widen`
- `fa023_human_close_hardening`
- `fa025_merge_all_heads`
- `fa028_merge_scoring_indexes`
- `fa030_merge_foreclosures_defendant`
- `fa045_add_expansion_candidates`
- `fa046_add_court_dockets_source_type`
- `fa049_merge_all_heads`
- `fa050_expansion_icp_channels`
- `fa051_merge_open_heads`
- `fa051_predictive_churn`
- `fa078_tracerfy_trace_type`
- `w7x8y9z0a1b2_add_referral_core_loop`
