# Alembic → scripts conversion: not auto-converted

Rendered verbatim SQL for every alembic migration into `scripts/apply_<slug>.py`. The migrations below were NOT converted — review/port by hand if ever needed.

## Unrenderable (27) — data migrations / offline-incompatible

- `13847230a4d6_merge_fa013_referral_team_broken_audit_` — empty (merge/data-only)
- `2750ffc7086f_merge_vendor_cost_monitor_with_main` — empty (merge/data-only)
- `5b3cff139150_merge_fa017_signup_source_owner_manager_` — empty (merge/data-only)
- `a1b2c3_add_agent_decisions_subscriber_started_idx` — empty (merge/data-only)
- `b1c2d3e4f5a6_add_trgm_indexes_for_matching` — AttributeError: 'NoneType' object has no attribute 'fetchone'
- `b4c8ae3057fa_merge_sunbiz_skiptrace_with_retune_` — empty (merge/data-only)
- `fa017_sms_send_logs` — empty (merge/data-only)
- `fa018_phone_normalize_backfill` — AttributeError: 'NoneType' object has no attribute 'fetchall'
- `fa020_dlq_reason_widen` — empty (merge/data-only)
- `fa023_human_close_hardening` — empty (merge/data-only)
- `fa025_merge_all_heads` — empty (merge/data-only)
- `fa028_merge_scoring_indexes` — empty (merge/data-only)
- `fa029_add_normalized_address` — AttributeError: 'NoneType' object has no attribute 'fetchone'
- `fa030_merge_foreclosures_defendant` — empty (merge/data-only)
- `fa045_add_expansion_candidates` — empty (merge/data-only)
- `fa046_add_court_dockets_source_type` — empty (merge/data-only)
- `fa049_merge_all_heads` — empty (merge/data-only)
- `fa050_expansion_icp_channels` — empty (merge/data-only)
- `fa051_merge_open_heads` — empty (merge/data-only)
- `fa051_predictive_churn` — empty (merge/data-only)
- `fa062_email_campaigns` — TypeError: upgrade() missing 1 required positional argument: 'conn'
- `fa063_campaign_instantly_settings` — TypeError: upgrade() missing 1 required positional argument: 'conn'
- `fa078_tracerfy_trace_type` — empty (merge/data-only)
- `fa_s0_reactivation_foundation` — AttributeError: 'NoneType' object has no attribute 'scalar'
- `p6q7r8s9t0u1_add_bundle_purchases` — AttributeError: 'NoneType' object has no attribute 'scalar'
- `w7x8y9z0a1b2_add_referral_core_loop` — empty (merge/data-only)
- `z0a1b2c3d4e5_add_chat_sessions_and_messages` — AttributeError: 'NoneType' object has no attribute 'scalar'

## Skipped: script already existed (23)

- `fa057_synthflow_inbound`
- `fa084_s1_revenue_engine`
- `fa085_m10_lead_delivery`
- `fa093_a2_lead_confidence`
- `fa096_macro_signals`
- `fa096_unified_subscriber_memory`
- `fa097_subscriber_memory_summary`
- `fa098_loan_lane_lenders_claim_tracking`
- `fa101_stream_diagnostics`
- `fa102_platform_daily_stats_stream_cols`
- `fa103_competitor_rate_sheets`
- `fa_5_2_seo_pages`
- `fa_5_2b_seo_city_index`
- `fa_6_3_churn_defense`
- `fa_a1_loss_autopsies`
- `fa_a3_scoring_weight_overrides`
- `fa_a4_enrichment_anomaly`
- `fa_a5_pre_decision_snapshots`
- `fa_a6_underwriting_feedback`
- `fa_outbound_pacing`
- `fa_s1_commission_ledger`
- `fa_s1_financing_intent_scores`
- `fa_s5_enhancement_loops`
