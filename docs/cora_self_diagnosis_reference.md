# Cora Self-Diagnosis Reference

**Status:** Supervised-mode policy reference
**Scope:** The 7 monitored kill-switch metrics, their thresholds, and what Cora is — and is not — allowed to do autonomously when each one breaches.

Source of truth (do not edit thresholds here; edit the code and regenerate this doc):
- Thresholds & action policy → `config/cora_guardrails.py` (`KILL_SWITCH`, `CORA_SELF_HEALING`)
- Stage 10 overrides → `config/stage10_config.py` (`STAGE10_KILL_SWITCH_OVERRIDES`)
- State machine & action dispatch → `src/tasks/cora_self_healing.py`
- Metric computation & baselines → `src/tasks/kill_switch_metric_ingest.py`
- Weekly Revenue Pulse scorecard → `src/tasks/revenue_pulse.py` (`_format_kill_switch_scorecard`)
- Cora guardrail ranges → `config/cora_guardrails.py` (`GUARDRAILS`)

Master switch: **`CORA_SELF_HEALING_ENABLED`** (default `false`). When off, the loop is a no-op — it reads nothing and takes no action. Schedule: hourly cron (half-hour offset), after the metric-ingest job.

---

## 1. How a metric is graded

Every run, per metric, Cora reads the **current value** (Redis cache, written daily by `kill_switch_metric_ingest`) and a **7-day rolling baseline** (`platform_daily_stats`), then grades severity against the metric's `green`/`red` thresholds:

| Direction | green | red | yellow |
|---|---|---|---|
| `higher_is_better` | `observed >= green` | `observed < red` | between |
| `lower_is_better` | `observed <= green` | `observed > red` | between |

- A value of `None` (metric not computable) → **`unknown`** → **no-op** (fail-safe; never actioned).
- All 7 monitored metrics below are `higher_is_better`.

### State machine (per metric × county)

| Current state | Open incident? | Action |
|---|---|---|
| green | no | no-op |
| green | yes | **close** incident (`action_taken=resolved`), Slack "resolved" |
| yellow / red | no | **open** incident (`no_op`), Slack "new" |
| yellow / red | yes, age `< duration_hours_for_action` | keep **observing**, no-op |
| yellow / red | yes, age `>= duration_hours_for_action`, not yet actioned | **take the metric's action** (see §3) |
| red | yes, age `>= kill_after_red_days` | **recommend kill** (recommendation only — never auto-disables) |

---

## 2. The 7 monitored metrics

These are the metrics that are **both** threshold-defined **and** computed with live data + baseline-tracked. (Three further metrics carry guardrails but no data yet — see §5.)

| # | Metric | Green | Yellow | Red | Action class | Duration → action | Kill window |
|---|---|---|---|---|---|---|---|
| 1 | `first_payment_rate` | ≥30% | 20–30% | <20% | **Escalate** (→ auto **variant promotion** under Stage 10) | 48h | 7d red |
| 2 | `saved_card_rate` | ≥70% | 50–70% | <50% | **Escalate only** | 48h | 7d red |
| 3 | `wallet_adoption` | ≥15% | 10–15% | <10% | **Auto-correct** (fallback) | 48h | 7d red |
| 4 | `lock_conversion` | ≥5% | 3–5% | <3% | **Auto-correct** (fallback) | 48h | 7d red |
| 5 | `retention_30d` | ≥70% | 55–70% | <55% | **Escalate only** | 72h | 14d red |
| 6 | `sms_reply_rate` | ≥8% | 5–8% | <5% | **Auto-correct** (fallback) | 48h | 7d red |
| 7 | `offer_acceptance_rate` | ≥15% | 10–15% | <10% | **Auto-correct** (fallback) | 48h | 7d red |

### FA-2B-v9 ambiguity: first_payment_rate below 25% for 48 hours

The FA-2B-v9-FINAL spec additionally says self-healing incident response should happen when `first_payment_rate` drops below **25% for 48 hours**.

**How this is covered by the existing implementation:**

| Value | Band | Behavior | 48h? |
|---|---|---|---|
| 31% | Green | No-op | — |
| 25–30% | Yellow (20–30%) | Opens incident → at 48h, escalates to human | ✅ Covered |
| 20–24% | Yellow (20–30%) | Opens incident → at 48h, escalates to human | ✅ Covered |
| <20% | Red (<20%) | Opens incident → at 48h, escalates to human | ✅ Covered |

The Yellow band (20–30%) with `duration_hours_for_action: 48` already triggers incident opening + escalation after 48 hours for **any value between 20% and 30%**, which includes the <25% zone. The exact threshold 25% is additionally used by Stage 10 (`stage10_alert_threshold: 25` in `config/stage10_config.py`): when Stage 10 is active and `first_payment_rate` drops to 25% or below for 48 hours, Cora can autonomously promote a winning message variant rather than just escalating to a human.

**No code changes needed.** The Yellow band covers this fully. This note documents the resolution.

### Weekly Revenue Pulse scorecard

Every Monday at 09:00 UTC, the weekly Revenue Pulse SMS includes a **per-metric Green/Yellow/Red scorecard** for all 7 active kill-switch metrics. The scorecard reads each metric's current value from the Redis cache (written daily by `kill_switch_metric_ingest`), grades it against its thresholds, and formats a compact line:

```
KS: FPR G | SCR Y | WA G | LC R | R30 G | SMS G | OAR G
```

This line is injected into the weekly SMS body alongside the existing incident summary and autonomy stats.

**Auto-correctable (4):** `wallet_adoption`, `lock_conversion`, `sms_reply_rate`, `offer_acceptance_rate` — all use a reversible Redis fallback flag, no human approval required.
**Escalation-only (2):** `saved_card_rate`, `retention_30d` — touch pricing/structural funnel; require human review.
**Conditional (1):** `first_payment_rate` — escalation-only by default; becomes auto-correctable via variant promotion **only when Stage 10 is active** (see §4).

---

## 3. What each metric does on breach (detail)

For each metric: what it measures, why it's classified the way it is, and exactly what "auto-correct" does.

### 1. `first_payment_rate` — % of free users who became paying within 30 days
- **Default policy:** `human_escalated`, `requires_approval=true`. Touches the checkout funnel + pricing — too sensitive for an autonomous fix. At 48h red, Cora opens an incident, posts Slack, and surfaces it in Revenue Pulse; a human decides.
- **Stage 10 behavior (when `STAGE10_KILL_SWITCH_OVERRIDES` is importable):** flips to `auto_action_type=variant_promotion`, `requires_approval=false`. Auto-correct = **promote the winning message variant** in sequence `wallet_push_v1` and auto-retire the losing slot (`variant_engine.promote_winner`). Recorded as `action_taken=auto_paused`. If no eligible promotion exists, it **falls back to human escalation**.

### 2. `saved_card_rate` — % of active subscribers with a saved card
- **Policy:** `human_escalated`, `requires_approval=true`. **No autonomous action.** Auto-correct = none. At 48h red, escalates to a human (Slack + Revenue Pulse). The intended human play is "default harder / bonus credits" — a pricing/offer decision Cora may not make alone.

### 3. `wallet_adoption` — % of saved-card users with a wallet balance
- **Policy:** `fallback_enabled`, `requires_approval=false`. **Auto-correct:** at 48h red, set Redis `kill_switch:accelerated_wallet_push_paused = red` (24h TTL). The `decision_hierarchy` subgraph reads this on the next decision and pauses the Accelerated Wallet Push path back to its baseline. Fully reversible.
- **Separate Day-35 floor:** if accelerated-wallet-push take-rate < **12%** after 35 days, `kill_switch_metric_ingest` independently flips `kill_switch:accelerated_wallet_push = red`.

### 4. `lock_conversion` — % of wallet subs upgrading to annual lock within 60 days
- **Policy:** `fallback_enabled`, `requires_approval=false`. **Auto-correct:** at 48h red, set Redis `kill_switch:lock_close_use_fallback = red` (24h TTL). The lock-close path drops from Claude-composed SMS to a static template. Reversible.

### 5. `retention_30d` — % of payers active 30 days ago still active today
- **Policy:** `human_escalated`, `requires_approval=true`, slower cadence (`duration=72h`, kill window 14d). **No autonomous action.** Retention drift is structural; a mid-stream automated fix can't address it. Escalates to a human.

### 6. `sms_reply_rate` — % of marketing SMS (7d) with a reply
- **Policy:** `fallback_enabled`, `requires_approval=false`. **Auto-correct:** at 48h red, set Redis `kill_switch:cora_use_static_copy = red` (24h TTL). Cora swaps from Claude-composed copy to the spec-approved static template (copy/timing issue is the usual cause). Reversible.

### 7. `offer_acceptance_rate` — % of wallet-push/bundle offers accepted (7d)
- **Policy:** `fallback_enabled`, `requires_approval=false`. **Auto-correct:** at 48h red, set Redis `kill_switch:offer_use_baseline_template = red` (24h TTL). Offer composition reverts to the baseline template. Reversible.

---

## 4. The three action types — what they mechanically do

| `auto_action_type` | Autonomous? | Mechanism | Reversible? | Recorded as |
|---|---|---|---|---|
| `fallback_enabled` | **Yes** (no approval) | `rset("kill_switch:{flag}", "red", ttl=24h)`. `decision_hierarchy` reads the flag and routes to the fallback/static/disabled path. | Yes — TTL expiry, or incident auto-closes when metric returns to green | `fallback_enabled` |
| `variant_promotion` (Stage 10) | **Yes** (no approval) | `variant_engine.promote_winner(sequence)` — auto-pauses the losing 3-variant slot, promotes the best. Falls back to escalation if nothing eligible. | Via variant engine's own revert/proving cycle | `auto_paused` |
| `human_escalated` | **No** | Records the incident, posts Slack "human required" + Revenue Pulse. **Makes no change to the system.** | N/A — human acts | `human_escalated` |

### Kill recommendation (all metrics, terminal)
When a metric is **red continuously for `kill_after_red_days`** (7d most; 14d for `retention_30d`), Cora records `action_taken=feature_killed` and writes a `cora_playbook` recommendation + Slack "kill_recommended". **This is a recommendation only** — Cora never disables a feature autonomously; a human approves the kill.

---

## 5. Monitored-by-config but not in the active 7

These carry `KILL_SWITCH` guardrails but aren't reliably computed with live data, so they're **not** part of the active 7. `cac_paid_channels` and `sms_cost_per_signup` are hardcoded to `None` (grade `unknown` → never actioned) until their data source lands. `free_tier_cost_ratio` is the exception — it's **partially computed** and can grade/escalate, but never auto-corrects (escalation-only). All three are `lower_is_better`.

| Metric | Status | Direction |
|---|---|---|
| `cac_paid_channels` | Always `None` — no ad-spend ledger in DB | lower_is_better (green ≤$25, red >$40) |
| `free_tier_cost_ratio` | Partially computed; grades only when 30-day revenue > 0 (ADR 0006), escalation-only — never auto-corrects. Cost-allocation via `api_usage_logs` excludes NULL-subscriber shared cost | lower_is_better (green ≤40%, red >50%) |
| `sms_cost_per_signup` | Always `None` — Telnyx per-send cost not tracked on `MessageOutcome` | lower_is_better (green ≤$1, red >$2) |

---

## 6. Safety rails (per-run limits)

From `CORA_SELF_HEALING` in `config/cora_guardrails.py`:

| Limit | Value | Purpose |
|---|---|---|
| `max_actions_per_run` | **3** | A multi-metric breach can't trigger a cascade of changes in one pass |
| `max_feature_kill_recommendations_per_day` | **1** | At most one kill recommendation for human review per day |
| `max_new_incidents_per_hour` | **5** | Prevents an incident storm from opening near-identical rows |
| `baseline_window_days` | **7** | Rolling window for `compute_baseline()` |

All incident I/O is to `cora_incident` via raw SQL. Every state transition posts to Slack (falls back to email if Slack unset — never a silent failure). A `--dry-run` mode reports intended actions without executing.

---

## 7. Supervised-mode quick policy

- **Cora may act alone on exactly 4 metrics** (`wallet_adoption`, `lock_conversion`, `sms_reply_rate`, `offer_acceptance_rate`) — and only via a reversible 24h Redis fallback flag, after 48h in breach.
- **`first_payment_rate`** acts alone *only* under Stage 10 (variant promotion); otherwise it escalates.
- **`saved_card_rate` and `retention_30d` always require a human.**
- **No metric is ever auto-killed** — kills are recommendations awaiting approval.
- Anything outside these ranges, or any pricing change, requires Josh approval via Revenue Pulse.

---

## 8. Cora Guardrail Ranges (from FA-2B-v9-FINAL §3)

Cora can optimise freely within these ranges. Anything outside requires Josh approval through Revenue Pulse.

### Pricing guardrails

| Decision | Allowed Range | Rollback Trigger |
|---|---|---|
| Lock pricing | **$147–$247/mo** | Conv rate drops >2 std devs vs control for 48 hrs |
| Wallet tier pricing | **$39–$249/mo** | Conv rate drops >2 std devs vs control for 48 hrs |
| Bundle pricing | **±25% of base price** | Margin drops below 60% |
| Discount max | **20% off list** | Never exceed (hard limit) |
| Credit bonus max | **10 credits** per event | Never exceed per event (hard limit) |

### Traffic and messaging guardrails

| Decision | Allowed Range | Rollback Trigger |
|---|---|---|
| A/B test traffic cap | **10% of segment** | Auto-rollback if losing variant >2 std devs |
| Message variant swap | Retire lowest of **3 after 200 sends** | New variant must beat retired within 200 sends or revert |
| Urgency window duration | **10–60 minutes** | Never shorten below 10 min |

### Offer and subscription guardrails

| Decision | Allowed Range | Rollback Trigger |
|---|---|---|
| Save offer (downgrade) | **$97 Data-Only or 60-day pause** only | No lower offers without approval |
| Annual discount | **2 months free** / **$1,970/yr** max | No deeper annual discounts (hard limit) |
| Auto-reload threshold | **below 5 credits** | Never change threshold without approval (hard limit) |

### Spend and activation guardrails

| Decision | Allowed Range | Rollback Trigger |
|---|---|---|
| Paid acquisition spend | **$500–$2,000/week per channel** | Pause if CAC >$25 for 7 days |
| County activation | Only when **all 7 gates are green** | Never override gates (hard limit) |

These bounds are enforced at runtime by modules that consume `config.cora_guardrails.GUARDRAILS`. The rollback triggers are monitored by the kill-switch automation; when a trigger fires, the feature auto-rolls back to its previous safe configuration.

---

## 9. Weekly Revenue Pulse Kill-Switch Scorecard

### Purpose
The FA-2B-v9-FINAL spec mandates that Green/Yellow/Red scoring across all kill-switch metrics be reviewed **every Monday** in Revenue Pulse. This gives the founder a health overview of the entire monetisation engine in one SMS.

### Schedule
- **Weekly Revenue Pulse:** Monday 09:00 UTC (`0 9 * * 1` in `crontab.txt`)
- **Autonomy summary card:** Written at Monday 08:45 UTC by `cora_autonomy_report.py` (15 minutes before Revenue Pulse)

### What the scorecard includes

| Component | Source | Format |
|---|---|---|
| Per-metric G/Y/R grades | Redis cache (`fa:ks_metric:{metric}`) via `get_cached_metric()` | `KS: FPR G \| SCR Y \| WA G \| LC R \| R30 G \| SMS G \| OAR G` |
| Incident activity (7d) | `cora_incident` table | `2 red / 4 yellow open, 3 resolved, 1 kill-pending` |
| Autonomy summary | `learning_cards` (card_type='autonomy_summary') | `Cora autonomy: 72% autonomous, 3% overridden, 4 adopted, +2 net playbooks` |

### The 7 graded metrics (abbreviations)

| Abbrev | Metric | What it grades |
|---|---|---|
| FPR | `first_payment_rate` | ≥30% G, 20–30% Y, <20% R |
| SCR | `saved_card_rate` | ≥70% G, 50–70% Y, <50% R |
| WA | `wallet_adoption` | ≥15% G, 10–15% Y, <10% R |
| LC | `lock_conversion` | ≥5% G, 3–5% Y, <3% R |
| R30 | `retention_30d` | ≥70% G, 55–70% Y, <55% R |
| SMS | `sms_reply_rate` | ≥8% G, 5–8% Y, <5% R |
| OAR | `offer_acceptance_rate` | ≥15% G, 10–15% Y, <10% R |

### Implementation

The scorecard is produced by `_format_kill_switch_scorecard()` in `src/tasks/revenue_pulse.py`. It reads each metric's current value from Redis (cached daily by `kill_switch_metric_ingest`), grades it using the same `_grade()` function that `cora_self_healing` uses, and formats the compact line. If no metric data is available (Redis unavailable, or metric returns `None`), the metric is shown as `?` (unknown) — the same fail-safe no-op contract as the self-healing loop.

### How it fits the weekly SMS

The weekly Revenue Pulse SMS body (capped at 320 characters) now carries:
1. Revenue estimate + subscriber counts (existing)
2. Learning card top insight (existing)
3. **Kill-switch scorecard line** (new — added by this implementation)
4. Incident summary line (existing)
5. Autonomy summary line (existing)