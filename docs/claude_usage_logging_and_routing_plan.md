# Claude Usage Logging & Cost-Aware Routing — Implementation Plan

**Branch:** `feat/claude-usage-logging` (off `dev`)
**Status:** Plan (post-grill). Decisions below are settled; this MD is the build spec.
**Author note:** Restates the original task after grilling against the codebase. The
original DoD assumed a Sonnet-heavy production baseline to cut by 30%. The code shows
the opposite, and the cost ledger is nearly empty — so the milestone is reframed.

---

## 0. Why the original framing was wrong (findings)

Grilling against the code surfaced four facts that invert the task premise:

1. **Routing already exists and is Haiku-first.** `src/services/claude_router.py` routes
   purely on `task_type` via `_TASK_ROUTING`. Docstring target is already ~80% Haiku.
2. **The "lock close / deal-win" graphs are already on Haiku.** `wallet_to_lock_close`,
   `ap_lite_close`, `fomo`, `accelerated_wallet_push`, `nws_urgency`, `abandonment` all set
   `CLAUDE_TASK_TYPE = "sms_copy"` → Haiku. The *only* agent path on Sonnet is `retention`.
   The plan's "reserve Sonnet for closes" would *raise* cost, not lower it.
3. **There is no baseline.** Production DB (`5.78.184.159/distress_db`) `api_usage_logs`
   holds **2 dev rows, $0.00**. LangSmith tracing defaults off; provided key returns 403 on
   read. No historical or trace data to mine.
4. **Logging is broken on the high-volume paths.** `_log_usage` only persists when a `db=`
   session is passed. The biggest paths omit it (see §2), so even with traffic the ledger
   would undercount. Cost is also fragmented across 3 tables (`api_usage_logs`,
   `agent_decisions.cost_usd`, `chat_messages.tokens_*`).

**Conclusion:** This is a *pre/early-launch instrumentation + measurement* milestone, not a
cut-the-Sonnet-bill milestone. Fix logging first, define a counterfactual baseline, build a
value-aware override lever (default Haiku), and make every upgrade eval-justified.

---

## 1. Settled decisions (from grill)

| # | Decision |
|---|----------|
| Goal | (C) Selective: downgrade where Haiku is provably as-good; upgrade only genuine deal-win moments where Haiku provably loses. Net cheaper. |
| Baseline | **Counterfactual** = cost vs naive "all-Sonnet" policy, computed from logged tokens. Secondary check: Haiku share of calls. |
| Quality signal | **Offline golden-set eval** makes day-one routing decisions (no live traffic exists). LangSmith deferred to post-launch validation. |
| Routing mechanism | Build a **`force_tier` override** in the router; **default everything Haiku**; upgrades are **eval-gated**. |
| Logging gap (D1) | Every `call_claude*` path passes `db=`. Independent-commit session per agent node. Thread `graph_name` (free, already in state). **No `decision_id` migration** (redundant with `agent_decisions`). |
| Pause gate | Re-arm vendor cost pause on the agent fleet, **plus** add `[BLOCKED]`-return handling so a paused fleet falls back to static copy (never texts "[BLOCKED]…"). |
| Cost ledger | **`api_usage_logs` is the single source of truth.** Chat writes to it too. `agent_decisions.cost_usd` / `chat_messages.tokens_*` are derived/display only — never cost-report inputs. |

---

## 2. Logging gap — exact call sites

Single ledger = every one of these passes `db=` (open own session where none exists).

| Call site | task_type | Today | Fix |
|---|---|---|---|
| `src/agents/subgraphs/compose_and_send.py:117` (**entire Cora fleet**) | sms_copy / retention_copy | ❌ no db | wrap in `Database().session_scope()`, pass `db`, `graph_name`, `pause_target` (from state) |
| `src/services/concierge_chat.py:152` | chat_response | ❌ no db | pass `db=db` (session already in scope) |
| `src/tasks/proactive_save.py:132` | email_copy | ❌ no db | pass `db=db` |
| `src/tasks/stripe_recovery_sweep.py:103,171,217` | email_copy | ❌ no db | pass `db=db` |
| `src/services/forward_pack_renderer.py:103` | sms_copy | ❌ no db, no session | open `Database().session_scope()` |
| `src/tasks/ap_pro_upsell.py:120` | — | ✅ | none |
| `src/services/referral_notifier.py:86,121` | — | ✅ | none |

`decision_id` and `graph_name` are **already in compose state** (`compose_and_send.py:37-38`),
so attribution threading is trivial. `call_claude_with_usage` already accepts `graph_name`
and `pause_target` params.

---

## 3. Work breakdown

### D1 — Single cost ledger (first deliverable, blocks everything)

1. **`compose_and_send._node_compose`**: wrap the `call_claude_with_usage` call in
   `with Database().session_scope() as session:` and pass
   `db=session, graph_name=state.get("graph_name"), pause_target=<resolved>`.
   - Independent commit: cost row persists even if the decision later aborts/fails compliance.
   - Matches existing node idiom (`accelerated_wallet_push.py:296`, `decision_hierarchy.py:192`).
2. **`[BLOCKED]` handling** in `_node_compose`: if `result["text"]` starts with `[BLOCKED]`,
   treat as fallback — use `ab_fallback_body` if present, else `terminal_status="aborted"`,
   `failure_reason="compose:vendor_pause"`. **Never** assign `[BLOCKED]…` to `message_body`.
3. **Chat / tasks / renderer**: add `db=` at the 5 sites in §2.
4. **Smoke verification**: a scripted run that exercises one agent decision, one chat turn,
   one task email → assert ≥3 `api_usage_logs` rows with non-null `graph_name` (agent) and
   correct `model`.

**Tests**
- `compose_and_send`: logs a row with `graph_name` set; aborted decision still logs cost.
- `[BLOCKED]` path returns fallback body, never sends the blocked string.
- chat/task sites: `call_claude_with_usage` called with `db=` (assert kwarg).

### D2 — Override lever (`force_tier`), defaults unchanged

1. Add optional `force_tier: Optional[str]` to `call_claude`, `call_claude_with_usage`
   (and `stream_claude` for symmetry). When set (`"haiku"|"sonnet"|"opus"`), it **overrides**
   `_TASK_ROUTING[task_type]`; otherwise behavior is identical to today.
2. Thread an optional `force_tier` key through compose state → `_node_compose`.
3. **Ship with no graph setting it** — every graph stays Haiku by default. The lever exists
   for eval-gated upgrades only.

**Tests**: `force_tier="sonnet"` on a `sms_copy` call selects the Sonnet model id; absent →
unchanged routing; invalid tier → falls back to task routing (no crash).

### D3 — Counterfactual savings report

1. Report (Python in `src/tasks/` or a `src/services/` function; reuse Revenue Pulse style)
   that, over a window, reads **only** `api_usage_logs` where `service='claude'` and computes:
   - `actual_cost = sum(cost_usd)`
   - `all_sonnet_cost = sum(input_tokens*3.00 + output_tokens*15.00)/1e6`
   - `savings_pct = 1 - actual_cost/all_sonnet_cost`
   - `haiku_share = count(model='haiku')/count(*)`
   - Grouped by `task_type` and `graph_name`.
2. Output: founder-facing summary line + per-group table. Honest reporting: if already 90%+
   Haiku, report the true (large) savings vs naive — do not inflate to a fixed 30%.

SQL sketch:
```sql
SELECT task_type, graph_name,
       COUNT(*) AS calls,
       SUM(cost_usd) AS actual_cost,
       SUM(input_tokens*3.00 + output_tokens*15.00)/1e6 AS all_sonnet_cost,
       1 - SUM(cost_usd) / NULLIF(SUM(input_tokens*3.00 + output_tokens*15.00)/1e6,0) AS savings_pct,
       AVG((model='haiku')::int) AS haiku_share
FROM api_usage_logs
WHERE service='claude' AND blocked_by_pause=false AND created_at >= :since
GROUP BY task_type, graph_name ORDER BY actual_cost DESC;
```

**Tests**: seeded rows produce correct savings_pct and haiku_share; zero-token guard.

### D4 — Golden-set eval harness

1. Curate 20–50 representative inputs per high-value `task_type`/graph (close, fomo,
   abandonment, chat, retention, email_copy). Real-ish prompts; reuse any existing fixtures.
2. Run each through Haiku **and** Sonnet via the router (`force_tier`).
3. Score with an LLM-as-judge rubric (faithfulness to system prompt, compliance-safe,
   persuasiveness/tone) + spot human review. Output a **routing recommendation per task**.
4. Host as a LangSmith dataset + evaluator **once a working key is confirmed**; until then
   run locally and store results under `docs/` or a results table.

**Deliverable:** a documented per-task verdict ("Haiku sufficient" / "upgrade to Sonnet")
that justifies any change to `_TASK_ROUTING` or any graph `force_tier`.

### D5 — Tracing for post-launch validation

1. Set `LANGSMITH_API_KEY` (working key) + `LANGSMITH_TRACING=true` in the agents runtime
   env (`config/agents.py` already reads these).
2. Confirm fleet runs emit traces. **Live-outcome validation is explicitly post-launch.**

---

## 4. Restated Definition of Done

Pre-launch (this milestone):
1. **Single ledger live** — every `call_claude*` path passes `db=`; smoke run shows agent +
   chat + task rows in `api_usage_logs` with `graph_name` populated for agents.
2. **Pause gate re-armed safely** — active pause → static fallback; `[BLOCKED]` never sent. Tested.
3. **Counterfactual report** — actual vs all-Sonnet + Haiku-share, per task_type/graph.
4. **`force_tier` lever shipped, defaults unchanged** — no speculative Sonnet upgrades. Tested.
5. **Golden-set eval** — per-task Haiku-vs-Sonnet verdict; every routing change is eval-justified.
6. **Tracing wired** — `LANGSMITH_TRACING` on with a working key.

Post-launch checkpoint (deferred, not this milestone):
- Live traffic confirms **≥75% Haiku share** and **counterfactual savings ≥30%** over a real
  window; eval-gated upgrades validated against actual conversion/reply outcomes.

---

## 5. Risks / watch-items

- **Re-arming pause is a behavior change.** Without D1.2, a paused fleet would text
  "[BLOCKED]…" to subscribers. D1.2 is mandatory, not optional.
- **Double-counting.** Do not sum cost across `api_usage_logs` + `agent_decisions` +
  `chat_messages`. `api_usage_logs` is the only cost-report input (decision §9).
- **Leaked LangSmith key** (`lsv2_sk_...a072ba2ae`) was pasted in chat — **rotate it.**
  Read access currently 403s; confirm key scope/workspace/region before D5.
- **Independent-commit sessions** add one short transaction per agent decision — acceptable;
  cost logging must not ride on decision-transaction success.
- **`stream_claude`** has no callers; do not invest in it. Chat is non-streaming via
  `handle_user_turn`.

## 6. Suggested commit order

1. D1 logging fix + `[BLOCKED]` handling + tests (the unblocker).
2. D3 counterfactual report (needs D1 data shape).
3. D2 `force_tier` lever + tests.
4. D4 golden-set eval (uses D2).
5. D5 tracing wiring.
