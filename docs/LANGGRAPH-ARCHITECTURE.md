# LangGraph Architecture — Cora's Runtime

**Purpose:** Detailed architecture for Cora's LangGraph layer. This is the "how" document. Preceded by `LANGGRAPH-PLATFORM-ROLE.md` (the "what"). Informed by `2B-V9-ORIENTATION.md` (the spec) and `client 2b report.md` (current state).

**Decisions this doc commits to:**

- **Monorepo** — agents live alongside the FastAPI code in the same git repo
- **Same VM, separate container** — agents run as a separate Docker container on the existing host
- **Separate Python process** — agents never share a process with FastAPI
- **Shared Postgres + Redis** — one database, one Redis, separate schemas where relevant
- **Tool wrappers over existing services** — no business logic duplicated in the agents layer
- **Full infrastructure on day one** — supervisor, tool registry, shared subgraphs, observability, kill switches, budget control. New graphs are additive from there.

---

## Part 1 · Topology

### 1.1 Process and Container Layout

```
┌──────────────────────── Host (existing VM) ────────────────────────┐
│                                                                    │
│   ┌─────────────────┐      ┌──────────────────────────────────┐    │
│   │  API container  │      │      Agents container            │    │
│   │  (FastAPI)      │      │      (LangGraph worker)          │    │
│   │                 │      │                                  │    │
│   │  - HTTP         │      │  - Supervisor                    │    │
│   │  - Webhooks     │      │  - Graph executor pool           │    │
│   │  - Cron jobs    │      │  - Event listener                │    │
│   │  - SMS cmds     │      │  - Checkpoint writer             │    │
│   │  - Stripe evts  │      │                                  │    │
│   └────────┬────────┘      └─────────────┬────────────────────┘    │
│            │                             │                         │
│            └──────┬───────────┬──────────┘                         │
│                   │           │                                    │
│             ┌─────▼───┐   ┌───▼────┐                               │
│             │ Postgres│   │ Redis  │                               │
│             │         │   │        │                               │
│             └─────────┘   └────────┘                               │
└────────────────────────────────────────────────────────────────────┘
        │                                                  │
┌───────▼──────┐                                     ┌─────▼──────┐
│  Stripe /    │                                     │ LangSmith  │
│  Twilio /    │                                     │ (external) │
│  Synthflow / │                                     └────────────┘
│  NWS / Claude│
└──────────────┘
```

### 1.2 Communication Rules

- **API ↔ Agents never talk directly.** They communicate exclusively through shared state: Postgres tables and Redis channels.
- **The API emits events** (to Redis Pub/Sub for low-latency or Postgres tables for durable queueing) that the agents process consumes.
- **The agents emit actions** (SMS via the compliance gate, Stripe API calls, table writes) the same way the API does, through the same service functions.
- **Neither process reads the other's in-memory state.** Every cross-process signal is persisted first.

This is the single most important architectural rule. It makes the two processes independently deployable, independently restartable, and independently scalable.

### 1.3 Scaling Path

| Stage | Topology | Trigger |
|---|---|---|
| **Phase 2B launch** | 1 VM · 2 containers · shared Postgres + Redis | Today |
| **Growth** | 1 VM · 2 containers · Postgres read replica added | Claude call latency becomes p95 bottleneck |
| **Split** | 2 VMs · API on one · Agents on the other | Agents container saturates CPU or contends with API on I/O |
| **Horizontal agents** | 2+ agents workers behind a shared checkpoint store | Sustained >5 concurrent graphs |

The split from stage one to stage three is a Docker-Compose change plus updating the agents container's DB/Redis URLs — no code changes.

---

## Part 2 · Repo & Code Layout

Monorepo. Agents live under `src/agents/`. Existing `src/` code (API, services, loaders, scrapers) is untouched.

```
src/
├── agents/                        # Cora's runtime — new
│   ├── __init__.py
│   ├── supervisor.py              # Entry point: python -m src.agents.supervisor
│   ├── router.py                  # Event → subgraph routing table
│   ├── state.py                   # Shared state TypedDict definitions
│   ├── runtime.py                 # Concurrency, budgets, circuit breakers
│   ├── checkpoint.py              # Postgres checkpoint configuration
│   ├── graphs/
│   │   ├── __init__.py
│   │   ├── supervisor_graph.py    # Top-level routing graph
│   │   ├── fomo.py                # FOMO Engine
│   │   ├── abandonment.py         # Abandonment Pressure
│   │   ├── retention.py           # Retention Summaries
│   │   ├── lock_close.py          # Cora Conversational Lock Close
│   │   └── auto_mode.py           # Auto Mode Execution
│   ├── subgraphs/
│   │   ├── __init__.py
│   │   ├── decision_hierarchy.py  # 6-step gate, reused by 4 graphs
│   │   └── compose_and_send.py    # Claude → compliance → send → log
│   ├── tools/
│   │   ├── __init__.py
│   │   ├── registry.py            # @tool decorator, typed registry
│   │   ├── read_tools.py          # 12 read tools
│   │   ├── write_tools.py         # 12 write/action tools
│   │   └── gating_tools.py        # 5 gating/safety tools
│   ├── prompts/
│   │   ├── fomo/
│   │   ├── abandonment/
│   │   ├── retention/
│   │   ├── lock_close/
│   │   └── auto_mode/
│   ├── events/
│   │   ├── __init__.py
│   │   ├── ingestion.py           # Redis Pub/Sub + Postgres listener startup
│   │   ├── handlers.py            # Per-source normalization to internal events
│   │   └── types.py               # Event dataclasses
│   └── observability/
│       ├── __init__.py
│       ├── langsmith.py           # Tracing config
│       ├── metrics.py             # Prometheus metric definitions
│       └── audit_log.py           # Agent decision audit trail

├── api/                           # Existing — untouched
├── services/                      # Existing — tools wrap these
├── core/                          # Existing — models, DB session
└── ...

config/
├── settings.py                    # Existing main Settings class
└── agents.py                      # NEW — AgentsSettings(Settings) subclass

scripts/
├── run_agents.py                  # Local dev entry point
└── replay_decision.py             # Debug: replay a checkpointed decision

docker/
├── api.Dockerfile                 # Existing
├── agents.Dockerfile              # NEW
└── docker-compose.yml             # Updated — adds `agents` service

tests/
└── agents/
    ├── tools/                     # Per-tool unit tests
    ├── graphs/                    # Per-graph integration tests (Claude mocked)
    ├── subgraphs/                 # Shared subgraph tests
    ├── events/                    # Event ingestion tests
    └── evals/                     # Nightly real-Claude eval suite
```

**Naming conventions:**

- Graphs: one file per top-level graph. Function name `build_<graph_name>_graph()` returns a compiled `StateGraph`.
- Tools: pure functions decorated with `@tool`. Type hints mandatory (used for Claude's tool schema inference).
- Nodes inside a graph: `_node_<verb>_<noun>()`. Private by convention.

---

## Part 3 · Config Architecture

### 3.1 Split Strategy

Two settings classes. The agents class **inherits** from the main one so everything shared is automatic and new keys are additive.

```
config/settings.py         config/agents.py
    Settings                    AgentsSettings(Settings)
         │                              │
         │ ─────── inherits ────────────┘
         │
    Used by API            Adds LangGraph-specific keys
```

**What the agents process reuses directly** (no duplication):

- `DATABASE_URL` (shared Postgres; checkpoint schema is a separate namespace)
- `REDIS_URL`
- `ANTHROPIC_API_KEY`
- `STRIPE_SECRET_KEY`
- `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_FROM_NUMBER`, `TWILIO_ENABLED`
- `SYNTHFLOW_API_KEY` (for outbound voice drop)
- All Cora guardrail config (already loaded as config)
- All Stripe price IDs
- `APP_BASE_URL` (for links in SMS)

**What AgentsSettings adds:**

| Key | Default | Purpose |
|---|---|---|
| `AGENTS_CHECKPOINT_SCHEMA` | `langgraph` | Postgres schema for checkpoint tables |
| `AGENTS_WORKER_CONCURRENCY` | `5` | Max concurrent graph executions |
| `AGENTS_MAX_TOKENS_PER_DECISION` | `3000` | Per-graph hard token cap |
| `AGENTS_MAX_COST_USD_PER_DECISION` | `0.10` | Per-graph hard cost cap |
| `LANGSMITH_API_KEY` | — | Tracing |
| `LANGSMITH_PROJECT` | `forced-action-agents` | Trace project name |
| `LANGSMITH_TRACING` | `true` | Enable/disable tracing |
| `AGENTS_LOG_LEVEL` | `INFO` | Logger level for the agents process |
| `AGENTS_GRAPHS_ENABLED` | `fomo,abandonment,retention,lock_close,auto_mode` | Comma-list of graphs that may run |
| `AGENTS_GLOBAL_KILL_SWITCH` | `false` | Master off-switch |
| `AGENTS_EVENT_SOURCE_REDIS` | `true` | Enable Redis Pub/Sub ingestion |
| `AGENTS_EVENT_SOURCE_POSTGRES` | `true` | Enable Postgres-listener ingestion |

### 3.2 Startup Validation

The agents process **refuses to start** if any of its required keys are missing or if the checkpoint schema is unreachable. This is the "loud early failure" principle — a silent boot with half-loaded config is the worst failure mode for an autonomous system.

---

## Part 4 · State Schema & Tool Registry

### 4.1 Graph State

All graphs use a shared base state (`CoraState`) with optional extension fields per graph. This is a TypedDict, enforced at type-check time.

```python
class CoraState(TypedDict):
    # Identifiers
    decision_id: str              # UUID, primary key in audit log
    subscriber_id: int
    graph_name: str
    event_type: str

    # Context (populated by decision_hierarchy subgraph)
    subscriber_profile: dict | None
    segment: str | None
    revenue_signal_score: int | None
    wallet_state: dict | None
    zip_activity: dict | None
    learning_card: dict | None
    ab_variant: str | None
    guardrails_in_scope: dict | None
    kill_switch_color: Literal["green", "yellow", "red"] | None

    # Decision outputs
    proposed_action: str | None
    action_allowed: bool | None
    compliance_allowed: bool | None

    # Message composition (when applicable)
    message_body: str | None
    campaign: str | None
    variant_id: str | None

    # Token/cost tracking
    tokens_used: int
    cost_usd: float

    # Outcome
    terminal_status: Literal["completed", "aborted", "escalated", "failed"] | None
    failure_reason: str | None
```

Each graph extends this with its own fields as needed — `FOMOState(CoraState)` adds `competitor_event_id` and `next_best_subscriber_id`, for instance.

### 4.2 Tool Registry

Tools are declared with a decorator. The decorator validates signatures, registers the tool in a module-level registry, and exposes it to Claude through the Anthropic SDK's tool-use format.

```python
# src/agents/tools/registry.py
def tool(
    *,
    category: Literal["read", "write", "gating"],
    idempotent: bool,
    requires_compliance: bool = False,
):
    """Register a function as a Cora tool."""
    ...

# src/agents/tools/write_tools.py
@tool(category="write", idempotent=True, requires_compliance=True)
def send_sms(
    subscriber_id: int,
    body: str,
    campaign: str,
    variant_id: str,
) -> SendSMSResult:
    """
    Send an SMS through the compliance gate.
    Idempotent: same (subscriber_id, campaign, variant_id) within 24h returns
    the previous send result without double-sending.
    """
    ...
```

**Properties of a well-formed tool:**

- **Typed.** Parameters and return value annotated.
- **Idempotent.** Same inputs return the same result. This is enforced by the `idempotent=True` claim — the tool registry validates it at registration.
- **Compliance-aware.** Tools that can emit to users (`send_sms`, `send_voicemail_drop`) must pass through the compliance gate internally. The `requires_compliance=True` flag is a hint to graph authors and an assertion enforced in tests.
- **Thin.** The tool body calls an existing service function. No business logic.

The registry exposes a `tools_for_graph(graph_name)` helper that returns only the tools a graph is allowed to call — no graph can call every tool. Scoping is declared per graph.

---

## Part 5 · Supervisor & Event Ingestion

### 5.1 Event Flow

```
Redis Pub/Sub ──┐
                │
Postgres LISTEN ─┼──►  Event Ingestion ──► Normalized Event ──► Supervisor Graph
                │       (per-source                               │
Cron ────────────┘      handlers)                                 │
                                                                  ▼
Admin API ──────┘                                        Route to Subgraph
                                                                  │
                                                     ┌────────────┼────────────┐
                                                     ▼            ▼            ▼
                                                   FOMO    Abandonment    Retention   ...
```

### 5.2 Event Sources

| Source | Transport | Example event | Latency budget |
|---|---|---|---|
| Redis Pub/Sub | Redis channel `cora:events` | `competitor_acted_on_lead` | <100ms to supervisor |
| Postgres LISTEN/NOTIFY | Channel `cora_events` | `subscriber_crossed_lock_threshold` | <500ms |
| Cron (APScheduler in-process) | Timer | `retention_summary_weekly_wallet` | Not latency-sensitive |
| Admin API (FastAPI → DB) | DB poll every 10s | `manual_override_run_save_flow` | <10s |

**Why both Redis and Postgres LISTEN?** Redis handles the latency-critical path (FOMO's 60-second budget); Postgres handles durable events that must not be lost if Redis is down. The supervisor reads from both; duplicate delivery is handled by the event's idempotency key.

### 5.3 Supervisor Graph

The supervisor is itself a LangGraph graph with four nodes. It does not compose messages or call Claude — it only routes.

```
  receive_event  ──►  classify_event  ──►  route_to_subgraph  ──►  checkpoint_root
```

`classify_event` is a Haiku call **only for edge cases** (unrecognized event type). For known event types the routing is a static dict lookup in `src/agents/router.py`. Ninety-nine percent of events never call Claude in the supervisor.

### 5.4 Registering a New Graph

Adding a new graph is three lines in `src/agents/router.py`:

```python
EVENT_TO_GRAPH = {
    "competitor_acted_on_lead": fomo_graph,
    "wall_session_abandoned": abandonment_graph,
    # ...
    "new_event_type": new_graph,   # ← added
}
```

No changes to the supervisor, the state schema, the tool registry, or the ingestion layer.

---

## Part 6 · Graph Patterns

### 6.1 Standard Node Flow

Every "send a message to a user" graph follows the same skeleton. Deviations are explicit.

```
 load_context ─► decision_hierarchy ─► compose ─► compliance ─► send ─► wait* ─► handle_reply ─► log_outcome
                                                                          │
                                                                          └── (* optional suspend-resume)
```

The `decision_hierarchy` and `compose → compliance → send → log_outcome` blocks are **shared subgraphs** — they are not written per-graph.

### 6.2 Composition Rules

- **One Claude call per message.** Complex multi-call composition is a smell; break into multiple graph steps if you need that.
- **Sonnet is opt-in.** Default is Haiku. Upgrading to Sonnet requires a per-graph config flag and a token-budget adjustment.
- **Prompts live in YAML, not Python.** `src/agents/prompts/<graph>/system.yaml` holds system prompts; `variants/` holds A/B variants. Code reads them by name.
- **No raw string concatenation into prompts.** Context is passed as structured data the prompt template renders.

### 6.3 Suspend & Resume

Nodes that wait (for a reply, for a scheduled follow-up, for an external API result) use LangGraph's built-in interrupt/resume. The state is checkpointed; when the event that wakes the graph arrives, a new worker picks up at the suspended node.

**Rule:** a graph that suspends for more than 10 minutes must persist enough context in state to survive a deploy. Specifically, all IDs needed to re-fetch live data from Postgres/Redis on resume.

---

## Part 7 · Shared Subgraphs

### 7.1 decision_hierarchy_check

Enforces the 6-step hierarchy. Input: the proposed action and subscriber ID. Output: allowed/blocked plus a fallback action if blocked.

**Nodes:**

1. `load_guardrail_for_decision` — pulls the numeric bound for this decision type
2. `check_guardrail` — validates proposed value against bound
3. `load_learning_card` — pulls latest Sunday card
4. `consult_learning_card` — if card says this action is underperforming, prefer fallback
5. `read_live_state` — Redis flags relevant to this subscriber/ZIP
6. `check_kill_switch` — per-feature color lookup

Any step returning blocked → control jumps to the end with `action_allowed=False` and a reason written to state.

### 7.2 compose_and_send_compliant_sms

Claude call → compliance gate → Twilio send → outcome log. Four nodes, reused by five of six graphs.

**Nodes:**

1. `compose_message` — Claude call with the appropriate prompt template and variant
2. `compliance_gate` — TCPA + DNC + rate-limit check
3. `dispatch_sms` — calls the existing send-SMS service function
4. `log_message_outcome` — writes `message_outcomes` row with attribution windows set

Compliance failures don't crash the graph — they're logged as `compliance_blocked` and the graph exits cleanly with `terminal_status=aborted`.

### 7.3 Subgraph as a Unit of Test

Both shared subgraphs have dedicated test suites. Every top-level graph's tests can **mock the subgraph** and focus on graph-specific logic. This is what keeps the graph test count manageable.

---

## Part 8 · Compliance & Guardrails Integration

### 8.1 Compliance

The compliance gate is **not reimplemented** in the agents layer. It is the same function the FastAPI outbound path calls. Tools that can emit SMS (`send_sms`, `send_voicemail_drop`) internally call the compliance function; no graph can bypass it.

The `compose_and_send_compliant_sms` subgraph also runs a compliance check **before** composing — so if the user is opted out or in a DNC window, we never even spend the tokens on generating a message we can't send.

### 8.2 Guardrails

Guardrails are numeric bounds in configuration, loaded at process start. The tool registry exposes a `guardrail_check` gating tool that every write tool consults before taking action.

**Two layers of guardrail enforcement:**

| Layer | Where | Purpose |
|---|---|---|
| Graph layer | `decision_hierarchy_check` subgraph | Reject a proposed action up-front |
| Tool layer | Inside each write tool | Backstop — even if a graph misses the hierarchy check, the tool blocks |

Two layers is intentional. The graph check is fast and composable. The tool check is the last line of defense.

---

## Part 9 · Persistence & Checkpointing

### 9.1 Checkpoint Store

`langgraph-checkpoint-postgres` writes to a dedicated schema (default `langgraph`) in the same Postgres instance. Migrations for this schema are managed by the LangGraph library itself — we do not author them.

**What's stored:**

- Full graph state at every node transition
- Pending interrupts (for suspend/resume)
- Thread identifiers, keyed by `decision_id`

**Retention:** 90 days. A daily cleanup job prunes older rows. We intentionally keep them long enough to debug production incidents from the previous quarter.

### 9.2 Audit Log

Separate from checkpoints, the agents layer writes an **agent audit log** to Postgres with one row per decision — not per node. This is the table a human reads to answer "why did Cora do X for user Y."

```
agent_decisions:
    decision_id       UUID PRIMARY KEY
    graph_name        text
    subscriber_id     int
    event_type        text
    started_at        timestamptz
    completed_at      timestamptz
    terminal_status   text  -- completed | aborted | escalated | failed
    tokens_used       int
    cost_usd          numeric(10, 6)
    summary           jsonb  -- key context, action taken, outcome
```

This is the first table we query during any incident.

### 9.3 Idempotency

Every graph takes an `idempotency_key` in its input event. Before starting a new graph run, the supervisor checks for a completed or in-flight decision with the same key — if found, it skips. This prevents duplicate events from firing two parallel graphs against the same subscriber.

---

## Part 10 · Observability

### 10.1 Three-Layer Visibility

| Layer | Tool | What it's for |
|---|---|---|
| Trace | LangSmith | "Walk me through this one decision" |
| Metrics | Prometheus + Grafana | "How is the system doing this hour" |
| Audit | `agent_decisions` table | "Why did Cora do X for user Y two weeks ago" |

### 10.2 LangSmith Traces

Every graph run produces a trace. Trace name = `{graph_name}:{decision_id}`. Traces are keyed by `subscriber_id` in metadata so searching "all decisions for user 12345" is one query.

**Tracing is always on in production.** The cost is negligible. The value during an incident is enormous.

### 10.3 Prometheus Metrics

Exposed by the agents container on an internal port, scraped alongside the API container.

| Metric | Type | Labels |
|---|---|---|
| `cora_graph_started_total` | counter | `graph` |
| `cora_graph_completed_total` | counter | `graph`, `terminal_status` |
| `cora_graph_duration_seconds` | histogram | `graph` |
| `cora_graph_tokens_used` | histogram | `graph`, `model` |
| `cora_graph_cost_usd` | histogram | `graph` |
| `cora_tool_calls_total` | counter | `tool`, `success` |
| `cora_compliance_blocks_total` | counter | `reason` |
| `cora_guardrail_blocks_total` | counter | `decision_type`, `reason` |
| `cora_kill_switch_fallbacks_total` | counter | `graph` |
| `cora_budget_circuit_breaker_fires_total` | counter | `graph` |

**Alerts:**

- `cora_budget_circuit_breaker_fires_total > 0 over 5 min` → page
- `cora_graph_completed_total{terminal_status="failed"} / cora_graph_started_total > 5%` → warn
- `cora_compliance_blocks_total{reason="no_opt_in"} > 10/hr` → warn (possible logic bug firing SMS at unconsented users)

---

## Part 11 · Failure Modes & Recovery

### 11.1 What Can Go Wrong

| Failure | Detection | Recovery |
|---|---|---|
| Agents container crash | Docker health check fails | Container restarts; LangGraph resumes from last checkpoint |
| Claude API outage | Tool call returns error | Graph retries up to N times, then writes `terminal_status=failed` and exits |
| Redis outage | Event listener reconnect loop | Postgres LISTEN keeps working; agents keep running on reduced input |
| Postgres outage | Tool calls fail | Agents halt cleanly — no partial state writes. API also halts. |
| Budget exceeded | `budget_check` gating tool fires | Graph writes `terminal_status=aborted` with reason and exits |
| Infinite loop in graph | Node count exceeds N per decision | Circuit breaker aborts; decision logged as failed |
| Bad variant shipped | Per-graph kill switch flipped in config | Supervisor reads kill-switch on every event; flips to fallback within seconds |

### 11.2 Idempotency Is Load-Bearing

Every tool marked `idempotent=True` must actually be idempotent. Violations of this are the most dangerous kind of bug because they only manifest during recovery — a checkpoint resume calls a non-idempotent tool twice and things double.

**Tool-level idempotency pattern:**

Tools that write to external services (Stripe, Twilio, Synthflow) use an idempotency key computed from `(subscriber_id, decision_id, tool_name, deterministic_args)`. The key is passed to the external service when the API supports it (Stripe: yes, Twilio: client-side only, Synthflow: client-side). For tools that write to our own Postgres, a unique constraint on the computed key prevents double-writes.

### 11.3 Deploys While Graphs Are In-Flight

- SIGTERM handler drains: new events stop being accepted; in-flight graphs finish up to a 60-second grace window.
- Graphs suspended in a long wait (e.g., Auto Mode's 24-hour sleep) simply resume on the new version of the code. Because graph definitions live in code and state lives in Postgres, the resume reads the state and picks up at the same node on the new code. As long as node names don't change, this is safe.
- Renaming a node is equivalent to a schema migration. It requires a two-phase deploy: ship new code that supports both old and new node names, then rename.

---

## Part 12 · Kill Switches & Budget Control

### 12.1 Kill Switch Hierarchy

Three levels, all readable at runtime:

| Level | Config key | Effect |
|---|---|---|
| Global | `AGENTS_GLOBAL_KILL_SWITCH=true` | Supervisor stops routing any events — agents container idles |
| Per-graph | `AGENTS_GRAPHS_ENABLED=<list>` | Supervisor drops events for disabled graphs |
| Per-feature | Existing Cora guardrail kill-switch (per feature, per color) | Graph reads color and falls back to simpler path |

### 12.2 Budget Circuit Breaker

Every graph tracks tokens and cost in state. Before each Claude call, `budget_check` runs. If the remaining budget is insufficient, the call is refused and the graph writes `terminal_status=aborted`. This cannot be overridden from inside a graph.

**Defaults (from config):**

- 3,000 tokens per decision
- $0.10 per decision
- Both checked per-Claude-call; whichever trips first wins

These are conservative starting points. Tuning happens after we see real distributions.

---

## Part 13 · Data Ownership Model

Both processes write to the same tables. Clear ownership prevents races and silent overwrites.

| Table | API writes | Agents writes | Owner |
|---|---|---|---|
| `subscribers` | Profile changes, preferences, tier on explicit user action | Tier changes on autonomous upgrade | **Mixed** — lock columns by convention. API owns identity columns; agents own tier, segment, flags set by autonomous decisions. |
| `wallet_transactions` | Every purchase, every Stripe webhook | Auto-reload, bonus credits, referral reward | Mixed — all writes are append-only, so races are benign |
| `message_outcomes` | — | Every outbound message Cora sends | **Agents** |
| `deal_outcomes` | User-submitted deal captures | — | **API** |
| `learning_cards` | — | Sunday cron (runs inside agents process) | **Agents** |
| `referral_events` | New referral on signup | Mark confirmed/rewarded on paid conversion | Mixed — distinct lifecycle states, no overlap |
| `agent_decisions` | — | Every graph run | **Agents** (exclusive) |
| `sms_opt_ins` | Inbound YES/START handling | — | **API** |
| `processed_events` (Stripe) | Stripe webhook handler | — | **API** |
| `bundle_purchases` | Purchase creation | Bundle expiry sweep | Mixed — phased by column |

**Rule of thumb:** if a column is written by both processes, document the ordering rule in the model file. If the two processes can write to the same column in the same second, use Postgres row-level locking at the service function level (the function is shared, so the locking is automatically consistent).

---

## Part 14 · Testing Strategy

### 14.1 Four Layers

| Layer | What | Speed | Runs |
|---|---|---|---|
| **Unit — tools** | Each tool with mocked external services | Fast (<1s each) | Every commit |
| **Unit — subgraphs** | decision_hierarchy and compose_and_send in isolation, with mocked tools | Fast | Every commit |
| **Integration — graphs** | Full graphs with mocked Claude (deterministic responses) | Medium (2–5s each) | Every commit |
| **Eval — real Claude** | Each graph against canned scenarios with real Claude calls; scored by rubric | Slow (30s+) and costly | Nightly + on release |

### 14.2 Mocking Claude

A fixture loader maps `(graph_name, node_name, input_hash)` to canned Claude responses. Tests register expected calls; unexpected calls fail the test. This lets graph tests be fully deterministic without the cost of real API calls.

### 14.3 Eval Suite

For each graph, a set of 5–10 representative scenarios (user profiles + event payloads) runs nightly against real Claude. Each run scored by a rubric:

- Did the graph reach `terminal_status=completed`?
- Did the composed message contain the required facts (live ZIP data, specific lead reference, CTA)?
- Did token usage stay inside budget?
- Did compliance block anything it shouldn't have?

Regressions in eval scores gate releases. This is the only way to catch "the prompts got worse" kinds of bugs.

---

## Part 15 · Local Dev & Deployment

### 15.1 Local Development

Docker Compose with two services plus Postgres + Redis. A developer can run any subset:

```
docker compose up api                 # just the API, against shared DB/Redis
docker compose up agents              # just the agents worker
docker compose up                     # both, as production
```

For agents development without Docker:

```
python -m src.agents.supervisor       # runs the worker process directly
```

Hot-reload is not supported in the agents process — LangGraph state definitions don't survive module reloads cleanly. Restart on change.

### 15.2 Production Deployment

The existing deploy pipeline ships a single image. We split that into two images:

- `forced-action/api:<sha>`
- `forced-action/agents:<sha>`

Both built from the same source tree with different Dockerfile targets. Both deployed together. Staggered rolling restarts: API first, agents second, so if the new agents code fails to start, the API is already healthy and serving users.

### 15.3 Secret Management

All secrets come from the same source (environment or secrets manager) — there's no split secret store. If you later split to two VMs, the secrets manager is already central; nothing changes.

---

## Part 16 · Migration Path from Today

Ordered steps to get from current state to Phase 2B LangGraph launch.

### Week 1: Infrastructure
1. Add `config/agents.py` with `AgentsSettings`
2. Add `src/agents/` skeleton (empty modules, no logic yet)
3. Add `agents.Dockerfile`, update `docker-compose.yml`
4. Set up `langgraph-checkpoint-postgres`, run its migration
5. Wire LangSmith tracing (no graphs running yet — just test a hello-world graph)
6. Implement the tool registry and the 29 tools, each a thin wrapper over existing services
7. Write tool unit tests

### Week 2: Shared Subgraphs + Supervisor
1. Build `decision_hierarchy_check` subgraph
2. Build `compose_and_send_compliant_sms` subgraph
3. Build the supervisor graph and event ingestion layer
4. Wire Prometheus metrics
5. Ship to staging — no graphs registered yet — confirm supervisor idles correctly

### Week 3: Priority-List Graphs
1. Build FOMO graph (simplest — good pathfinder)
2. Build Abandonment Pressure graph
3. Build Retention Summaries graph
4. Integration tests against mocked Claude
5. Eval suite with real Claude, gated on staging

### Week 4: Additional Graphs + Hardening
1. Build Cora Conversational Lock Close graph (the one Sonnet graph)
2. Build Auto Mode Execution graph
3. Failure-mode drills in staging (kill Redis, kill Postgres, fire bad prompts)
4. Dry-run all graphs for 48 hours in staging with production data
5. Launch to production with 10% traffic, then 50%, then 100% over one week

---

## Summary

One VM, two containers, shared data. Monorepo with a clean `src/agents/` boundary. Config inherits from the main settings class. All tools are typed wrappers over existing services. All graphs share two subgraphs (decision hierarchy + compose-and-send) so node count stays manageable. All autonomous decisions are checkpointed, audit-logged, metric-tracked, and kill-switchable. The path from "add a graph" to "it's in production" is one file plus one router entry.

Build the infrastructure once. Add graphs forever.
