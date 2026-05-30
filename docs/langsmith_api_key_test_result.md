# LangSmith Monitoring — Test Result

**Date:** 2026-05-29
**Script:** `scripts/verify_langsmith_monitoring.py` (real calls, no mocks)

## Summary

| Check | Status | Notes |
|-------|--------|-------|
| DB Logging | PASS | Row written to `api_usage_logs` per call |
| Routing Model | PASS | Haiku/Sonnet routing per task_type |
| LangSmith READ | PASS | `list_runs` 200 with the `lsv2_pt_…` key |
| Trace emit + visible | PASS | Real Haiku call traced; run visible in project |

The earlier `lsv2_sk_…` service key was **dead** (403 read+write, US+EU). The
`lsv2_pt_…` personal token works.

## Two things had to be true (both now fixed)

1. **Instrumentation** — `claude_router._build_client()` now wraps the Anthropic
   client with `langsmith.wrappers.wrap_anthropic()` when tracing is on. Before,
   the router used a plain client and emitted **zero** traces.
2. **Config bridge** — the LangSmith SDK reads `LANGSMITH_*` from `os.environ` at
   emit time; pydantic loading them from `.env` does **not** populate `os.environ`.
   `observability/langsmith.configure_tracing()` bridges settings → `os.environ`
   and is called at agents startup (`events/ingestion.run_forever`). Without it,
   traces silently went to the `default` project.

## To enable in production

- Set `LANGSMITH_API_KEY`, `LANGSMITH_TRACING=true`, `LANGSMITH_PROJECT=<real prod project>`
  in the agents runtime env (or `.env` — `configure_tracing()` bridges it).
- `LANGSMITH_PROJECT` is currently `forced-action-smoke-test`; change to the real
  project name for prod.
- Prefer a **workspace service key** (`lsv2_sk_`) with `run:create` scope for a
  server runtime over a personal token (`lsv2_pt_`, tied to a user account).

## Security

- **Rotate both pasted keys** — the old `lsv2_sk_…a072ba2ae` and the new
  `lsv2_pt_…3b028995ef` were both pasted in chat.
