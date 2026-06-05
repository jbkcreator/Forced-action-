FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Required env vars (injected via docker-compose or --env-file):
#   DATABASE_URL, REDIS_URL, ANTHROPIC_API_KEY, TELNYX_API_KEY,
#   AGENTS_EVENT_SOURCE_REDIS=true, AGENTS_EVENT_SOURCE_POSTGRES=true
#
# Optional:
#   LANGSMITH_API_KEY, LANGSMITH_PROJECT, LANGSMITH_TRACING=true
#   AGENTS_GLOBAL_KILL_SWITCH=false, AGENTS_GRAPHS_ENABLED=...
#   AGENTS_LOG_LEVEL=INFO

# Health check — docker will mark the container unhealthy if this fails.
# Runs --health which checks Postgres, Redis, API key, and checkpoint schema.
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -m src.agents --health

CMD ["python", "-m", "src.agents", "--serve"]
