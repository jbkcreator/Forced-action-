"""Lending web app: the dialer call webhook, served on its own port.

Run: gunicorn src.lending.api:app -k uvicorn.workers.UvicornWorker --bind 127.0.0.1:8010
(deploy/systemd/lending-api.service). Deliberately does not import src.api.main.
"""
from fastapi import FastAPI

from src.lending.webhooks import router

app = FastAPI(title="Lending API", docs_url=None, redoc_url=None)
app.include_router(router)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
