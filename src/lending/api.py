"""Lending web app: every /webhooks/lending/* route, served on its own port, apart from fa-api.

Run: gunicorn src.lending.api:app -k uvicorn.workers.UvicornWorker --bind 127.0.0.1:8010
(deploy/systemd/lending-api.service; nginx sends /webhooks/lending/ here). Deliberately does not
import src.api.main, so a lending deploy or crash never touches fa-api. A new lending router is
mounted here, never in src/api/main.py.
"""
from fastapi import FastAPI

from src.api.lending_ghl_router import router as lending_ghl_router

app = FastAPI(title="Lending API", docs_url=None, redoc_url=None)
app.include_router(lending_ghl_router)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
