"""Lending web app: every /webhooks/lending/* route and the /api/lending/* website endpoint, served on
its own port, apart from fa-api.

Run: gunicorn src.lending.api:app -k uvicorn.workers.UvicornWorker --bind 127.0.0.1:8010
(deploy/systemd/lending-api.service; nginx sends /webhooks/lending/ and /api/lending/ here).
Deliberately does not import src.api.main, so a lending deploy or crash never touches fa-api. A new
lending router is mounted here, never in src/api/main.py.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config.settings import get_settings
from src.api.lending_ghl_router import router as lending_ghl_router
from src.lending.booking_webhook import router as lending_booking_webhook_router
from src.lending.reply_webhook import router as lending_reply_webhook_router
from src.api.lending_web_router import router as lending_web_router

app = FastAPI(title="Lending API", docs_url=None, redoc_url=None)
# The website form posts cross-origin when the site is served from another host; the allowed
# origins are the same WL_ALLOWED_ORIGINS fa-api reads. Server-to-server webhooks ignore CORS.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in get_settings().wl_allowed_origins.split(",") if o.strip()],
    allow_methods=["POST"],
    allow_headers=["*"],
)
app.include_router(lending_ghl_router)
app.include_router(lending_booking_webhook_router)
app.include_router(lending_reply_webhook_router)
app.include_router(lending_web_router)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
