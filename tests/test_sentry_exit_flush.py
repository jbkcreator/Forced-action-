"""P0.3 review: a short-lived process (a cron task) that reports an unset Slack channel and exits at once must still
deliver the Sentry event. Runs a real subprocess against a local fake Sentry endpoint (no network)."""
from __future__ import annotations

import gzip
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

pytest.importorskip("sentry_sdk")

ROOT = Path(__file__).resolve().parent.parent

CHILD = """
from src.utils.sentry import capture_error
assert capture_error("Lending Slack post dropped: LENDING_DAILY_CHANNEL not set",
                     tags={"post": "scoreboard"}, fingerprint=["lending_slack_channel_unset", "scoreboard"])
raise SystemExit(0)  # exit immediately after queueing, as a cron task does
"""


class _FakeSentry(BaseHTTPRequestHandler):
    received: list[bytes] = []

    def do_POST(self):  # noqa: N802 (http.server API)
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length)
        if self.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)
        _FakeSentry.received.append(body)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *args):
        pass


@pytest.fixture
def fake_sentry():
    _FakeSentry.received = []
    server = HTTPServer(("127.0.0.1", 0), _FakeSentry)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server.server_address[1]
    server.shutdown()


def _run_child(port: int) -> subprocess.CompletedProcess:
    env = dict(os.environ, SENTRY_DSN=f"http://publickey@127.0.0.1:{port}/1", SENTRY_ENVIRONMENT="test",
               ANTHROPIC_API_KEY="x", FIRECRAWL_API_KEY="x", COURT_LISTENER_API_KEY="x",
               PYTHONPATH=str(ROOT))
    return subprocess.run([sys.executable, "-c", CHILD], cwd=ROOT, env=env, capture_output=True, text=True,
                          timeout=60)


def test_an_event_queued_right_before_exit_is_still_delivered(fake_sentry):
    result = _run_child(fake_sentry)
    assert result.returncode == 0, result.stderr[-2000:]
    assert len(_FakeSentry.received) == 1
    assert b"LENDING_DAILY_CHANNEL not set" in _FakeSentry.received[0]


def test_the_exit_flush_prints_nothing_to_the_cron_output(fake_sentry):
    result = _run_child(fake_sentry)
    assert "Sentry is attempting to send" not in result.stderr + result.stdout
