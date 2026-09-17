"""Backflip handoff adapter — WP-7 WI-6.

The client's own words: whether Backflip's prequal accepts pre-filled
parameters by URL or API "changes the implementation materially" and he is
"still asking Bailey" (plan §7 Q1). One small interface, several bodies,
ship the one that does not depend on an answer — the client's own port
pattern rule (spec Part 3, "non-negotiable").

Every path checks Backflip suppression and consent before firing (WI-4),
then writes the same events regardless of which adapter is selected, so
analytics don't change when the adapter does (see submit_and_handoff below).
"""
from __future__ import annotations

import logging
import secrets
from dataclasses import dataclass
from typing import Protocol

from config.settings import settings

logger = logging.getLogger(__name__)


@dataclass
class HandoffPayload:
    session_token: str
    prefill_fields: dict
    confirmations: dict
    contact: dict


@dataclass
class HandoffResult:
    handoff_ref: str
    redirect_url: str


class BackflipPort(Protocol):
    def handoff(self, payload: HandoffPayload) -> HandoffResult: ...


class FakeBackflipPort:
    """Tests and local dev. Records the call, returns a deterministic ref —
    the client's "every external service testable without network" rule."""

    def __init__(self) -> None:
        self.calls: list[HandoffPayload] = []

    def handoff(self, payload: HandoffPayload) -> HandoffResult:
        self.calls.append(payload)
        return HandoffResult(
            handoff_ref=f"fake-{payload.session_token}",
            redirect_url=f"https://fake-backflip.test/prequal?ref=fake-{payload.session_token}",
        )


class HandoffOnlyPort:
    """v1 default. Client-confirmed (2026-09-16): keep pre-filled information
    inside Forced Action and hand off to the prequal flow only at the final
    step. Redirects to the affiliate prequal URL with attribution preserved;
    the pre-fill stays on our side — nothing about the borrower's answers
    crosses to Backflip except through their own prequal form."""

    def __init__(self, prequal_base_url: str) -> None:
        self.prequal_base_url = prequal_base_url.rstrip("/")

    def handoff(self, payload: HandoffPayload) -> HandoffResult:
        ref = secrets.token_urlsafe(12)
        return HandoffResult(
            handoff_ref=ref,
            redirect_url=f"{self.prequal_base_url}?fa_ref={payload.session_token}",
        )


class UrlBackflipPort:
    """If Backflip accepts pre-filled URL parameters. Field mapping lives in
    config, not code, so a mapping change is a config edit."""

    def __init__(self, prequal_base_url: str, field_mapping: dict[str, str]) -> None:
        self.prequal_base_url = prequal_base_url.rstrip("/")
        self.field_mapping = field_mapping

    def handoff(self, payload: HandoffPayload) -> HandoffResult:
        params = {
            self.field_mapping[key]: value
            for key, value in {**payload.prefill_fields, **payload.confirmations}.items()
            if key in self.field_mapping
        }
        params["fa_ref"] = payload.session_token
        query = "&".join(f"{k}={v}" for k, v in params.items())
        ref = secrets.token_urlsafe(12)
        return HandoffResult(handoff_ref=ref, redirect_url=f"{self.prequal_base_url}?{query}")


class ApiBackflipPort:
    """If Backflip exposes an API. Not implemented yet — nothing exists to
    call. Raises rather than pretending to succeed; do not build speculative
    request/response shapes against an unknown format (plan's own rule for
    the portal-start observation applies equally here)."""

    def __init__(self, api_base_url: str, api_key: str) -> None:
        self.api_base_url = api_base_url
        self.api_key = api_key

    def handoff(self, _payload: HandoffPayload) -> HandoffResult:
        raise NotImplementedError(
            "ApiBackflipPort has no real implementation yet — Backflip's API "
            "shape is unknown (plan §7 Q1). Select HandoffOnlyPort or "
            "UrlBackflipPort until Backflip answers."
        )


def get_backflip_port() -> BackflipPort:
    """Selection by settings key, defaulting to HandoffOnlyPort — the
    client-confirmed v1 default."""
    adapter = (settings.backflip_adapter or "handoff_only").lower()
    prequal_url = settings.backflip_prequal_url or "https://app.backflip.com/prequal"

    if adapter == "fake":
        return FakeBackflipPort()
    if adapter == "url":
        raise NotImplementedError("UrlBackflipPort field_mapping is not configured yet")
    if adapter == "api":
        raise NotImplementedError("ApiBackflipPort has no real implementation yet")
    return HandoffOnlyPort(prequal_url)
