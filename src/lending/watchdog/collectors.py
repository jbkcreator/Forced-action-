"""Watchdog collectors: each turns one probe (systemctl, HTTP) into Observations. No secrets or PII in details."""
from __future__ import annotations

import logging
import subprocess
from typing import Callable, Sequence

import requests

from config.lending_watchdog import DOORS, Door
from src.lending.watchdog.rules import Observation

logger = logging.getLogger(__name__)

_warned_doors_off = False


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=10)


def unit_observations(
    units: Sequence[str], run: Callable[[list[str]], subprocess.CompletedProcess] = _run
) -> list[Observation]:
    try:
        lines = run(["systemctl", "is-active", *units]).stdout.splitlines()
    except Exception as exc:
        logger.warning("[watchdog] systemctl failed (%s)", type(exc).__name__)
        return [Observation(f"unit:{u}", False, f"systemctl unavailable: {type(exc).__name__}") for u in units]
    states = [lines[i].strip() if i < len(lines) else "unknown" for i in range(len(units))]
    return [Observation(f"unit:{u}", s == "active", s) for u, s in zip(units, states)]


def health_observation(url: str, get: Callable[..., requests.Response] = requests.get) -> Observation:
    key = "health:lending-api"
    try:
        resp = get(url, timeout=5)
        if resp.status_code != 200:
            return Observation(key, False, f"HTTP {resp.status_code}")
        if resp.json().get("status") != "ok":
            return Observation(key, False, "health body not ok")
        return Observation(key, True, "ok")
    except Exception as exc:
        return Observation(key, False, type(exc).__name__)


def door_observations(
    base_url: str,
    doors: Sequence[Door] = DOORS,
    request: Callable[..., requests.Response] = requests.request,
) -> list[Observation]:
    global _warned_doors_off
    if not base_url:
        if not _warned_doors_off:
            _warned_doors_off = True
            logger.warning("LENDING_WATCHDOG_PUBLIC_BASE_URL unset; door checks off")
        return []
    base = base_url.rstrip("/")
    observations = []
    for door in doors:
        key = f"door:{door.name}"
        try:
            resp = request(door.method, base + door.path, json={}, timeout=10, allow_redirects=False)
            ok = resp.status_code in door.healthy
            observations.append(Observation(key, ok, "ok" if ok else f"HTTP {resp.status_code}"))
        except Exception as exc:
            observations.append(Observation(key, False, type(exc).__name__))
    return observations
