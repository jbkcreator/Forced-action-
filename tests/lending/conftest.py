"""Shared fixtures for the lending tests."""
from __future__ import annotations

import logging

import pytest


@pytest.fixture(autouse=True)
def _src_logs_reach_caplog():
    """config/logging.yaml sets the ``src`` logger to propagate=False, and any test that
    imports a module loading it would hide every later ``src.*`` record from caplog.
    Re-enable propagation (and DEBUG) for each test, then restore."""
    src = logging.getLogger("src")
    saved = (src.propagate, src.level)
    src.propagate, src.level = True, logging.NOTSET
    yield
    src.propagate, src.level = saved
