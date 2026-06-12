"""
Centralized phase-level time tracking with live JSONL output.

Measures named spans of a pipeline run and appends one JSON line per span to
a per-component log file the moment the span completes, plus one aggregate
summary line at run end — so a long run can be watched live:

    Get-Content logs\\timing\\master_loader.jsonl -Wait -Tail 20   (PowerShell)
    tail -f logs/timing/master_loader.jsonl                        (bash)

Public API
----------
    tracker = TimeTracker(component="master_loader",
                          meta={"county": "pinellas", "dry_run": True})

    with tracker.span("preload_existing") as s:
        ...
        s["rows"] = 428_079          # enrich the span line mid-block

    tracker.event("download_skipped", reason="--skip-download")

    tracker.finish(inserted=0, updated=412)   # summary line, end of run

Design constraints (deliberate):
- Phase-level only — callers must never open a span per row; a run should
  produce tens of lines, not thousands.
- The tracker can never break the host pipeline: every internal failure
  (unwritable path, serialization error, full disk) logs ONE warning and
  self-disables; host exceptions raised inside a span still propagate, but
  the span line is written first (try/finally).
- No DB, no threads, no settings/env coupling. Append-open-close per line so
  partial runs keep their timings after a crash.
"""

import json
import logging
import time
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Union

logger = logging.getLogger(__name__)

_DEFAULT_LOG_DIR = Path("logs") / "timing"


class TimeTracker:
    """Per-run, phase-level wall-clock tracker writing live JSONL records."""

    def __init__(
        self,
        component: str,
        run_id: Optional[str] = None,
        log_path: Optional[Union[str, Path]] = None,
        enabled: bool = True,
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.component = component
        self.enabled = enabled
        self.run_id = run_id or (
            f"{component}_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}"
        )
        self.log_path = Path(log_path) if log_path else _DEFAULT_LOG_DIR / f"{component}.jsonl"
        self._run_meta: Dict[str, Any] = dict(meta) if meta else {}
        self._durations: Dict[str, list] = defaultdict(list)
        self._wall_start = time.perf_counter()
        self._dir_ready = False

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    @contextmanager
    def span(self, name: str, **meta: Any) -> Iterator[Dict[str, Any]]:
        """Time a block; appends a ``start`` marker line on entry and the
        timed ``span`` line on exit (even if the block raises — the host
        exception still propagates). A hung phase is therefore visible in the
        live tail as a ``start`` with no matching ``span``. Yields a dict the
        caller may enrich; its contents land in the span line's ``meta``."""
        if not self.enabled:
            yield {}
            return
        span_meta: Dict[str, Any] = dict(meta)
        # Marker written BEFORE the clock starts so its I/O cost (sub-ms)
        # never pollutes the measured duration.
        self._write({"type": "start", "name": name, "meta": dict(meta)})
        t0 = time.perf_counter()
        try:
            yield span_meta
        finally:
            seconds = time.perf_counter() - t0
            self._durations[name].append(seconds)
            self._write({
                "type": "span",
                "name": name,
                "seconds": round(seconds, 4),
                "meta": span_meta,
            })

    def event(self, name: str, **meta: Any) -> None:
        """Append a zero-duration marker line."""
        if not self.enabled:
            return
        self._write({"type": "event", "name": name, "meta": meta})

    def finish(self, **meta: Any) -> None:
        """Append the run-summary line: total wall time plus per-span
        aggregates (calls/total/mean/max/% of wall), sorted by total desc."""
        if not self.enabled:
            return
        wall = time.perf_counter() - self._wall_start
        spans = {
            name: {
                "calls": len(times),
                "total_seconds": round(sum(times), 4),
                "mean_seconds": round(sum(times) / len(times), 4),
                "max_seconds": round(max(times), 4),
                "pct_wall": round(100.0 * sum(times) / wall, 1) if wall > 0 else 0.0,
            }
            for name, times in sorted(
                self._durations.items(), key=lambda kv: -sum(kv[1])
            )
        }
        self._write({
            "type": "summary",
            "wall_seconds": round(wall, 3),
            "spans": spans,
            "meta": meta,
        })

    # ------------------------------------------------------------------
    # Output (must never raise into the host)
    # ------------------------------------------------------------------

    def _write(self, record: Dict[str, Any]) -> None:
        if not self.enabled:
            # A failed write mid-span self-disables; the span's exit write
            # must then be a silent no-op, not a second warning.
            return
        try:
            if not self._dir_ready:
                self.log_path.parent.mkdir(parents=True, exist_ok=True)
                self._dir_ready = True
            line = json.dumps(
                {
                    "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                    "component": self.component,
                    "run_id": self.run_id,
                    "run_meta": self._run_meta,
                    **record,
                },
                default=str,  # Decimal/Path/datetime in meta must not break a run
            )
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception as exc:
            # One warning, then go silent — the tracker must never become
            # the reason a multi-hour load fails.
            self.enabled = False
            logger.warning(
                "TimeTracker disabled (cannot write %s): %s", self.log_path, exc
            )
