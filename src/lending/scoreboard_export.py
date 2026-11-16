"""P2.5: the 7:20pm closing scoreboard's numbers to Drive, ``NDL Ledger Exports/{ET date}/scoreboard.csv``.

One CSV, long format (``day, section, name, metric, value``), built from the same data as the board and written just
before it posts, so a Slack outage cannot lose the day's numbers. Counts only, plus caller (staff) and campaign names:
no borrower name, phone or email, the same rule as the nightly ledger (brief section 6). Names are free text (a caller
can fall back to GHL's ``booked_by``), so they go through the ledger's ``redact`` like its own free-text columns.
Stuck deals are counted per owner, never listed.

The file sits in the dated folder the nightly ledger export (P1.2) also writes; it is not part of that export's
``manifest.json``, which marks the nightly tables only. A re-run replaces the file (``DriveUploader``).
"""
from __future__ import annotations

import csv
import io
from datetime import date
from typing import Any, Iterator

from config.lending_dispositions import SCOREBOARD_EXPORT_FILE
from src.lending.ledger_export import Uploader, redact
from src.lending.scoreboard import Row, ScoreboardData

COLUMNS = ("day", "section", "name", "metric", "value")
ROW_METRICS = ("dials", "live", "gated", "booked", "nurture", "showed", "long_calls", "transfers")


def _row_metrics(section: str, row: Row) -> Iterator[tuple[str, str, str, Any]]:
    for metric in ROW_METRICS:
        yield section, row.name, metric, getattr(row, metric)


def metric_rows(data: ScoreboardData) -> Iterator[tuple[str, str, str, Any]]:
    """(section, name, metric, value) for the totals, every breakdown, and the pipeline when it was read."""
    yield from _row_metrics("total", data.total)
    yield "total", "TOTAL", "booked_stage", "" if data.booked_stage is None else data.booked_stage
    for section, rows in (("caller", data.by_caller), ("campaign", data.by_campaign), ("hook", data.by_hook),
                          ("seat", data.by_seat), ("source", data.by_source)):
        for row in rows:
            yield from _row_metrics(section, row)
    pipeline = data.pipeline
    if pipeline is None:
        return
    yield "pipeline", "pipeline", "available", int(pipeline.available)
    if not pipeline.available:
        return
    yield "pipeline", "pipeline", "active_deals", pipeline.active
    yield "pipeline", "pipeline", "total_deals", pipeline.total
    for stage in pipeline.by_stage:
        yield "stage", stage.stage_name, "deals", stage.deals
    yield "blockers", "blockers", "stuck_deals", len(pipeline.blockers)
    for owner, count in pipeline.blockers_by_owner:
        yield "blockers", owner, "stuck_deals", count


def build_csv(data: ScoreboardData, day: date) -> bytes:
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(COLUMNS)
    for section, name, metric, value in metric_rows(data):
        writer.writerow((day.isoformat(), section, redact(name), metric, value))
    return out.getvalue().encode("utf-8")


def export(data: ScoreboardData, day: date, *, folder_id: str, upload: Uploader) -> str:
    """Upload the CSV into the day's folder; returns the file name. Raises when there is no folder or Drive fails."""
    if not folder_id:
        raise ValueError("no Drive folder")
    upload(folder_id, day.isoformat(), SCOREBOARD_EXPORT_FILE, build_csv(data, day))
    return SCOREBOARD_EXPORT_FILE
