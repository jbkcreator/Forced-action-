"""Scoreboard from the disposition log (client Part 5.6, launch runbook Line 11): the 7:20pm board and the hourly day-so-far board."""
from __future__ import annotations

from dataclasses import dataclass, field
import logging
from datetime import date, datetime, time, timedelta
from typing import Mapping, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

from config.lending_compliance import DEFAULT_TZ
from config.lending_dispositions import (
    ANSWERED_CODES, BOOKED_CODE, GATED_CODES, LIVE_CONVERSATION_CODES, LONG_CONVERSATION_SECONDS, NURTURE_SENT_CODES,
    SHOWED_STAGE_KEYS, TRANSFER_CODES,
)

logger = logging.getLogger(__name__)

UNATTRIBUTED = "(unattributed)"
_KEYS = {  # SQL literals, never user input
    "caller": "COALESCE(NULLIF(caller_name, ''), NULLIF(caller_seat, ''), '(unknown)')",
    "campaign": "COALESCE(NULLIF(dialer_campaign_id, ''), '')",
    "hook": "COALESCE(NULLIF(campaign_tag, ''), '')",
    "seat": "COALESCE(NULLIF(caller_seat, ''), '(unknown)')",
    "source": "COALESCE(NULLIF(source_tag, ''), '')",
}


@dataclass(frozen=True)
class Row:
    name: str
    dials: int
    live: int
    gated: int
    booked: int
    nurture: int
    showed: int = 0
    long_calls: int = 0
    transfers: int = 0

    @property
    def connect_rate(self) -> float:
        return self.live / self.dials if self.dials else 0.0

    @property
    def show_rate(self) -> Optional[float]:
        """Showed (Held) / Booked; None when nothing was booked, so there is no division."""
        return self.showed / self.booked if self.booked else None

    @property
    def book_rate(self) -> float:
        return self.booked / self.live if self.live else 0.0


@dataclass(frozen=True)
class NumberRow:
    number: str
    dials: int
    answered: int

    @property
    def answer_rate(self) -> float:
        return self.answered / self.dials if self.dials else 0.0


@dataclass(frozen=True)
class ScoreboardData:
    by_caller: list[Row]
    by_campaign: list[Row]
    by_hook: list[Row]
    total: Row
    by_number: list[NumberRow] = field(default_factory=list)
    by_seat: list[Row] = field(default_factory=list)
    by_source: list[Row] = field(default_factory=list)


def _bounds(day: date) -> tuple[datetime, datetime]:
    tz = ZoneInfo(DEFAULT_TZ)
    return (datetime.combine(day, time.min, tzinfo=tz), datetime.combine(day + timedelta(days=1), time.min, tzinfo=tz))


def _query(db, key: str, day: date) -> list[tuple[str, int, int, int, int, int, int, int]]:
    start, end = _bounds(day)
    params = {"live": sorted(LIVE_CONVERSATION_CODES), "gated": sorted(GATED_CODES), "nurture": sorted(NURTURE_SENT_CODES),
              "booked": BOOKED_CODE, "long": LONG_CONVERSATION_SECONDS, "start": start, "end": end}
    if TRANSFER_CODES:
        transfers = "count(*) FILTER (WHERE disposition = ANY(:transfer))"
        params["transfer"] = sorted(TRANSFER_CODES)
    else:
        transfers = "0"  # the dialer records no transfer yet; the board shows n/a
    rows = db.execute(
        text(f"SELECT {_KEYS[key]} AS k, count(*) AS dials, "
             "count(*) FILTER (WHERE disposition = ANY(:live)) AS live, "
             "count(*) FILTER (WHERE disposition = ANY(:gated)) AS gated, "
             "count(*) FILTER (WHERE disposition = :booked AND NOT booking_blocked) AS booked, "
             "count(*) FILTER (WHERE disposition = ANY(:nurture)) AS nurture, "
             "count(*) FILTER (WHERE talk_duration_sec >= :long) AS long_calls, "
             f"{transfers} AS transfers "
             "FROM lending.call_dispositions "
             "WHERE direction = 'outbound' AND call_ended_at >= :start AND call_ended_at < :end "
             "GROUP BY 1 ORDER BY dials DESC, k"),
        params,
    ).all()
    return [tuple(r) for r in rows]


def _by_number(db, day: date) -> list[NumberRow]:
    """Answer rate per outbound caller-ID number, so a number that drops or gets spam-flagged is visible."""
    start, end = _bounds(day)
    rows = db.execute(
        text("SELECT COALESCE(NULLIF(caller_id_number, ''), '(unknown)') AS n, count(*) AS dials, "
             "count(*) FILTER (WHERE disposition = ANY(:answered)) AS answered "
             "FROM lending.call_dispositions "
             "WHERE direction = 'outbound' AND call_ended_at >= :start AND call_ended_at < :end "
             "GROUP BY 1 ORDER BY dials DESC, n"),
        {"answered": sorted(ANSWERED_CODES), "start": start, "end": end},
    ).all()
    return [NumberRow(*r) for r in rows]


def _showed(db, key: str, day: date) -> dict[str, int]:
    """Opportunities that entered a "showed" GHL stage on ``day``, credited to the caller / campaign of the
    latest BOOKED call to that phone (the event's own booked_by when no such call is logged).

    Showed is additive: if its table is missing or the query fails, the report still posts with Showed 0
    rather than losing every other column. The savepoint keeps the outer transaction usable."""
    start, end = _bounds(day)
    try:
        with db.begin_nested():
            rows = db.execute(
                text(f"SELECT {_KEYS[key]} AS k, count(*) FROM ("
                     "SELECT COALESCE(NULLIF(b.caller_name, ''), e.booked_by) AS caller_name, b.caller_seat, "
                     "b.dialer_campaign_id, b.campaign_tag, b.source_tag FROM lending.ghl_stage_events e "
                     "LEFT JOIN LATERAL (SELECT caller_name, caller_seat, dialer_campaign_id, campaign_tag, source_tag "
                     "FROM lending.call_dispositions d WHERE d.phone = e.phone AND d.disposition = :booked "
                     "ORDER BY d.call_ended_at DESC NULLS LAST LIMIT 1) b ON true "
                     "WHERE e.stage_key = ANY(:showed) AND e.event_at >= :start AND e.event_at < :end) s GROUP BY 1"),
                {"booked": BOOKED_CODE, "showed": sorted(SHOWED_STAGE_KEYS), "start": start, "end": end},
            ).all()
    except Exception as exc:
        logger.warning("[lending-scoreboard] showed query failed, reporting Showed 0: %s", type(exc).__name__)
        return {}
    return {k: n for k, n in rows}


def _rows(raw, showed: dict[str, int], name_of) -> list[Row]:
    by_key = {k: vals for k, *vals in raw}
    for k in showed:  # booked on an earlier day, showed today: no dials today, still counted
        by_key.setdefault(k, [0, 0, 0, 0, 0, 0, 0])
    # query order: dials, live, gated, booked, nurture, long_calls, transfers; Row puts showed after nurture
    return [Row(name_of(k), *vals[:5], showed.get(k, 0), *vals[5:]) for k, vals in by_key.items()]


def build_scoreboard(db, day: date, campaign_names: Optional[Mapping[str, str]] = None) -> ScoreboardData:
    names = campaign_names or {}
    callers = _rows(_query(db, "caller", day), _showed(db, "caller", day), lambda k: k)
    campaigns = _rows(_query(db, "campaign", day), _showed(db, "campaign", day),
                      lambda k: names.get(k, f"Campaign {k}") if k else UNATTRIBUTED)
    hooks = _rows(_query(db, "hook", day), _showed(db, "hook", day), lambda k: k or UNATTRIBUTED)
    seats = _rows(_query(db, "seat", day), _showed(db, "seat", day), lambda k: k)
    sources = _rows(_query(db, "source", day), _showed(db, "source", day), lambda k: k or UNATTRIBUTED)
    total = Row("TOTAL", *(sum(getattr(r, f) for r in callers)
                           for f in ("dials", "live", "gated", "booked", "nurture", "showed", "long_calls", "transfers")))
    return ScoreboardData(callers, campaigns, hooks, total, _by_number(db, day), seats, sources)


_COLUMNS = ("Dials", "Contacts", "2m+", "Gated", "Booked", "Xfer", "Showed", "Show%", "Nurture", "Connect", "Book rate")
_NUMBER_COLUMNS = ("Dials", "Answered", "Answer rate")
MAX_TABLE_ROWS = 20  # a Slack section holds 3000 chars
MAX_NAME_CHARS = 26
LEGEND = ("Contacts = real conversation · 2m+ = conversations of 2+ min · Gated = booked or failed the gate · "
          "Xfer = transferred to Josh (n/a until the dialer records transfers) · Nurture = nurture sent · "
          "Showed = moved to Held in GHL, credited to the caller who booked it · Show% = Showed ÷ Booked · "
          "Connect = Contacts ÷ Dials · Book rate = Booked ÷ Contacts")
TOTALS_PER_SECTION = 6  # a Slack section holds at most 10 fields


def _transfers(n: int) -> str:
    return str(n) if TRANSFER_CODES else "n/a"


def _show_rate(r: Row) -> str:
    return f"{r.show_rate:.0%}" if r.show_rate is not None else "–"


def _cells(r: Row) -> list[str]:
    return [str(r.dials), str(r.live), str(r.long_calls), str(r.gated), str(r.booked), _transfers(r.transfers),
            str(r.showed), _show_rate(r), str(r.nurture), f"{r.connect_rate:.0%}", f"{r.book_rate:.0%}"]


def _number_cells(r: NumberRow) -> list[str]:
    return [str(r.dials), str(r.answered), f"{r.answer_rate:.0%}"]


def _name(raw: str) -> str:
    clean = raw.replace("`", "'")
    return clean if len(clean) <= MAX_NAME_CHARS else clean[:MAX_NAME_CHARS - 1] + "…"


def _render(label: str, columns: tuple[str, ...], raw_names: list[str], body: list[list[str]], total: int) -> str:
    """Aligned monospace table in a code fence (Slack has no native tables)."""
    names = [_name(n) for n in raw_names]
    first = max(len(label), *map(len, names))
    widths = [max(len(h), *(len(c[i]) for c in body)) for i, h in enumerate(columns)]
    lines = [label.ljust(first) + "  " + "  ".join(h.rjust(w) for h, w in zip(columns, widths))]
    lines.append("-" * len(lines[0]))
    lines += [n.ljust(first) + "  " + "  ".join(c.rjust(w) for c, w in zip(cells, widths)) for n, cells in zip(names, body)]
    if total > len(body):
        lines.append(f"... and {total - len(body)} more")
    return "```\n" + "\n".join(lines) + "\n```"


def _table(label: str, rows: list[Row]) -> str:
    if not rows:
        return "_no dials_"
    shown = rows[:MAX_TABLE_ROWS]
    return _render(label, _COLUMNS, [r.name for r in shown], [_cells(r) for r in shown], len(rows))


def _number_table(rows: list[NumberRow]) -> str:
    if not rows:
        return "_no dials_"
    shown = rows[:MAX_TABLE_ROWS]
    return _render("Number", _NUMBER_COLUMNS, [r.number for r in shown], [_number_cells(r) for r in shown], len(rows))


def _sections(data: ScoreboardData) -> list[tuple[str, str, list[Row]]]:
    return [("By seat", "Seat", data.by_seat), ("By source", "Source", data.by_source),
            ("By caller", "Caller", data.by_caller), ("By campaign", "Campaign", data.by_campaign),
            ("By hook (campaign tag): which hook works", "Hook", data.by_hook)]


NUMBER_TITLE = "By caller-ID number: answer rate"


def _title(day: date) -> str:
    return f"Lending scoreboard · {day:%a %b} {day.day}, {day.year}"


def _totals(t: Row) -> list[tuple[str, str]]:
    return [("Dials", str(t.dials)), ("Contacts", str(t.live)), ("2+ min", str(t.long_calls)), ("Gated", str(t.gated)),
            ("Booked", str(t.booked)), ("Transfers", _transfers(t.transfers)), ("Showed", str(t.showed)),
            ("Show rate", _show_rate(t)), ("Nurture", str(t.nurture)), ("Connect", f"{t.connect_rate:.0%}"),
            ("Book rate", f"{t.book_rate:.0%}")]


def _clock(moment: datetime) -> str:
    return f"{moment.hour % 12 or 12}:{moment.minute:02d} {'am' if moment.hour < 12 else 'pm'} ET"


def format_slack(data: ScoreboardData, day: date, as_of: Optional[datetime] = None) -> str:
    """Plain mrkdwn version: the notification preview and the fallback when blocks are unavailable.
    ``as_of`` (Eastern) marks an hourly day-so-far board; without it this is the closing 7:20pm board."""
    totals = " | ".join(f"{k} {v}" for k, v in _totals(data.total))
    span = f"as of {_clock(as_of)}" if as_of else "through 7:15pm ET"
    out = [f"*{_title(day)}* ({span})", f"TOTAL: {totals}"]
    for title, label, rows in _sections(data):
        out += ["", f"*{title}*", _table(label, rows)]
    out += ["", f"*{NUMBER_TITLE}*", _number_table(data.by_number)]
    return "\n".join(out)


def format_blocks(data: ScoreboardData, day: date, as_of: Optional[datetime] = None) -> list[dict]:
    span = f"Outbound dials today, as of {_clock(as_of)}" if as_of else "Outbound dials through 7:15pm ET"
    fields = [{"type": "mrkdwn", "text": f"*{k}*\n{v}"} for k, v in _totals(data.total)]
    blocks: list[dict] = [
        {"type": "header", "text": {"type": "plain_text", "text": f":telephone_receiver: {_title(day)}", "emoji": True}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": span}]},
    ]
    blocks += [{"type": "section", "fields": fields[i:i + TOTALS_PER_SECTION]}
               for i in range(0, len(fields), TOTALS_PER_SECTION)]
    for title, label, rows in _sections(data):
        blocks += [{"type": "divider"},
                   {"type": "section", "text": {"type": "mrkdwn", "text": f"*{title}*\n{_table(label, rows)}"}}]
    blocks += [{"type": "divider"},
               {"type": "section", "text": {"type": "mrkdwn", "text": f"*{NUMBER_TITLE}*\n{_number_table(data.by_number)}"}}]
    blocks += [{"type": "divider"}, {"type": "context", "elements": [{"type": "mrkdwn", "text": LEGEND}]}]
    return blocks
