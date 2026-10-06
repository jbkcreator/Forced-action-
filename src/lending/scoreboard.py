"""7:20pm scoreboard from the disposition log (client Part 5.6): per caller and per campaign."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Mapping, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

from config.lending_compliance import DEFAULT_TZ
from config.lending_dispositions import BOOKED_CODE, GATED_CODES, LIVE_CONVERSATION_CODES, NURTURE_SENT_CODES

UNATTRIBUTED = "(unattributed)"
_KEYS = {  # SQL literals, never user input
    "caller": "COALESCE(NULLIF(caller_name, ''), NULLIF(caller_seat, ''), '(unknown)')",
    "campaign": "COALESCE(NULLIF(dialer_campaign_id, ''), '')",
    "hook": "COALESCE(NULLIF(campaign_tag, ''), '')",
}


@dataclass(frozen=True)
class Row:
    name: str
    dials: int
    live: int
    gated: int
    booked: int
    nurture: int

    @property
    def connect_rate(self) -> float:
        return self.live / self.dials if self.dials else 0.0

    @property
    def book_rate(self) -> float:
        return self.booked / self.live if self.live else 0.0


@dataclass(frozen=True)
class ScoreboardData:
    by_caller: list[Row]
    by_campaign: list[Row]
    by_hook: list[Row]
    total: Row


def _bounds(day: date) -> tuple[datetime, datetime]:
    tz = ZoneInfo(DEFAULT_TZ)
    return (datetime.combine(day, time.min, tzinfo=tz), datetime.combine(day + timedelta(days=1), time.min, tzinfo=tz))


def _query(db, key: str, day: date) -> list[tuple[str, int, int, int, int, int]]:
    start, end = _bounds(day)
    rows = db.execute(
        text(f"SELECT {_KEYS[key]} AS k, count(*) AS dials, "
             "count(*) FILTER (WHERE disposition = ANY(:live)) AS live, "
             "count(*) FILTER (WHERE disposition = ANY(:gated)) AS gated, "
             "count(*) FILTER (WHERE disposition = :booked AND NOT booking_blocked) AS booked, "
             "count(*) FILTER (WHERE disposition = ANY(:nurture)) AS nurture "
             "FROM lending.call_dispositions "
             "WHERE direction = 'outbound' AND call_ended_at >= :start AND call_ended_at < :end "
             "GROUP BY 1 ORDER BY dials DESC, k"),
        {"live": sorted(LIVE_CONVERSATION_CODES), "gated": sorted(GATED_CODES), "nurture": sorted(NURTURE_SENT_CODES),
         "booked": BOOKED_CODE, "start": start, "end": end},
    ).all()
    return [tuple(r) for r in rows]


def _rows(raw, name_of) -> list[Row]:
    return [Row(name_of(k), *vals) for k, *vals in raw]


def build_scoreboard(db, day: date, campaign_names: Optional[Mapping[str, str]] = None) -> ScoreboardData:
    names = campaign_names or {}
    callers = _rows(_query(db, "caller", day), lambda k: k)
    campaigns = _rows(_query(db, "campaign", day), lambda k: names.get(k, f"Campaign {k}") if k else UNATTRIBUTED)
    hooks = _rows(_query(db, "hook", day), lambda k: k or UNATTRIBUTED)
    total = Row("TOTAL", *(sum(getattr(r, f) for r in callers) for f in ("dials", "live", "gated", "booked", "nurture")))
    return ScoreboardData(callers, campaigns, hooks, total)


_COLUMNS = ("Dials", "Live", "Gated", "Booked", "Nurture", "Connect", "Book rate")
MAX_TABLE_ROWS = 25  # a Slack section holds 3000 chars
MAX_NAME_CHARS = 26
LEGEND = ("Live = real conversation · Gated = booked or failed the gate · Nurture = nurture sent · "
          "Connect = Live ÷ Dials · Book rate = Booked ÷ Live · Showed comes from GHL (not connected yet)")


def _cells(r: Row) -> list[str]:
    return [str(r.dials), str(r.live), str(r.gated), str(r.booked), str(r.nurture),
            f"{r.connect_rate:.0%}", f"{r.book_rate:.0%}"]


def _name(raw: str) -> str:
    clean = raw.replace("`", "'")
    return clean if len(clean) <= MAX_NAME_CHARS else clean[:MAX_NAME_CHARS - 1] + "…"


def _table(label: str, rows: list[Row]) -> str:
    """Aligned monospace table in a code fence (Slack has no native tables)."""
    if not rows:
        return "_no dials_"
    shown = rows[:MAX_TABLE_ROWS]
    names, body = [_name(r.name) for r in shown], [_cells(r) for r in shown]
    first = max(len(label), *map(len, names))
    widths = [max(len(h), *(len(c[i]) for c in body)) for i, h in enumerate(_COLUMNS)]
    lines = [label.ljust(first) + "  " + "  ".join(h.rjust(w) for h, w in zip(_COLUMNS, widths))]
    lines.append("-" * len(lines[0]))
    lines += [n.ljust(first) + "  " + "  ".join(c.rjust(w) for c, w in zip(cells, widths)) for n, cells in zip(names, body)]
    if len(rows) > len(shown):
        lines.append(f"... and {len(rows) - len(shown)} more")
    return "```\n" + "\n".join(lines) + "\n```"


def _sections(data: ScoreboardData) -> list[tuple[str, str, list[Row]]]:
    return [("By caller", "Caller", data.by_caller), ("By campaign", "Campaign", data.by_campaign),
            ("By hook (campaign tag): which hook works", "Hook", data.by_hook)]


def _title(day: date) -> str:
    return f"Lending scoreboard · {day:%a %b} {day.day}, {day.year}"


def _totals(t: Row) -> list[tuple[str, str]]:
    return [("Dials", str(t.dials)), ("Live", str(t.live)), ("Gated", str(t.gated)), ("Booked", str(t.booked)),
            ("Nurture", str(t.nurture)), ("Showed", "n/a (GHL)"), ("Connect", f"{t.connect_rate:.0%}"),
            ("Book rate", f"{t.book_rate:.0%}")]


def format_slack(data: ScoreboardData, day: date) -> str:
    """Plain mrkdwn version: the notification preview and the fallback when blocks are unavailable."""
    totals = " | ".join(f"{k} {v}" for k, v in _totals(data.total))
    out = [f"*{_title(day)}* (through 7:15pm ET)", f"TOTAL: {totals}"]
    for title, label, rows in _sections(data):
        out += ["", f"*{title}*", _table(label, rows)]
    return "\n".join(out)


def format_blocks(data: ScoreboardData, day: date) -> list[dict]:
    blocks: list[dict] = [
        {"type": "header", "text": {"type": "plain_text", "text": f":telephone_receiver: {_title(day)}", "emoji": True}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": "Outbound dials through 7:15pm ET"}]},
        {"type": "section", "fields": [{"type": "mrkdwn", "text": f"*{k}*\n{v}"} for k, v in _totals(data.total)]},
    ]
    for title, label, rows in _sections(data):
        blocks += [{"type": "divider"},
                   {"type": "section", "text": {"type": "mrkdwn", "text": f"*{title}*\n{_table(label, rows)}"}}]
    blocks += [{"type": "divider"}, {"type": "context", "elements": [{"type": "mrkdwn", "text": LEGEND}]}]
    return blocks
