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


def _line(r: Row) -> str:
    return (f"{r.name}: Dials {r.dials} | Live {r.live} | Gated {r.gated} | Booked {r.booked} | "
            f"Showed n/a (GHL) | Nurture {r.nurture} | Connect {r.connect_rate:.0%} | Book rate {r.book_rate:.0%}")


def format_slack(data: ScoreboardData, day: date) -> str:
    out = [f"*Lending scoreboard {day.isoformat()} (through 7:15pm ET)*", _line(data.total), "", "*By caller*"]
    out += [_line(r) for r in data.by_caller] or ["no dials"]
    out += ["", "*By campaign*"] + ([_line(r) for r in data.by_campaign] or ["no dials"])
    out += ["", "*By hook (campaign tag): which hook works*"] + ([_line(r) for r in data.by_hook] or ["no dials"])
    return "\n".join(out)
