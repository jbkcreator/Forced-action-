"""
Shared infrastructure for Vera's standing-job checks.

Extracted from live_state.py once a second check module (revenue_truth.py,
VERA-v2.2 sub-task V3) needed the identical logic — two real call sites is
the point where this earns its own module rather than being duplicated or
imported across a module-private boundary.
"""
from __future__ import annotations

import html as _html


def report_recipients() -> list[str]:
    from config.settings import get_settings
    settings = get_settings()
    raw = settings.report_recipients
    if not raw:
        return []
    return [addr.strip() for addr in raw.split(",") if addr.strip()]


# ─────────────────────────────────────────────────────────────────────────────
# HTML report rendering — shared dark-card theme.
#
# Matches src/tasks/report_emailer.py's existing daily/weekly ops-report style
# (#0f172a background, #fbbf24 gold headers/accents) so every Forced Action
# system email looks like one product, not a new look invented here. Every
# render_*_report() (live_state.py, revenue_truth.py) uses these primitives
# rather than hand-rolling its own markup.
# ─────────────────────────────────────────────────────────────────────────────

def esc(value) -> str:
    """HTML-escape any value before interpolating it into a report cell —
    every field here ultimately traces back to external data (git output,
    Stripe customer IDs, migration filenames), so nothing is trusted raw."""
    return _html.escape(str(value), quote=True)


def html_shell(title: str, subtitle: str, body_html: str) -> str:
    """The outer dark card every report renders inside."""
    return f"""
    <div style="max-width:680px;margin:0 auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;padding:24px;border-radius:12px;">
        <div style="text-align:center;margin-bottom:20px;">
            <h1 style="color:#fbbf24;font-size:22px;margin:0;">{esc(title)}</h1>
            <p style="color:#94a3b8;font-size:14px;margin:4px 0 0;">{esc(subtitle)}</p>
        </div>
        {body_html}
        <div style="text-align:center;margin-top:24px;padding-top:16px;border-top:1px solid #2a2a3a;">
            <span style="color:#64748b;font-size:11px;">Forced Action &mdash; Vera, Truth &amp; Verification Agent</span>
        </div>
    </div>
    """


def html_headline(label: str, value: str) -> str:
    """The single most important number — one highlighted tile up top,
    matching report_emailer.py's KPI-tile style."""
    return (
        '<div style="text-align:center;background:#1e293b;border-radius:8px;'
        'padding:14px;margin-bottom:20px;">'
        f'<div style="color:#94a3b8;font-size:11px;text-transform:uppercase;'
        f'letter-spacing:0.1em;">{esc(label)}</div>'
        f'<div style="color:#fbbf24;font-size:20px;font-weight:700;margin-top:4px;">{esc(value)}</div>'
        "</div>"
    )


def html_section(header: str, inner_html: str) -> str:
    """One section: a gold underlined header followed by its content."""
    return (
        f'<h2 style="color:#fbbf24;font-size:15px;margin:20px 0 8px;'
        f'border-bottom:1px solid #2a2a3a;padding-bottom:6px;">{esc(header)}</h2>'
        f"{inner_html}"
    )


def html_note(text: str) -> str:
    """A small muted footnote — used for methodology/caveat lines."""
    return f'<p style="color:#94a3b8;font-size:12px;margin:8px 0 0;">{esc(text)}</p>'


def html_warning(text: str) -> str:
    """A highlighted callout — used for abstentions/unreachable-source
    notices, so they can't be mistaken for routine body text."""
    return (
        '<div style="background:#450a0a;border:1px solid #ef4444;border-radius:8px;'
        f'padding:10px 12px;margin:8px 0;color:#fca5a5;font-size:13px;">{esc(text)}</div>'
    )


def html_kv_rows(pairs: list) -> str:
    """A borderless two-column key/value table — label left, value right."""
    rows = "".join(
        "<tr>"
        f'<td style="padding:4px 12px 4px 0;color:#94a3b8;white-space:nowrap;">{esc(k)}</td>'
        f'<td style="padding:4px 0;color:#e2e8f0;text-align:right;width:100%;">{esc(v)}</td>'
        "</tr>"
        for k, v in pairs
    )
    return f'<table style="width:100%;border-collapse:collapse;">{rows}</table>'


def html_table(headers: list, rows: list) -> str:
    """A bordered data table — gold header row, plain body rows."""
    th = "".join(
        f'<th style="padding:6px 12px;border-bottom:2px solid #fbbf24;'
        f'text-align:left;color:#fbbf24;font-weight:600;">{esc(h)}</th>'
        for h in headers
    )
    body = "".join(
        "<tr>"
        + "".join(
            f'<td style="padding:6px 12px;border-bottom:1px solid #2a2a3a;'
            f'color:#e2e8f0;">{esc(cell)}</td>'
            for cell in row
        )
        + "</tr>"
        for row in rows
    )
    return f'<table style="width:100%;border-collapse:collapse;"><tr>{th}</tr>{body}</table>'


def html_list(items: list) -> str:
    """A simple bulleted list for freeform string items."""
    lis = "".join(
        f'<li style="color:#e2e8f0;font-size:13px;margin:2px 0;">{esc(item)}</li>'
        for item in items
    )
    return f'<ul style="margin:6px 0;padding-left:20px;">{lis}</ul>'
