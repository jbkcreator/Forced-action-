"""Shared Forced Action branded email shell (dark + gold).

Single source of truth for the outbound customer-email look, so a brand change is
one edit instead of N. Callers supply the inner content (rows or paragraphs) and
optionally a single primary CTA; this wraps the branded header, dark shell,
button, and footer.

Design tokens match the distress-digest / lead-delivery emails:
  - page background  #1a1f2e
  - card background  #0c1221
  - accent (gold)    #d4a040  -> ACCENT
  - 680px table layout, uppercase gold CTA
"""
from __future__ import annotations

from typing import Optional

ACCENT = "#d4a040"
_PAGE_BG = "#1a1f2e"
_CARD_BG = "#0c1221"
_TEXT = "#f0f2f5"
_MUTED = "#6b7280"


def paragraph(text: str, *, muted: bool = False) -> str:
    """A body paragraph inside the shell."""
    color = _MUTED if muted else "#c7cdd6"
    return (
        f'<tr><td style="padding:6px 32px;font-size:14px;line-height:1.55;color:{color};">'
        f'{text}</td></tr>'
    )


def lead_row(*, title: str, sub: str = "", meta: str = "") -> str:
    """One gold-bordered detail row (a lead, a line item, etc.)."""
    sub_html = (
        f'<div style="margin-top:5px;font-size:12px;color:#7a8396;">{sub}</div>' if sub else ""
    )
    meta_html = (
        f'<div style="margin-top:6px;font-size:12px;color:{_MUTED};">{meta}</div>' if meta else ""
    )
    return (
        f'<tr><td style="padding:16px 28px;border-bottom:1px solid #ffffff12;'
        f'border-left:3px solid {ACCENT};">'
        f'<div style="font-size:16px;font-weight:700;color:{_TEXT};letter-spacing:0.02em;">'
        f'{title}</div>{sub_html}{meta_html}</td></tr>'
    )


def render_email_shell(
    *,
    headline: str,
    subhead: Optional[str] = None,
    inner_html: str = "",
    cta_text: Optional[str] = None,
    cta_url: Optional[str] = None,
    footer_note: Optional[str] = None,
    preheader: Optional[str] = None,
) -> str:
    """Wrap content in the branded dark+gold email shell.

    headline   — big title in the header.
    subhead    — small line under the headline (optional).
    inner_html — rows/paragraphs built with lead_row()/paragraph() (optional).
    cta_text/cta_url — single primary gold button (both required to render one).
    footer_note — extra line above the standard footer (optional).
    preheader  — hidden inbox-preview text (optional).
    """
    subhead_html = (
        f'<div style="margin-top:8px;font-size:13px;color:{_MUTED};">{subhead}</div>'
        if subhead
        else ""
    )
    body_html = (
        f'<tr><td><table role="presentation" width="100%" cellpadding="0" cellspacing="0">'
        f'{inner_html}</table></td></tr>'
        if inner_html
        else ""
    )
    cta_html = (
        f'<tr><td style="padding:24px 32px 28px;border-top:1px solid #ffffff0a;text-align:center;">'
        f'<a href="{cta_url}" style="display:block;padding:16px 36px;background:{ACCENT};'
        f'color:{_CARD_BG};font-size:14px;font-weight:700;letter-spacing:0.08em;'
        f'text-transform:uppercase;text-decoration:none;">{cta_text} &rarr;</a></td></tr>'
        if cta_text and cta_url
        else ""
    )
    footer_extra = (
        f'<tr><td style="padding:0 32px 16px;font-size:13px;color:{_MUTED};">{footer_note}</td></tr>'
        if footer_note
        else ""
    )
    preheader_html = (
        f'<div style="display:none;max-height:0;overflow:hidden;opacity:0;">{preheader}</div>'
        if preheader
        else ""
    )

    return f"""<html><head><meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1"></head>
    <body style="margin:0;background:{_PAGE_BG};font-family:Arial,Helvetica,sans-serif;">
    {preheader_html}
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:{_PAGE_BG};padding:32px 16px;">
      <tr><td align="center">
        <table role="presentation" width="680" cellpadding="0" cellspacing="0" style="max-width:680px;width:100%;background:{_CARD_BG};border:1px solid #ffffff14;">
          <tr><td style="padding:28px 32px 24px;border-bottom:1px solid #ffffff0f;">
            <div style="font-size:18px;font-weight:700;color:{_TEXT};">Forced <span style="color:{ACCENT};">Action</span></div>
            <div style="margin-top:16px;font-size:26px;font-weight:700;color:{_TEXT};line-height:1.15;">{headline}</div>
            {subhead_html}
          </td></tr>
          {body_html}
          {footer_extra}
          {cta_html}
        </table>
        <div style="margin-top:16px;font-size:11px;color:#2d3344;">ForcedActionLeads.com &middot; noreply@forcedactionleads.com</div>
      </td></tr>
    </table></body></html>"""
