"""Shared chrome for the borrower-facing pages.

Dark-glass styling matching the platform's design language (see
Forced-action-ui/src/config/theme.json for the source palette). Inlined
rather than linked because these routes have no static-asset pipeline of
their own.

Lives here rather than inside one router because a borrower can meet more
than one of these pages in a single conversation — a pre-fill form and then
a booking page — and two copies of the stylesheet would drift into two
different-looking products. No brand name or logo beyond the badge by
design; brand separation from Backflip is still undecided.
"""
from __future__ import annotations

import html

PAGE_STYLE = """
:root{
  --bg-0:#070b14;--bg-1:#0f172a;--bg-2:#131c33;
  --card:rgba(255,255,255,.04);--border:rgba(255,255,255,.08);--border-strong:rgba(255,255,255,.14);
  --text:#f8fafc;--text-2:#94a3b8;--text-3:#64748b;
  --primary:#fbbf24;--primary-dark:#f59e0b;--accent:#a855f7;
  --radius-lg:1rem;--radius-md:.75rem;--radius-sm:.5rem;
}
*{box-sizing:border-box;}
html,body{margin:0;padding:0;}
body{
  font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif;
  color:var(--text);
  background:linear-gradient(135deg,var(--bg-0) 0%,var(--bg-1) 30%,var(--bg-2) 50%,var(--bg-1) 70%,var(--bg-0) 100%);
  background-attachment:fixed;
  min-height:100vh;
  padding:2rem 1rem 4rem;
}
.wrap{max-width:640px;margin:0 auto;}
.brand{display:flex;align-items:center;gap:.6rem;margin-bottom:1.75rem;}
.brand-badge{
  width:2.25rem;height:2.25rem;border-radius:var(--radius-sm);flex-shrink:0;
  display:flex;align-items:center;justify-content:center;font-weight:900;font-size:.8rem;
  color:#0f172a;background:linear-gradient(135deg,#facc15,#f59e0b);
}
.brand-name{font-weight:700;color:var(--text);}
.brand-name .accent{color:var(--primary);}
.eyebrow{
  display:inline-flex;align-items:center;gap:.4rem;font-size:.75rem;font-weight:600;letter-spacing:.02em;
  color:var(--primary);background:rgba(251,191,36,.1);border:1px solid rgba(251,191,36,.25);
  border-radius:999px;padding:.3rem .75rem;margin-bottom:1rem;
}
h1{font-size:1.6rem;font-weight:800;margin:0 0 .4rem;line-height:1.25;}
.sub{color:var(--text-2);font-size:.95rem;margin:0 0 1.75rem;line-height:1.5;}
.card{
  background:var(--card);border:1px solid var(--border);border-radius:var(--radius-lg);
  padding:1.5rem;margin-bottom:1.25rem;backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);
}
.card h2{
  font-size:.8rem;text-transform:uppercase;letter-spacing:.06em;color:var(--text-2);font-weight:700;
  margin:0 0 1.15rem;display:flex;align-items:center;gap:.6rem;
}
.step-num{
  display:inline-flex;align-items:center;justify-content:center;width:1.5rem;height:1.5rem;border-radius:50%;
  background:linear-gradient(135deg,rgba(251,191,36,.2),rgba(168,85,247,.15));border:1px solid rgba(251,191,36,.3);
  color:var(--primary);font-size:.75rem;font-weight:800;flex-shrink:0;
}
.field{margin-bottom:1rem;}
.field:last-child{margin-bottom:0;}
label{display:block;font-size:.8rem;font-weight:600;color:var(--text-2);margin-bottom:.4rem;}
input[type=text],input[type=email],input[type=tel],input[type=date],select{
  width:100%;padding:.7rem .85rem;background:rgba(255,255,255,.03);border:1px solid var(--border-strong);
  border-radius:var(--radius-sm);color:var(--text);font-size:.95rem;font-family:inherit;
  transition:border-color .2s ease,background .2s ease,box-shadow .2s ease;
  color-scheme:dark;
}
input::placeholder{color:var(--text-3);}
input:focus,select:focus{
  outline:none;border-color:var(--primary);background:rgba(255,255,255,.05);
  box-shadow:0 0 0 3px rgba(251,191,36,.15);
}
select option{background:#1a1d2e;color:#f1f5f9;}
/* Chrome/Edge force a light autofill background by default — this keeps
   an autofilled or browser-suggested value on the dark theme instead of a
   jarring white cell. */
input:-webkit-autofill,
input:-webkit-autofill:hover,
input:-webkit-autofill:focus {
  -webkit-box-shadow: 0 0 0 1000px rgba(255,255,255,.05) inset !important;
  -webkit-text-fill-color: #f8fafc !important;
  caret-color: #f8fafc;
  transition: background-color 9999s ease-in-out 0s;
}
.found-badge{
  display:inline-flex;align-items:center;gap:.4rem;font-size:.75rem;font-weight:700;color:#4ade80;
  background:rgba(34,197,94,.1);border:1px solid rgba(34,197,94,.25);border-radius:999px;
  padding:.3rem .7rem;margin-bottom:1.1rem;
}
.help-text{color:var(--text-3);font-size:.85rem;margin:0 0 1rem;line-height:1.5;}
.consent{
  display:flex;gap:.65rem;align-items:flex-start;background:rgba(255,255,255,.02);
  border:1px solid var(--border);border-radius:var(--radius-sm);padding:.9rem 1rem;
}
.consent input{width:auto;margin-top:.2rem;accent-color:var(--primary);}
.consent label{margin:0;font-weight:400;color:var(--text-2);font-size:.85rem;line-height:1.45;}
.btn{
  display:block;width:100%;padding:.95rem 1.5rem;margin-top:.5rem;
  background:linear-gradient(135deg,var(--primary),var(--primary-dark));color:#1a1200;
  font-weight:800;font-size:1rem;font-family:inherit;border:none;border-radius:var(--radius-md);cursor:pointer;
  transition:transform .2s cubic-bezier(.4,0,.2,1),box-shadow .2s ease;
}
.btn:hover{transform:translateY(-2px);box-shadow:0 8px 24px rgba(251,191,36,.3);}
.btn:active{transform:translateY(0) scale(.99);}
.btn:disabled{opacity:.6;cursor:not-allowed;transform:none;box-shadow:none;}
.form-error{
  color:#fca5a5;background:rgba(239,68,68,.1);border:1px solid rgba(239,68,68,.3);
  border-radius:var(--radius-sm);padding:.8rem 1rem;font-size:.85rem;line-height:1.5;margin:0 0 1rem;
}
.footnote{text-align:center;color:var(--text-3);font-size:.75rem;margin-top:1.25rem;line-height:1.5;}
.notice-wrap{max-width:480px;margin:4rem auto 0;text-align:center;}
.notice-wrap .brand{justify-content:center;margin-bottom:2rem;}
.notice-card{
  background:var(--card);border:1px solid var(--border);
  border-radius:var(--radius-lg);padding:2.25rem 1.75rem;backdrop-filter:blur(16px);
}
.notice-card p{color:var(--text-2);font-size:.95rem;line-height:1.6;margin:0;}
.slot-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(7.5rem,1fr));gap:.6rem;}
.slot{
  padding:.7rem .5rem;background:rgba(255,255,255,.03);border:1px solid var(--border-strong);
  border-radius:var(--radius-sm);color:var(--text);font-size:.9rem;font-family:inherit;font-weight:600;
  cursor:pointer;text-align:center;transition:border-color .2s ease,background .2s ease;
}
.slot:hover{border-color:var(--primary);background:rgba(255,255,255,.06);}
.slot[aria-pressed="true"]{
  border-color:var(--primary);background:rgba(251,191,36,.15);color:var(--primary);
}
.day-label{
  font-size:.8rem;font-weight:700;color:var(--text-2);text-transform:uppercase;
  letter-spacing:.05em;margin:1.25rem 0 .65rem;
}
.day-label:first-of-type{margin-top:0;}
@media (max-width:480px){
  body{padding:1.25rem .85rem 3rem;}
  .card{padding:1.15rem;}
  h1{font-size:1.35rem;}
}
"""

PAGE_HEAD = (
    '<meta charset="utf-8">'
    '<meta name="viewport" content="width=device-width, initial-scale=1">'
    '<meta name="robots" content="noindex, nofollow">'
    '<link rel="preconnect" href="https://fonts.googleapis.com">'
    '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
    '<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">'
    f"<style>{PAGE_STYLE}</style>"
)

BRAND_MARKUP = (
    '<div class="brand"><div class="brand-badge">FA</div>'
    '<span class="brand-name">Forced <span class="accent">Action</span></span></div>'
)


def escape_html(value) -> str:
    """Escape before interpolating into HTML.

    Values reaching these pages are scraped from county portals or typed by
    a stranger, so nothing rendered here is trusted.
    """
    return html.escape(str(value), quote=True)


def render_page(title: str, body: str) -> str:
    return (
        f'<!doctype html><html lang="en"><head><title>{escape_html(title)}</title>'
        f"{PAGE_HEAD}</head><body>{body}</body></html>"
    )
