"""
DBPR outbound email template renderer.

Generates personalized HTML + plain-text email bodies for the DBPR contractor
outreach campaign. Each email is tailored to the contractor's license vertical
(roofing, hvac, plumbing, general, remediation) and links to the free signup
page with source attribution pre-filled.
"""

from urllib.parse import urlencode
from config.settings import get_settings

# ---------------------------------------------------------------------------
# Vertical-specific copy
# ---------------------------------------------------------------------------

_VERTICAL_COPY: dict[str, dict] = {
    "roofing": {
        "lead_type":    "storm-damaged and aging roof leads",
        "pain_point":   "homeowners with roof damage flagged by insurance claims, storm reports, and code violations",
        "action":       "These homeowners are actively looking for licensed contractors right now.",
    },
    "hvac": {
        "lead_type":    "HVAC replacement leads",
        "pain_point":   "properties with failing systems flagged by code violations and active permit pulls",
        "action":       "Reach them before a competitor does — many are in active distress.",
    },
    "plumbing": {
        "lead_type":    "plumbing and water damage leads",
        "pain_point":   "properties with active water damage, mold reports, and plumbing code violations",
        "action":       "These are high-urgency jobs — homeowners who can't wait.",
    },
    "general": {
        "lead_type":    "renovation and rehab leads",
        "pain_point":   "distressed properties across Hillsborough County flagged for major structural work",
        "action":       "Free access to scored leads ranked by distress severity.",
    },
    "remediation": {
        "lead_type":    "mold and water damage remediation leads",
        "pain_point":   "properties with active mold and flood damage reports from FEMA and code inspectors",
        "action":       "These homeowners have an urgent, time-sensitive need.",
    },
}

_DEFAULT_COPY = {
    "lead_type":  "distressed property leads",
    "pain_point": "homeowners in Hillsborough County who need licensed contractor work",
    "action":     "Free access to scored, verified leads in your area.",
}


def _first_name(full_name: str) -> str:
    """Extract first name from DBPR 'LAST, FIRST' format."""
    name = (full_name or "").strip()
    if "," in name:
        parts = name.split(",", 1)
        first = parts[1].strip().split()[0] if parts[1].strip() else name
        return first.title()
    parts = name.split()
    return parts[0].title() if parts else "there"


def _signup_url(email: str, vertical: str, county_id: str) -> str:
    settings = get_settings()
    base = (settings.app_base_url or "https://app.forcedaction.io").rstrip("/")
    params = urlencode({
        "source":       "dbpr_email",
        "email":        email,
        "vertical":     vertical or "general",
        "county_id":    county_id or "hillsborough",
        "utm_source":   "dbpr",
        "utm_medium":   "email",
        "utm_campaign": "contractor_outreach",
    })
    return f"{base}/signup?{params}"


# ---------------------------------------------------------------------------
# Subject line
# ---------------------------------------------------------------------------

def render_subject(full_name: str, vertical: str, county_id: str) -> str:
    first = _first_name(full_name)
    copy  = _VERTICAL_COPY.get(vertical, _DEFAULT_COPY)
    county_label = county_id.replace("_", " ").title() if county_id else "Hillsborough"
    return f"{first}, free {copy['lead_type']} in {county_label} County"


# ---------------------------------------------------------------------------
# Plain text
# ---------------------------------------------------------------------------

def render_text(full_name: str, vertical: str, county_id: str, email: str) -> str:
    first  = _first_name(full_name)
    copy   = _VERTICAL_COPY.get(vertical, _DEFAULT_COPY)
    url    = _signup_url(email, vertical, county_id)
    county_label = county_id.replace("_", " ").title() if county_id else "Hillsborough"

    return f"""Hi {first},

We built a free tool for licensed contractors in {county_label} County.

It surfaces {copy['pain_point']} — ranked by distress severity so you can focus on the highest-value jobs first.

{copy['action']}

Claim your free account and view leads now:
{url}

No credit card. No sales call.

--
Forced Action
Distressed Property Intelligence for Florida Contractors

To unsubscribe, reply with UNSUBSCRIBE in the subject line.
"""


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------

def render_html(full_name: str, vertical: str, county_id: str, email: str) -> str:
    first  = _first_name(full_name)
    copy   = _VERTICAL_COPY.get(vertical, _DEFAULT_COPY)
    url    = _signup_url(email, vertical, county_id)
    county_label = county_id.replace("_", " ").title() if county_id else "Hillsborough"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Free leads for your area</title>
  <style>
    body {{ margin: 0; padding: 0; background: #f4f4f5; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; color: #111; }}
    .wrapper {{ max-width: 560px; margin: 32px auto; background: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 4px rgba(0,0,0,.08); }}
    .header {{ background: #111827; padding: 28px 32px; }}
    .header h1 {{ margin: 0; font-size: 18px; font-weight: 700; color: #ffffff; letter-spacing: -.3px; }}
    .header p {{ margin: 4px 0 0; font-size: 13px; color: #9ca3af; }}
    .body {{ padding: 32px; }}
    .body p {{ margin: 0 0 16px; font-size: 15px; line-height: 1.6; color: #374151; }}
    .highlight {{ background: #f9fafb; border-left: 3px solid #2563eb; padding: 14px 16px; border-radius: 0 6px 6px 0; margin: 20px 0; font-size: 14px; color: #1e40af; line-height: 1.5; }}
    .cta {{ display: block; text-align: center; background: #2563eb; color: #ffffff !important; text-decoration: none; font-size: 15px; font-weight: 600; padding: 14px 28px; border-radius: 6px; margin: 28px 0 20px; }}
    .sub {{ font-size: 13px; color: #9ca3af; text-align: center; margin: 8px 0 0; }}
    .footer {{ padding: 20px 32px; border-top: 1px solid #f3f4f6; }}
    .footer p {{ margin: 0; font-size: 12px; color: #9ca3af; line-height: 1.5; }}
    .footer a {{ color: #6b7280; }}
  </style>
</head>
<body>
  <div class="wrapper">
    <div class="header">
      <h1>Forced Action</h1>
      <p>Distressed Property Intelligence &mdash; {county_label} County</p>
    </div>
    <div class="body">
      <p>Hi {first},</p>
      <p>
        We built a free tool for licensed contractors in {county_label} County that surfaces
        <strong>{copy['pain_point']}</strong> &mdash; ranked by distress severity so you focus
        on the highest-value jobs first.
      </p>
      <div class="highlight">
        {copy['action']}
      </div>
      <p>
        Every lead includes property address, distress signals (permits, liens, storm damage,
        code violations), owner contact info, and a composite score so you know exactly
        how urgent the job is.
      </p>
      <a class="cta" href="{url}">View My Free {copy['lead_type'].title()}</a>
      <p class="sub">No credit card &nbsp;&bull;&nbsp; No sales call &nbsp;&bull;&nbsp; Free forever on the base tier</p>
    </div>
    <div class="footer">
      <p>
        You&rsquo;re receiving this because you hold an active Florida contractor license
        (License: {vertical.upper() if vertical else "FL"}) in {county_label} County.<br>
        Forced Action &mdash; Tampa, FL &nbsp;|&nbsp;
        <a href="mailto:unsubscribe@forcedaction.io?subject=UNSUBSCRIBE">Unsubscribe</a>
      </p>
    </div>
  </div>
</body>
</html>"""
