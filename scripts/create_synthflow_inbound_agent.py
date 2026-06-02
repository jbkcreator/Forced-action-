"""
One-time script: create the Synthflow inbound voice agent for the missed-call
signup flow (Phase 0 of SYNTHFLOW_VOICE_FLOW_PLAN.md).

What it does:
  1. Reads credentials from .env (SYNTHFLOW_API_KEY, SYNTHFLOW_API_BASE).
  2. Checks if SYNTHFLOW_INBOUND_AGENT already set → confirms agent exists.
  3. If not, POSTs to {base}/assistants to create an inbound voice agent.
  4. Attempts to provision a phone number via POST {base}/phone_numbers.
  5. Writes SYNTHFLOW_INBOUND_AGENT and SYNTHFLOW_INBOUND_DID to .env.

Usage:
    python scripts/create_synthflow_inbound_agent.py

Requires:
    - requests
    - python-dotenv (or reading .env manually — script does both)
"""

from __future__ import annotations

import logging
import os
import re
import sys
from pathlib import Path

# ── Ensure src is on sys.path so we can import settings ─────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger("create_synthflow_inbound_agent")

# ── Load .env manually (fast, no dependency) ────────────────────────────────
_DOTENV_PATH = _PROJECT_ROOT / ".env"


def _load_dotenv_simple(path: Path) -> dict[str, str]:
    """Read .env into a dict — minimal, no external dependency."""
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        env[key.strip()] = val.strip().strip('"').strip("'")
    return env


def _write_env_var(path: Path, key: str, value: str) -> bool:
    """Append or update a key=value line in .env. Returns True if changed."""
    if value is None or value == "":
        logger.warning("Skipping empty value for %s — not writing to .env", key)
        return False

    if not path.exists():
        path.write_text(f"{key}={value}\n", encoding="utf-8")
        logger.info("Created .env with %s=%s", key, value)
        return True

    lines = path.read_text(encoding="utf-8").splitlines()
    replaced = False
    new_lines: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#") or "=" not in stripped:
            new_lines.append(line)
            continue
        k, _, _ = stripped.partition("=")
        if k.strip() == key:
            new_lines.append(f"{key}={value}")
            replaced = True
            logger.info("Updated %s=%s in .env", key, value)
        else:
            new_lines.append(line)

    if not replaced:
        new_lines.append(f"{key}={value}")
        logger.info("Appended %s=%s to .env", key, value)

    path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    return True


# ── Main ────────────────────────────────────────────────────────────────────


def main() -> int:
    env = _load_dotenv_simple(_DOTENV_PATH)

    api_key = env.get("SYNTHFLOW_API_KEY") or os.environ.get("SYNTHFLOW_API_KEY")
    if not api_key:
        logger.error("SYNTHFLOW_API_KEY not found in .env or environment")
        return 1

    api_base = env.get("SYNTHFLOW_API_BASE") or os.environ.get("SYNTHFLOW_API_BASE", "https://api.us.synthflow.ai/v2")
    api_base = api_base.rstrip("/")

    app_base = env.get("APP_BASE_URL") or os.environ.get("APP_BASE_URL", "https://forcedactionleads.com")
    app_base = app_base.rstrip("/")

    existing_agent_id = env.get("SYNTHFLOW_INBOUND_AGENT") or os.environ.get("SYNTHFLOW_INBOUND_AGENT")

    # ── Step 1: Check existing agent ────────────────────────────────────────
    if existing_agent_id:
        logger.info("SYNTHFLOW_INBOUND_AGENT already set to %s — verifying...", existing_agent_id)
        try:
            import requests
            resp = requests.get(
                f"{api_base}/assistants/{existing_agent_id}",
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                logger.info("Agent exists: name=%s type=%s", data.get("name"), data.get("type"))
                logger.info("Skipping creation. Agent ID: %s", existing_agent_id)
                return 0
            elif resp.status_code == 404:
                logger.warning("Agent %s not found on Synthflow — will re-create", existing_agent_id)
            else:
                logger.warning(
                    "Unexpected status %s verifying agent: %s — will re-create",
                    resp.status_code, resp.text[:200],
                )
        except Exception as exc:
            logger.warning("Could not verify agent %s: %s — will re-create", existing_agent_id, exc)

    # ── Step 2: Create agent ────────────────────────────────────────────────
    logger.info("Creating inbound voice agent on %s ...", api_base)

    prompt = (
        "You are the Forced Action inbound signup agent. "
        "Your job is to capture a contractor's ZIP code and the contractor vertical they serve "
        "(roofing, restoration, remediation, HVAC, solar, or fix-and-flip). "
        "Greet them briefly, ask for their ZIP code first, then ask which vertical they work in. "
        "Confirm both back to them and let them know we'll text them sample distressed property "
        "leads immediately to the number they called from. "
        "Do not answer questions about pricing or account details — the SMS follow-up handles that. "
        "Keep the call under 60 seconds. "
        "If the caller is unwilling to provide both ZIP and vertical, politely end the call."
    )

    payload = {
        "type": "inbound",
        "name": "Forced Action Inbound Signup",
        "agent": {
            "prompt": prompt,
            "greeting_message": (
                "Thanks for calling Forced Action. I can send you sample distressed property "
                "leads right now. First — what ZIP code do you want leads in?"
            ),
            "llm": "gpt-4.1-Mini",
            "language": "en-US",
            "voice_id": "eleven_turbo_v2",
        },
        "external_webhook_url": f"{app_base}/webhooks/synthflow/inbound",
        "inbound_call_webhook_url": f"{app_base}/webhooks/synthflow/inbound-call-router",
        "is_recording": False,
    }

    try:
        import requests
        resp = requests.post(
            f"{api_base}/assistants",
            json=payload,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )
        logger.info("POST /assistants -> %s", resp.status_code)
        if resp.status_code not in (200, 201):
            logger.error("Failed to create agent: %s %s", resp.status_code, resp.text[:1000])
            return 1

        agent_data = resp.json()
        # Synthflow v2 response shape: {status, response: {model_id, ...}, details}
        agent_id = agent_data.get("id") or agent_data.get("model_id")
        if not agent_id:
            inner = agent_data.get("response") or agent_data.get("agent") or agent_data.get("assistant") or {}
            agent_id = inner.get("id") or inner.get("model_id")
        if not agent_id:
            logger.error("Could not extract agent ID from response: %s", resp.text[:500])
            return 1

        logger.info("✅ Agent created successfully: id=%s", agent_id)
        _write_env_var(_DOTENV_PATH, "SYNTHFLOW_INBOUND_AGENT", agent_id)

    except Exception as exc:
        logger.error("Exception creating agent: %s", exc)
        return 1

    # ── Step 3: Attach a phone number ───────────────────────────────────────
    logger.info("Assigning a phone number to the agent...")
    try:
        import requests
        # GET /numbers lists available phone numbers
        avail_resp = requests.get(
            f"{api_base}/numbers",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10,
        )
        if avail_resp.status_code == 200:
            avail_data = avail_resp.json()
            numbers = avail_data.get("response", {}).get("phone_numbers", [])
            available = [n for n in numbers if n.get("is_available", False)]
            if available:
                did = available[0]["number"]
                # Assign via PUT /assistants/{agent_id} with phone_number field
                assign_resp = requests.put(
                    f"{api_base}/assistants/{agent_id}",
                    json={"phone_number": did},
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                    },
                    timeout=15,
                )
                if assign_resp.status_code == 200:
                    logger.info("✅ Phone number assigned: %s", did)
                    _write_env_var(_DOTENV_PATH, "SYNTHFLOW_INBOUND_DID", did)
                else:
                    logger.warning(
                        "PUT /assistants/{id} failed: %s %s\n"
                        "  Number %s is available but could not be assigned.",
                        assign_resp.status_code, assign_resp.text[:300], did,
                    )
            else:
                logger.warning("No available numbers in GET /numbers response.")
        else:
            logger.warning(
                "GET /numbers returned %s — cannot auto-assign.\n"
                "  Please assign a DID manually in the Synthflow dashboard\n"
                "  and set SYNTHFLOW_INBOUND_DID in .env.",
                avail_resp.status_code,
            )
    except Exception as exc:
        logger.warning(
            "Exception assigning number: %s\n"
            "  Please assign a DID manually in the Synthflow dashboard\n"
            "  and set SYNTHFLOW_INBOUND_DID in .env.",
            exc,
        )

    logger.info("Done. Agent ID written to SYNTHFLOW_INBOUND_AGENT in .env")
    logger.info("Next steps (per SYNTHFLOW_VOICE_FLOW_PLAN.md):")
    logger.info("  - Set SYNTHFLOW_WEBHOOK_SECRET in .env (shared secret for auth)")
    logger.info("  - Wire up Phase 4: POST /webhooks/synthflow/inbound endpoint")
    return 0


if __name__ == "__main__":
    sys.exit(main())