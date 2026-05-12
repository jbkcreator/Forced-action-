"""
Smoke test for cf_session_manager.ensure_ready against the renamed
Pinellas Clerk profile. Runs validation only — no warming, no scraping.
"""
import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

if "DATABASE_URL" not in os.environ:
    env_path = Path(__file__).resolve().parents[2] / ".env"
    for line in env_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("DATABASE_URL="):
            os.environ["DATABASE_URL"] = line.split("=", 1)[1].strip()
            break

from src.utils.cf_session_manager import ensure_ready, CFBypassFailedError


async def main() -> int:
    try:
        profile_dir = await ensure_ready(
            profile_name="pinellas_clerk",
            county_id="pinellas",
            portal_url="https://officialrecords.mypinellasclerk.gov/search/SearchTypeDocType",
        )
        print(f"\n[+] Profile ready at: {profile_dir}")
        return 0
    except CFBypassFailedError as exc:
        print(f"\n[!] Profile not ready: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
