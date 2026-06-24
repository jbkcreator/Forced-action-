"""
Set a kill switch override via Redis.

Usage:
    PYTHONPATH=. .venv/Scripts/python.exe scripts/set_kill_switch.py <feature> <color> [ttl_seconds]

Examples:
    python scripts/set_kill_switch.py retention_30d green
    python scripts/set_kill_switch.py retention_30d green 7200
    python scripts/set_kill_switch.py retention_30d red
"""

import sys
from src.core.redis_client import rset, rget, rttl

VALID_COLORS = {"green", "yellow", "red"}
DEFAULT_TTL = 3600


def main() -> None:
    if len(sys.argv) < 3 or sys.argv[2] not in VALID_COLORS:
        print(__doc__)
        sys.exit(1)

    feature = sys.argv[1]
    color = sys.argv[2]
    ttl = int(sys.argv[3]) if len(sys.argv) > 3 else DEFAULT_TTL

    key = f"kill_switch_override:{feature}"
    rset(key, color, ttl_seconds=ttl)

    current = rget(key)
    remaining = rttl(key)
    print(f"\n  kill_switch_override:{feature} = {current}  (expires in {remaining}s)\n")


if __name__ == "__main__":
    main()
