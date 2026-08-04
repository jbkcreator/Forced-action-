from pathlib import Path

_CRONTAB = Path(__file__).parent.parent / "scripts" / "cron" / "crontab.txt"


def _read_crontab() -> str:
    return _CRONTAB.read_text(encoding="utf-8")


def test_crontab_revenue_canary_sweep_present():
    assert "src.tasks.revenue_canary_sweep" in _read_crontab()


def test_crontab_revenue_canary_sweep_every_five_minutes():
    lines = _read_crontab().splitlines()
    sweep_line = next(ln for ln in lines if "src.tasks.revenue_canary_sweep" in ln)
    assert sweep_line.strip().startswith("*/5 * * * *"), (
        f"expected every-5-minutes cadence (decision E1), got: {sweep_line}"
    )
