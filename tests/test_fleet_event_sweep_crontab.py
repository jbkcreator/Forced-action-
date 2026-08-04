from pathlib import Path

_CRONTAB = Path(__file__).parent.parent / "scripts" / "cron" / "crontab.txt"


def _read_crontab() -> str:
    return _CRONTAB.read_text(encoding="utf-8")


def test_crontab_fleet_event_sweep_present():
    """fleet_event_sweep is scheduled in crontab.txt."""
    content = _read_crontab()
    assert "src.tasks.fleet_event_sweep" in content


def test_crontab_fleet_event_sweep_every_two_minutes():
    """Runs on the 0-59/2 cadence, staggered off outcome_dispatch_sweep's 1-59/2."""
    lines = _read_crontab().splitlines()
    sweep_line = next(ln for ln in lines if "src.tasks.fleet_event_sweep" in ln)
    assert sweep_line.strip().startswith("0-59/2 * * * *")
