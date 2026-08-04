"""Verify the agent_pnl_monthly cron entry is present and correctly scheduled."""


def test_agent_pnl_monthly_in_crontab():
    with open("scripts/cron/crontab.txt") as f:
        crontab = f.read()
    assert "src.tasks.agent_pnl_monthly" in crontab, \
        "agent_pnl_monthly must have a crontab entry"


def test_agent_pnl_monthly_schedule():
    with open("scripts/cron/crontab.txt") as f:
        for line in f:
            if "agent_pnl_monthly" in line and not line.strip().startswith("#"):
                assert line.startswith("0 8 1 * *"), \
                    f"agent_pnl_monthly must run at 0 8 1 * *, got: {line.strip()}"
                return
    raise AssertionError("No non-comment crontab entry for agent_pnl_monthly")
