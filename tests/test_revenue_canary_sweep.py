from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from sqlalchemy import text

from src.services.revenue_canary import CanaryResult
from src.tasks.revenue_canary_sweep import run_sweep


def _all_ok():
    return [
        CanaryResult("checkout", True, "ok", 10),
        CanaryResult("payment_link", True, "ok", 10),
        CanaryResult("entitlement", True, "ok", 10),
        CanaryResult("delivery", True, "ok", 10),
        CanaryResult("mail", True, "ok", 10),
        CanaryResult("model_api", True, "ok", 10),
    ]


def test_run_sweep_all_ok_alerts_nothing(fresh_db):
    with patch("src.tasks.revenue_canary_sweep.run_all_checks", return_value=_all_ok()):
        result = run_sweep(fresh_db)
    assert result["alerted"] == []
    assert all(r["ok"] for r in result["results"])


def test_run_sweep_failure_sends_alert_and_records_dedup_row(fresh_db):
    failing = _all_ok()
    failing[0] = CanaryResult("checkout", False, "boom", 10)
    with patch("src.tasks.revenue_canary_sweep.run_all_checks", return_value=failing), \
         patch("src.services.email.send_alert", return_value=True) as alert:
        result = run_sweep(fresh_db)
    assert result["alerted"] == ["checkout"]
    alert.assert_called_once()
    row = fresh_db.execute(text(
        "SELECT check_name FROM revenue_canary_alert_log WHERE check_name = 'checkout'"
    )).fetchone()
    assert row is not None


def test_run_sweep_respects_cooldown(fresh_db):
    fresh_db.execute(text(
        "INSERT INTO revenue_canary_alert_log (check_name, alerted_at) VALUES ('checkout', NOW())"
    ))
    fresh_db.commit()
    failing = _all_ok()
    failing[0] = CanaryResult("checkout", False, "boom again", 10)
    with patch("src.tasks.revenue_canary_sweep.run_all_checks", return_value=failing), \
         patch("src.services.email.send_alert", return_value=True) as alert:
        result = run_sweep(fresh_db)
    assert result["alerted"] == []
    alert.assert_not_called()


def test_run_sweep_dry_run_never_alerts(fresh_db):
    failing = _all_ok()
    failing[0] = CanaryResult("checkout", False, "boom", 10)
    with patch("src.tasks.revenue_canary_sweep.run_all_checks", return_value=failing), \
         patch("src.services.email.send_alert") as alert:
        result = run_sweep(fresh_db, dry_run=True)
    assert result["alerted"] == []
    alert.assert_not_called()


def test_run_sweep_passes_kill_through_to_run_all_checks(fresh_db):
    with patch("src.tasks.revenue_canary_sweep.run_all_checks", return_value=_all_ok()) as checks:
        run_sweep(fresh_db, kill="mail")
    assert checks.call_args.kwargs["kill"] == "mail"
