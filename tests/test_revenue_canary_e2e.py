from unittest.mock import patch

from sqlalchemy import text

from src.services.revenue_canary import CanaryResult
from src.tasks.revenue_canary_sweep import run_sweep


def _mostly_ok_with_one_failure(failing_name: str):
    names = ["checkout", "payment_link", "entitlement", "delivery", "mail", "model_api"]
    return [
        CanaryResult(name, name != failing_name,
                     "boom" if name == failing_name else "ok", 5)
        for name in names
    ]


def test_full_canary_pipeline_one_failure_alerts_once_then_suppresses(fresh_db):
    # QUALITY-v2.2 Q4 note: run_sweep's alert path commits internally, which
    # the fresh_db fixture's rollback does not undo (this session's finding)
    # — clear this test's own check_names first so a leftover dedup row from
    # an earlier test in the same suite run can't suppress this one.
    fresh_db.execute(text("DELETE FROM revenue_canary_alert_log WHERE check_name IN ('mail', 'model_api')"))
    fresh_db.commit()
    try:
        with patch("src.tasks.revenue_canary_sweep.run_all_checks",
                   return_value=_mostly_ok_with_one_failure("mail")), \
             patch("src.services.email.send_alert", return_value=True) as alert:
            first = run_sweep(fresh_db)

        assert first["alerted"] == ["mail"]
        alert.assert_called_once()

        # second tick, same incident still failing — must be suppressed by the
        # 1h cooldown (revenue_canary_alert_log), not re-alert every 5 minutes.
        with patch("src.tasks.revenue_canary_sweep.run_all_checks",
                   return_value=_mostly_ok_with_one_failure("mail")), \
             patch("src.services.email.send_alert", return_value=True) as alert2:
            second = run_sweep(fresh_db)

        assert second["alerted"] == []
        alert2.assert_not_called()

        # a DIFFERENT check failing must still alert independently — dedup is
        # scoped per check_name, not global.
        with patch("src.tasks.revenue_canary_sweep.run_all_checks",
                   return_value=_mostly_ok_with_one_failure("model_api")), \
             patch("src.services.email.send_alert", return_value=True) as alert3:
            third = run_sweep(fresh_db)

        assert third["alerted"] == ["model_api"]
        alert3.assert_called_once()

        rows = fresh_db.execute(text(
            "SELECT check_name FROM revenue_canary_alert_log "
            "WHERE check_name IN ('mail', 'model_api') ORDER BY check_name"
        )).fetchall()
        assert sorted(r.check_name for r in rows) == ["mail", "model_api"]
    finally:
        # QUALITY-v2.2 Q4 note: run_sweep's alert path commits internally
        # (_record_alerted), which — per this session's finding — the
        # fresh_db fixture's rollback does not undo. Clean up explicitly
        # rather than trust the fixture teardown.
        fresh_db.execute(text("DELETE FROM revenue_canary_alert_log"))
        fresh_db.commit()
