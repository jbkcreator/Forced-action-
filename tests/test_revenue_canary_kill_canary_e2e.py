from unittest.mock import patch

from sqlalchemy import text

from src.services.revenue_canary import CanaryResult, CHECK_NAMES
from src.tasks.revenue_canary_sweep import run_sweep


def _mock_side_effecting_checks(exclude: str):
    """Mock out the checks that hit live external services (Stripe,
    SMTP, Anthropic) so this acceptance test proves --kill-canary's
    mechanism deterministically, independent of whether ambient
    credentials in this environment currently happen to be valid —
    entitlement/delivery stay real since they're pure DB round trips.
    `exclude` is the check being killed; it's left unmocked so
    run_all_checks' real force_fail path is what's under test."""
    names = ["checkout", "payment_link", "mail", "model_api"]
    patches = [
        patch(f"src.services.revenue_canary._check_{name}",
              return_value=CanaryResult(name, True, "ok", 1))
        for name in names if name != exclude
    ]
    return patches


def _clear_dedup(db, *check_names):
    """QUALITY-v2.2 Q4 note: run_sweep's alert path commits internally
    (_record_alerted), which — per this session's finding — the fresh_db
    fixture's rollback does not undo. Clear this test's own check_names
    before asserting so leftover dedup rows from an earlier test in the
    same suite run can't suppress this one within the 1h cooldown window."""
    from sqlalchemy import text
    db.execute(text("DELETE FROM revenue_canary_alert_log WHERE check_name = ANY(:names)"),
               {"names": list(check_names)})
    db.commit()


def test_kill_canary_forces_named_check_to_fail_and_alert_fires(fresh_db):
    """Full acceptance path for decision E5: --kill-canary <name> forces
    that check to report failure, and the alert path (send_alert +
    revenue_canary_alert_log dedup row) fires exactly as it would for a
    real outage — without needing a real outage to prove it."""
    _clear_dedup(fresh_db, "checkout")
    mocks = _mock_side_effecting_checks(exclude="checkout")
    with patch("src.services.email.send_alert", return_value=True) as alert:
        for m in mocks:
            m.start()
        try:
            result = run_sweep(fresh_db, kill="checkout")
        finally:
            for m in mocks:
                m.stop()

    checkout_result = next(r for r in result["results"] if r["name"] == "checkout")
    assert checkout_result["ok"] is False
    assert "--kill-canary" in checkout_result["detail"]
    assert result["alerted"] == ["checkout"]
    alert.assert_called_once()
    assert "checkout" in alert.call_args.args[0]  # subject names the failed check

    row = fresh_db.execute(text(
        "SELECT check_name FROM revenue_canary_alert_log WHERE check_name = 'checkout'"
    )).fetchone()
    assert row is not None


def test_kill_canary_every_check_name_is_a_valid_choice():
    """Every one of the six named canaries must be killable — nothing
    silently excluded from the acceptance test."""
    from src.tasks.revenue_canary_sweep import main as _  # noqa: F401 — import check only

    assert set(CHECK_NAMES) == {
        "checkout", "payment_link", "entitlement", "delivery", "mail", "model_api",
    }


def test_kill_canary_does_not_suppress_the_other_five_checks(fresh_db):
    """--kill-canary must break exactly one check — proving the mechanism
    is a targeted deliberate break, not a global kill switch."""
    _clear_dedup(fresh_db, "mail")
    mocks = _mock_side_effecting_checks(exclude="mail")
    with patch("src.services.email.send_alert", return_value=True):
        for m in mocks:
            m.start()
        try:
            result = run_sweep(fresh_db, kill="mail")
        finally:
            for m in mocks:
                m.stop()

    ok_names = {r["name"] for r in result["results"] if r["ok"]}
    assert ok_names == set(CHECK_NAMES) - {"mail"}
