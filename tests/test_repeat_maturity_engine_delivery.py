from datetime import date
from unittest.mock import Mock

import src.services.repeat_maturity_engine as engine


def _alert() -> engine.MonitorAlert:
    return engine.MonitorAlert(
        monitor_type="next_project",
        buyer_entity_id=7,
        canonical_name="Acme Holdings",
        source_event_id=42,
        property_address="123 Main St",
        event_date=date(2026, 9, 15),
        days_since=1,
        total_purchase_count=2,
        total_cash_volume=250_000,
        buyer_type="flipper",
        financing_signal="financed",
    )


def _only_one_alert(monkeypatch, alert):
    monkeypatch.setattr(engine, "_check_loan_maturity", lambda *_: [])
    monkeypatch.setattr(engine, "_check_next_project", lambda *_: [alert])
    monkeypatch.setattr(engine, "_check_dscr_day120", lambda *_: [])
    monkeypatch.setattr(engine, "_check_portfolio_expansion", lambda *_: [])


def test_preview_returns_candidates_without_delivery_or_idempotency(monkeypatch):
    alert = _alert()
    _only_one_alert(monkeypatch, alert)
    post = Mock()
    record = Mock()
    monkeypatch.setattr(engine, "_post_alert", post)
    monkeypatch.setattr(engine, "_record_fired", record)

    alerts = engine.run_monitors(Mock(), today=date(2026, 9, 16), deliver=False)

    assert alerts == [alert]
    post.assert_not_called()
    record.assert_not_called()


def test_failed_delivery_is_not_marked_fired(monkeypatch):
    alert = _alert()
    _only_one_alert(monkeypatch, alert)
    monkeypatch.setattr(engine, "_post_alert", Mock(return_value=None))
    record = Mock()
    monkeypatch.setattr(engine, "_record_fired", record)

    alerts = engine.run_monitors(Mock(), today=date(2026, 9, 16))

    assert alerts == [alert]
    record.assert_not_called()


def test_successful_delivery_is_marked_fired(monkeypatch):
    alert = _alert()
    _only_one_alert(monkeypatch, alert)
    monkeypatch.setattr(engine, "_post_alert", Mock(return_value="123.456"))
    record = Mock()
    monkeypatch.setattr(engine, "_record_fired", record)

    engine.run_monitors(Mock(), today=date(2026, 9, 16))

    record.assert_called_once_with(record.call_args.args[0], alert, "123.456")
