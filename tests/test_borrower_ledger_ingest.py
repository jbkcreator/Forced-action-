from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

from src.services import borrower_ledger_ingest as ingest


class _Mappings:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


def test_incremental_ingest_materializes_acquisition_sale_and_permit_transitions(monkeypatch):
    rows = [
        SimpleNamespace(buyer_entity_id=1, event_type="deed_acquisition", event_date=date(2026, 9, 15), source_table="deeds", source_id=10, property_id=100, summary="Acquired", amount=100_000, meta_json=None),
        SimpleNamespace(buyer_entity_id=2, event_type="deed_sale", event_date=date(2026, 9, 15), source_table="deeds", source_id=10, property_id=100, summary="Sold", amount=100_000, meta_json=None),
        SimpleNamespace(buyer_entity_id=1, event_type="permit_filed", event_date=date(2026, 9, 1), source_table="building_permits", source_id=20, property_id=100, summary="Permit filed", amount=None, meta_json=None),
        SimpleNamespace(buyer_entity_id=1, event_type="permit_closed", event_date=date(2026, 9, 16), source_table="building_permits", source_id=20, property_id=100, summary="Permit closed", amount=None, meta_json=None),
    ]
    session = Mock()
    session.execute.return_value = _Mappings(rows)
    record = Mock(return_value=True)
    monkeypatch.setattr(ingest, "record_event", record)

    stats = ingest.sync_borrower_ledger(session, as_of=date(2026, 9, 16))

    assert stats.inserted == 4
    assert [call.kwargs["event_type"] for call in record.call_args_list] == [
        "deed_acquisition", "deed_sale", "permit_filed", "permit_closed",
    ]
    assert record.call_args_list[0].kwargs["source_id"] == record.call_args_list[1].kwargs["source_id"]


def test_incremental_ingest_is_idempotent(monkeypatch):
    row = SimpleNamespace(buyer_entity_id=1, event_type="deed_sale", event_date=date(2026, 9, 15), source_table="deeds", source_id=10, property_id=100, summary="Sold", amount=100_000, meta_json=None)
    session = Mock()
    session.execute.return_value = _Mappings([row])
    monkeypatch.setattr(ingest, "record_event", Mock(return_value=False))

    stats = ingest.sync_borrower_ledger(session, as_of=date(2026, 9, 16))

    assert stats.inserted == 0
    assert stats.skipped == 1
