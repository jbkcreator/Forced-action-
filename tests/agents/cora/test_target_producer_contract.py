from unittest.mock import patch

from src.agents.cora.tools.read_tools import get_contact_channel


def test_get_contact_channel_returns_confidence(fresh_db):
    result = get_contact_channel(fresh_db, buyer_entity_id=-1)  # no such id — the None-path
    assert set(result.keys()) >= {"email", "phone", "contact_confidence"}
    assert result["email"] is None
    assert result["phone"] is None
    assert result["contact_confidence"] is None
