"""R1 1.1 contact spine config: the merge kinds, the client's three systems, a display-name rank per source."""
from config import lending_contact_spine as cfg


def test_kinds_are_the_five_merge_kinds():
    assert cfg.IDENTIFIER_KINDS == ("phone", "email", "entity", "ghl_contact", "batchdialer_contact")


def test_hubspot_and_salesmsg_are_not_systems():
    assert cfg.SYSTEMS == ("ghl", "batchdialer", "instantly")
    assert not {"hubspot", "salesmsg"} & set(cfg.SYSTEMS + cfg.FUTURE_SYSTEMS)


def test_every_source_has_a_rank():
    assert set(cfg.NAME_RANK) == set(cfg.SOURCE_NAMES)
    assert cfg.NAME_RANK["loan_files"] < cfg.NAME_RANK["dialer_load_records"]


def test_setting_defaults_off():
    from config.settings import AppSettings

    assert AppSettings.model_fields["lending_contact_spine_enabled"].default is False


def test_source_ref_fits_the_longest_dialer_call_id():
    from src.lending.models import LendingCallDisposition, LendingContactSource

    assert LendingContactSource.__table__.c.source_ref.type.length >= LendingCallDisposition.__table__.c.dialer_call_id.type.length
