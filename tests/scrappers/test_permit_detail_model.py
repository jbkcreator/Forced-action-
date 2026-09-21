"""Stage C — model extension: verify BuildingPermit exposes detail fields."""
from src.core.models import BuildingPermit

DETAIL_FIELDS = [
    "contractor_name",
    "holder_name",
    "job_value",
    "completion_status",
    "contractor_license",
    "contractor_license_type",
    "contractor_phone",
    "contractor_email",
    "applicant_name",
    "owner_name",
]


def test_building_permit_has_all_detail_fields():
    mapper = BuildingPermit.__mapper__
    col_names = {c.key for c in mapper.columns}
    missing = [f for f in DETAIL_FIELDS if f not in col_names]
    assert not missing, f"BuildingPermit missing columns: {missing}"


def test_detail_fields_are_nullable():
    mapper = BuildingPermit.__mapper__
    for field in DETAIL_FIELDS:
        col = mapper.columns[field]
        assert col.nullable, f"{field} should be nullable"


def test_migration_script_importable():
    import importlib
    mod = importlib.import_module("migrations.apply_permit_detail_fields")
    assert callable(getattr(mod, "main", None))
