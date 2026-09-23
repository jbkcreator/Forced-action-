"""Stage B — detail page parser: unit tests against saved Hillsborough fixture."""
from pathlib import Path
import pytest
from src.scrappers.permit.detail_parse import parse_permit_detail, PermitDetail

FIXTURE = Path(__file__).parent / "fixtures" / "hillsborough_permit_detail.html"


@pytest.fixture(scope="module")
def hillsborough_html() -> str:
    return FIXTURE.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def detail(hillsborough_html) -> PermitDetail:
    return parse_permit_detail(hillsborough_html)


class TestParseContractorFields:
    def test_contractor_name_extracted(self, detail):
        assert detail.licensed_professional_name is not None
        assert "KEVIN WELLS" in detail.licensed_professional_name.upper()

    def test_contractor_license_number(self, detail):
        assert detail.contractor_license == "CFC055692"

    def test_contractor_license_type(self, detail):
        assert detail.contractor_license_type is not None
        assert "PLUMBING" in detail.contractor_license_type.upper()

    def test_contractor_phone(self, detail):
        # Mobile from Licensed Professional section
        assert detail.contractor_phone is not None
        assert "8134774447" in detail.contractor_phone.replace("-", "").replace(" ", "")

    def test_contractor_email(self, detail):
        assert detail.contractor_email is not None
        assert "REVKEVWELLS@GMAIL.COM" in detail.contractor_email.upper()


class TestParseApplicantOwner:
    def test_applicant_name(self, detail):
        assert detail.applicant_name is not None
        assert "KEVIN WELLS" in detail.applicant_name.upper()

    def test_owner_name(self, detail):
        assert detail.owner_name is not None
        assert "DUNNING" in detail.owner_name.upper()


class TestParseMetaFields:
    def test_completion_status(self, detail):
        assert detail.completion_status is not None
        assert "COMPLETE" in detail.completion_status.upper()

    def test_project_description_present(self, detail):
        assert detail.project_description is not None
        assert len(detail.project_description) > 10


class TestMissingFields:
    def test_no_licensed_professional_returns_none(self):
        minimal_html = "<html><body><div>Some permit with no contractor info</div></body></html>"
        result = parse_permit_detail(minimal_html)
        assert result.licensed_professional_name is None
        assert result.contractor_license is None
        assert result.contractor_license_type is None
        assert result.contractor_phone is None
        assert result.contractor_email is None

    def test_returns_permit_detail_dataclass(self, detail):
        assert isinstance(detail, PermitDetail)
