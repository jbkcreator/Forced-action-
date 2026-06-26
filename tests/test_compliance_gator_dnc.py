from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from src.services.compliance_gator import validate_outbound


class _Result:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _Db:
    def __init__(self, *, opt_out=None, dnc_row=None, owner_row=None):
        self.opt_out = opt_out
        self.dnc_row = dnc_row
        self.owner_row = owner_row

    def execute(self, sql, params=None):
        text = str(sql)
        if "FROM sms_opt_outs" in text:
            return _Result(self.opt_out)
        if "FROM dnc_phone_checks" in text:
            return _Result(self.dnc_row)
        if "FROM owners" in text:
            return _Result(self.owner_row)
        return _Result(None)


def _settings():
    return SimpleNamespace(dnc_recheck_days=30)


def test_blocks_when_no_dnc_check_exists():
    with patch("src.services.compliance_gator.get_settings", return_value=_settings()):
        result = validate_outbound("+18135550100", "sms", _Db())

    assert result.allowed is False
    assert result.reason == "dnc_check_required"


def test_blocks_when_dnc_check_is_stale():
    old = datetime.now(timezone.utc) - timedelta(days=31)
    row = {"national_dnc": False, "litigator": False, "checked_at": old}

    with patch("src.services.compliance_gator.get_settings", return_value=_settings()):
        result = validate_outbound("+18135550100", "sms", _Db(dnc_row=row))

    assert result.allowed is False
    assert result.reason == "dnc_check_required"


def test_blocks_positive_dnc_check_even_without_sms_opt_out_row():
    row = {"national_dnc": True, "litigator": False, "checked_at": datetime.now(timezone.utc)}

    with patch("src.services.compliance_gator.get_settings", return_value=_settings()):
        result = validate_outbound("+18135550100", "sms", _Db(dnc_row=row))

    assert result.allowed is False
    assert result.reason == "dnc_or_opted_out"
