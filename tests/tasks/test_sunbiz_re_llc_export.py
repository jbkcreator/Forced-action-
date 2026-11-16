"""P3.2: the daily real estate LLC CSV and its cron entry point. Made-up records, fake SFTP / Drive; no network."""
from __future__ import annotations

import csv
import io
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from config.sunbiz_re_llc import SFTP_HOST_KEY
from src.tasks import sunbiz_re_llc_export as mod
from tests.scrappers.test_sunbiz_daily_filings import _record

DAY = date(2026, 11, 15)


class FakeSource:
    def __init__(self, data: bytes | None = None, error: Exception | None = None):
        self.data, self.error, self.days = data, error, []

    def fetch(self, day):
        self.days.append(day)
        if self.error:
            raise self.error
        return self.data


class FakeUpload:
    def __init__(self, existing: bool = False, error: Exception | None = None):
        self.calls, self.existing, self.error, self.checked = [], existing, error, []

    def __call__(self, parent_id, subfolder, name, data):
        if self.error:
            raise self.error
        self.calls.append((parent_id, subfolder, name, data))

    def has_file(self, parent_id, subfolder, name):
        self.checked.append((parent_id, subfolder, name))
        return self.existing


def _file(*lines: str) -> bytes:
    return "\n".join(lines).encode("latin-1")


def _rows(data: bytes) -> list[dict]:
    return list(csv.DictReader(io.StringIO(data.decode("utf-8"))))


# ── export_day ────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("now,expected", [
    (datetime(2026, 11, 16, 16, 0, tzinfo=timezone.utc), date(2026, 11, 15)),   # 11:00 ET run
    (datetime(2026, 11, 16, 3, 0, tzinfo=timezone.utc), date(2026, 11, 14)),    # still Nov 15 in ET
    (datetime(2026, 3, 9, 15, 0, tzinfo=timezone.utc), date(2026, 3, 8)),       # day after the DST change
])
def test_export_day_is_yesterday_in_eastern_time(now, expected):
    assert mod.export_day(now) == expected


# ── CSV ───────────────────────────────────────────────────────────────────────

def test_csv_columns_and_no_tax_number():
    assert len(mod.COLUMNS) == 24 + 6 * 7
    assert len(set(mod.COLUMNS)) == len(mod.COLUMNS)
    assert not [c for c in mod.COLUMNS if "fei" in c.lower() or "tax" in c.lower()]
    assert mod.build_csv([]).decode("utf-8") == ",".join(mod.COLUMNS) + "\n"


def test_csv_row_holds_the_filing_and_blank_officer_slots():
    from src.scrappers.sunbiz.daily_filings import parse_record

    filing = parse_record(_record(name="SAMPLE, HOMES LLC",
                                  officers=({"title": "MGR", "name": "DOE JANE", "state": "FL"},)))
    rows = _rows(mod.build_csv([(filing, "HOMES")]))
    assert len(rows) == 1
    row = rows[0]
    assert row["entity_name"] == "SAMPLE, HOMES LLC"                # comma quoted, not split
    assert row["matched_keyword"] == "HOMES" and row["file_date"] == "2026-11-14"
    assert (row["officer_1_title"], row["officer_1_name"], row["officer_1_state"]) == ("MGR", "DOE JANE", "FL")
    assert all(row[f"officer_{n}_name"] == "" for n in range(2, 7))


# ── run ───────────────────────────────────────────────────────────────────────

def test_run_uploads_the_matches_into_the_days_subfolder():
    source = FakeSource(_file(_record(document_number="A1", name="ONE HOMES LLC"),
                              _record(document_number="A2", name="TWO PROPERTIES LLC"),
                              _record(document_number="A3", name="THREE REI LLC", status="I")))
    upload = FakeUpload()
    result = mod.run(DAY, source=source, folder_id="FOLDER", upload=upload)
    assert source.days == [DAY]
    assert (result.state_file, result.filings, result.rejected, result.matched) == (True, 3, 0, 1)
    assert result.uploaded == "sunbiz_re_llcs_2026-11-15.csv"
    [(parent, sub, name, data)] = upload.calls
    assert (parent, sub, name) == ("FOLDER", "2026-11-15", "sunbiz_re_llcs_2026-11-15.csv")
    assert [r["document_number"] for r in _rows(data)] == ["A1"]


def test_run_uploads_a_header_only_file_when_nothing_matched():
    upload = FakeUpload()
    result = mod.run(DAY, source=FakeSource(_file(_record(name="PLAIN LLC"))), folder_id="F", upload=upload)
    assert result.matched == 0 and _rows(upload.calls[0][3]) == []


def test_no_state_file_uploads_nothing():
    upload = FakeUpload()
    result = mod.run(DAY, source=FakeSource(None), folder_id="F", upload=upload)
    assert not result.state_file and result.uploaded is None and upload.calls == []


def test_dry_run_counts_and_uploads_nothing():
    upload = FakeUpload()
    result = mod.run(DAY, source=FakeSource(_file(_record())), folder_id="", upload=upload, dry_run=True)
    assert result.matched == 1 and result.uploaded is None and upload.calls == []


def test_some_bad_lines_are_skipped_and_the_rest_exported():
    upload = FakeUpload()
    result = mod.run(DAY, source=FakeSource(_file(_record(), "garbage")), folder_id="F", upload=upload)
    assert (result.rejected, result.matched) == (1, 1) and len(upload.calls) == 1


def test_a_file_with_no_readable_record_raises_instead_of_uploading_an_empty_csv():
    upload = FakeUpload()
    with pytest.raises(ValueError):
        mod.run(DAY, source=FakeSource(_file("garbage", "more garbage")), folder_id="F", upload=upload)
    assert upload.calls == []


@pytest.mark.parametrize("folder_id,upload", [("", FakeUpload()), ("F", None)])
def test_a_real_run_without_a_folder_or_uploader_raises(folder_id, upload):
    with pytest.raises(ValueError):
        mod.run(DAY, source=FakeSource(_file(_record())), folder_id=folder_id, upload=upload)


# ── main (cron entry point) ──────────────────────────────────────────────────

def _settings(**over):
    values = dict(sunbiz_re_llc_export_enabled=True, lending_sunbiz_llc_export_folder_id="FOLDER",
                  sunbiz_sftp_host="host.example", sunbiz_sftp_host_key="", sunbiz_sftp_username="user",
                  sunbiz_sftp_password=SecretStr("secret-pw"))
    values.update(over)
    return SimpleNamespace(**values)


@pytest.fixture
def wired(monkeypatch):
    """main() with fake settings, SFTP, Drive and ops notice."""
    import config.settings
    import src.lending.drive_upload
    import src.scrappers.sunbiz.daily_filings

    state = SimpleNamespace(settings=_settings(), source=FakeSource(_file(_record())), upload=FakeUpload(),
                            notices=[], sftp_args=[])

    def sftp(host, username, password, host_key):
        state.sftp_args.append((host, username, password, host_key))
        return state.source

    monkeypatch.setattr(config.settings, "get_settings", lambda: state.settings)
    monkeypatch.setattr(src.lending.drive_upload, "DriveUploader", lambda: state.upload)
    monkeypatch.setattr(src.scrappers.sunbiz.daily_filings, "SunbizSftp", sftp)
    monkeypatch.setattr(mod, "_notify", state.notices.append)
    return state


def test_main_exports_the_given_day(wired):
    assert mod.main(["--day", "2026-11-15"]) == 0
    assert wired.sftp_args == [("host.example", "user", "secret-pw", SFTP_HOST_KEY)]
    assert [c[:3] for c in wired.upload.calls] == [("FOLDER", "2026-11-15", "sunbiz_re_llcs_2026-11-15.csv")]
    assert wired.notices == []


@pytest.mark.parametrize("over", [{"sunbiz_re_llc_export_enabled": False},
                                  {"lending_sunbiz_llc_export_folder_id": ""}])
def test_main_does_nothing_while_off(wired, over):
    wired.settings = _settings(**over)
    assert mod.main(["--day", "2026-11-15"]) == 0
    assert wired.source.days == [] and wired.upload.calls == []


def test_main_dry_run_works_while_off_and_uploads_nothing(wired):
    wired.settings = _settings(sunbiz_re_llc_export_enabled=False, lending_sunbiz_llc_export_folder_id="")
    assert mod.main(["--day", "2026-11-15", "--dry-run"]) == 0
    assert wired.source.days == [DAY] and wired.upload.calls == []


@pytest.mark.parametrize("retry,notices,code", [(False, 0, 1), (True, 1, 0)])
def test_main_without_the_sftp_login_fails_and_alerts_only_on_the_retry_run(wired, retry, notices, code):
    """The retry run exits 0 after alerting: a non-zero exit would make run.sh retry and alert again."""
    wired.settings = _settings(sunbiz_sftp_password=None)
    assert mod.main(["--day", "2026-11-15"] + (["--retry"] if retry else [])) == code
    assert wired.source.days == [] and len(wired.notices) == notices


def test_retry_run_skips_a_day_already_in_drive(wired):
    wired.upload.existing = True
    assert mod.main(["--day", "2026-11-15", "--retry"]) == 0
    assert wired.upload.checked == [("FOLDER", "2026-11-15", "sunbiz_re_llcs_2026-11-15.csv")]
    assert wired.source.days == [] and wired.upload.calls == [] and wired.notices == []


def test_first_run_failure_exits_1_without_alerting(wired):
    wired.source.error = ConnectionError("sftp down at host.example")
    assert mod.main(["--day", "2026-11-15"]) == 1
    assert wired.notices == []


def test_retry_run_failure_alerts_once_without_error_details_and_exits_0(wired, caplog):
    wired.upload.error = RuntimeError("drive said no for folder FOLDER, password secret-pw")
    assert mod.main(["--day", "2026-11-15", "--retry"]) == 0     # 0: run.sh must not retry and alert again
    assert len(wired.notices) == 1
    assert "2026-11-15" in wired.notices[0] and "RuntimeError" in wired.notices[0]
    for text in wired.notices + [caplog.text]:
        assert "secret-pw" not in text and "FOLDER" not in text


def test_a_host_key_setting_overrides_the_pinned_one(wired):
    wired.settings = _settings(sunbiz_sftp_host_key="ssh-ed25519 NEWKEY")
    assert mod.main(["--day", "2026-11-15"]) == 0
    assert wired.sftp_args[0][3] == "ssh-ed25519 NEWKEY"


@pytest.mark.parametrize("retry,notices", [(False, 0), (True, 1)])
def test_a_missing_state_file_is_flagged_on_the_retry_run_only(wired, retry, notices):
    wired.source.data = None
    assert mod.main(["--day", "2026-11-15"] + (["--retry"] if retry else [])) == 0
    assert wired.upload.calls == [] and len(wired.notices) == notices
    if notices:
        assert "2026-11-15" in wired.notices[0]


def test_warnings_reach_the_cron_log(wired, monkeypatch):
    """run.sh keeps only lines matching " - (WARNING|ERROR|CRITICAL) - "; main's log format must produce them."""
    import logging
    import re

    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", [])
    stream = io.StringIO()
    monkeypatch.setattr(logging, "basicConfig", lambda **kw: root.addHandler(
        _handler(stream, kw["format"])))
    wired.source.error = ConnectionError("down")
    mod.main(["--day", "2026-11-15"])
    root.handlers = []
    assert any(re.search(r" - (WARNING|ERROR|CRITICAL) - ", ln) for ln in stream.getvalue().splitlines())


def _handler(stream, fmt):
    import logging

    h = logging.StreamHandler(stream)
    h.setFormatter(logging.Formatter(fmt))
    return h


def test_main_defaults_to_yesterday_eastern(wired, monkeypatch):
    monkeypatch.setattr(mod, "export_day", lambda now: date(2026, 11, 14))
    assert mod.main([]) == 0
    assert wired.source.days == [date(2026, 11, 14)]


# ── wiring ────────────────────────────────────────────────────────────────────

def test_export_is_off_by_default():
    from config.settings import AppSettings

    assert AppSettings.model_fields["sunbiz_re_llc_export_enabled"].default is False
    assert not AppSettings.model_fields["lending_sunbiz_llc_export_folder_id"].default


def test_cron_runs_the_export_and_the_retry():
    crontab = (Path(__file__).resolve().parents[2] / "scripts" / "cron" / "crontab.txt").read_text(encoding="utf-8")
    lines = [ln.strip() for ln in crontab.splitlines() if "sunbiz_re_llc_export" in ln and not ln.lstrip().startswith("#")]
    assert lines == ["0 11 * * * $PROJECT/scripts/cron/run.sh src.tasks.sunbiz_re_llc_export",
                     "30 13 * * * $PROJECT/scripts/cron/run.sh src.tasks.sunbiz_re_llc_export --retry"]
