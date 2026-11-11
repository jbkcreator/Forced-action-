"""Nightly ledger export (Cora brief P1.2): the CSV shape, the no-PII rule and the Drive hand-off. No database: the
row builders take plain dicts, and the uploader is a fake."""
from __future__ import annotations

import csv
import io
from datetime import date, datetime, timezone

import pytest

from src.lending import ledger_export as le

DAY = date(2031, 1, 6)
AT = datetime(2031, 1, 6, 15, 0, tzinfo=timezone.utc)


def keys(**by_phone):
    return {phone: le.ContactKey(contact_id=cid, ghl_contact_id=ghl) for phone, (cid, ghl) in by_phone.items()}


def parse(data: bytes) -> list[dict]:
    return list(csv.DictReader(io.StringIO(data.decode("utf-8"))))


# ── contact key ─────────────────────────────────────────────────────────────

def test_key_columns_carry_ghl_id_contact_id_and_last4_only():
    k = le.ContactKey(contact_id=7, ghl_contact_id="g7")
    assert le.key_columns("+18135550123", {"+18135550123": k}) == {
        "ghl_contact_id": "g7", "contact_id": 7, "phone_last4": "0123"}


def test_an_unknown_phone_still_gets_its_last4_and_blank_ids():
    assert le.key_columns("+18135550123", {}) == {"ghl_contact_id": "", "contact_id": "", "phone_last4": "0123"}


def test_a_missing_phone_gives_all_blank():
    assert le.key_columns(None, {}) == {"ghl_contact_id": "", "contact_id": "", "phone_last4": ""}


# ── tables ──────────────────────────────────────────────────────────────────

def test_every_table_is_written_even_when_empty():
    files = le.build_files(le.LedgerData())
    assert set(files) == {f"{name}.csv" for name in le.TABLES}
    for data in files.values():
        assert parse(data) == []
        assert data.decode().splitlines()[0].startswith("ghl_contact_id,contact_id,phone_last4")


def test_no_full_phone_or_email_appears_in_any_file():
    phone, email = "+18135550123", "person@example.com"
    data = le.LedgerData(
        keys=keys(**{phone: (1, "g1")}),
        contacts=[{"phone": phone, "do_not_contact": False, "nurture": False, "line_type": "mobile",
                   "last_dnc_scrub": AT, "created_at": AT}],
        consents=[{"phone": phone, "kind": "text_consent", "detail": "web_form", "at": AT, "revoked_at": None}],
        touches=[{"phone": phone, "at": AT, "kind": "call", "direction": "outbound", "outcome": "NO_ANSWER"}],
        dispositions=[{"phone": phone, "dialer_call_id": "c1", "direction": "outbound", "disposition": "BOOKED",
                       "disposition_raw": "Booked", "talk_duration_sec": 60, "call_source": "batchdialer",
                       "queue": "q", "call_started_at": AT}],
        outcomes=[{"phone": phone, "at": AT, "kind": "stage", "outcome": "Funded", "ref": "opp1"}],
        scores=[{"phone": phone, "lead_source": "web", "lead_id": 3, "borrower_score": 4,
                 "missing": ["loan_amount"], "received_at": AT}],
    )
    blob = b"".join(le.build_files(data).values()).decode()
    assert "8135550123" not in blob and email not in blob and "0123" in blob


def test_score_row_lists_missing_inputs():
    data = le.LedgerData(scores=[{"phone": "+18135550123", "lead_source": "lendingflow", "lead_id": 9,
                                  "borrower_score": 2, "missing": ["contract", "experience"], "received_at": AT}])
    row = parse(le.build_files(data)["scores.csv"])[0]
    assert row["borrower_score"] == "2" and row["missing"] == "contract;experience"


# ── window ──────────────────────────────────────────────────────────────────

def test_window_is_the_et_calendar_day():
    start, end = le.day_window(DAY)
    assert start == datetime(2031, 1, 6, 5, 0, tzinfo=timezone.utc)
    assert end == datetime(2031, 1, 7, 5, 0, tzinfo=timezone.utc)


def test_default_day_is_yesterday_in_et():
    now = datetime(2031, 1, 7, 5, 30, tzinfo=timezone.utc)  # 00:30 ET on the 7th
    assert le.export_day(now) == DAY


# ── upload ──────────────────────────────────────────────────────────────────

class FakeUploader:
    def __init__(self):
        self.calls = []

    def __call__(self, folder_id, subfolder, name, data):
        self.calls.append((folder_id, subfolder, name, data))


def test_publish_uploads_each_file_into_the_dated_folder():
    up = FakeUploader()
    names = le.publish({"a.csv": b"x", "b.csv": b"y"}, day=DAY, folder_id="F", upload=up)
    assert names == ["a.csv", "b.csv"]
    assert [(c[0], c[1], c[2]) for c in up.calls] == [("F", "2031-01-06", "a.csv"), ("F", "2031-01-06", "b.csv")]


def test_publish_needs_a_folder():
    with pytest.raises(ValueError):
        le.publish({"a.csv": b"x"}, day=DAY, folder_id="", upload=FakeUploader())


# ── Drive uploader (fake Drive API) ─────────────────────────────────────────

class _Req:
    def __init__(self, value):
        self.value = value

    def execute(self):
        return self.value


class FakeFiles:
    """Just enough of drive.files(): list by parent+name+kind, create, update, get_media."""

    def __init__(self):
        self.items = {}  # id -> dict(name, parent, folder, data)
        self.updates = 0

    def _match(self, q):
        import re
        parent = re.search(r"'([^']+)' in parents", q).group(1)
        name = re.search(r"name = '([^']+)'", q)
        folder = "mimeType = '" in q
        return [(i, f) for i, f in self.items.items() if f["parent"] == parent and f["folder"] == folder
                and (not name or f["name"] == name.group(1))]

    def list(self, q, orderBy=None, **_):
        found = self._match(q)
        if orderBy == "name desc":
            found.sort(key=lambda x: x[1]["name"], reverse=True)
        return _Req({"files": [{"id": i, "name": f["name"]} for i, f in found]})

    def create(self, body, media_body=None, **_):
        new_id = f"id{len(self.items)}"
        self.items[new_id] = {"name": body["name"], "parent": body["parents"][0],
                              "folder": media_body is None, "data": media_body and media_body.getbytes(0, 10**6)}
        return _Req({"id": new_id})

    def update(self, fileId, media_body, **_):
        self.updates += 1
        self.items[fileId]["data"] = media_body.getbytes(0, 10**6)
        return _Req({"id": fileId})

    def get_media(self, fileId, **_):
        return _Req(self.items[fileId]["data"])


class FakeDrive:
    def __init__(self):
        self.f = FakeFiles()

    def files(self):
        return self.f


def test_uploader_creates_the_dated_folder_once_and_replaces_a_rerun():
    from src.lending.drive_upload import DriveUploader

    drive = FakeDrive()
    up = DriveUploader(drive)
    up("ROOT", "2031-01-06", "a.csv", b"1")
    up("ROOT", "2031-01-06", "b.csv", b"2")
    DriveUploader(drive)("ROOT", "2031-01-06", "a.csv", b"3")
    folders = [f for f in drive.f.items.values() if f["folder"]]
    files = {f["name"]: f["data"] for f in drive.f.items.values() if not f["folder"]}
    assert len(folders) == 1 and files == {"a.csv": b"3", "b.csv": b"2"} and drive.f.updates == 1
    assert up.has_file("ROOT", "2031-01-06", "b.csv") and not up.has_file("ROOT", "2031-01-07", "b.csv")


def test_latest_manifest_reads_the_newest_folder():
    from src.lending.drive_upload import DriveUploader, latest_manifest

    drive = FakeDrive()
    up = DriveUploader(drive)
    up("ROOT", "2031-01-05", "manifest.json", b'{"day": "2031-01-05"}')
    up("ROOT", "2031-01-06", "manifest.json", b'{"day": "2031-01-06"}')
    up("ROOT", "2031-01-07", "contacts.csv", b"partial")  # no manifest: incomplete, skipped
    assert latest_manifest("ROOT", service=drive)["day"] == "2031-01-06"
    assert latest_manifest("EMPTY", service=drive) is None


# ── task ────────────────────────────────────────────────────────────────────

def test_run_uploads_every_table_then_the_manifest_last(monkeypatch):
    import json
    from src.tasks import lending_ledger_export as task

    monkeypatch.setattr(le, "collect", lambda db, day: le.LedgerData(
        touches=[{"phone": "+18135550123", "at": AT, "kind": "call"}]))
    up = FakeUploader()
    counts = task.run(None, day=DAY, folder_id="F", upload=up, now=AT)
    names = [c[2] for c in up.calls]
    assert names[-1] == "manifest.json" and len(names) == len(le.TABLES) + 1
    assert counts["touches.csv"] == 1
    assert json.loads(up.calls[-1][3])["rows"]["touches.csv"] == 1


def test_dry_run_uploads_nothing(monkeypatch):
    from src.tasks import lending_ledger_export as task

    monkeypatch.setattr(le, "collect", lambda db, day: le.LedgerData())
    up = FakeUploader()
    task.run(None, day=DAY, folder_id="F", upload=up, now=AT, dry_run=True)
    assert up.calls == []


# ── Cora tool ───────────────────────────────────────────────────────────────

def _toolkit(manifest):
    from src.lending.cora_tools import CoraToolkit
    return CoraToolkit(None, None, ledger_manifest=manifest)


def test_cora_tool_reports_the_latest_export():
    out = _toolkit(lambda: {"folder": "2031-01-06", "rows": {"touches.csv": 3}}).ledger_export_status(None, None)
    assert out == {"status": "found", "folder": "2031-01-06", "rows": {"touches.csv": 3}}


def test_cora_tool_reports_missing_and_failing_reads():
    assert _toolkit(None).ledger_export_status(None, None) == {"status": "not configured"}
    assert _toolkit(lambda: None).ledger_export_status(None, None) == {"status": "no export yet"}

    def boom():
        raise RuntimeError("x")
    assert _toolkit(boom).ledger_export_status(None, None)["status"] == "unreadable"


def test_a_failed_drive_step_is_logged_by_name_and_re_raised(caplog):
    from src.lending.drive_upload import upload_bytes

    class Failing:
        def execute(self):
            raise TimeoutError()

    class Broken(FakeDrive):
        def files(self):
            self.f.list = lambda **_: Failing()
            return self.f

    with pytest.raises(TimeoutError):
        upload_bytes(Broken(), "F", "a.csv", b"x")
    assert "find file failed (TimeoutError)" in caplog.text


# ── review fixes: free text, partial re-run, stalled main run ───────────────

@pytest.mark.parametrize("raw, expected", [
    ("Booked", "Booked"),
    ("ghl:no-answer", "ghl:no-answer"),
    ("call 813-555-0123 or a.b@example.com", "call [redacted] or [redacted]"),
    ("+18135550123", "[redacted]"),
    ("Funded 2026", "Funded 2026"),
])
def test_free_text_is_redacted(raw, expected):
    assert le.redact(raw) == expected


def test_disposition_text_with_a_phone_and_email_is_not_exported():
    data = le.LedgerData(dispositions=[{"phone": "+18135550123", "dialer_call_id": "1234567890",
                                        "disposition_raw": "cb 813 555 0199 x@y.com"}])
    out = le.build_files(data)["dispositions.csv"].decode()
    assert "0199" not in out and "x@y.com" not in out
    assert "1234567890" in out  # our own call id is kept


def test_rerun_removes_the_old_manifest_before_replacing_any_csv(monkeypatch):
    from src.tasks import lending_ledger_export as task

    monkeypatch.setattr(le, "collect", lambda db, day: le.LedgerData())
    events = []

    def failing_upload(folder_id, sub, name, data):
        events.append(("upload", name))
        if name == "touches.csv":
            raise TimeoutError()

    with pytest.raises(TimeoutError):
        task.run(None, day=DAY, folder_id="F", upload=failing_upload, now=AT,
                 remove=lambda f, sub, name: events.append(("remove", name)))
    assert events[0] == ("remove", "manifest.json")
    assert ("upload", "manifest.json") not in events


def test_partial_rerun_leaves_no_manifest_so_retry_redoes_the_day(monkeypatch):
    from src.lending.drive_upload import DriveUploader
    from src.tasks import lending_ledger_export as task

    monkeypatch.setattr(le, "collect", lambda db, day: le.LedgerData())
    drive = FakeDrive()
    drive.f.delete = lambda fileId, **_: _Req(drive.f.items.pop(fileId))
    up = DriveUploader(drive)
    task.run(None, day=DAY, folder_id="F", upload=up, now=AT, remove=up.remove)
    assert up.has_file("F", DAY.isoformat(), "manifest.json")

    def fail_on_touches(folder_id, sub, name, data):
        if name == "touches.csv":
            raise TimeoutError()
        up(folder_id, sub, name, data)

    with pytest.raises(TimeoutError):
        task.run(None, day=DAY, folder_id="F", upload=fail_on_touches, now=AT, remove=up.remove)
    assert not up.has_file("F", DAY.isoformat(), "manifest.json")


class _Done:
    def __init__(self, done=False, broken=False):
        self.done, self.broken = done, broken

    def has_file(self, *_):
        if self.broken:
            raise TimeoutError()
        return self.done


@pytest.mark.parametrize("uploader, alerted", [(_Done(done=True), False), (_Done(), True), (_Done(broken=True), True)])
def test_stalled_main_run_alerts_unless_the_day_is_done(monkeypatch, uploader, alerted):
    from src.tasks import lending_ledger_export as task

    sent = []
    monkeypatch.setattr(task, "_alert", lambda day, problem: sent.append(problem))
    assert task._alert_if_unfinished(uploader, "F", DAY) == 0
    assert bool(sent) is alerted


# ── P1.1 alignment: GHL id per row, Cora sends ──────────────────────────────

def test_a_rows_own_ghl_id_wins_and_keys_an_email_only_send():
    k = le.ContactKey(contact_id=7, ghl_contact_id="from-lead")
    assert le.key_columns("+18135550123", {"+18135550123": k}, "from-draft")["ghl_contact_id"] == "from-draft"
    assert le.key_columns(None, {}, "g9") == {"ghl_contact_id": "g9", "contact_id": "", "phone_last4": ""}


def test_cora_send_touch_is_keyed_by_its_ghl_id():
    data = le.LedgerData(touches=[{"phone": None, "ghl_contact_id": "g9", "at": AT, "kind": "cora_ghl_email",
                                   "direction": "outbound", "outcome": "sent"}])
    row = parse(le.build_files(data)["touches.csv"])[0]
    assert row["ghl_contact_id"] == "g9" and row["kind"] == "cora_ghl_email" and "ghl_contact_id" not in le.TABLES["touches"]
