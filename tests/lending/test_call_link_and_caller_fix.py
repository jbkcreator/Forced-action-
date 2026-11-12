"""The call link and caller-name data fix against the real lending schema (rolled back; skipped without
TEST_DATABASE_URL)."""
from __future__ import annotations

from sqlalchemy import text

from migrations.apply_lending_call_link_and_caller_fix import HARI_K_DOT_ROW_IDS, run_steps

VIEW = "https://app.batchdialer.com/contacts/view/559662160"


def _insert(conn, call_id, *, source="batchdialer", status=None, url=None, caller=None, row_id=None) -> int:
    return conn.execute(text(
        "INSERT INTO lending.call_dispositions (id, dialer_call_id, call_source, direction, recording_status, "
        "recording_view_url, caller_name, raw_event) "
        "VALUES (COALESCE(:id, nextval(pg_get_serial_sequence('lending.call_dispositions', 'id'))), :c, :s, "
        "'outbound', :st, :u, :n, '{}'::jsonb) RETURNING id"),
        {"id": row_id, "c": f"fix-{call_id}", "s": source, "st": status, "u": url, "n": caller}).scalar()


def _get(conn, row_id, column):
    return conn.execute(text(f"SELECT {column} FROM lending.call_dispositions WHERE id = :id"), {"id": row_id}).scalar()


def test_fix_clears_unrecorded_links_and_respells_callers_by_id_once(lending_db):
    conn = lending_db.connection()
    recorded = _insert(conn, "rec", status="recorded", url=VIEW)
    unrecorded = _insert(conn, "none", status="none", url=VIEW)
    ghl_link = _insert(conn, "ghl", source="ghl", status=None, url="https://app.gohighlevel.com/x")
    lower_david = _insert(conn, "david", source="ghl", caller="david willis")
    hari_dot = _insert(conn, "hari-dot", source="ghl", caller="Hari K", row_id=HARI_K_DOT_ROW_IDS[0])
    other_hari = _insert(conn, "hari", source="ghl", caller="Hari K")  # a real "Hari K" call stays his

    run_steps(conn)

    assert _get(conn, recorded, "recording_view_url") == VIEW
    assert _get(conn, unrecorded, "recording_view_url") is None
    assert _get(conn, ghl_link, "recording_view_url") == "https://app.gohighlevel.com/x"  # GHL rows untouched
    assert _get(conn, lower_david, "caller_name") == "David Willis"
    assert _get(conn, hari_dot, "caller_name") == "Hari K."
    assert _get(conn, other_hari, "caller_name") == "Hari K"
    assert run_steps(conn) == {"bd_unrecorded_link": 0, "ghl_caller_case": 0, "ghl_hari_k_dot": 0}
