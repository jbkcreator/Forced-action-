"""R1 0.2: admin/caller roles for borrower documents."""
from __future__ import annotations

from datetime import timedelta

from config.lending_closing import DOC_VIEW_LINK_PATH, DOC_VIEW_LINK_TTL, STAFF_TOKEN_TTL
from config.settings import get_settings
from src.lending.security import rbac


def test_approver_is_admin_everyone_else_caller(monkeypatch):
    monkeypatch.setattr(get_settings(), "lending_cora_approver_user_ids", "U_JOSH, U_X", raising=False)
    assert rbac.role_for_slack_user("U_JOSH") is rbac.Role.ADMIN
    assert rbac.role_for_slack_user("U_DAVID") is rbac.Role.CALLER
    assert rbac.role_for_slack_user("") is rbac.Role.CALLER


def test_no_approvers_means_nobody_is_admin(monkeypatch):
    monkeypatch.setattr(get_settings(), "lending_cora_approver_user_ids", "", raising=False)
    assert rbac.role_for_slack_user("U_JOSH") is rbac.Role.CALLER


def test_constants_pinned():
    assert set(rbac.DOCUMENT_TABLES) == {"doc_chase_uploads", "doc_view_links", "partner_file_uploads"}
    assert rbac.CALLER_DB_ROLE == "lending_caller"
    assert DOC_VIEW_LINK_TTL == timedelta(minutes=15) and STAFF_TOKEN_TTL == timedelta(days=90)
    assert DOC_VIEW_LINK_PATH == "/api/lending/docs/view/{token}"
