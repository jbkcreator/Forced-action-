"""tests/agents/reply_concierge/test_backflip_mailbox_poller.py"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.agents.reply_concierge.backflip_mailbox_poller import poll_backflip_mailbox


def _settings(mailbox_email="josh@example.com", app_password="app-pw-1234",
              sender_domain="backflip.com"):
    settings = MagicMock()
    settings.fa_max_backflip_mailbox_email = mailbox_email
    if app_password is None:
        settings.fa_max_backflip_mailbox_app_password = None
    else:
        settings.fa_max_backflip_mailbox_app_password.get_secret_value.return_value = app_password
    settings.fa_max_backflip_notification_sender_domain = sender_domain
    return settings


def _raw_message(subject: str, body: str, from_addr: str) -> bytes:
    return (
        f"From: {from_addr}\r\n"
        f"Subject: {subject}\r\n"
        f"Message-ID: <abc123@backflip.com>\r\n"
        f"Content-Type: text/plain\r\n"
        f"\r\n"
        f"{body}\r\n"
    ).encode("utf-8")


class TestPollBackflipMailbox:
    def test_missing_settings_no_ops(self):
        db = MagicMock()
        with patch("config.settings.get_settings", return_value=_settings(app_password=None)):
            result = poll_backflip_mailbox(db)
        assert result == 0
        db.execute.assert_not_called()

    def test_successful_event_marks_seen_and_counts(self):
        db = MagicMock()
        raw = _raw_message(
            "Application BF-10293 -- Now Under Review",
            "Your application BF-10293 has moved to Under Review.",
            "notifications@backflip.com",
        )
        mock_conn = MagicMock()
        mock_conn.search.return_value = ("OK", [b"1"])
        mock_conn.fetch.return_value = ("OK", [(b"1 (BODY[])", raw)])
        mock_conn.__enter__.return_value = mock_conn

        with patch("config.settings.get_settings", return_value=_settings()), \
             patch("imaplib.IMAP4_SSL", return_value=mock_conn), \
             patch(
                 "src.agents.reply_concierge.backflip_stage_ingest.apply_parsed_event",
                 return_value=True,
             ) as mock_apply:
            result = poll_backflip_mailbox(db)

        assert result == 1
        mock_apply.assert_called_once()
        event = mock_apply.call_args.args[1]
        assert event.stage == "under_review"
        mock_conn.store.assert_called_once_with(b"1", "+FLAGS", "\\Seen")

    def test_unresolved_ref_left_unread_for_retry(self):
        db = MagicMock()
        raw = _raw_message(
            "Application BF-99999 -- Now Under Review",
            "Your application BF-99999 has moved to Under Review.",
            "notifications@backflip.com",
        )
        mock_conn = MagicMock()
        mock_conn.search.return_value = ("OK", [b"1"])
        mock_conn.fetch.return_value = ("OK", [(b"1 (BODY[])", raw)])
        mock_conn.__enter__.return_value = mock_conn

        with patch("config.settings.get_settings", return_value=_settings()), \
             patch("imaplib.IMAP4_SSL", return_value=mock_conn), \
             patch(
                 "src.agents.reply_concierge.backflip_stage_ingest.apply_parsed_event",
                 return_value=False,
             ):
            result = poll_backflip_mailbox(db)

        assert result == 0
        mock_conn.store.assert_not_called()

    def test_unparseable_email_marked_seen_not_retried_forever(self):
        db = MagicMock()
        raw = _raw_message("Weekly newsletter", "Nothing relevant here.", "notifications@backflip.com")
        mock_conn = MagicMock()
        mock_conn.search.return_value = ("OK", [b"1"])
        mock_conn.fetch.return_value = ("OK", [(b"1 (BODY[])", raw)])
        mock_conn.__enter__.return_value = mock_conn

        with patch("config.settings.get_settings", return_value=_settings()), \
             patch("imaplib.IMAP4_SSL", return_value=mock_conn):
            result = poll_backflip_mailbox(db)

        assert result == 0
        mock_conn.store.assert_called_once_with(b"1", "+FLAGS", "\\Seen")

    def test_sender_outside_domain_skipped_defensively(self):
        # Defends against IMAP SEARCH FROM being a loose substring match --
        # a message whose From doesn't actually end in @<domain> must never
        # reach the parser, even if the server's search returned it.
        db = MagicMock()
        raw = _raw_message(
            "Application BF-1 -- Now Under Review", "moved to Under Review.",
            "someone@notbackflip.com",
        )
        mock_conn = MagicMock()
        mock_conn.search.return_value = ("OK", [b"1"])
        mock_conn.fetch.return_value = ("OK", [(b"1 (BODY[])", raw)])
        mock_conn.__enter__.return_value = mock_conn

        with patch("config.settings.get_settings", return_value=_settings()), \
             patch("imaplib.IMAP4_SSL", return_value=mock_conn), \
             patch(
                 "src.agents.reply_concierge.backflip_stage_ingest.apply_parsed_event",
             ) as mock_apply:
            result = poll_backflip_mailbox(db)

        assert result == 0
        mock_apply.assert_not_called()
        mock_conn.store.assert_not_called()

    def test_sender_filter_as_full_address_matches_only_that_sender(self):
        # sender_domain containing "@" narrows matching to that one exact
        # address -- needed when the poll mailbox and the sender share a
        # domain (e.g. Gmail-to-Gmail in dev/test), where a bare-domain
        # filter would also catch unrelated mail in the same inbox.
        db = MagicMock()
        raw = _raw_message(
            "Application BF-10293 -- Now Under Review",
            "Your application BF-10293 has moved to Under Review.",
            "sender@gmail.com",
        )
        mock_conn = MagicMock()
        mock_conn.search.return_value = ("OK", [b"1"])
        mock_conn.fetch.return_value = ("OK", [(b"1 (BODY[])", raw)])
        mock_conn.__enter__.return_value = mock_conn

        with patch(
            "config.settings.get_settings",
            return_value=_settings(sender_domain="sender@gmail.com"),
        ), \
             patch("imaplib.IMAP4_SSL", return_value=mock_conn), \
             patch(
                 "src.agents.reply_concierge.backflip_stage_ingest.apply_parsed_event",
                 return_value=True,
             ) as mock_apply:
            result = poll_backflip_mailbox(db)

        assert result == 1
        mock_apply.assert_called_once()

    def test_sender_filter_as_full_address_rejects_other_sender_same_domain(self):
        db = MagicMock()
        raw = _raw_message(
            "Application BF-10293 -- Now Under Review",
            "moved to Under Review.",
            "someone-else@gmail.com",
        )
        mock_conn = MagicMock()
        mock_conn.search.return_value = ("OK", [b"1"])
        mock_conn.fetch.return_value = ("OK", [(b"1 (BODY[])", raw)])
        mock_conn.__enter__.return_value = mock_conn

        with patch(
            "config.settings.get_settings",
            return_value=_settings(sender_domain="sender@gmail.com"),
        ), \
             patch("imaplib.IMAP4_SSL", return_value=mock_conn), \
             patch(
                 "src.agents.reply_concierge.backflip_stage_ingest.apply_parsed_event",
             ) as mock_apply:
            result = poll_backflip_mailbox(db)

        assert result == 0
        mock_apply.assert_not_called()
        mock_conn.store.assert_not_called()

    def test_imap_login_failure_returns_zero_not_raises(self):
        db = MagicMock()
        with patch("config.settings.get_settings", return_value=_settings()), \
             patch("imaplib.IMAP4_SSL", side_effect=OSError("network unreachable")):
            result = poll_backflip_mailbox(db)
        assert result == 0
