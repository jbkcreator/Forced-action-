from types import SimpleNamespace
from unittest.mock import patch

from src.services.email_suppression import is_email_suppressed, suppress_contact


class _Result:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _Db:
    """Fake session: canned SELECT results, captured INSERT/UPDATE statements."""

    def __init__(self, *, opt_out_row=None, subscriber_row=None, dbpr_row=None):
        self.opt_out_row = opt_out_row
        self.subscriber_row = subscriber_row
        self.dbpr_row = dbpr_row
        self.executed = []
        self.committed = False

    def execute(self, sql, params=None):
        text = str(sql)
        self.executed.append((text, params))
        if "FROM email_opt_outs" in text:
            return _Result(self.opt_out_row)
        if "FROM subscribers" in text:
            return _Result(self.subscriber_row)
        if "FROM dbpr_contacts" in text:
            return _Result(self.dbpr_row)
        return _Result(None)

    def commit(self):
        self.committed = True


def test_is_email_suppressed_true_when_row_exists():
    db = _Db(opt_out_row=(1,))
    assert is_email_suppressed(db, "blocked@example.com") is True


def test_is_email_suppressed_false_when_no_row():
    db = _Db(opt_out_row=None)
    assert is_email_suppressed(db, "clean@example.com") is False


def test_is_email_suppressed_false_for_empty_email():
    db = _Db()
    assert is_email_suppressed(db, "") is False


def test_suppress_contact_cascades_email_to_phone_via_subscriber():
    db = _Db(subscriber_row=("+18135550100",))
    suppress_contact(db, email="Person@Example.com", source="unsubscribe_link")

    email_insert = next(p for sql, p in db.executed if "INSERT INTO email_opt_outs" in sql)
    phone_insert = next(p for sql, p in db.executed if "INSERT INTO sms_opt_outs" in sql)

    assert email_insert["email"] == "person@example.com"
    assert phone_insert["phone"] == "+18135550100"
    # suppress_contact must NOT commit — the caller owns the transaction.
    assert db.committed is False


def test_suppress_contact_cascades_email_to_phone_via_dbpr_contact():
    # No subscriber match, but the DBPR (prospect) record carries the phone.
    db = _Db(subscriber_row=None, dbpr_row=("+18135550123",))
    suppress_contact(db, email="prospect@example.com", source="unsubscribe_link")

    phone_insert = next(p for sql, p in db.executed if "INSERT INTO sms_opt_outs" in sql)
    assert phone_insert["phone"] == "+18135550123"


def test_suppress_contact_cascades_phone_to_email_via_subscriber():
    db = _Db(subscriber_row=("sibling@example.com",))
    suppress_contact(db, phone="+18135550199", source="cascaded_from_sms")

    email_insert = next(p for sql, p in db.executed if "INSERT INTO email_opt_outs" in sql)
    phone_insert = next(p for sql, p in db.executed if "INSERT INTO sms_opt_outs" in sql)

    assert email_insert["email"] == "sibling@example.com"
    assert phone_insert["phone"] == "+18135550199"


def test_suppress_contact_normalizes_raw_phone():
    db = _Db(subscriber_row=None)
    suppress_contact(db, phone="(813) 555-0199", source="cascaded_from_sms")

    phone_insert = next(p for sql, p in db.executed if "INSERT INTO sms_opt_outs" in sql)
    assert phone_insert["phone"] == "+18135550199"


def test_suppress_contact_only_writes_known_channel_when_no_sibling():
    db = _Db(subscriber_row=None, dbpr_row=None)
    suppress_contact(db, email="lonely@example.com", source="manual")

    email_inserts = [p for sql, p in db.executed if "INSERT INTO email_opt_outs" in sql]
    phone_inserts = [p for sql, p in db.executed if "INSERT INTO sms_opt_outs" in sql]

    assert len(email_inserts) == 1
    assert len(phone_inserts) == 0


def test_suppress_contact_noop_when_no_identifiers():
    db = _Db()
    suppress_contact(db, source="manual")
    assert db.executed == []


def _smtp_settings():
    return SimpleNamespace(
        smtp_host="smtp.example.com",
        smtp_port=587,
        smtp_user="user@example.com",
        smtp_pass=SimpleNamespace(get_secret_value=lambda: "secret"),
        email_from=None,
    )


def test_send_email_skips_suppressed_recipient():
    from src.services import email as email_module

    with patch("src.services.email.get_settings", return_value=_smtp_settings()), \
         patch("src.core.database.get_db_context") as mock_ctx, \
         patch("src.services.email_suppression.is_email_suppressed", return_value=True), \
         patch("smtplib.SMTP") as mock_smtp:
        mock_ctx.return_value.__enter__.return_value = _Db()
        result = email_module.send_email(
            to="blocked@example.com", subject="Hi", body_text="body",
        )

    assert result is False
    mock_smtp.assert_not_called()


def test_send_email_sends_when_not_suppressed():
    from src.services import email as email_module

    with patch("src.services.email.get_settings", return_value=_smtp_settings()), \
         patch("src.core.database.get_db_context") as mock_ctx, \
         patch("src.services.email_suppression.is_email_suppressed", return_value=False), \
         patch("smtplib.SMTP") as mock_smtp:
        mock_ctx.return_value.__enter__.return_value = _Db()
        result = email_module.send_email(
            to="clean@example.com", subject="Hi", body_text="body",
        )

    assert result is True
    mock_smtp.assert_called_once()


def test_send_email_sets_list_unsubscribe_header():
    from src.services import email as email_module

    with patch("src.services.email.get_settings", return_value=_smtp_settings()), \
         patch("src.core.database.get_db_context") as mock_ctx, \
         patch("src.services.email_suppression.is_email_suppressed", return_value=False), \
         patch("smtplib.SMTP") as mock_smtp:
        mock_ctx.return_value.__enter__.return_value = _Db()
        email_module.send_email(
            to="clean@example.com", subject="Hi", body_text="body",
            list_unsubscribe_url="https://app.example.com/api/email/unsubscribe?token=abc",
        )

    sent_msg = mock_smtp.return_value.__enter__.return_value.sendmail.call_args[0][2]
    assert "List-Unsubscribe:" in sent_msg
    assert "List-Unsubscribe-Post: List-Unsubscribe=One-Click" in sent_msg
