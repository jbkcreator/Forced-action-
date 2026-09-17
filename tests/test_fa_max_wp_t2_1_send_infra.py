"""
WP-T2-1 — Own-Lane Send Infrastructure.

Covers the production-execution review's findings on the original plan:
  1. The SMS relay channel must exist and must turn a False/dry-run result
     from sms_compliance.send_sms() into a recorded failure, never a false
     'sent'.
  2. No FA Max code may call sms_compliance.send_sms or telnyx_sms.
     send_message directly — only src.services.relay.channels_sms may, and
     only relay.enqueue() -> the engine may reach that dispatcher. This is a
     structural (grep-style) test because the relay guard that protects the
     relay path cannot protect a future direct caller that skips relay
     entirely.
  3. A failed suppression sync must defer the batch rather than send on
     stale data, and a missing campaign config must be distinguishable from
     a sync failure.
  4. FakeMail/FakeSMS report acceptance, never delivery.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from src.services.relay.fakes import FAKE_MAIL, FAKE_SMS, reset_fakes

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"


@pytest.fixture(autouse=True)
def _clean_fakes():
    """FAKE_MAIL/FAKE_SMS are process-wide singletons (fakes.py) so a test
    that errors before reaching its own reset_fakes() call would otherwise
    leak sent-message state into whichever test runs next. Reset before AND
    after every test in this module so isolation doesn't depend on call
    order or a test reaching its happy path."""
    reset_fakes()
    yield
    reset_fakes()

# sms_compliance.send_sms() is the repo-wide single SMS sender (CLAUDE.md)
# and is correctly called directly by the rest of the Lifecycle/subscriber
# platform for its own (non-FA-Max) traffic — banning that call site
# entirely would be wrong, not a real invariant. The invariant this WP
# actually needs is narrower: nothing but sms_compliance.py itself (the
# wrapper) and the one FA Max relay dispatcher may call the raw vendor
# function telnyx_sms.send_message() — that is the only way to send an SMS
# while skipping the compliance gate entirely, which is the real risk.
_ALLOWED_RAW_VENDOR_CALLERS = {
    "src/services/sms_compliance.py",       # the compliant wrapper itself
    "src/services/relay/channels_sms.py",   # the one FA Max relay dispatcher
    "src/services/owner_alert.py",          # pre-existing, non-FA-Max feature
    "src/tasks/sold_out_reactivation.py",   # pre-existing, non-FA-Max feature
}

_FORBIDDEN_CALLS = {"send_message"}

# fa_max_send_governance.require_consent() is the FA Max-specific consent/
# 10DLC-aware gate relay.guards runs before any dispatch. A future direct
# sms_compliance.send_sms() caller (a legitimate call site for everyone
# else's traffic, so that function itself can't be forbidden) that wants to
# be FA Max-aware would need this module to check consent correctly --
# restricting who may import it means a new send path can't silently
# replicate "looks like it checks FA Max consent" outside the relay/guards
# choke point. queue.py needs it too (enqueue-time payload/consent
# pre-validation, not a second send path).
_ALLOWED_GOVERNANCE_IMPORTERS = {
    "src/services/fa_max_send_governance.py",  # the module itself
    "src/services/relay/guards.py",
    "src/services/relay/queue.py",
    "src/services/relay/channels_sms.py",
}


def _iter_python_files(exclude: set[str] = _ALLOWED_RAW_VENDOR_CALLERS):
    for path in SRC_ROOT.rglob("*.py"):
        rel = path.relative_to(REPO_ROOT).as_posix()
        if rel in exclude:
            continue
        yield rel, path


def _imports_module(path: Path, module: str) -> bool:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == module:
            return True
        if isinstance(node, ast.Import):
            if any(alias.name == module for alias in node.names):
                return True
    return False


def _calls_forbidden_sms_function(path: Path) -> str | None:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
            if name in _FORBIDDEN_CALLS:
                return name
    return None


def test_no_new_direct_telnyx_vendor_callers():
    """Nothing but sms_compliance.py (the compliant wrapper) and the one FA
    Max relay dispatcher may call telnyx_sms.send_message() directly — that
    raw vendor call is the only way to send an SMS while skipping every
    compliance/consent/DNC gate entirely."""
    offenders = []
    for rel, path in _iter_python_files():
        hit = _calls_forbidden_sms_function(path)
        if hit:
            offenders.append(f"{rel} calls {hit}(...)")
    assert not offenders, (
        "New direct telnyx_sms.send_message() caller(s) found outside the "
        "compliant wrapper — route through sms_compliance.send_sms (or, for "
        "FA Max, relay.enqueue() -> channels_sms.send_sms) instead, or add "
        "to _ALLOWED_RAW_VENDOR_CALLERS with a documented reason:\n"
        + "\n".join(offenders)
    )


def test_fa_max_send_governance_only_imported_by_relay():
    """A future SMS send path that calls sms_compliance.send_sms() directly
    (a legitimate call site for the rest of the platform's own traffic, so
    banning that function itself would be wrong -- see
    test_no_new_direct_telnyx_vendor_callers's docstring) cannot silently
    replicate FA Max's consent/10DLC checks outside relay's guards.py choke
    point without importing this module -- so restricting who may import it
    closes the gap the raw-vendor-call check alone leaves open (code-review
    finding: 'the full invariant is a code-review/architecture convention,
    not a fully automatable check')."""
    offenders = []
    for rel, path in _iter_python_files(exclude=_ALLOWED_GOVERNANCE_IMPORTERS):
        if _imports_module(path, "src.services.fa_max_send_governance"):
            offenders.append(rel)
    assert not offenders, (
        "New fa_max_send_governance importer(s) found outside the relay "
        "dispatch path -- route the send through relay.enqueue() -> "
        "channels_sms.send_sms instead, or add to "
        "_ALLOWED_GOVERNANCE_IMPORTERS with a documented reason:\n"
        + "\n".join(offenders)
    )


def test_sms_channel_is_registered():
    import src.services.relay.channels_sms  # noqa: F401
    from src.services.relay.channels import DISPATCHERS

    assert "sms" in DISPATCHERS


def test_fake_sms_dispatch_records_send_without_network(monkeypatch):
    from config.settings import get_settings
    from src.services.relay.channels_sms import send_sms
    from src.services.relay.queue import QueueItem

    monkeypatch.setattr(get_settings(), "fa_max_relay_send_mode", "fake", raising=False)

    item = QueueItem(
        id=1, idempotency_key="k1", batch_id=None, thread_id=None,
        channel="sms", recipient="+15550001111", payload={"body": "hi"},
        status="approved", slack_message_ts=None, decided_by="josh",
        decided_at=None, error=None, dispatched_at=None,
        created_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
        venture_key="fa_max_lending",
    )
    send_sms(item)  # must not raise
    assert len(FAKE_SMS.sent) == 1
    assert FAKE_SMS.sent[0]["to"] == "+15550001111"


def test_fake_sms_dispatch_raises_on_vendor_refusal(monkeypatch):
    from config.settings import get_settings
    from src.services.relay.channels_sms import send_sms
    from src.services.relay.queue import QueueItem

    monkeypatch.setattr(get_settings(), "fa_max_relay_send_mode", "fake", raising=False)
    FAKE_SMS.fail_recipients.add("+15550009999")

    item = QueueItem(
        id=2, idempotency_key="k2", batch_id=None, thread_id=None,
        channel="sms", recipient="+15550009999", payload={"body": "hi"},
        status="approved", slack_message_ts=None, decided_by="josh",
        decided_at=None, error=None, dispatched_at=None,
        created_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
        venture_key="fa_max_lending",
    )
    with pytest.raises(RuntimeError):
        send_sms(item)


def test_live_mode_refuses_when_telnyx_dry_run_enabled(monkeypatch):
    """A live-mode approved item must never be silently recorded as sent
    while the environment is still in Telnyx dry-run (production-execution
    review finding #1)."""
    from config.settings import get_settings
    from src.services.relay.channels_sms import send_sms
    from src.services.relay.queue import QueueItem

    settings = get_settings()
    monkeypatch.setattr(settings, "fa_max_relay_send_mode", "live", raising=False)
    monkeypatch.setattr(settings, "telnyx_sms_enabled", False, raising=False)

    item = QueueItem(
        id=3, idempotency_key="k3", batch_id=None, thread_id=None,
        channel="sms", recipient="+15550002222", payload={"body": "hi"},
        status="approved", slack_message_ts=None, decided_by="josh",
        decided_at=None, error=None, dispatched_at=None,
        created_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
        venture_key="fa_max_lending",
    )
    with pytest.raises(RuntimeError, match="TELNYX_SMS_ENABLED"):
        send_sms(item)


def test_suppression_sync_distinguishes_not_configured_from_synced(monkeypatch):
    from src.services.relay import suppression_sync

    class _NoCampaign:
        relay_instantly_campaign_id = None

    # Patch the name as bound inside suppression_sync's own module namespace
    # (it did `from ... import get_venture_config`), not the source module —
    # patching the source module's attribute would not affect an already
    #-imported reference.
    monkeypatch.setattr(suppression_sync, "get_venture_config", lambda key: _NoCampaign())
    result = suppression_sync.sync_unsubscribes(venture_key="fa_max_lending")
    assert result.status == "not_configured"
    assert result.count == 0


def test_suppression_sync_failure_raises_distinct_exception(monkeypatch):
    from src.services.relay import suppression_sync

    class _WithCampaign:
        relay_instantly_campaign_id = "camp_1"

    def _boom(*a, **kw):
        raise RuntimeError("network down")

    monkeypatch.setattr(suppression_sync, "get_venture_config", lambda key: _WithCampaign())
    monkeypatch.setattr(suppression_sync.instantly, "list_leads", _boom)

    with pytest.raises(suppression_sync.SuppressionSyncFailed):
        suppression_sync.sync_unsubscribes(venture_key="fa_max_lending")


def test_sweep_defers_whole_batch_on_suppression_sync_failure(monkeypatch):
    """A failed suppression sync must leave approved rows completely
    untouched (never claimed/sent) rather than sending against a possibly
    stale suppression list (production-execution review, finding 3)."""
    from src.services.relay import sweep

    def _boom(**kw):
        raise sweep.SuppressionSyncFailed("network down")

    claimed = []
    monkeypatch.setattr(sweep, "sync_unsubscribes", _boom)
    monkeypatch.setattr(sweep, "post_exceptions_alert", lambda **kw: True)
    monkeypatch.setattr(sweep.queue, "approved_batch", lambda **kw: [object(), object()])

    def _fail_if_called(*a, **kw):
        claimed.append(True)
        raise AssertionError("execute_batch must not be called when suppression sync fails")

    monkeypatch.setattr(sweep, "execute_batch", _fail_if_called)

    result = sweep.run_sweep(venture_key="fa_max_lending")
    assert not claimed
    assert result.deferred == 2
    assert result.sent == 0


def test_fake_receipts_never_claim_delivery():
    receipt = FAKE_MAIL.send(recipient="a@b.com", subject="s", body="b", campaign_id="c")
    assert receipt.accepted is True
    assert receipt.delivered is None  # never claimed — see fakes.py module docstring

    receipt = FAKE_SMS.send(to="+15550001111", body="hi")
    assert receipt.accepted is True
    assert receipt.delivered is None
