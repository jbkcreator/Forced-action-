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
    # Read-only governance checks added by WP-T2-2+; no send path involved:
    "src/services/state_engine.py",        # reads FA_MAX_ALLOWED_SOURCE_TYPES constant for validation
    "src/agents/fa_max/tool_registry.py",  # reads suppression_reason before dispatch decision
    "src/agents/fa_max/agent_graph.py",    # imports GovernanceBlocked exception for error handling
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


# Item 2 (WP-T2-1 go-live review, 2026-09): the two checks above close two
# narrower gaps -- a raw vendor call, and a governance-module import -- but
# neither catches a NEW file that is FA-Max-aware (imports FA Max's own
# durable-state/governance/model symbols) and ALSO calls the *compliant*
# wrapper (sms_compliance.send_sms / instantly_service.add_leads) directly,
# skipping relay.enqueue() -> guards.evaluate() entirely. That wrapper can't
# be banned outright -- ~15 unrelated Lifecycle/subscriber features call it
# legitimately for their own, non-FA-Max traffic (verified: none of them
# import any FA-Max-specific module or model). So the enforceable invariant
# is narrower and real: a file that is BOTH FA-Max-aware AND calls a raw
# send function, outside the two sanctioned dispatchers, is forbidden.
#
# This is still static analysis, not a runtime capability system -- Python
# has no way to prove a call chain actually crossed guards.evaluate() without
# either a forgeable caller-supplied flag (rejected: any caller can set a
# flag) or restricting who may call the pure governance/gate functions
# (rejected: this repo's own tests call those functions directly for unit
# coverage -- see test_fa_max_wp2.py -- restricting their callers would
# either break that pattern or, if test files are allowlisted, not actually
# guard anything, since a bypass simply never calls the gate it's skipping
# in the first place). A determined future engineer who hardcodes a phone
# number instead of reading it from an FA-Max table would not be caught by
# this check -- documented here, not silently assumed away.
_FA_MAX_SIGNAL_MODULES = {
    "src.services.fa_max_send_governance",
    "src.services.state_engine",  # WP-1 durable state engine — FA Max only
}
_FA_MAX_SANCTIONED_SENDERS = {
    "src/services/relay/channels_sms.py",
    "src/services/relay/channels_email.py",
    "src/services/fa_max_send_governance.py",  # governance module itself
    "src/services/state_engine.py",
    "src/services/relay/guards.py",
    "src/services/relay/queue.py",
    "src/services/relay/fakes.py",  # test doubles, never touch a real vendor
    # File-level granularity limitation, verified by hand: admin_router.py is
    # a large shared router. Its FA-Max-awareness comes from
    # _handle_relay_decision (imports state_engine for the Slack
    # approve/reject -> transition() flow) -- a DIFFERENT function from the
    # one that calls send_sms() (the Lifecycle marketing-message approval
    # endpoint at "/lifecycle-messages/approve", message_type="marketing",
    # subscriber_id-scoped -- unrelated to FA Max). This check can't see
    # function boundaries, only file-level imports+calls; a real per-file
    # split would remove the need for this entry.
    "src/api/admin_router.py",
}
_RAW_SEND_CALL_NAMES = {"send_message", "send_sms", "add_leads"}


def _is_fa_max_aware(tree: ast.AST) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module in _FA_MAX_SIGNAL_MODULES:
            return True
        if isinstance(node, ast.Import) and any(
            alias.name in _FA_MAX_SIGNAL_MODULES for alias in node.names
        ):
            return True
        # src.core.models is imported broadly for unrelated reasons; only
        # count it as an FA Max signal when a FaMax* symbol is actually
        # pulled from it.
        if isinstance(node, ast.ImportFrom) and node.module == "src.core.models":
            if any(alias.name.startswith("FaMax") for alias in node.names):
                return True
    return False


def _calls_any_raw_send(tree: ast.AST) -> str | None:
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
            if name in _RAW_SEND_CALL_NAMES:
                return name
    return None


def test_no_fa_max_aware_direct_send_callers():
    """A file that reads FA Max's own governance/durable-state modules and
    ALSO calls a raw send function outside the two sanctioned dispatchers is
    exactly the shape of a future feature built without going through
    relay.enqueue() -> guards.evaluate() -> channels_sms.py/channels_email.py
    -- it would skip the 10DLC gate, the backlog-release gate, and FA Max
    consent entirely. See the module-level comment above for what this check
    can and cannot prove."""
    offenders = []
    for rel, path in _iter_python_files(exclude=_FA_MAX_SANCTIONED_SENDERS):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        if not _is_fa_max_aware(tree):
            continue
        hit = _calls_any_raw_send(tree)
        if hit:
            offenders.append(f"{rel} is FA-Max-aware and calls {hit}(...)")
    assert not offenders, (
        "New FA-Max-aware direct send caller(s) found outside the sanctioned "
        "relay dispatchers -- route through relay.enqueue() -> "
        "channels_sms.py/channels_email.py instead, or add to "
        "_FA_MAX_SANCTIONED_SENDERS with a documented reason:\n"
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
    monkeypatch.setattr(sweep.exceptions_alert_queue, "enqueue_and_attempt", lambda **kw: True)
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


def test_fa_max_aware_direct_sender_detector_catches_a_synthetic_bypass():
    """test_no_fa_max_aware_direct_send_callers proves today's repo is clean;
    this proves the DETECTOR itself would actually catch the bypass shape it
    claims to catch, rather than just happening to pass because nothing in
    the current tree triggers it. Parses synthetic source text directly --
    no real file needs to exist on disk for this."""
    bypass_source = (
        "from src.services.fa_max_send_governance import require_consent\n"
        "from src.services import sms_compliance\n"
        "def notify_borrower(phone, body, db):\n"
        "    sms_compliance.send_sms(phone, body, db, message_type='transactional')\n"
    )
    tree = ast.parse(bypass_source, filename="<synthetic-bypass>")
    assert _is_fa_max_aware(tree) is True
    assert _calls_any_raw_send(tree) == "send_sms"

    # Sanity check: a file that's FA-Max-aware but never sends is fine --
    # e.g. a state-machine read/decision function with no send call.
    aware_but_safe_source = (
        "from src.services.state_engine import get_person_state\n"
        "def check(person_id, db):\n"
        "    return get_person_state(session=db, person_id=person_id)\n"
    )
    tree = ast.parse(aware_but_safe_source, filename="<synthetic-safe>")
    assert _is_fa_max_aware(tree) is True
    assert _calls_any_raw_send(tree) is None

    # Sanity check: a file that sends but has no FA Max awareness at all
    # (the ~15 legitimate Lifecycle/subscriber callers' actual shape) is
    # correctly not flagged.
    unrelated_sender_source = (
        "from src.services import sms_compliance\n"
        "def notify_subscriber(phone, body, db, subscriber_id):\n"
        "    sms_compliance.send_sms(phone, body, db, message_type='marketing', "
        "subscriber_id=subscriber_id)\n"
    )
    tree = ast.parse(unrelated_sender_source, filename="<synthetic-unrelated>")
    assert _is_fa_max_aware(tree) is False
