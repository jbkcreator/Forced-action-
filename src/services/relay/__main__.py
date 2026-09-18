"""
Relay — process entry point (RELAY-v2.2 sub-tasks R1 + R2).

Usage:
    python -m src.services.relay --health
    python -m src.services.relay --go-live-readiness --venture fa_max_lending
    python -m src.services.relay --sweep
    python -m src.services.relay --sweep --venture venture_two
    python -m src.services.relay --seed --channel noop --recipient test@example.com --payload-json '{"subject": "Hi"}'
    python -m src.services.relay --setup-email-channel

Every command takes --venture (default 'hillsborough_distress', CLONE-v2.2 /
CL3). One --sweep run covers one venture, since the send window, daily
ceiling, Slack channel and kill-switch key are all per-venture — a second
venture means a second cron line, not a wider batch.

--health is R1's scaffolding check (DB/table/kill-switch/Slack reachable) —
a pure diagnostic whose exit code does NOT reflect go-live readiness (WP-T2-1
go-live checks print informationally under --health but never fail it).
--go-live-readiness (WP-T2-1, 2026-09) is the actual scriptable go/no-go
gate: exits 1 if an automatable pre-launch requirement (mailbox, SPF, DMARC,
monitor/drain cron registration, EXCEPTIONS Slack config) isn't met while
send gates are still closed — see docs/fa-max-go-live.md. --sweep runs one
approval-queue sweep (what cron calls every 30 minutes — see
scripts/cron/crontab.txt). --seed is R1's stand-in for Cora (Phase 2, not
yet built): it writes a 'pending' row via src.services.relay.queue.enqueue()
and posts it to Slack for approval — the exact same call Cora will make
later, so nothing here changes when she lands. --setup-email-channel is R2's
one-time, idempotent setup command: finds or creates the "Relay passthrough"
Instantly campaign and prints the id to set as RELAY_INSTANTLY_CAMPAIGN_ID in
.env — see src.services.relay.channels_email and
RELAY-R2-Implementation-Plan.md.

Modeled on src/agents/vera/__main__.py's CLI shape, but Relay is a
deterministic non-agent service (see Locked decision #1 in
RELAY-R1-Implementation-Plan.md) — this is src/services/relay, not
src/agents/relay.
"""
from __future__ import annotations

import argparse
import json
import sys
import uuid

from config.venture_template import DEFAULT_VENTURE_KEY
from src.utils.logger import setup_logging

# Import for their registration side effect only — makes the real 'email'
# and 'sms' (WP-T2-1) channels available in DISPATCHERS before any
# --sweep/--seed runs (R1's channels.py ships only the 'noop' test channel).
import src.services.relay.channels_email  # noqa: F401
import src.services.relay.channels_sms  # noqa: F401

if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

_IS_TTY = sys.stderr.isatty()
_OK = "\033[32m✓\033[0m" if _IS_TTY else "[OK]"
_WARN = "\033[33m⚠\033[0m" if _IS_TTY else "[WARN]"
_FAIL = "\033[31m✗\033[0m" if _IS_TTY else "[FAIL]"


def _line(msg: str) -> None:
    print(msg, file=sys.stderr)


# ---------------------------------------------------------------------------
# WP-T2-1 go-live checks (2026-09 review). Every check here returns one of
# three states, never just true/false: "verified" (positively confirmed --
# a real DNS record was found and parses), "configured" (a setting is
# present but this check cannot independently confirm it's correct — e.g.
# DKIM's selector is provider-specific and unknown to this repo), or
# "missing"/"not_configured" (nothing there). Collapsing "configured" into
# "verified" is exactly the false-confidence failure mode a go-live check
# exists to prevent — see docs/fa-max-go-live.md.
# ---------------------------------------------------------------------------

def _resolve_txt_records(fqdn: str) -> list[str] | None:
    """Returns a list of TXT record strings, or None if the lookup itself
    failed (NXDOMAIN, timeout, no resolver, etc — never raises)."""
    try:
        import dns.resolver
        answer = dns.resolver.resolve(fqdn, "TXT", lifetime=5.0)
        return [b"".join(rdata.strings).decode("utf-8", errors="replace") for rdata in answer]
    except Exception:
        return None


def check_spf(domain: str) -> dict:
    """SPF lives as a TXT record on the domain root — directly verifiable."""
    records = _resolve_txt_records(domain)
    if records is None:
        return {"state": "missing", "detail": f"no TXT records resolvable for {domain}"}
    spf = [r for r in records if r.lower().startswith("v=spf1")]
    if not spf:
        return {"state": "missing", "detail": f"no v=spf1 TXT record found on {domain}"}
    return {"state": "verified", "detail": spf[0]}


def check_dmarc(domain: str) -> dict:
    """DMARC lives as a TXT record on _dmarc.<domain> — directly verifiable."""
    fqdn = f"_dmarc.{domain}"
    records = _resolve_txt_records(fqdn)
    if records is None:
        return {"state": "missing", "detail": f"no TXT records resolvable for {fqdn}"}
    dmarc = [r for r in records if r.lower().startswith("v=dmarc1")]
    if not dmarc:
        return {"state": "missing", "detail": f"no v=DMARC1 TXT record found on {fqdn}"}
    return {"state": "verified", "detail": dmarc[0]}


def check_dkim(domain: str) -> dict:
    """DKIM's selector (the 'foo' in foo._domainkey.<domain>) is chosen by
    the sending provider (Instantly), not this repo -- there is no
    well-known location the way there is for SPF/DMARC. This check can
    truthfully report only "configured" (the operator says DNS auth is
    done) or "unknown" -- never a false "verified" for a record this repo
    has no reliable way to locate. See docs/fa-max-go-live.md's manual DKIM
    verification step (Instantly's dashboard shows DKIM status directly)."""
    return {
        "state": "unknown",
        "detail": (
            "DKIM selector is provider-assigned (Instantly) and not known to "
            "this repo -- cannot be checked by domain name alone. Verify in "
            "Instantly's dashboard (Campaigns > Settings > Domain Authentication) "
            "or via the selector Instantly's setup email specifies, then confirm "
            "manually per docs/fa-max-go-live.md."
        ),
    }


def check_cron_registered(module_names: list[str], crontab_path: str | None = None) -> dict:
    """Static check: is this module name present in scripts/cron/crontab.txt?
    Proves REGISTRATION, not that a cron daemon is actually running it --
    an honest, narrower claim than "monitoring is active"."""
    import pathlib
    path = pathlib.Path(crontab_path) if crontab_path else (
        pathlib.Path(__file__).resolve().parents[3] / "scripts" / "cron" / "crontab.txt"
    )
    try:
        text_content = path.read_text(encoding="utf-8")
    except OSError as exc:
        return {"state": "missing", "detail": f"could not read {path}: {exc}"}
    missing = [name for name in module_names if name not in text_content]
    if missing:
        return {"state": "missing", "detail": f"not found in {path.name}: {', '.join(missing)}"}
    return {"state": "verified", "detail": f"all registered in {path.name}"}


def check_go_live_flags(settings) -> dict:
    """FA Max's three closed-by-default send gates. The expected, SAFE
    pre-go-live state is all three False/"fake" -- this check's job is to
    make an accidentally-flipped flag loud, not to bless the closed state
    as itself proof of anything (closed is just the default)."""
    mode = getattr(settings, "fa_max_relay_send_mode", "fake")
    dlc = getattr(settings, "fa_max_10dlc_registered", False)
    backlog = getattr(settings, "fa_max_send_backlog_release_confirmed", False)
    open_flags = []
    if mode == "live":
        open_flags.append("fa_max_relay_send_mode=live")
    if dlc:
        open_flags.append("fa_max_10dlc_registered=true")
    if backlog:
        open_flags.append("fa_max_send_backlog_release_confirmed=true")
    if open_flags:
        return {"state": "open", "detail": f"LIVE-SEND GATE(S) OPEN: {', '.join(open_flags)}"}
    return {"state": "closed", "detail": "all three gates closed (expected pre-go-live state)"}


def cmd_health(venture_key: str) -> int:
    from config.settings import get_settings
    from src.core.database import get_db_context
    from src.services.kill_switch_service import get_kill_switch_status
    from src.utils.venture_config import get_venture_config
    from sqlalchemy import text

    settings = get_settings()
    venture = get_venture_config(venture_key)
    all_ok = True

    _line(f"\nRelay — health check [venture={venture_key}]")
    _line("-" * 40)

    dsn_set = bool(settings.database_url)
    _line(f"  {_OK if dsn_set else _FAIL} DATABASE_URL set")
    all_ok &= dsn_set

    table_ok = False
    if dsn_set:
        try:
            with get_db_context() as session:
                session.execute(text("SELECT 1 FROM relay_approval_queue LIMIT 1"))
            table_ok = True
        except Exception as exc:
            _line(f"  {_FAIL} relay_approval_queue not reachable: {exc}")
        else:
            _line(f"  {_OK} relay_approval_queue table reachable")
    all_ok &= table_ok

    status = get_kill_switch_status(venture.kill_switch_feature)
    color = status.get("color", "unknown")
    icon = _OK if color in ("green", "unknown") else (_WARN if color == "yellow" else _FAIL)
    _line(f"  {icon} kill switch [{venture.kill_switch_feature}] = {color}")

    slack_configured = bool(settings.slack_bot_token and venture.relay_slack_channel)
    icon = _OK if slack_configured else _WARN
    _line(f"  {icon} Slack configured (channel={venture.relay_slack_channel or 'unset'} + bot token)")

    email_configured = bool(venture.relay_instantly_campaign_id)
    icon = _OK if email_configured else _WARN
    _line(f"  {icon} email channel provisioned (Instantly campaign id set)")

    mailbox_configured = bool(venture.relay_instantly_sender_email)
    icon = _OK if mailbox_configured else _WARN
    _line(f"  {icon} sending mailbox configured (relay_instantly_sender_email="
          f"{venture.relay_instantly_sender_email or 'unset'})")

    _line(
        f"  ·  send window {venture.relay_send_window_start:02d}:00-"
        f"{venture.relay_send_window_end:02d}:00 {venture.relay_send_window_timezone}, "
        f"ceiling {venture.relay_daily_ceiling}/channel/day, brand {venture.brand_name!r}"
    )

    # WP-T2-1 go-live checks (2026-09 review) — DNS auth, monitor/drain
    # registration, and the three closed-by-default send gates. Distinct
    # "verified"/"configured"/"unknown" states throughout: a check that
    # can't independently confirm correctness (DKIM, or any check before
    # mailbox_configured is even true) reports that honestly rather than a
    # false pass. These only really mean something for the FA Max venture,
    # but are harmless (and informative) to run for any venture with a
    # configured sender domain.
    _line("-" * 40)
    _line("WP-T2-1 go-live checks:")
    if mailbox_configured:
        domain = venture.relay_instantly_sender_email.split("@")[-1]
        spf = check_spf(domain)
        icon = _OK if spf["state"] == "verified" else _WARN
        _line(f"  {icon} SPF [{spf['state']}] {spf['detail']}")

        dmarc = check_dmarc(domain)
        icon = _OK if dmarc["state"] == "verified" else _WARN
        _line(f"  {icon} DMARC [{dmarc['state']}] {dmarc['detail']}")

        dkim = check_dkim(domain)
        _line(f"  {_WARN} DKIM [{dkim['state']}] {dkim['detail']}")
    else:
        _line(f"  {_WARN} SPF/DMARC/DKIM [skipped] no sending mailbox configured yet")

    cron = check_cron_registered([
        "src.tasks.fa_max_send_health_monitor",
        "src.tasks.fa_max_exceptions_alert_drain",
    ])
    icon = _OK if cron["state"] == "verified" else _WARN
    _line(f"  {icon} monitor + drain worker cron registration [{cron['state']}] {cron['detail']}")

    exceptions_channel_configured = bool(
        settings.slack_bot_token and getattr(settings, "fa_max_slack_channel_exceptions", "")
    )
    icon = _OK if exceptions_channel_configured else _WARN
    _line(
        f"  {icon} FA Max EXCEPTIONS Slack lane configured "
        f"(fa_max_slack_channel_exceptions="
        f"{getattr(settings, 'fa_max_slack_channel_exceptions', '') or 'unset'})"
    )

    gates = check_go_live_flags(settings)
    # Code-review finding (2026-09): this used to flip all_ok (and so
    # --health's own exit code) to False whenever gates were open -- which
    # meant --health, run as a routine post-launch diagnostic, would report
    # "failing" for the CORRECT, intentional post-go-live state. --health
    # answers "is Relay's plumbing working," not "should we go live" — it
    # stays a pure diagnostic (icon-only, informational) regardless of gate
    # state. --go-live-readiness (see cmd_go_live_readiness below) is the
    # actual scriptable readiness gate, and is the only thing that should
    # fail a script/CI step over unmet launch requirements.
    icon = _OK if gates["state"] == "closed" else _WARN
    _line(f"  {icon} send gates [{gates['state']}] {gates['detail']}")

    _line("-" * 40)
    if all_ok:
        _line(f"{_OK} Relay scaffolding healthy\n")
    else:
        _line(f"{_FAIL} Relay scaffolding has failing checks\n")
    return 0 if all_ok else 1


def check_go_live_readiness(
    *, mailbox_configured: bool, spf_state: str, dmarc_state: str,
    cron_state: str, exceptions_channel_configured: bool, send_mode: str,
    backlog_release_confirmed: bool,
) -> dict:
    """Pre-launch readiness verdict, distinct from --health's diagnostic
    output (code-review finding, 2026-09). This is the one that's actually
    supposed to FAIL a script/CI step when a required, automatable check
    isn't satisfied -- --health's checks above are informational (WARN)
    precisely so this function can own the pass/fail decision instead of
    every diagnostic line independently deciding whether it's launch-
    blocking.

    Takes `send_mode` (fa_max_relay_send_mode's raw value) directly, NOT
    check_go_live_flags()'s aggregate "open"/"closed" state (code-review
    finding, second round, 2026-09 — a real bug in the first version of
    this function). check_go_live_flags() reports "open" if ANY of the
    three gates is set, including fa_max_10dlc_registered alone with
    fa_max_relay_send_mode still "fake" -- a legitimate, common PRE-launch
    state (confirming 10DLC registration ahead of flipping send mode, per
    docs/fa-max-go-live.md Step 3, which explicitly precedes Step 5's mode
    flip). Treating that as "already_live" skipped every remaining
    readiness check for a venture that wasn't actually sending anything.

    "already_live" ALSO requires backlog_release_confirmed (code-review
    finding, third round, 2026-09 — a second bug in this same function):
    guards.py's evaluate() gates dispatch on BOTH fa_max_relay_send_mode ==
    "live" AND fa_max_send_backlog_release_confirmed being true (see
    guards.py's two sequential DEFER checks) -- send_mode alone does not
    make the venture live, nothing dispatches until both are set. Reported
    "already_live" for send_mode="live" with backlog_release_confirmed=
    False and every other check missing, which is wrong twice over: the
    venture isn't actually live (Relay is deferring every item), AND a real
    launch-blocking misconfiguration (mode flipped without the deliberate
    backlog-release step docs/fa-max-go-live.md Step 5 requires) was
    reported as a completed, healthy launch instead of a blocker.

    If already live (both flags true), a "not ready" verdict is meaningless
    (the launch already happened) -- reports "already_live" instead of
    re-litigating readiness for a decision that's done. Any other
    combination -- including this mode/backlog mismatch -- runs the normal
    blocker list, with the mismatch itself added as an explicit blocker.

    DKIM and the mechanics of the approved-backlog review are deliberately
    NOT included as automatable blockers: neither is automatable (see
    check_dkim's own docstring and docs/fa-max-go-live.md Step 5) -- a
    check that can't be verified by code must not silently count as passed
    just because it's absent from this function's blocker list. The
    backlog_release_confirmed FLAG's state, unlike the review it's supposed
    to attest to, is directly readable, so its inconsistency with send_mode
    specifically is checked.
    """
    if send_mode == "live" and backlog_release_confirmed:
        return {
            "state": "already_live",
            "detail": "fa_max_relay_send_mode is \"live\" and "
                      "fa_max_send_backlog_release_confirmed is true — this venture "
                      "is live; re-running readiness checks is no longer meaningful",
        }
    blockers = []
    if send_mode == "live" and not backlog_release_confirmed:
        blockers.append(
            "fa_max_relay_send_mode is \"live\" but fa_max_send_backlog_release_confirmed "
            "is false — Relay is deferring every FA Max item until both are set together "
            "(see docs/fa-max-go-live.md Step 5); this is a real misconfiguration, not a "
            "completed launch"
        )
    if not mailbox_configured:
        blockers.append("no sending mailbox configured")
    if spf_state != "verified":
        blockers.append(f"SPF not verified (state={spf_state})")
    if dmarc_state != "verified":
        blockers.append(f"DMARC not verified (state={dmarc_state})")
    if cron_state != "verified":
        blockers.append(f"monitor/drain worker cron not fully registered (state={cron_state})")
    if not exceptions_channel_configured:
        blockers.append("FA Max EXCEPTIONS Slack lane not configured")
    if blockers:
        return {"state": "not_ready", "detail": "; ".join(blockers)}
    return {
        "state": "ready",
        "detail": "all automatable pre-launch checks pass — DKIM verification and the "
                  "approved-backlog review still require manual confirmation "
                  "per docs/fa-max-go-live.md before flipping the send gates",
    }


def cmd_go_live_readiness(venture_key: str) -> int:
    """The actual scriptable go/no-go gate for WP-T2-1 (code-review finding,
    2026-09) — separate from --health, which stays a pure diagnostic whose
    exit code is unaffected by go-live/gate state either way."""
    from config.settings import get_settings
    from src.utils.venture_config import get_venture_config

    settings = get_settings()
    venture = get_venture_config(venture_key)

    _line(f"\nRelay — go-live readiness [venture={venture_key}]")
    _line("-" * 40)

    mailbox_configured = bool(venture.relay_instantly_sender_email)
    spf_state = dmarc_state = "missing"
    if mailbox_configured:
        domain = venture.relay_instantly_sender_email.split("@")[-1]
        spf_state = check_spf(domain)["state"]
        dmarc_state = check_dmarc(domain)["state"]

    cron_state = check_cron_registered([
        "src.tasks.fa_max_send_health_monitor",
        "src.tasks.fa_max_exceptions_alert_drain",
    ])["state"]

    exceptions_channel_configured = bool(
        settings.slack_bot_token and getattr(settings, "fa_max_slack_channel_exceptions", "")
    )

    readiness = check_go_live_readiness(
        mailbox_configured=mailbox_configured, spf_state=spf_state, dmarc_state=dmarc_state,
        cron_state=cron_state, exceptions_channel_configured=exceptions_channel_configured,
        send_mode=getattr(settings, "fa_max_relay_send_mode", "fake"),
        backlog_release_confirmed=getattr(settings, "fa_max_send_backlog_release_confirmed", False),
    )
    icon = _OK if readiness["state"] in ("ready", "already_live") else _FAIL
    _line(f"  {icon} [{readiness['state']}] {readiness['detail']}")
    _line("-" * 40)
    return 0 if readiness["state"] in ("ready", "already_live") else 1


def cmd_sweep(venture_key: str) -> int:
    from src.services.relay.sweep import run_sweep

    result = run_sweep(venture_key=venture_key)
    _line(
        f"sweep[{venture_key}]: sent={result.sent} skipped={result.skipped} "
        f"failed={result.failed} halted={result.halted} "
        f"processed={len(result.processed_ids)}"
    )
    return 0 if not result.halted else 1


def cmd_setup_email_channel(venture_key: str) -> int:
    from src.utils.venture_config import get_venture_config
    from src.services import instantly_service as instantly
    from src.services.relay.channels_email import PASSTHROUGH_CAMPAIGN_NAME

    venture = get_venture_config(venture_key)
    # One campaign per venture — Instantly's duplicate-contact guard is
    # per-campaign, so a shared campaign would make venture B's first email
    # to a prospect look like a repeat of venture A's and fail the send.
    # Venture #1 keeps the original unsuffixed name so re-running this
    # command still FINDS its existing live campaign instead of creating a
    # second one and orphaning the id already set in .env.
    campaign_name = (
        PASSTHROUGH_CAMPAIGN_NAME if venture_key == DEFAULT_VENTURE_KEY
        else f"{PASSTHROUGH_CAMPAIGN_NAME} — {venture_key}"
    )

    if not venture.relay_instantly_sender_email:
        _line(f"{_FAIL} venture {venture_key} has no relay_instantly_sender_email configured")
        return 2

    existing = [
        c for c in instantly.list_campaigns()
        if c.get("name") == campaign_name
    ]
    if existing:
        campaign_id = existing[0].get("id")
        _line(f"{_OK} found existing passthrough campaign id={campaign_id}")
    else:
        schedule = {
            "schedules": [{
                "name": "always-on",
                "timing": {"from": "00:00", "to": "23:59"},
                "days": {str(i): True for i in range(7)},
                "timezone": "America/Detroit",
            }],
        }
        sequence_steps = [{
            "type": "email",
            "delay": 0,
            "variants": [{"subject": "{{ra_subject}}", "body": "{{ra_body}}"}],
        }]
        result = instantly.create_campaign(
            name=campaign_name,
            schedule=schedule,
            sequence_steps=sequence_steps,
            email_list=[venture.relay_instantly_sender_email],
        )
        if not result or not result.get("id"):
            _line(f"{_FAIL} campaign creation failed — check INSTANTLY_API_KEY/INSTANTLY_ENABLED and logs")
            return 1
        campaign_id = result["id"]
        instantly.activate_campaign(campaign_id)
        _line(f"{_OK} created + activated passthrough campaign id={campaign_id}")

    if venture_key == DEFAULT_VENTURE_KEY:
        _line(f"\nSet this in .env, then restart Relay:\n  RELAY_INSTANTLY_CAMPAIGN_ID={campaign_id}\n")
    else:
        _line(
            f"\nSet this on the venture row, then restart Relay:\n"
            f"  UPDATE ventures SET relay_instantly_campaign_id = '{campaign_id}' "
            f"WHERE venture_key = '{venture_key}';\n"
        )
    return 0


def cmd_seed(args: argparse.Namespace) -> int:
    from src.services.relay import queue as relay_queue
    from src.services.relay.slack_post import post_for_approval

    try:
        payload = json.loads(args.payload_json) if args.payload_json else {}
    except Exception as exc:
        _line(f"{_FAIL} --payload-json is not valid JSON: {exc}")
        return 2

    idempotency_key = args.idempotency_key or f"seed-{uuid.uuid4().hex[:16]}"
    item = relay_queue.enqueue(
        idempotency_key=idempotency_key,
        channel=args.channel,
        recipient=args.recipient,
        payload=payload,
        thread_id=args.thread_id,
        venture_key=args.venture,
        # --seed is a founder/dev manual-testing tool, not real Cora/THROUGH
        # traffic -- it has always allowed --thread-id to be omitted
        # (QUALITY-v2.2 Q3's contract guard would otherwise reject that).
        skip_contract_validation=True,
    )
    post_for_approval(item)
    _line(
        f"{_OK} seeded item id={item.id} venture={item.venture_key} "
        f"idempotency_key={item.idempotency_key!r} status={item.status}"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m src.services.relay",
        description="Relay — deterministic execution service (non-agent)",
    )
    parser.add_argument("--health", action="store_true", help="Run scaffolding health check and exit")
    parser.add_argument(
        "--go-live-readiness", action="store_true",
        help="WP-T2-1: check whether automatable pre-launch requirements are met (exit 1 if not, "
             "distinct from --health, whose exit code is a pure scaffolding diagnostic and does "
             "not reflect go-live readiness) and exit",
    )
    parser.add_argument(
        "--sweep", action="store_true",
        help="Run one approval-queue sweep (execute all 'approved' rows as a batch) and exit",
    )
    parser.add_argument(
        "--seed", action="store_true",
        help="Seed one 'pending' row and post it to Slack for approval (Cora's Phase 2 stand-in) and exit",
    )
    parser.add_argument(
        "--post-pending-fa-max", action="store_true",
        help="Retry pending FA Max Slack cards that have not been posted",
    )
    parser.add_argument(
        "--setup-email-channel", action="store_true",
        help="Find or create the Relay passthrough Instantly campaign and print its id (one-time setup) and exit",
    )
    parser.add_argument(
        "--venture", default=DEFAULT_VENTURE_KEY,
        help=f"Venture key to operate on (default: {DEFAULT_VENTURE_KEY}). Scopes "
             "--sweep's batch, --seed's new row, --health's report and "
             "--setup-email-channel's campaign.",
    )
    parser.add_argument("--channel", default="noop", help="Channel for --seed (default: noop)")
    parser.add_argument("--recipient", help="Recipient for --seed (email/phone)")
    parser.add_argument("--payload-json", help="JSON payload for --seed, e.g. '{\"subject\": \"Hi\"}'")
    parser.add_argument("--thread-id", help="Opportunity Thread ID for --seed (optional)")
    parser.add_argument("--idempotency-key", help="Idempotency key for --seed (default: random)")
    args = parser.parse_args(argv)

    setup_logging()

    if args.health:
        return cmd_health(args.venture)

    if args.go_live_readiness:
        return cmd_go_live_readiness(args.venture)

    if args.sweep:
        return cmd_sweep(args.venture)

    if args.post_pending_fa_max:
        from src.services.relay.slack_post import post_unposted_fa_max_cards
        _line(f"FA Max pending cards checked: {post_unposted_fa_max_cards()}")
        return 0

    if args.seed:
        if not args.recipient:
            parser.error("--seed requires --recipient")
        return cmd_seed(args)

    if args.setup_email_channel:
        return cmd_setup_email_channel(args.venture)

    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
