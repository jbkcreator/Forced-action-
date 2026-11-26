"""R1 0.6 dependency policy: one row per outside service lending depends on, plus the opt-out feeds and lead feeds
whose freshness decides whether automated outbound may run.

Each dependency row records its request timeout (the clients read it from here; values are the ones the clients
used before this table existed), what happens when it is down, and the restart rule of the units that call it.

Each opt-out feed row records how often it runs, how old its last success may be before lending can no longer
verify suppression (3 missed runs, at least 5 minutes), and the automated channels it protects. A stale feed pauses
those channels (``src.lending.dependency_health.outbound_allowed``) while ``LENDING_FAILCLOSED_PAUSE_ENABLED`` is on;
off, it only alerts that it would have paused.

A leaf module: no imports from ``src``, so any client can read it.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

# ── Automated outbound channels the pause can stop ────────────────────────────
# The first three are the 0.4 breaker channels (same names); ghl_crm = writes to GHL that can start a GHL
# workflow (tags, stage moves, notes); dialer = loading numbers into BatchDialer or GHL's call list.
CHANNEL_GHL_SMS = "ghl_sms"
CHANNEL_GHL_EMAIL = "ghl_email"
CHANNEL_INSTANTLY = "instantly"
CHANNEL_GHL_CRM = "ghl_crm"
CHANNEL_DIALER = "dialer"
ALL_CHANNELS: tuple[str, ...] = (CHANNEL_GHL_SMS, CHANNEL_GHL_EMAIL, CHANNEL_INSTANTLY, CHANNEL_GHL_CRM, CHANNEL_DIALER)
EMAIL_CHANNELS: tuple[str, ...] = (CHANNEL_GHL_EMAIL, CHANNEL_INSTANTLY)

# Every lending systemd unit restarts itself; a guard test keeps it that way.
RESTART_POLICY = "systemd Restart=always (RestartSec=10-15) on every lending unit in deploy/systemd"

FEED_PULL = "pull"    # we fetch it on a schedule: its age is measurable
FEED_PUSH = "push"    # it is sent to us: quiet can be normal, so no timer
FEED_NONE = "none"    # a service we call on demand; no feed


@dataclass(frozen=True)
class DependencyPolicy:
    key: str
    label: str
    timeout_s: Optional[float]
    fallback: str
    quick_timeout_s: Optional[float] = None
    enforced: bool = True  # False: the client is not on dev yet (Clay, PR #480)
    restart: str = RESTART_POLICY


DEPENDENCIES: tuple[DependencyPolicy, ...] = (
    DependencyPolicy("ghl", "GoHighLevel", 15, quick_timeout_s=4,
                     fallback="Sends are single attempts, never retried; an unknown outcome is held (0.4). Its "
                              "opt-out feed going stale pauses every automated channel."),
    DependencyPolicy("batchdialer", "BatchDialer", 20, quick_timeout_s=5,
                     fallback="An unconfirmed create is held, never retried blind; a refused load loads nothing."),
    DependencyPolicy("clay", "Clay", 20, enforced=False,
                     fallback="Email gap-fill skipped on error (client on PR #480; enforced once it merges)."),
    DependencyPolicy("propertyradar", "PropertyRadar", 90,
                     fallback="A failed pull is retried by the next run; a pull older than 26 h loads nothing and "
                              "posts the data's age. Old rows are never re-staged."),
    DependencyPolicy("sunbiz", "Sunbiz SFTP", 60,
                     fallback="The job exits and the --retry cron run plus the 3-day catch-up fetch it later."),
    DependencyPolicy("lendingflow", "LendingFlow", None,
                     fallback="Inbound only: the lead is stored before anything else. No lead in 4 business hours "
                              "raises an alert; 0.5's nightly reconcile recovers failed deliveries."),
    DependencyPolicy("tracerfy", "Tracerfy DNC scrub", 30,
                     fallback="A failed scrub blocks every phone in it (SCRUB_FAILED): nothing unscrubbed loads."),
    DependencyPolicy("telnyx", "Telnyx line-type lookup", 10,
                     fallback="A failed lookup blocks the text (0.11)."),
    DependencyPolicy("instantly", "Instantly", 20,
                     fallback="Sends are single attempts; an unknown outcome is held (0.4). Its reply webhook is a "
                              "push feed covered by the 0.3 door watchdog and Instantly's own unsubscribe list."),
)
BY_KEY: dict[str, DependencyPolicy] = {d.key: d for d in DEPENDENCIES}


def timeout(key: str) -> float:
    """The request timeout (seconds) a client uses for ``key``."""
    value = BY_KEY[key].timeout_s
    if value is None:
        raise KeyError(f"{key} has no request timeout (inbound only)")
    return value


def quick_timeout(key: str) -> float:
    value = BY_KEY[key].quick_timeout_s
    if value is None:
        raise KeyError(f"{key} has no quick timeout")
    return value


# ── Opt-out feeds: freshness decides whether suppression can be verified ──────

MIN_STALE_AFTER_S = 5 * 60
MISSED_RUNS = 3


def stale_after(run_every_s: int) -> int:
    """3 missed runs, at least 5 minutes: one slow run never pauses sends."""
    return max(MISSED_RUNS * run_every_s, MIN_STALE_AFTER_S)


@dataclass(frozen=True)
class FeedPolicy:
    key: str  # the lending.dependency_health row the feed writes
    label: str
    kind: str
    run_every_s: Optional[int]
    protects: tuple[str, ...]  # channels paused while this feed is stale
    note: str = ""

    @property
    def stale_after_s(self) -> Optional[int]:
        return stale_after(self.run_every_s) if self.kind == FEED_PULL and self.run_every_s else None

    @property
    def timed(self) -> bool:
        return self.stale_after_s is not None and bool(self.protects)


FEED_GHL_DND_BACKSTOP = "ghl_dnd_backstop"
FEED_FA_OPT_OUT_POLLER = "fa_opt_out_poller"
FEED_IMAP_OPT_OUT = "imap_opt_out"
FEED_GHL_DND_PUSH = "ghl_dnd_push"
FEED_INSTANTLY_REPLIES = "instantly_reply_webhook"
FEED_LENDINGFLOW_INBOUND = "lendingflow_inbound"

OPT_OUT_FEEDS: tuple[FeedPolicy, ...] = (
    FeedPolicy(FEED_GHL_DND_BACKSTOP, "GHL opt-out sync (DND backstop)", FEED_PULL, 15 * 60, ALL_CHANNELS,
               "A STOP made inside GHL reaches lending only through this pull (and GHL's webhook)."),
    FeedPolicy(FEED_FA_OPT_OUT_POLLER, "FA opt-out poller", FEED_PULL, 15, ALL_CHANNELS,
               "FA-side opt-outs (texts, calls, emails) mirrored into lending."),
    FeedPolicy(FEED_IMAP_OPT_OUT, "Opt-out mailbox reader", FEED_PULL, 5 * 60, EMAIL_CHANNELS,
               "Email opt-outs only, so only email channels pause."),
    FeedPolicy(FEED_GHL_DND_PUSH, "Lending opt-outs pushed to GHL DND", FEED_PULL, 15, (),
               "Recorded for visibility; the GHL senders re-read DND themselves before a cold send."),
    FeedPolicy(FEED_INSTANTLY_REPLIES, "Instantly reply webhook", FEED_PUSH, None, (),
               "Push feed, no timer: the 0.3 watchdog checks the door; Instantly suppresses unsubscribes itself."),
)
FEEDS_BY_KEY: dict[str, FeedPolicy] = {f.key: f for f in OPT_OUT_FEEDS}


def feeds_protecting(channel: str) -> tuple[FeedPolicy, ...]:
    """The timed feeds whose staleness pauses ``channel``."""
    return tuple(f for f in OPT_OUT_FEEDS if f.timed and channel in f.protects)


# ── Lead feeds ────────────────────────────────────────────────────────────────

# A PropertyRadar pull older than this is never loaded into the dialer (one daily pull + 2 h slack).
PROPERTYRADAR_MAX_AGE_HOURS = 26

# No LendingFlow lead for this many business hours = alert (client question 28 confirms the window).
LENDINGFLOW_SILENCE_BUSINESS_HOURS = 4
BUSINESS_TZ = "America/New_York"
BUSINESS_DAY_START_HOUR = 9
BUSINESS_DAY_END_HOUR = 17  # Monday-Friday, 9:00-17:00 ET
