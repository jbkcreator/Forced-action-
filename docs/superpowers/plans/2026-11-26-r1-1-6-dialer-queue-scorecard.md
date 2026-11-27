# R1 1.6 David's Dialer Queue Check & 2:45 PM Caller Scorecard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every weekday David gets two posts: at 6 AM ET, an honest count of who he can actually call today, or a replenish alert naming each lead source that ran dry, never a padded list. At 2:45 PM ET, a scorecard of his day, with a Slack notes form for each connected call that has no recording.

**Architecture:**
- **Queue check (`queue_manager.py`).** A read-only check. It reads David's callbacks and warm leads with 1.3's readers, and the active `dialer_load_records` per pool. It removes duplicate phones in a fixed source order and drops anything the compliance rules block for the whole day. It does not push anything to BatchDialer: the existing loaders keep doing that.
- **Scorecard (`scorecard.py`).** Reuses `david_activity.batchdialer_counts` and `stage_counts`, plus one query per call for quality, top 10 and unrecorded calls.
- **Notes form (`call_notes.py`).** Rides on the lending Slack app's existing `/webhooks/lending/slack-interactivity` route. It saves into `lending.call_extractions`, marked `source='caller_notes'`.

**Tech Stack:** Python 3.11, SQLAlchemy `text()`, FastAPI (lending-api), Slack Block Kit via `slack_sdk.WebClient`, pytest with SQLite and an attached `lending` schema (the 1.3 test pattern).

**Spec:** [Release1_Engineering_Implementation_Directive.md](../../../task_specs/Next-deal-lending/NDL_10-11-26/Release1_Engineering_Implementation_Directive.md), Item 1.6. Decisions from the 2026-11-26 planning session are listed below.

## Decisions this plan builds to

| # | Decision |
|---|---|
| D1 | The queue manager is a **readiness check**. It counts and alerts, and pushes nothing to BatchDialer. The existing loaders (`lending_dialer_load`, `lending_wholesaler_load`, `lending_pr_dialer_autoload`, `lendingflow_dialer`) stay as they are. |
| D2 | **"Exhausted"** comes from the compliance rules: suppression, the stored DNC/litigator verdict, total attempt history (`MAX_ATTEMPTS_TOTAL`=6 over 10 business days) and a stale scrub. The call window and the 3-per-24h cap clear up within the day, so they don't count against today. It is **not** `replacement_eligibility`, which is a LendingFlow vendor credit rule. |
| D3 | The **5-point quality score** is a provisional rubric (`provisional_v1`) of 5 yes/no checks. A call with no extraction row scores n/a. It is added to the tracker as a client question so Josh can confirm or replace it. |
| D4 | The **notes template** is a Slack modal opened from an "Add notes" button in the scorecard thread, one button per connected call that has no recording and no notes. It saves to `lending.call_extractions`. |
| D5 | The **scope** is David only: BatchDialer seat `LENDING_DAVID_BD_AGENT_ID`, channel `LENDING_DAVID_CHANNEL` (#isa-daily). The replenish alert also goes to `post_ops_notice`. |
| D6 | **Top 10:** connected calls, booked first, then talk time descending, then call id. |
| D7 | **Recording gate:** evidence only. BatchDialer plays the disclosure and controls recording, and no code here starts or stops it. The scorecard counts recorded calls with no `recording_disclosure_logged`. |
| D8 | **Overlap with 0.6 (decided 2026-11-26, Sarath owns it; Lesly informed):** 1.6 owns the eligible-queue count and the "no eligible work / source ran dry" alert. 0.6 calls `read_queue()` in its outage case instead of building a second rule. Tell Lesly. |
| D9 | **Two different "50"s.** The 1.3 morning list (Q13) fills Slack cards from the calling queue, and that is unchanged. 1.6's no-padding rule is about the dialer queue count. 1.6 never counts 1.3's `COLD_TOP` items; it reads the pools directly and applies the compliance blocks. |

## Global Constraints

- Branch: `feat/r1-1-6-dialer-queue-scorecard`, created from `origin/feat/r1-13-morning-list` (PR #480). The PR is stacked on #480, following the tracker's "Stacked on #…" convention, and is retargeted to `dev` once #480 merges. PR title: `[Next Deal Lending · Release 1] feat(lending): R1 1.6 David's dialer queue check & 2:45 PM caller scorecard`.
- From #480, import only `read_callbacks`, `read_warm_leads`, `ListItem` and the `config.lending_morning_list` constants, so rebases stay cheap. Do not edit #480's files.
- Every switch is off by default: `LENDING_DIALER_QUEUE_CHECK_ENABLED`, `LENDING_CALLER_SCORECARD_ENABLED`.
- Times are ET via `ZoneInfo(DEFAULT_TZ)`. Each cron has two UTC lines and the task acts only in its ET hour, the same pattern as `lending_david_summary`. Weekdays only (Mon–Fri).
- Never log or post a full phone number: last 4 digits only. No `print()` in `src/`. Phones go through `phone_utils.normalize`.
- SQL is `text()` with bind parameters. One query per source, never a query inside a loop.
- An unset channel or token goes through `slack_channels.report_unset`.
- Migrations are new scripts in `migrations/`, are idempotent, and come with a matching `src/lending/models.py` update.
- Tests never touch the shared DB. Compliance functions use Postgres-only SQL, so the queue reader takes an injected `blocks` callable.

## Review Focus

1. **A 6 AM run must not mark everyone ineligible.** The call window and the 24h cap block everyone at 6 AM, so they must be ignored. Test: `test_call_window_and_daily_cap_do_not_count_as_ineligible`.
2. **A source that fails to read must not be reported as dry.** It shows as "not available" and is left out of the dry list, while the total is still computed from the sources that did read. Test: `test_failed_source_is_not_available_not_dry`.
3. **The same phone in several sources is counted once,** in the first source by `QUEUE_SOURCES` order. A callback that is also in a pool counts as a callback. Test: `test_phone_in_two_sources_counts_once_in_the_first`.
4. **The notes form must never overwrite a transcript extraction,** and only allowed users may submit it. Tests: `test_notes_refused_when_call_has_transcript_extraction`, `test_notes_refused_for_user_not_allowed`.
5. **A day with zero calls still posts a scorecard with zeros.** Weekends post nothing. Daylight saving picks exactly one of the two cron lines. Tests: `test_zero_call_day_renders_zeros`, `test_task_acts_only_at_14_et_on_weekdays`.

---

### Task 1: Branch, config, settings

**Files:**
- Create: `config/lending_dialer_queue.py`
- Modify: `config/settings.py`, next to `lending_david_channel`
- Test: `tests/lending/test_dialer_queue_config.py`

**Interfaces:**
- Produces, in `config/lending_dialer_queue.py`:
  - `QUEUE_TARGET = 50`
  - `QUEUE_CHECK_HOUR_ET = 6`
  - `SCORECARD_HOUR_ET = 14`
  - `WORKDAYS = frozenset({0, 1, 2, 3, 4})`
  - `CALLBACKS = "callbacks"`, `LF_LEADS = "lendingflow_leads"`, `WEB_LEADS = "web_leads"`
  - `QUEUE_SOURCES: tuple[str, ...] = (CALLBACKS, LF_LEADS, WEB_LEADS, *DIAL_ORDER, LENDINGFLOW_POOL, WEBSITE_POOL)`
  - `SOURCE_LABELS: dict[str, str]`: "Callbacks", "New LendingFlow leads", "New website leads", "Partners", "Wholesalers", "Transaction ready", "Builders", "Verified maturity", "LendingFlow (dialer)", "Website (dialer)"
  - `TRANSIENT_BLOCKS = frozenset({ReasonCode.OUTSIDE_CALL_WINDOW.value, ReasonCode.ATTEMPT_CAP_REACHED.value})`
  - `QUALITY_RUBRIC_VERSION = "provisional_v1"`
  - `QUALITY_CHECKS: tuple[str, ...] = ("disposition logged", "2+ min talk", "next action", "decision maker", "objection or deal status")`
  - `TOP_CALLS = 10`, `NOTES_LIST_MAX = 20`, `IN_USE_DAYS = 14`
  - `NOTES_OPEN_ACTION_ID = "call_notes_open"`, `NOTES_CALLBACK_ID = "call_notes_submit"`
- Produces, in settings: `lending_dialer_queue_check_enabled: bool = False` (`LENDING_DIALER_QUEUE_CHECK_ENABLED`) and `lending_caller_scorecard_enabled: bool = False` (`LENDING_CALLER_SCORECARD_ENABLED`).

- [ ] **Step 1:** Create the branch:
  ```bash
  git fetch origin && git checkout -b feat/r1-1-6-dialer-queue-scorecard origin/feat/r1-13-morning-list
  ```
- [ ] **Step 2:** Write the failing test.
  ```python
  def test_every_source_has_a_label_and_sources_are_unique():
      assert len(set(QUEUE_SOURCES)) == len(QUEUE_SOURCES)
      assert set(SOURCE_LABELS) == set(QUEUE_SOURCES)

  def test_callbacks_and_warm_leads_come_before_pools():
      assert QUEUE_SOURCES[:3] == (CALLBACKS, LF_LEADS, WEB_LEADS)

  def test_rubric_has_five_checks():
      assert len(QUALITY_CHECKS) == 5
  ```
- [ ] **Step 3:** Run `pytest tests/lending/test_dialer_queue_config.py -v`. Expected: FAIL with an ImportError.
- [ ] **Step 4:** Implement the config module and the two settings.
- [ ] **Step 5:** Run the test again. Expected: PASS.
- [ ] **Step 6:** Commit: `feat(lending): R1 1.6 dialer queue and scorecard config`.

### Task 2: Queue readiness check (`queue_manager.py`)

**Files:**
- Create: `src/lending/dialer/__init__.py` (empty), `src/lending/dialer/queue_manager.py`
- Test: `tests/lending/test_dialer_queue_manager.py`. Use SQLite with an attached `lending` schema, as `tests/lending/test_morning_list_digest.py` does. Tables: `call_dispositions`, `lendingflow_leads`, `lendingflow_enrichment`, `web_leads`, `dialer_load_records`.

**Interfaces:**
- Consumes:
  - from #480: `read_callbacks(db, now) -> list[ListItem]`, `read_warm_leads(db, now, *, lead_days) -> list[ListItem]` (refs prefixed `lf:` / `web:`), and `JOSH_CALLER_PREFIXES`
  - `compliance.dial_blocks`, `compliance.suppressed_phones`, `compliance.scrub_verdict_blocks`
- Produces:
  ```python
  Blocks = Callable[[Session, list[str], datetime], dict[str, str]]   # phone -> reason code value

  @dataclass(frozen=True)
  class SourceCount:
      source: str; loaded: int; eligible: int
      blocked: Mapping[str, int]            # reason -> count
      available: bool = True
      staged: int = 0                       # PropertyRadar records staged for the 9:05 ET autoload (pools only)
      in_use: bool = True                   # pools: a load row created in the last IN_USE_DAYS, or staged > 0

  @dataclass(frozen=True)
  class QueueReport:
      day: date; target: int; sources: tuple[SourceCount, ...]
      eligible_total: int                   # property: sum over available sources (staged not counted)
      dry_sources: tuple[str, ...]          # property: POOLS only, available and in_use and eligible == 0 and staged == 0
      short: bool                           # property: eligible_total < target

  def compliance_blocks(db: Session, phones: list[str], now: datetime) -> dict[str, str]
  def read_queue(db: Session, now: datetime, *, lead_days: int,
                 blocks: Blocks = compliance_blocks, target: int = QUEUE_TARGET) -> QueueReport
  def render_queue_post(report: QueueReport) -> str
  ```
- `compliance_blocks` combines `dial_blocks(..., now=now)` with reasons in `TRANSIENT_BLOCKS` removed, `suppressed_phones` (reason `"SUPPRESSED"`) and `scrub_verdict_blocks`, keeping the first reason per phone.
  - Reasons are stored as `ReasonCode(...).value` strings.
  - Known limit: `dial_blocks` reports only one reason per phone. A phone at the 24h cap that is also exhausted shows as the transient cap and so counts as eligible. Exhausted phones are already deactivated by `on_attempt_recorded` (`active=false`), so this is rare. Note it in a `shortcut:` comment.
- `read_queue`:
  - Callbacks are kept only when `caller_name` does not start with any `JOSH_CALLER_PREFIXES`.
  - Pools are read in one query: `SELECT pool, phone FROM lending.dialer_load_records WHERE active AND pool IN :pools`.
  - Each source is read in its own `db.begin_nested()`. A failure gives `available=False` and logs the exception class only.
  - Phones are deduplicated across sources in `QUEUE_SOURCES` order, and `blocks` is called **once** with every phone.
  - A `NO_FRESH_SCRUB` block is dropped for phones in `compliance.lendingflow_lead_phones`: by client decision, LendingFlow leads need no DNC scrub. `dial_blocks` already exempts them only once they are loaded into the dialer.
  - Pool usage is read in one query: `SELECT pool, count(*) FROM lending.dialer_load_records WHERE created_at >= :since GROUP BY pool`, with `since = now - IN_USE_DAYS`.
  - The staged count comes from `src.tasks.lending_pr_dialer_autoload.staged_pr_records(db)`, grouped by `pool`. It is read in its own savepoint, and a failure leaves `staged=0`.
  - Callbacks and warm leads are inbound and are never "dry" (zero is a normal day). Only pools can be named as dry.
  - A pool not in use is shown as "not in use", not as dry.
- `render_queue_post`:
  - The headline is "Dialer queue ready: N eligible" when the queue is full, or ":warning: Only N of 50 eligible — replenish:" when it is short.
  - When N == 0, the headline is ":no_entry: No eligible work today".
  - Then one line per source: label, eligible count of loaded count, the top block reasons, "+N load at 9:05" when staged, and "DRY", "not in use" or "not available".
  - Phones never appear in the text.

- [ ] **Step 1:** Write the failing tests. `now` is 2026-11-25 11:00 UTC (6 AM ET). The fake `blocks` returns a dict built per test.
  ```python
  def test_short_queue_names_every_dry_source(engine): ...
      # seed: 3 wholesalers rows, 0 builders rows, 1 David callback
      report = read_queue(db, NOW, lead_days=14, blocks=lambda *_: {})
      assert report.eligible_total == 4 and report.short
      assert "builders" in report.dry_sources and "wholesalers" not in report.dry_sources
      assert "Only 4 of 50 eligible" in render_queue_post(report)

  def test_blocked_phones_are_not_eligible_and_never_padded(engine): ...
      # 2 transaction_ready rows; blocks marks one ATTEMPT_HISTORY_EXCEEDED
      tr = next(s for s in report.sources if s.source == "transaction_ready")
      assert (tr.loaded, tr.eligible, dict(tr.blocked)) == (2, 1, {"ATTEMPT_HISTORY_EXCEEDED": 1})
      assert report.eligible_total == 1          # never topped up to 50

  def test_phone_in_two_sources_counts_once_in_the_first(engine): ...
      # same phone: callback (David) + wholesalers row
      assert counts["callbacks"].eligible == 1 and counts["wholesalers"].loaded == 0

  def test_josh_callbacks_are_not_davids(engine): ...   # caller_name "Josh Smith" -> not counted

  def test_failed_source_is_not_available_not_dry(engine): ...
      # drop lending.web_leads before reading
      web = counts["web_leads"]
      assert not web.available and "web_leads" not in report.dry_sources

  def test_zero_eligible_says_no_eligible_work(): ...
      assert render_queue_post(QueueReport(...all zero...)).startswith(":no_entry: No eligible work today")

  def test_call_window_and_daily_cap_do_not_count_as_ineligible(monkeypatch):
      # patch compliance.dial_blocks -> {p1: OUTSIDE_CALL_WINDOW, p2: ATTEMPT_CAP_REACHED, p3: NO_FRESH_SCRUB}
      # patch suppressed_phones -> set(), scrub_verdict_blocks -> {}
      assert compliance_blocks(None, ["p1", "p2", "p3"], NOW) == {"p3": "NO_FRESH_SCRUB"}

  def test_post_never_contains_a_phone(engine): ...   # no seeded phone digits in the text

  def test_lendingflow_lead_without_scrub_is_eligible(monkeypatch):
      # dial_blocks -> {p_lf: NO_FRESH_SCRUB, p_cold: NO_FRESH_SCRUB}; lendingflow_lead_phones -> {p_lf}
      assert compliance_blocks(None, ["p_lf", "p_cold"], NOW) == {"p_cold": "NO_FRESH_SCRUB"}

  def test_pool_with_no_recent_loads_is_not_in_use_not_dry(engine): ...
      # builders: one row created 30 days ago, inactive
      assert "builders" not in report.dry_sources and not counts["builders"].in_use

  def test_pool_with_staged_records_is_loading_not_dry(engine, monkeypatch): ...
      # transaction_ready: 0 active rows, staged_pr_records -> 5 records in that pool
      assert counts["transaction_ready"].staged == 5 and "transaction_ready" not in report.dry_sources
      assert "+5 load at 9:05" in render_queue_post(report)

  def test_zero_callbacks_is_never_dry(engine): ...
  ```
- [ ] **Step 2:** Run `pytest tests/lending/test_dialer_queue_manager.py -v`. Expected: FAIL with an ImportError.
- [ ] **Step 3:** Implement `queue_manager.py` to the interfaces above.
- [ ] **Step 4:** Run the tests again. Expected: PASS.
- [ ] **Step 5:** Commit: `feat(lending): R1 1.6 dialer queue readiness check`.

### Task 3: Morning queue-check task, cron, runbook

**Files:**
- Create: `src/tasks/lending_dialer_queue_check.py`
- Modify: `scripts/cron/crontab.txt`
- Test: `tests/lending/test_dialer_queue_check_task.py`

**Interfaces:**
- Consumes: `read_queue` and `render_queue_post` (Task 2), `_post_with_retry` and `report_unset` (the same helpers `lending_david_summary` uses), `post_ops_notice`.
- Produces: `main(argv=None, *, now: Optional[datetime] = None, post: Callable[[str], bool] | None = None) -> int`, with flags `--force` and `--dry-run`.
- Behaviour:
  - Exit 0 and do nothing unless the switch is on, the ET hour is `QUEUE_CHECK_HOUR_ET` and it is a `WORKDAYS` day (`--force` skips the hour and day checks).
  - Post to `LENDING_DAVID_CHANNEL` with the `LENDING_SLACK_BOT_TOKEN` bot. When `report.short`, also call `post_ops_notice(text)`.
  - `lead_days` comes from `settings.lending_morning_list_lead_days` (#480).

- [ ] **Step 1:** Write the failing tests: `test_acts_only_at_6_et_on_weekdays` (5:59 → nothing, 6:10 Tue → posts, 6:10 Sat → nothing), `test_short_queue_also_goes_to_ops`, `test_full_queue_does_not_ping_ops`, `test_switch_off_posts_nothing`. Inject `post` and a stubbed `read_queue`.
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3:** Implement the task.
- [ ] **Step 4:** Add the cron lines. They fire at 6:20 ET in winter and summer, and only one acts.
  ```
  # R1 1.6 David's dialer queue check (6:xx ET, Mon-Fri; off until LENDING_DIALER_QUEUE_CHECK_ENABLED). Two lines for EST/EDT.
  20 10,11 * * 1-5 $PROJECT/scripts/cron/run.sh src.tasks.lending_dialer_queue_check
  ```
- [ ] **Step 5:** Run the tests again. Expected: PASS.
- [ ] **Step 6:** Commit: `feat(lending): R1 1.6 morning dialer queue check task`.

### Task 4: Caller notes column and form (`call_notes.py`)

**Files:**
- Create: `migrations/apply_lending_call_extraction_source.py`
- Create: `src/lending/dialer/call_notes.py`
- No `models.py` change: `lending.call_extractions` has no ORM model, and the original table is migration-only (`apply_lending_call_extractions.py`). Tests create the table with the two new columns in their own DDL.
- Modify: `src/lending/soft_approval_webhook.py` in `slack_interactivity`: dispatch to call notes **after** the moved-Cora check and **before** the `lending_soft_approval_enabled` return
- Test: `tests/lending/test_call_notes.py`

**Interfaces:**
- Migration: `ALTER TABLE lending.call_extractions ADD COLUMN IF NOT EXISTS source VARCHAR(20) NOT NULL DEFAULT 'transcript'` and `ADD COLUMN IF NOT EXISTS entered_by VARCHAR(64)`.
- Produces:
  ```python
  def notes_button(dialer_call_id: str) -> dict                       # action_id NOTES_OPEN_ACTION_ID, value = call id
  def build_notes_view(*, dialer_call_id: str, channel: str, message_ts: str) -> dict   # callback_id NOTES_CALLBACK_ID
  def parse_notes(state_values: Mapping[str, Any]) -> tuple[Optional[CallExtraction], dict[str, str]]
  def allowed_note_users() -> frozenset[str]   # LENDING_DAVID_SLACK_USER_ID + LENDING_CORA_APPROVER_USER_IDS
  def save_notes(db: Session, *, dialer_call_id: str, extraction: CallExtraction, user_id: str,
                 now: datetime) -> Literal["saved", "has_transcript", "unknown_call"]
  def handle_notes_interaction(payload: dict) -> Optional[dict]       # None = not a notes payload
  ```
- Form fields are the `CallExtraction` fields **except** `kill_reason_verbatim`, which needs a transcript:
  - `next_action`, `deadline` (datepicker), `decision_maker`, `objection`
  - `deal_status` (static select from `DEAL_STATUSES`), `credit_band` (static select from `CREDIT_BANDS`)
  - `completed_projects_3y` (number), `has_liquidity` (select: yes / no / unknown)
  - `property_address`, `target_market`
  - `referral_names` and `missing_file_items` (multiline, one per line)
  - Every field is optional, but at least `next_action` is required.
- `save_notes`:
  - Returns `"unknown_call"` when the call id is not in `call_dispositions`.
  - Returns `"has_transcript"` when a `source='transcript'` row exists.
  - Otherwise it upserts with `source='caller_notes'` and `entered_by=user_id`, taking `phone` from `call_dispositions`.
- `handle_notes_interaction`:
  - `block_actions` with `NOTES_OPEN_ACTION_ID` from an allowed user calls `views.open` (`slack_client()` from soft_approval.slack_card).
  - `view_submission` with `NOTES_CALLBACK_ID` returns `{"response_action": "errors", ...}` for parse errors, a refused user, `has_transcript` or `unknown_call`, and returns `{}` once saved.
  - Both paths are gated by `lending_caller_scorecard_enabled`.
  - The route is async. Call the handler through `run_in_threadpool`, as `_open_form` is, so `views.open` and the DB save never block the event loop. The open has to happen inside Slack's 3-second `trigger_id` window.

- [ ] **Step 1:** Write the failing tests:
  ```python
  def test_parse_notes_requires_next_action(): ...
      ext, errors = parse_notes(state(next_action=""))
      assert ext is None and "next_action" in errors

  def test_notes_saved_as_caller_notes(engine):
      assert save_notes(db, dialer_call_id="c1", extraction=ext, user_id="U_DAVID", now=NOW) == "saved"
      assert row("c1") == ("caller_notes", "U_DAVID")

  def test_notes_refused_when_call_has_transcript_extraction(engine):
      # pre-seed call_extractions c1 source='transcript'
      assert save_notes(...) == "has_transcript" and row("c1")[0] == "transcript"

  def test_notes_refused_for_user_not_allowed(monkeypatch):
      resp = handle_notes_interaction(submission(user="U_OTHER"))
      assert resp["response_action"] == "errors"

  def test_other_payloads_are_not_ours():
      assert handle_notes_interaction({"type": "block_actions", "actions": [{"action_id": "x"}]}) is None
  ```
- [ ] **Step 2:** Run `pytest tests/lending/test_call_notes.py -v`. Expected: FAIL.
- [ ] **Step 3:** Write the migration, update the models, write `call_notes.py`, and add the webhook dispatch:
  ```python
  handled = await run_in_threadpool(handle_notes_interaction, payload)
  if handled is not None:
      return handled
  ```
- [ ] **Step 4:** Run the tests, then the existing soft-approval webhook tests: `pytest tests/lending -k "soft_approval or call_notes" -v`. Expected: PASS, with no change to soft-approval behaviour.
- [ ] **Step 5:** Commit: `feat(lending): R1 1.6 caller notes form for unrecorded calls`.

### Task 5: Scorecard data and rendering (`scorecard.py`)

**Files:**
- Create: `src/lending/dialer/scorecard.py`
- Test: `tests/lending/test_caller_scorecard.py`

**Interfaces:**
- Consumes:
  - `david_activity.batchdialer_counts(db, day, agent_id)`: `bd_dials`, `bd_connected`, `bd_booked_reported`
  - `david_activity.stage_counts(db, day) -> (booked, held)`
  - `LONG_CONVERSATION_SECONDS`, `BOOKED_CODE`
  - `dispositions.contact_view_url`
  - `notes_button` (Task 4)
- Produces:
  ```python
  @dataclass(frozen=True)
  class ScoredCall:
      dialer_call_id: str; phone_last4: str; talk_sec: int; disposition: Optional[str]
      recorded: bool; disclosure_logged: bool; dialer_contact_id: Optional[str]
      extraction: Optional[Mapping[str, Any]]; notes_source: Optional[str]

  def quality_score(call: ScoredCall) -> Optional[int]     # None when extraction is None; else 0..5 per QUALITY_CHECKS
  def top_calls(calls: Sequence[ScoredCall], n: int = TOP_CALLS) -> list[ScoredCall]
  def needs_notes(calls: Sequence[ScoredCall]) -> list[ScoredCall]   # talk_sec>0, not recorded, extraction None

  @dataclass(frozen=True)
  class Scorecard:
      day: date; as_of: datetime; dials: int; connects: int; long_conversations: int
      booked_stage: int; booked_reported: int; calls: tuple[ScoredCall, ...]; read_failed: bool

  def build_scorecard(db: Session, day: date, *, agent_id: str, as_of: datetime) -> Scorecard
  def render_scorecard(card: Scorecard) -> list[dict]       # main post blocks
  def render_notes_thread(card: Scorecard) -> list[dict]    # one section+button per needs_notes call, cap NOTES_LIST_MAX
  ```
- Quality checks, one point each:
  1. `disposition` is set
  2. `talk_sec >= LONG_CONVERSATION_SECONDS`
  3. `next_action` is set
  4. `decision_maker` is set
  5. any of `objection`, `kill_reason_verbatim` or `deal_status` is set
- Per-call query, run once:
  ```sql
  SELECT c.dialer_call_id, c.phone, c.talk_duration_sec, c.disposition, c.recording_status,
         c.recording_disclosure_logged, c.dialer_contact_id, e.fields, e.source
  FROM lending.call_dispositions c
  LEFT JOIN lending.call_extractions e ON e.dialer_call_id = c.dialer_call_id
  WHERE c.call_source = 'batchdialer' AND c.direction = 'outbound' AND c.caller_seat = :agent_id
    AND c.call_ended_at >= :start AND c.call_ended_at < :end
  ```
  `recorded` means `recording_status = 'recorded'`. The query runs in a savepoint; a failure sets `read_failed=True` and returns empty calls.
- The post shows:
  - "Dials (all dials in his campaigns)", "Connects", "Conversations 2+ min", "Booked (GHL stage, all callers) / he reported N"
  - "Call quality (provisional): 3.4 / 5 over 12 of 15 calls (3 without notes)"
  - "Recorded calls with no disclosure reported by the dialer: N". The flag is set only when the dialer event reports it (`dispositions.py`, `ev.disclosure_logged`), so this is worded as not reported, not as a compliance failure.
  - Top 10 lines: `…1234 · 6m · BOOKED · quality 4/5 · BatchDialer ↗`

- [ ] **Step 1:** Write the failing tests:
  ```python
  def test_quality_score_counts_five_checks():
      assert quality_score(call(disposition="BOOKED", talk=130, ext={"next_action": "send docs",
                         "decision_maker": "owner", "deal_status": "under_contract"})) == 5
      assert quality_score(call(disposition=None, talk=30, ext={})) == 0
      assert quality_score(call(ext=None)) is None

  def test_top_calls_booked_first_then_talk_time():
      ids = [c.dialer_call_id for c in top_calls([c1_talk600, c2_booked_talk60, c3_talk0])]
      assert ids == ["c2", "c1"]                     # unconnected calls left out

  def test_needs_notes_only_unrecorded_connected_without_notes(): ...

  def test_zero_call_day_renders_zeros():
      text = flatten(render_scorecard(empty_card))
      assert "Dials" in text and "0" in text and "quality" in text.lower()

  def test_build_scorecard_reads_only_davids_seat(engine): ...   # second seat's calls excluded
  def test_notes_thread_caps_at_20_and_says_how_many_more(): ...
  def test_no_full_phone_in_blocks(): ...
  ```
- [ ] **Step 2:** Run `pytest tests/lending/test_caller_scorecard.py -v`. Expected: FAIL.
- [ ] **Step 3:** Implement `scorecard.py`.
- [ ] **Step 4:** Run the tests again. Expected: PASS.
- [ ] **Step 5:** Commit: `feat(lending): R1 1.6 caller scorecard data and blocks`.

### Task 6: 2:45 PM scorecard task, cron, runbook, docs

**Files:**
- Create: `src/tasks/lending_caller_scorecard.py`
- Create: `docs/lending/dialer-queue-scorecard.md` (runbook)
- Modify: `scripts/cron/crontab.txt`, `CLAUDE.md` (one line under Lending)
- Test: `tests/lending/test_caller_scorecard_task.py`

**Interfaces:**
- Consumes: `build_scorecard`, `render_scorecard` and `render_notes_thread` (Task 5).
- Produces: `main(argv=None, *, now=None, client=None) -> int`, with flags `--force` and `--dry-run`.
- Behaviour:
  - Acts only when the switch is on, the ET hour is `SCORECARD_HOUR_ET` and it is a `WORKDAYS` day.
  - Needs `LENDING_DAVID_CHANNEL`, `LENDING_DAVID_BD_AGENT_ID` and `LENDING_SLACK_BOT_TOKEN`; any of them unset → `report_unset`.
  - Posts the main message. When `needs_notes` is not empty, it posts the notes thread under it with the same bot, so the buttons reach the lending app's interactivity URL.
- Runbook contents, also stated in the PR body:
  - **The post's count is the authoritative number.** BatchDialer has no remove-from-campaign endpoint (`campaign_remove` is `None`), and only opt-out or handed-to-GHL removals delete a contact. So David's BatchDialer screen can show contacts the count excludes. That is not padding: the dialer sweep still blocks them at dial time.
  - What each post means, the D2 rules for eligible contacts, the quality rubric and that it is provisional, the notes form (Slack app Interactivity must point at `/webhooks/lending/slack-interactivity` with `LENDING_SLACK_SIGNING_SECRET` set), the switches, the migration command, how to run the "Done when" checks with `--force --dry-run`, and the 0.6 reuse note (D8).

- [ ] **Step 1:** Write the failing tests: `test_task_acts_only_at_14_et_on_weekdays`, `test_notes_thread_posted_only_when_calls_need_notes`, `test_missing_agent_id_reports_unset_and_posts_nothing`. Use a fake `client` with a `chat_postMessage` recorder.
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3:** Implement the task, the cron line, the runbook and the CLAUDE.md line.
  ```
  # R1 1.6 David's 2:45 PM caller scorecard (Mon-Fri; off until LENDING_CALLER_SCORECARD_ENABLED). Two lines for EST/EDT.
  45 18,19 * * 1-5 $PROJECT/scripts/cron/run.sh src.tasks.lending_caller_scorecard
  ```
- [ ] **Step 4:** Run `pytest tests/lending -q`. Expected: everything passes with no regressions in the 1.3 or soft-approval tests.
- [ ] **Step 5:** Commit: `feat(lending): R1 1.6 2:45 PM caller scorecard task and runbook`.

### Task 7: Tracker and PR

- [ ] **Step 1:** Add a client question to the tracker's Client Questions tab:
  > The 2:45 PM scorecard rates each connected call out of 5. Until you say otherwise, a call gets one point each for a logged result, 2+ minutes of talk, a next action, the decision maker, and an objection or deal status. Is that the right measure?

  Set item 1.6 to "In progress", with the branch name in its notes.
- [ ] **Step 2:** Push, then open the PR with base `feat/r1-13-morning-list`. Note "Stacked on #480" and the D1–D9 decisions in the body.
- [ ] **Step 3:** After #480 merges, rebase onto `dev`, retarget the PR to `dev`, and re-run `pytest tests/lending -q`.
- [ ] **Step 4:** Run the migration on the shared DB: `PYTHONPATH=. python migrations/apply_lending_call_extraction_source.py`. Then record the "Done when" check:
  ```bash
  python -m src.tasks.lending_dialer_queue_check --force --dry-run
  python -m src.tasks.lending_caller_scorecard --force --dry-run
  ```
