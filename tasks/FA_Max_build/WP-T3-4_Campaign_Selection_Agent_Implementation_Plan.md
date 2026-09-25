# WP-T3-4 — Campaign Selection Agent: Implementation Plan

**Owner:** Developer 2
**Tier:** 3 (Amendment item 16)
**Feeds:** WP-T3-5 Outreach Agent and WP-T3-6 Partner Nurture Agent (both Developer 2)

**Sources:**
- `forced-action-max-amendment-1-detail.md` (Part 4 item 16, Part 6 failure behavior, items 5/8/31)
- `Forced_Action_MAX_Client_Responses_and_Open_Clarifications.md` (Part B Q5; client comments 18/9, 21/9, 23/9)
- `Forced_Action_MAX_Tier3_4_Developer_Split.md` (WP-T3-4, Section 7 contracts)
- The current FA Max code in this repo (checked file by file; references below)

---

## 0. Revision notes (review pass 2)

Re-checking the plan against the client spec, the client comments and the actual send-gate code found these problems. All are fixed below.

| # | Problem found | Fix |
|---|---|---|
| R1 | **Cold outreach cannot send today.** `require_consent()` requires an explicit opt-in row in `fa_max_person_consent` for **email as well as SMS**. It is enforced at Relay draft time (`relay/queue.py:345`) and again at send time (`relay/guards.py:164`). Cash buyers, maturity borrowers and imported partners have no opt-in rows, so every campaign message would be refused. | The plan does not weaken this gate. Contacts with no consent for a step's channel are **held, not dropped**. A client decision on the consent basis for cold B2B email is added as **blocking question Q-C1** (Section 11). Cold SMS is treated as not possible without opt-in. |
| R2 | **Channel split would block Campaign 3.** `channel_split_reason()` only lets FA Max contact people whose `fa_max_persons.source` is one of `deed / permit / distress / maturity / partner`. The first draft's import would have written `source='manual_import'` and been refused. | Imported people get `fa_max_persons.source='partner'`. `manual_import` goes on the `fa_max_partners.source` column only. |
| R3 | **The "active wholesaler" rule would match nobody.** Partner mining's `persist.py` writes `status='identified'` and never updates it; nothing in `src/` sets a partner to `active`. | The wholesaler rule uses `rank <= 25` (config) instead of `status`. The status mismatch between `rank.py` (docstring says top 25 → active) and `persist.py` is reported to the partner-mining owner, not fixed here. |
| R4 | **Import as "active" would grant auto-send rights.** `autonomous_tier_context_verified()` treats an `active` partner as valid Tier B auto-send context (`fa_max_send_governance.py:125`). | Imported partners are written as `status='identified'`. Importing a list never changes autonomy. |
| R5 | **Partners were handed to the wrong agent.** The Tier 3 split says Campaign Selection feeds **both** the Outreach Agent and the Partner Nurture Agent. Partner Nurture enforces the client's "value first, always" rule. | Hand-off target is set per audience: investor contacts → Outreach, partner contacts (wholesalers, Campaign 3) → Partner Nurture. |
| R6 | **Repeat borrowers could be enrolled.** Item 31 says "Top partners and repeat borrowers always route to me. Never to an automated sequence." | Anyone with a funded opportunity is blocked now, without waiting for WP-T3-10. The top-partner question is added as Q-C2. |
| R7 | **No history of enrollment changes.** The client confirmed (Part A Q2) that every record keeps "a full timestamped and attributed history". | New append-only `fa_max_campaign_enrollment_events` table. |
| R8 | **Wrong contact source.** `fa_max_person_contact_identifiers` holds operator-verified identifiers for suppression; it is not where a person's reachable email and phone live. | Reachable contact = `fa_max_persons.email` / `phone`. Suppression still checks every identifier through `suppression_reason(person_id=…)`. |
| R9 | **Opt-out hooks keyed by the wrong field.** The email unsubscribe and SMS STOP paths know only an email or phone, not a `person_id`. | Opt-outs cancel by identifier lookup. The existing cross-channel cascade (ADR 0028, `sms_compliance.record_opt_out`) plus a person-level `do_not_contact` transition enforce "stop contacting that lead". |
| R10 | **Missing handling for:** bounces, resuming paused enrollments, missing merge values, send volume vs domain warm-up, statewide bought deeds, and Georgia county tagging. | Added in Sections 6.4, 6.6, 6.8, 6.9, 6.3 and 7. |
| R11 | **No read-only tool.** Spec item 6 says "every agent as a tool". | Added a read-only `get_campaign_enrollment` tool so Cora and the command center can answer "why is this person in Exit Desk?". |

---

## 1. What this task is, in plain English

Josh, the client, wants to reach out to three kinds of people. Each group gets its own series of messages, called a **sequence**. For example: an email on day 0, a text on day 3, and a follow-up email on day 7.

The **Campaign Selection Agent** answers three questions automatically for every contact:

1. **Should this person be in a campaign at all?** No, if they have opted out, are already talking to Josh, or are a repeat borrower.
2. **Which campaign?** Exactly one, never two at once.
3. **Where are they in it?** Which step they are on and when the next step is due.

When a step comes due, this agent **does not write or send anything**. It hands the step to the right drafting agent: Outreach for investors, Partner Nurture for partners. That agent drafts the message, and it can only go out through the existing approval, consent and suppression gates.

This agent is the **traffic controller**, not the messenger.

> **Done when** (Tier 3 split): a new contact is assigned to exactly one sequence based on trigger and role, with no manual classification step.

---

## 2. Decisions made for this plan

These were confirmed with the team lead.

| # | Question | Decision | Why |
|---|---|---|---|
| 1 | Which sequences? | **Campaigns 1, 2 and 3** replace the spec's "investors / builders / partners". | The client's 23/9 instruction is the most recent: "start with 1, 2, and 3 … do not build anything specific to the others yet." Campaigns 1 and 2 are investor audiences and Campaign 3 is a partner audience. Builders are Campaign 4, which is parked. |
| 2 | Contact fits two campaigns | **Fixed priority order in config:** Exit Desk (2) > Capital Desk Loop (1) > Rescue Circuit (3). | The client called Exit Desk "hardest and most important." A fixed order is predictable, needs no manual step, and changes with a config edit. |
| 3 | A better trigger appears mid-sequence | **Switch** to the higher-priority campaign, with a minimum gap since the last touch. | Maturities are time-sensitive, and the gap prevents back-to-back messages. |
| 4 | Channels | **Email and SMS only. No call steps.** Calls are made manually by Josh (later, a hired caller) from the existing dial list. **Every email carries an unsubscribe link and every SMS carries "Reply STOP". Any opt-out stops all contact with that lead.** | Team lead decision. |
| 5 | Weekly deal drop / Campaign 3 list | **Weekly deal drop is out of scope** (it's a broadcast; estimate separately). **The CSV import of Josh's partner list is in scope.** | Without the import, Campaign 3 has nobody to enroll. |
| 6 | What ends or pauses a sequence | **End** on reply, opt-out, suppression or operator rejection. **Pause** while the contact has an open deal. **Resume** when that clears. | A reply means a human conversation has started, and the Reply Concierge takes over. |
| 7 | Content format | **Spreadsheet (CSV) template** plus a merge-field list per campaign. | The client asked for "the format you need sequences in so they load cleanly." |
| 8 | Before the Outreach / Partner Nurture agents exist | **Enrollment and scheduling run for real, as internal records only.** Due steps become queued work items. **This task contains no send code.** | Safe, and it gives WP-T3-5 and WP-T3-6 a real queue on day one. |

---

## 3. The three campaigns (client's definitions, 23/9)

### Campaign 1 — Capital Desk Loop (priority 2)

**Audience:** wholesalers, cash buyers and active investors.

**Who qualifies:**
- **Recent cash buyer:** a deed recorded to an LLC or other entity, with no mortgage recorded within 72 hours.
- **Active investor:** an entity with 2 or more purchases in 24 months, together with its buy box (property type, zip codes, price range).
- **Wholesaler:** a partner-mining row with `partner_class='wholesaler'` ranked in the top 25 (see R3).

**Hand-off:** investors go to the Outreach Agent; wholesalers go to Partner Nurture.

**Not in this task:** the weekly deal drop.

### Campaign 2 — Exit Desk (priority 1, highest)

**Audience:** entity borrowers whose private, bridge or hard-money loan is aging.

**Who qualifies:** a recorded mortgage from a private, bridge or hard-money lender to an entity, 8 to 15 months old, with no satisfaction recorded and no active listing.

**Flip-to-rent add-on:** a renovated, entity-owned property whose listing expired, was withdrawn, or has been on the market 45+ days.

**Data needed:** lender name, loan amount, recording date, borrower entity, and satisfaction/release. This comes from the **bought data feed being set up now** (Gap C).

**Hand-off:** Outreach Agent.

### Campaign 3 — Rescue Circuit (priority 3)

**Audience:** title companies, Georgia closing attorneys, mortgage brokers and loan officers.

**Who qualifies:** people on the partner list **Josh builds himself**. No property data is needed.

**Hand-off:** Partner Nurture Agent (give-first).

---

## 4. What already exists and is reused

Nothing below is rebuilt; this task plugs into it.

| Need | Existing piece | Where |
|---|---|---|
| People, states, reachable contact | `fa_max_persons` (`lifecycle_state`, `source`, `email`, `phone`) | `src/core/models.py` (`FaMaxPerson`) |
| Audited state changes (direct UPDATE is blocked by a DB trigger) | `state_engine.transition()` | `src/services/state_engine.py:102` |
| Person ↔ property links | `fa_max_property_associations` | `src/core/models.py:11702` |
| Consent per channel | `require_consent()`, `set_consent()`, `fa_max_person_consent` | `src/services/fa_max_send_governance.py:65, :296` |
| Suppression (Backflip campaigns, email opt-out, SMS compliance / DNC) | `suppression_reason(session, recipient=, channel=, person_id=)` | `src/services/fa_max_send_governance.py:134` |
| FA vs Backflip lane | `channel_split_reason()`: allowed person sources are deed/permit/distress/maturity/partner | `src/services/fa_max_send_governance.py:21, :260` |
| Partners, class and rank | `fa_max_partners`, filled daily by partner mining (06:00 UTC) | `src/services/partner_mining/` |
| Open / funded deals | `fa_max_opportunities.outcome` (`open` / `funded` / …) | `src/core/models.py:10856` |
| Buy box for merge fields | `fa_max_person_profiles` (`buy_box_geography`, `buy_box_property_types`, `buy_box_price_band`, `velocity_purchases_per_year`) | `src/core/models.py:11980` |
| Reply-based opt-out → `do_not_contact` | `handle_opt_out()` | `src/agents/reply_concierge/opt_out.py` |
| SMS STOP, cascading to email (ADR 0028) | `sms_compliance.record_opt_out()` | `src/services/sms_compliance.py:125` |
| Email unsubscribe link on every Relay email | `unsubscribe_url()` in the Relay footer | `src/services/relay/channels_email.py:57` |
| Email unsubscribe endpoint | `email_unsubscribe_router.py` / `email_unsubscribe.py` | `src/api/`, `src/services/` |
| Bounce → suppression | `relay/bounce_webhook.py` | `src/services/relay/` |
| "Nothing sends until Josh signs off" | `fa_max_relay_send_mode != "live"` check | `src/services/relay/guards.py:183` |
| Agent work queue | `state_engine.enqueue_work_item()` → `fa_max_work_queue` | `src/services/state_engine.py:1357` |
| Read-only agent tools | `@fa_max_tool` decorator → `FA_MAX_TOOL_REGISTRY` | `src/agents/fa_max/tool_registry.py` |
| Cash-purchase query pattern | `_CASH_SQL` | `src/services/dial_list/repository.py:113` |
| Per-person touch schedule to copy | `abandonment_sequences` + polling worker | `migrations/apply_abandonment_sequences.py` |

---

## 5. Gaps in the current code (facts, with the planned answer)

**Gap A — No borrower / partner / both role column.**
*Plan:* derive the role instead of adding a column. A person is a partner if they have a `fa_max_partners` row. They are on the investor side if they are linked to properties.

**Gap B — No trigger-events table.**
*Plan:* one eligibility rule (a SQL query) per campaign, all in one file.

**Gap C — No mortgage data with a lender name or satisfaction status.**
`deeds.mortgage_amount` exists, but there is no lender and no release field. That is exactly what Exit Desk needs.
*Plan:* write the rule against an agreed table shape, test it with fixtures, and ship it **switched off** until bought data lands. The client wants "first real leads in FA Max by Friday Sept 25", so this is expected soon.

**Gap D — Campaign 3's partner classes aren't usable.**
`title_rep` and `broker` are reserved and raise an error (`partner_mining/classify.py:35`). "Closing attorney" and "loan officer" don't exist at all.
*Plan:* add import-only classes `closing_attorney` and `loan_officer`, and allow `title_rep` and `broker` for rows created by the import. The automatic classifier keeps refusing them, so partner mining behaves exactly as before. Partner mining's upsert conflicts only on (`person_id`, `partner_class`) for its own classes, so it cannot overwrite imported rows.

**Gap E — No "never automate / route to Josh" flag yet** (WP-T3-10, Developer 4).
*Plan:* one function, `automation_block_reason(person_id)`. Today it checks for an open deal, a funded deal (repeat borrower) and `do_not_contact`. Developer 4's flag becomes one extra line in it. **Agree the flag name with Developer 4 first.**

**Gap F (new, R1) — Cold outreach has no consent rows.**
*Plan:* this task never writes consent. It records which channels each person can currently be reached on, and holds any step whose channel lacks consent (Section 6.5). The consent basis for cold email is a client decision (Q-C1).

---

## 6. The design

### 6.1 Words used below

| Word | Meaning |
|---|---|
| **Campaign** | `capital_desk_loop`, `exit_desk` or `rescue_circuit`. |
| **Audience** | `investor` or `partner`. Decides which drafting agent gets the step. |
| **Sequence** | The ordered steps of one campaign, loaded from the writer's spreadsheet and versioned. |
| **Step** | Step number, days after the previous step, channel (email/sms), subject, body template. |
| **Enrollment** | One person in one campaign. **Only one active or paused enrollment per person, enforced by the database.** |
| **Touch** | One step for one person: due time and status (scheduled / held / handed_off / sent / skipped / cancelled). |

### 6.2 How it works, end to end

```text
 Daily, after data loads and partner mining
 ┌───────────────────────────────────────────────────┐
 │ 1. ENROLLMENT SWEEP                                │
 │  - run each enabled campaign rule -> candidates    │
 │  - drop anyone blocked (6.4)                       │
 │  - matched several? keep highest priority          │
 │  - respect per-campaign daily cap (6.9)            │
 │  - new -> enrollment + first touch                 │
 │  - in a lower-priority campaign -> switch (6.7)    │
 │  - paused and block cleared -> resume              │
 │  - post one summary to Slack RELATIONSHIPS         │
 └───────────────────────────────────────────────────┘
                     │
 Every 15 minutes    ▼
 ┌───────────────────────────────────────────────────┐
 │ 2. DUE-STEP SWEEP                                  │
 │  - touches whose due time has passed               │
 │  - re-check blocks + opt-outs (safety net)         │
 │  - SMS step, no SMS consent -> skip step           │
 │  - email step, no email consent -> HOLD (6.5)      │
 │  - missing required merge value -> HOLD            │
 │  - otherwise -> work item for the audience's agent │
 │      investor -> "fa_max_outreach"  (WP-T3-5)      │
 │      partner  -> "fa_max_partner_nurture" (WP-T3-6)│
 │    touch -> handed_off                             │
 └───────────────────────────────────────────────────┘
                     │
                     ▼
   Drafting agent: draft -> Josh approval / autonomy tier ->
   consent + suppression + channel split (Relay, unchanged) -> send
   -> calls mark_touch_sent() -> we schedule the next step

 Any moment (event-driven):
   opt-out / unsubscribe / STOP / reply / hard bounce -> 6.6
```

**Why the next step waits for "sent":** each step is timed from when the previous message actually went out. Until the drafting agents exist and Josh switches Relay to live, sequences simply wait. Nothing piles up, and nobody gets a burst of back-dated messages later.

### 6.3 Eligibility rules (one per campaign)

All rules live in `src/services/fa_max_campaigns/eligibility.py`.

**How every rule works:**
- It is a `text()` SQL query run **once per sweep**, with results streamed, never one query per person.
- It returns `person_id`, `audience`, `trigger_type`, `trigger_reason`, `property_id` (if any), `county_id` and `state`.
- It is switched on or off in config.
- It only returns people who have a resolved `fa_max_persons` row (not merged away) **and** a non-empty `email` or `phone`.
- Linked properties with no resolved person or contact are **counted and logged, not enrolled**. That number shows how much identity or skip-trace work remains.
- County and state are kept on every enrollment, as the client asked: "Keep county on every Georgia record … I may need to filter out the Atlanta metro later". **No county filter is built now.**

**Capital Desk Loop:**
- **Recent cash buyer:**
  - A deed whose grantee is an entity (LLC / INC / CORP / TRUST / LP / LTD).
  - No mortgage-type deed on the same property recorded within 72 hours after it.
  - Recorded within `CASH_BUYER_RECENT_DAYS` (Q-C4).
  - The existing `_CASH_SQL` only checks the same deed row, so the 72-hour check is new.
- **Active investor:** 2 or more purchases in 24 months across the person's linked properties.
- **Wholesaler:** `fa_max_partners.partner_class='wholesaler'` and `rank <= WHOLESALER_TOP_N` (default 25, matching the spec's "Top 25 in each class").

**Exit Desk (off until bought data lands):**
- Reads an agreed table or view, working name `lending_mortgage_records`, with columns `property_id`, `borrower_entity`, `lender_name`, `lender_type` (private / bridge / hard_money / other), `loan_amount`, `recording_date`, `satisfied_bool`, `county_id`, `state`.
- A person qualifies when a linked property has an unsatisfied private, bridge or hard-money mortgage to an entity, 8–15 months old, with no active listing.
- The flip-to-rent clause is switched on only if the bought source provides listing status.

**Rescue Circuit:** a `fa_max_partners` row with `partner_class IN ('title_rep','closing_attorney','broker','loan_officer')` and `source='manual_import'`.

**Data location note:** Capital Desk Loop reads `deeds`, which today holds only the three scraped counties. Once statewide bought deeds land, the rule must read wherever the loader puts them (the same table, or an agreed view). This is part of contract D-1.

### 6.4 Who is blocked (checked at enrollment AND before every step)

A person is never enrolled or advanced if any of these is true:

1. `lifecycle_state` is `do_not_contact` or `suppressed`.
2. `suppression_reason(...)` returns a reason on **every** channel the campaign uses. This covers email opt-out, SMS compliance / DNC / STOP, and an active Backflip campaign touch (client Q5: "Forced Action holds off").
3. `channel_split_reason(...)` blocks them. They are in Backflip's lane, or their source is not an FA channel.
4. `automation_block_reason(person_id)` returns a reason:
   - **Open deal** → pause.
   - **Funded deal (repeat borrower)** → never enrolled, because item 31 says they always route to Josh.
   - Later, the WP-T3-10 flag.

Every block is written with its reason code, so "why isn't this person in a campaign?" always has an answer.

### 6.5 Consent (R1): hold, never bypass

The Relay gate needs an opt-in consent row for the step's channel. This task keeps that rule exactly as it is.

- **SMS step, no SMS consent:** the step is **skipped** with reason `no_sms_consent` and the sequence moves on. Cold SMS without prior opt-in is not something this system does.
- **Email step, no email consent:** the touch is **held** (status `held`, reason `no_email_consent`) and the sequence does not advance. Held touches are counted in the daily Slack summary ("Exit Desk: 212 people held — waiting on email consent basis").
  - When a consent row exists, the next sweep releases the touch.
  - If the client approves a consent basis for cold B2B email (Q-C1), recording it is a **separate, explicit piece of work** with its own audit source. It is not done inside this agent.
- This task **never writes to `fa_max_person_consent`.**

### 6.6 Opt-out: the hard rule

**Once a lead opts out on any channel, we stop contacting them on every channel, permanently.** There are three layers.

**1. Every message carries a way out.**
- **Email:** the Relay footer already adds a one-click unsubscribe link, so the writer doesn't need to.
- **SMS:** the loader rejects any SMS step without "Reply STOP to opt out" (or equivalent), unless the SMS send path already appends it. This is checked in Build Step 1 so we don't end up with a double or missing STOP line.

**2. The opt-out takes effect immediately, across all channels.**
- **SMS STOP:** the existing `sms_compliance.record_opt_out()` already cascades to email (ADR 0028). We add a hook that finds the FA Max person by phone.
- **Email unsubscribe link:** we add a hook to the unsubscribe endpoint that finds the FA Max person by email.
- **Reply opt-out** (e.g. "remove me"): the existing `handle_opt_out()` already moves the person to `do_not_contact`. We add our cancel call.

For the SMS and email hooks, when the identifier maps to an FA Max person, we call the existing `handle_opt_out()`. It moves the person to `do_not_contact` through the state engine and suppresses their email. That turns a single-channel opt-out into "stop contacting this lead", as the team lead required. After that, `cancel_enrollments(person_id, reason)` cancels the enrollment and all future touches, with reason `opt_out_email`, `opt_out_sms` or `opt_out_reply`.

**3. The safety net.**
- The due-step sweep re-runs Section 6.4 before every hand-off.
- Relay checks consent and suppression again at draft time and send time.
- A person who opted out is never re-enrolled, because Section 6.4 excludes them.

**Hard bounce:** the bounce webhook already suppresses the address. The person is not opted out, so remaining email steps are skipped (`email_bounced`). SMS steps continue only with SMS consent. If no channel is left, the enrollment ends with `no_reachable_channel`.

**Reply of any kind:** the enrollment ends (`replied`). The hook sits next to where the abandonment sequence is cancelled on reply (`reply_concierge/router.py`).

### 6.7 One campaign, switching, pausing

- **Priority:** `CAMPAIGN_PRIORITY = ["exit_desk", "capital_desk_loop", "rescue_circuit"]`. The winner is enrolled and other matches are stored in `also_matched`.
- **Switching up only:**
  - The old enrollment ends with `preempted_by:<campaign>`.
  - The new one starts at step 1.
  - Its first step is due no earlier than `last_touch_sent_at + MIN_GAP_DAYS`. The default is 14, the spacing rule ported from Banks' governance.
- **Pause and resume:**
  - An open deal pauses the enrollment.
  - If the deal ends `dead` / `recycled`, the next sweep resumes it at the step where it stopped, with the same gap rule.
  - If the deal is `funded`, the enrollment ends (`became_borrower`), because that person is now a repeat borrower.
- **Finished sequence:** no re-enrollment in the **same** campaign for `REENROLL_COOLDOWN_DAYS`.

### 6.8 Sequence content format (for the content writer)

**One CSV per campaign, one row per step:**

| Column | Example | Rules |
|---|---|---|
| `campaign` | `exit_desk` | One of the three |
| `step` | `1` | 1, 2, 3 … with no gaps |
| `days_after_previous` | `0`, `3`, `7` | Whole days. Step 1 is counted from enrollment |
| `channel` | `email` / `sms` | Only these two |
| `subject` | `About {{property_street}}` | Email only |
| `body` | `Hi {{first_name\|there}}, …` | Only that campaign's merge fields. `\|` gives a fallback |
| `notes` | anything | Ignored |

**Loader:** `python -m src.services.fa_max_campaigns.load_sequences <file.csv> [--dry-run]`. It **refuses the whole file** if any of these is true:
- an unknown merge field
- an SMS step without STOP text (6.6)
- an SMS body that is too long
- a step gap
- an unknown channel
- text blocked by the existing `validate_safe_payload()` pattern (rates, terms, commitments, borrower financial words)

Each load creates a **new version**, and people mid-sequence finish on their original version. This matches the client's "config change … not a rebuild" (18/9).

**Merge fields (v1)** are generated from code into a one-page doc for the writer, so the doc and the loader can never disagree. Each field is marked **required** or **optional**:
- A missing **required** value holds the touch (`missing_merge:<field>`).
- A missing **optional** value uses the `|fallback` text.

| Campaign | Merge fields |
|---|---|
| All | `first_name` (optional), `sender_name`, `sender_title`, `calendar_link`, plus the footer added automatically |
| Capital Desk Loop | `entity_name`, `property_street`, `property_city`, `purchase_date`, `county`, `buy_box_zips` (opt.), `buy_box_price_range` (opt.), `purchase_count_24m` (opt.) |
| Exit Desk | `entity_name`, `property_street`, `property_city`, `county`, `loan_age_months`, `lender_name` (**held back until Q-C3 is answered**) |
| Rescue Circuit | `company_name`, `partner_type`, `state` |

Other rules for content:
- Sender name, address, footer and lender routing stay **placeholders** until Josh confirms his lender seat (23/9). The title line is "Loan Officer" (18/9 #11).
- The spec requires a calendar link on every outbound, hence `calendar_link`.
- **No merge field may carry pricing, rates, terms or borrower financial data.**

### 6.9 Volume control

Enrolling hundreds of people at once would flood Josh's approval queue and risk the new sending domain. The client warned that "Domains burned in week one cannot be unburned."

- Config sets `MAX_NEW_ENROLLMENTS_PER_DAY` per campaign. Highest-urgency candidates go first (e.g. oldest loan age for Exit Desk, most recent purchase for Capital Desk Loop), and the rest wait for the next day.
- The proposed start is small (**25 per campaign per day**), stepped up alongside domain warm-up. **The team lead sets the real value.**
- For reference, the client's planning figure is about 1,960 triggered prospects a month (18/9 #7).
- Relay's own daily send ceiling and send window still apply at send time, unchanged.

### 6.10 Hand-off contract (to WP-T3-5 and WP-T3-6)

`enqueue_work_item(queue_name=<"fa_max_outreach" | "fa_max_partner_nurture">, idempotency_key="campaign_touch:<touch_id>", payload={...})`

**Payload:** `touch_id`, `enrollment_id`, `person_id`, `audience`, `campaign_key`, `sequence_version`, `step`, `channel`, `subject_template`, `body_template`, `merge_values`, `trigger_type`, `trigger_reason`, `property_id`, `county_id`, `state`.

**Callbacks this task provides:**
- `mark_touch_sent(touch_id, sent_at, relay_item_id)`: schedules the next step.
- `mark_touch_not_sent(touch_id, reason)`:
  - A suppression, consent or opt-out block cancels or holds the enrollment, following 6.5 and 6.6.
  - A rejection by Josh ends it with `rejected_by_operator`.
  - Any other failure keeps the touch waiting. It is never auto-retried past a block, in line with the spec's rule for a send that fails suppression.

Until WP-T3-5 and WP-T3-6 are running, items wait on their queues. That is expected, not a fault.

### 6.11 Attribution tagging (WP-T3-12, Developer 4)

Every enrollment stores `campaign_key`, `audience`, `trigger_type`, `trigger_reason`, `source` (rule name or `manual_import`), `sequence_version` and `enrolled_at`. Every touch links to its enrollment, and a sent touch stores its `relay_item_id`.

A funded deal can therefore be traced back to the campaign and trigger that started it. **Agree these fields with Developer 4 before anything ships to draft state** (Tier 3 split, Section 7).

### 6.12 Campaign 3 partner list import

`python -m src.services.fa_max_campaigns.import_partners <file.csv> [--dry-run]`

**Columns:** name, company, email, phone, partner type (title company / closing attorney / mortgage broker / loan officer), state, county, notes.

**For each row:**
- Find or create the person through the existing identity resolution. The client asked us not to "contact the same owner twice".
- New people are created with `fa_max_persons.source='partner'` so the channel split allows them (R2).
- Phones go through `phone_utils.normalize`.
- Create `fa_max_partners` with `source='manual_import'` and `status='identified'` (R4).
- Store Josh's supplied email and phone in `fa_max_person_contact_identifiers` (`source='manual_import'`). They count as operator-provided, which widens person-level suppression.
- **Do not write consent** (6.5).
- If the person is already suppressed, they are imported but never enrolled.

**Output:** created / matched-existing / skipped counts. The client prefers Slack commands for tools (18/9 #5), so a Slack file-upload version is offered as a follow-up.

### 6.13 Visibility

- **Daily Slack summary in RELATIONSHIPS:**
  - new enrollments per campaign
  - switches, pauses and resumes
  - cancellations by reason
  - touches held (no consent / missing merge)
  - candidates skipped for no person or contact
  - rules switched off ("Exit Desk: waiting for bought data")
- **Read-only tool `get_campaign_enrollment(person_id)`**, registered with `@fa_max_tool`. It returns the current and past enrollments, the reasons, and the next due step. Cora and the command center can use it; it has no write capability.

---

## 7. Database changes

One idempotent migration, `migrations/apply_fa_max_wp_t3_4_campaign_selection.py`, plus matching models in `src/core/models.py`.

**`fa_max_campaign_sequence_steps`**
- Columns: `campaign_key`, `sequence_version`, `step`, `days_after_previous`, `channel` (CHECK email/sms), `subject`, `body_template`, `loaded_at`, `loaded_by`.
- Unique on (`campaign_key`, `sequence_version`, `step`).

**`fa_max_campaign_enrollments`**
- Columns:
  - `enrollment_id` (uuid)
  - `person_id` (FK)
  - `campaign_key`, `audience` (CHECK investor/partner), `sequence_version`
  - `status` (CHECK active / paused / completed / cancelled / preempted)
  - `trigger_type`, `trigger_reason`, `source`, `property_id`, `county_id`, `state`, `also_matched` (jsonb)
  - `enrolled_at`, `ended_at`, `end_reason`, `last_touch_sent_at`
- **Partial unique index** on `person_id` where `status IN ('active','paused')`. The database guarantees one campaign per person.
- Indexes: (`campaign_key`, `status`).

**`fa_max_campaign_enrollment_events`** (append-only, R7)
- Columns: `id`, `enrollment_id`, `event` (enrolled / paused / resumed / preempted / cancelled / completed / touch_held / touch_released), `reason`, `actor`, `created_at`.

**`fa_max_campaign_touches`** (shape copied from `abandonment_sequences`)
- Columns: `touch_id`, `enrollment_id` (FK), `step`, `channel`, `due_at`, `status` (CHECK scheduled / held / handed_off / sent / skipped / cancelled), `status_reason`, `work_item_id`, `relay_item_id`, `sent_at`, `idempotency_key` (unique).
- Index on `due_at` where `status IN ('scheduled','held')`.

**No borrower financial fields anywhere** (project schema boundary).

---

## 8. Build steps, in order

**Step 1 — Confirm contracts (before coding).**
- **With Developer 4:** the "route to Josh" flag name (Gap E) and the attribution fields (6.11).
- **With the bought-data loader owner (D-1):** the Exit Desk table shape, where statewide deeds land, and that loaded people get an allowed `fa_max_persons.source` (e.g. `deed` or `maturity`). Without that, the channel split blocks them.
- **Check the SMS send path** to see whether it appends STOP text (6.6).
- **Tell the partner-mining owner** about the `status` mismatch (R3).

**Step 2 — Config.**
`config/fa_max_campaigns.py`:
- campaigns, audiences, hand-off queues, priority
- on/off per rule
- `MIN_GAP_DAYS`, `REENROLL_COOLDOWN_DAYS`, `CASH_BUYER_RECENT_DAYS`, `WHOLESALER_TOP_N`, `MAX_NEW_ENROLLMENTS_PER_DAY`
- sweep times
- `validate_campaign_config()`

**Step 3 — Schema.** Models plus the migration (Section 7). Run once against the shared DB.

**Step 4 — Content.**
- `content.py`: the merge-field registry (required/optional) and value resolver.
- `load_sequences` CLI.
- The writer's one-pager, generated from the registry.

**Step 5 — Blocks.**
`blocks.py`:
- `automation_block_reason()`
- `enrollment_block_reason()`, which wraps lifecycle state, `suppression_reason`, `channel_split_reason` and `automation_block_reason`
- `channel_readiness()`, which wraps `require_consent`

Everything reuses existing functions.

**Step 6 — Eligibility rules** (6.3), with Exit Desk switched off.

**Step 7 — Selection.**
`selection.py`: priority, the daily cap, switching, pause/resume, cool-off, batch inserts, event rows. Also `cancel_enrollments`, `mark_touch_sent` and `mark_touch_not_sent`.

**Step 8 — Hooks** (6.6):
- the email unsubscribe endpoint
- `sms_compliance.record_opt_out`
- `handle_opt_out`
- the reply router
- the bounce webhook

**Step 9 — Sweeps.**
- `src/tasks/fa_max_campaign_enrollment_sweep.py`: daily, after partner mining and the morning loads. Has `--dry-run`.
- `src/tasks/fa_max_campaign_due_steps.py`: every 15 minutes.
- Add both to `scripts/cron/crontab.txt`.

**Step 10 — Campaign 3 import** (6.12), plus the partner-class change (Gap D).

**Step 11 — Visibility.** The Slack daily summary and the read-only tool (6.13).

**Step 12 — Docs.**
- `CLAUDE.md`: migration command, two scheduled tasks, new config file, new package.
- `docs/PLATFORM-OPERATIONS-GUIDE.md`.
- The writer's one-pager.

---

## 9. Testing plan

Unit tests in `tests/fa_max/test_campaign_selection*.py`, using fixtures and fakes with no network.

- **Priority and uniqueness:**
  - Exit Desk beats Capital Desk Loop, and the loser is recorded in `also_matched`.
  - A second active enrollment is rejected by the database index.
- **Switching / pause / resume:**
  - An upward switch respects `MIN_GAP_DAYS`, and there is never a downward switch.
  - An open deal pauses; a dead deal resumes at the same step; a funded deal ends with `became_borrower`.
- **Opt-out (the team lead's hard rule):**
  - Email unsubscribe, SMS STOP and a reply opt-out each cancel the enrollment and its future touches, and move the person to `do_not_contact`.
  - A later sweep never re-enrolls them.
  - An SMS STOP also stops email steps.
- **Bounce:** email steps are skipped, SMS continues only with consent, and with no channel left the enrollment ends as `no_reachable_channel`.
- **Consent (R1):**
  - Without email consent, the touch is **held** and not handed off.
  - A consent row added later releases it on the next sweep.
  - Without SMS consent, the step is skipped.
  - The agent never writes consent.
- **Blocks:**
  - Backflip-active → not enrolled.
  - Channel split refuses → not enrolled.
  - Funded borrower → never enrolled.
  - No email and no phone → counted, not enrolled.
- **Import:**
  - A duplicate person is resolved, not duplicated.
  - `fa_max_persons.source='partner'` passes the channel split.
  - The partner status is `identified`, so the import grants no Tier B context.
  - A suppressed person is imported but never enrolled.
  - Phones are normalized.
- **Loader:**
  - Rejects unknown merge fields, SMS without STOP, step gaps, bad channels, and prohibited financial wording.
  - A new version doesn't affect people mid-sequence.
- **Merge:** a missing required value holds the touch; a missing optional value uses its fallback.
- **Eligibility:**
  - The 72-hour no-mortgage rule and the entity filter work on fixture deeds.
  - The wholesaler rule uses rank, not status.
  - The Exit Desk rule works on fixture rows and returns nothing when switched off.
- **Cap:** only `MAX_NEW_ENROLLMENTS_PER_DAY` people are enrolled per campaign, most urgent first.
- **Hand-off:**
  - Exactly one work item per due touch.
  - Investors go to `fa_max_outreach`, partners to `fa_max_partner_nurture`.
  - The next step is not scheduled until `mark_touch_sent`.

**Real-data check (once bought data lands):**
- Run the daily sweep with `--dry-run` on the real DB.
- Spot-check about 20 people per campaign by hand against the client's definitions.
- Report the counts, including how many are held for consent. This is the "rough real leads in FA Max" demo the client asked for.

---

## 10. Out of scope

- Writing message content (the client's writer does this).
- Drafting or sending messages (WP-T3-5 and WP-T3-6), and any change to the Relay gates.
- Writing consent records.
- Call steps and the dial list, direct mail, and the weekly deal drop.
- Campaigns 4–10, and segmentation finer than three campaigns (waits for item 36 attribution).
- A general trigger-events table, and county filtering (county is stored, not filtered).

---

## 11. Open items

None of these are assumptions; each needs a confirmation.

| # | Item | Recommendation | Who | Blocking? |
|---|---|---|---|---|
| **Q-C1** | **Consent basis for cold email.** Relay requires an opt-in consent row for email, and cold contacts (cash buyers, maturity borrowers, Josh's partner list) don't have one. Does the client want cold B2B email to these contacts? If so, what consent source should be recorded (e.g. "business contact from public record, unsubscribe in every message") and who approves it? This may also need Backflip compliance review, which is still pending on the footer (Q9). | Ask the client now. Until then, touches are held and counted in Slack so the size of the waiting pool is visible. | Team lead → client | **Blocks any campaign email from sending.** Does not block building or enrolling. |
| Q-C2 | **Top partners vs campaigns.** Item 31 says top partners "always route to me. Never to an automated sequence." Campaign 1 targets wholesalers, and Campaign 3 targets Josh's own partner list. | In v1 every draft needs Josh's approval, so nothing is automated. Enroll them, hand them to Partner Nurture (give-first), and let WP-T3-10's definition override later. | Team lead → client | No |
| Q-C3 | **Can Exit Desk emails name the person's current lender** (`lender_name`)? | Leave it out of the writer's list until answered. It can read as intrusive. | Client | No |
| Q-C4 | **How recent is a "recent cash buyer"?** The client gave 72 hours for the no-mortgage check, but no purchase window. | Config default of 90 days, to confirm. | Client | No |
| Q-C5 | **Confirm** that Campaigns 1–3 replace the spec's investors / builders / partners for item 16. | We are building on this reading. Confirm in one line on the next report. | Team lead → client | No |
| Q-C6 | **Weekly deal drop** (Campaign 1) is a separate broadcast product. | Send an estimate, since the client said "estimate it". | Team lead | No |
| D-1 | Bought data: the Exit Desk table shape, where statewide deeds land, and an allowed `fa_max_persons.source` for loaded people. | Agree in Build Step 1. | Bought-data loader owner | **Blocks Exit Desk going live only** |
| D-2 | "Route to Josh" flag name (WP-T3-10) and the attribution fields (WP-T3-12). | Agree in Build Step 1. | Developer 4 | No |
| D-3 | Does the SMS send path already append STOP text? | Check in Build Step 1. | Developer 2 | No |
| D-4 | Partner mining never sets `status='active'` (R3). | Report to the owner; this task doesn't depend on it. | Partner-mining owner | No |
| D-5 | Starting value for `MAX_NEW_ENROLLMENTS_PER_DAY`. | 25 per campaign, raised with domain warm-up. | Team lead | No |

---

## 12. Done when

1. A contact matching a live rule is placed in **exactly one** campaign automatically, by priority, with no manual step. The database enforces one active or paused enrollment per person.
2. Each enrollment tracks its step and next due time, with a full event history. Due steps become work items for the right agent (investors → Outreach, partners → Partner Nurture).
3. **Any opt-out (email unsubscribe, SMS STOP or reply) cancels the lead's enrollment immediately, moves them to `do_not_contact`, and they are never re-enrolled.** Proven by tests on all three paths.
4. Repeat borrowers, Backflip-active contacts and channel-split contacts are never enrolled. Contacts with an open deal are paused and resumed correctly.
5. No step bypasses consent: email steps without consent are held and visible in Slack, and SMS steps without consent are skipped.
6. The writer has a spreadsheet template and a merge-field list per campaign, and bad files are rejected with a clear reason.
7. Josh's Campaign 3 CSV imports without duplicates, passes the channel split, grants no autonomy, and the imported partners enroll into Rescue Circuit.
8. The Exit Desk rule is built and tested with fixtures. Report it as **"Infra complete, awaiting bought data"**.
9. No code in this task can send a message or write consent.
