# Concierge Chat — Pre-Launch Checklist

Four conditions required before AI Chat goes live to prospects.

---

## A — System prompt guardrails ✓

Implemented in `src/services/concierge_chat.py` (`_SYSTEM_TEMPLATE`).

| Guardrail | Trigger | Bot response |
|---|---|---|
| Refund redirect | asks about refund / cancellation / billing dispute | Fixed copy → email support |
| ZIP availability | asks if a specific ZIP is available/taken | "I can't confirm in chat" → forcedactionleads.com / support |
| Competitive comparison | names a competitor or alternative (PropStream, MLS, etc.) | Hard decline + product pivot |
| Abusive user | abusive / threatening language | Warning once → canned refusal on all subsequent turns (soft end) |

---

## B — Test conversations

### B1 · Refund guardrail

> **User:** I want a refund for my last payment, this isn't working.
>
> **Bot:** Refund and cancellation requests are handled by our support team —
> email support@forcedactionleads.com with your account email and they'll respond
> within one business day.

### B2 · ZIP availability guardrail

> **User:** Is ZIP 33601 available? I'm a roofer in Tampa.
>
> **Bot:** I can't confirm ZIP availability in chat — check the live map at
> forcedactionleads.com or email support@forcedactionleads.com. They can tell you what's
> open in your area right now.

### B3 · Competitive comparison guardrail

> **User:** How does Forced Action compare to PropStream?
>
> **Bot:** I'm not able to compare Forced Action to other services here.
> I can tell you exactly what we do — want me to walk you through how it works?
>
> **User:** Sure, go ahead.
>
> **Bot:** Forced Action monitors public distress signals — storms, foreclosures,
> tax delinquencies, code violations, and more — and delivers scored, real-time
> leads for the ZIP codes you've locked. You get the signal before anyone else
> in your territory.
>
> What's your trade — roofing, restoration, or something else?

### B4 · Abusive user — warn → soft end

> **User:** [abusive message]
>
> **Bot:** I need to keep this conversation professional. If you'd like help
> with Forced Action, I'm here — otherwise I won't be able to continue.
>
> **User:** [continues abuse]
>
> **Bot:** I'm not able to continue this conversation. Please email
> support@forcedactionleads.com if you need assistance.
>
> **User:** [any further message]
>
> **Bot:** I'm not able to continue this conversation. Please email
> support@forcedactionleads.com if you need assistance.

### B5 · Happy-path signup

> **User:** What's the difference between Starter and Pro?
>
> **Bot:** Starter covers 1 ZIP at $800/month — daily lead feed, CDS scoring,
> all event types, rate locked forever. Pro covers 3 ZIPs at $1,500/month and
> adds priority lead delivery and skip-traced phone numbers.
>
> **User:** I work two ZIPs in Tampa. Which one should I get?
>
> **Bot:** Pro is the right fit — it covers up to 3 ZIPs and includes
> skip-traced contacts, which matters for restoration and roofing where
> speed-to-call is everything.
>
> **User:** How do I sign up?
>
> **Bot:** Head to forcedactionleads.com — you can choose your plan, pick your ZIPs,
> and check if founding-member rates are still available. The whole setup takes
> a few minutes.

---

## C — Cost projection

Model: **Claude Haiku 4.5** (`chat_response` task type, already in production).
Pricing: $0.80/M input tokens, $4.00/M output tokens.
System prompt caching: enabled (`cache_control: ephemeral`).
Cache read rate: 0.1× input price = $0.08/M tokens.

**Per-turn assumptions (cache hot):**

| Component | Tokens | Cost |
|---|---|---|
| System prompt (cache read, ~2,200 tokens) | 220 effective | $0.000176 |
| Conversation history (4 msgs × 50 tokens avg) | 200 | $0.000160 |
| User message | 40 | $0.000032 |
| **Total input** | **460** | **$0.000368** |
| Output (avg 120 of 256 max) | 120 | $0.000480 |
| **Per turn** | | **$0.000848** |

**Per conversation (5 turns avg):** $0.00424

**Volume projections:**

| Convos/day | $/day | $/month |
|---|---|---|
| 100 | $0.42 | $12.70 |
| 500 | $2.12 | $63.60 |
| 1,000 | $4.24 | $127.20 |

**Daily session cap** (`_DAILY_CAP_USD = 0.50`): a single session can sustain
~590 turns before hitting the cap. No realistic conversation approaches this.

**Haiku vs Sonnet:** Sonnet costs 3.75× more per input token and 3.75× more
per output token. At 1,000 convos/day that's ~$477/month vs $127/month.
The knowledge base is fully structured markdown; Haiku handles it without
quality degradation. Use Sonnet only if eval shows Haiku misreads the KB
or misapplies a guardrail — not before.

---

## D — Buying-intent triggers

Detection is frontend-only (see ADR-0004). The chat backend is unchanged.

**Implementation location:** `Forced-action-ui/src/components/concierge/ChatBody.jsx`
Detect on `input` change (or `onSubmit`), before the message is sent to the API.

### Trigger map

| Trigger | Chat mode | Keywords (case-insensitive) | Frontend action |
|---|---|---|---|
| `wallet` | both | "wallet", "add credits", "buy credits", "top up", "credits" | Open wallet plan selector → `/api/checkout` with wallet SKU |
| `lock` | `post_signup` only | "lock", "territory lock", "lock my zip", "add zip", "another zip", "expand territory" | Open territory lock flow → `/api/checkout` with lock SKU + ZIP selector |
| `annual` | both | "annual", "yearly", "pay yearly", "year contract" | Inline message: "Annual pricing varies by plan — email support@forcedactionleads.com with your current plan and they'll set it up." + mailto link |
| `unlock` | — | — | **Out of scope v1.** Belongs in dashboard lead-card UI. |

### End-to-end flow (wallet, pre_signup)

1. User types "I want to add credits to my wallet"
2. Frontend detects `wallet` keyword before submit
3. Frontend opens wallet-plan selector overlay (Starter / Growth / Power)
4. User selects plan → frontend calls `POST /api/checkout` with wallet SKU
5. Stripe checkout session created → user redirected to Stripe
6. Chat message still sent normally; bot replies about wallet plans from KB
7. Stripe webhook `checkout.session.completed` confirms wallet top-up

### End-to-end flow (lock, post_signup)

1. User types "I want to lock another ZIP"
2. Frontend detects `lock` keyword; confirms mode is `post_signup`
3. Frontend opens ZIP selector overlay (shows subscriber's current ZIPs, prompts for new one)
4. User selects ZIP → frontend calls `POST /api/checkout` with territory lock SKU + zip
5. Stripe checkout session created → user redirected to Stripe
6. Stripe webhook `checkout.session.completed` locks the territory via `ZipTerritory` row

### Test transcript — wallet trigger (pre_signup)

> **User types:** "Can I add credits before I even subscribe?"
>
> **Frontend detects** "credits" keyword → renders wallet-plan selector inline.
>
> **User selects** Growth Wallet ($99/mo).
>
> **Frontend calls** `POST /api/checkout` → Stripe session URL returned.
>
> **User redirected** to Stripe hosted checkout.
>
> **Stripe webhook fires** `checkout.session.completed` → wallet credited.
>
> **Bot reply (normal KB response):** "Yes — wallet plans let you pre-load
> credits. Growth Wallet is $99/month for 50 credits with Auto Mode included.
> The interface just showed you the options."
