# Frontend Buying-Intent Detection — Implementation Plan

Implements `docs/chat-launch-checklist.md` §D per ADR-0004 (frontend-only).
Backend (`concierge_chat.py`) is not modified.

## Scope decisions (locked)

1. **Lock trigger — deferred to v2.** No territory-lock checkout API exists
   for the frontend to call. Treat `lock` like `unlock`: out of scope for v1,
   belongs in the dashboard lead-card UI when the backend endpoint lands.
2. **Pre_signup wallet — no overlay.** Pre-signup users can't top up (no
   `feedUuid`). Do not detect/intercept; let the LLM answer from the KB
   (it already explains wallet plans require an active subscription).
3. **Wallet trigger — post_signup only**, reuses existing
   `WalletTopupModal` (`src/components/dashboard/WalletTopupModal.jsx`) and
   `POST /api/wallet/topup` (PaymentIntent, not Checkout redirect).
4. **Annual trigger — both modes**, injects an inline assistant message with
   a `mailto:support@forcedaction.ai` link. No Stripe.

### Effective v1 trigger map

| Trigger | Mode | Action |
|---|---|---|
| `wallet` | `post_signup` only | Open `WalletTopupModal` + send message normally |
| `annual` | both | Inject inline assistant message with mailto + send message normally |
| `lock`   | — | **Deferred to v2** (no backend endpoint) |
| `unlock` | — | **Out of scope** (dashboard lead-card UI) |

Update `docs/chat-launch-checklist.md` §D + `CONTEXT.md` glossary
(`lock trigger` becomes "planned, v2") to match.

## Files to change

| File | Change |
|---|---|
| `src/components/concierge/intent.js` *(new)* | Pure `detectIntent(text)` |
| `src/components/concierge/ConciergeChat.jsx` | Accept + forward `mode`, `feedUuid` props |
| `src/components/concierge/ChatDrawer.jsx` | Forward `mode`, `feedUuid` |
| `src/components/concierge/ChatFullScreen.jsx` | Forward `mode`, `feedUuid` |
| `src/components/concierge/ChatBody.jsx` | Intent dispatch in `submit()`; render `WalletTopupModal` when triggered |
| `src/hooks/useConciergeChat.js` | Expose `injectAssistantMessage(text)` for the annual case |
| `src/pages/LandingPage.jsx` | `<ConciergeChat mode="pre_signup" />` |
| `src/pages/DashboardPage.jsx` | `<ConciergeChat mode="post_signup" feedUuid={feedUuid} />` |

## Module 1 — `intent.js` (pure detection)

```js
// src/components/concierge/intent.js
const PATTERNS = {
  wallet: /\b(wallet|add\s+credits|buy\s+credits|top[\s-]?up|credits?)\b/i,
  annual: /\b(annual|yearly|pay\s+yearly|year\s+contract)\b/i,
  // lock: deferred v2 — no backend endpoint
};

export function detectIntent(text) {
  for (const [kind, re] of Object.entries(PATTERNS)) {
    const m = re.exec(text);
    if (m) return { kind, matched: m[0] };
  }
  return null;
}
```

Order matters: `wallet` first. `lock` regex omitted in v1 to avoid silent
false negatives that would mislead users.

## Module 2 — Dispatch in `ChatBody.submit()`

Add props: `mode` (`'pre_signup' | 'post_signup'`), `feedUuid`,
`onInjectAssistantMessage`.

```jsx
const [walletModalOpen, setWalletModalOpen] = useState(false);

const submit = (text) => {
  const trimmed = text.trim();
  if (!trimmed || isLoading) return;

  const intent = detectIntent(trimmed);
  if (intent?.kind === 'wallet' && mode === 'post_signup') {
    setWalletModalOpen(true);
  } else if (intent?.kind === 'annual') {
    onInjectAssistantMessage(
      "Annual pricing depends on your tier and ZIP count — " +
      "email [support@forcedaction.ai](mailto:support@forcedaction.ai) " +
      "and they'll set it up."
    );
    // Skip onSend — injected message stands in for the bot reply
    setPinned(true);
    setInput('');
    return;
  }
  // wallet (pre_signup) and all non-intent: fall through to onSend
  setPinned(true);
  onSend(trimmed);
  setInput('');
};
```

For `wallet` post_signup: dispatch overlay **and** still call `onSend` so
the bot's KB reply lands alongside the modal — transcript stays consistent.

For `annual`: synthetic assistant message replaces the bot turn (mailto is
the answer; no LLM call needed). Saves a Haiku call per annual ask.

## Module 3 — `WalletTopupModal` rendered from ChatBody

```jsx
{walletModalOpen && feedUuid && (
  <WalletTopupModal
    feedUuid={feedUuid}
    onClose={() => setWalletModalOpen(false)}
  />
)}
```

If `WalletTopupModal` is dashboard-coupled (styling, portals), import as-is
first; only lift to `concierge/` if it visually breaks inside the chat
drawer.

## Module 4 — `useConciergeChat` exposes `injectAssistantMessage`

```js
const injectAssistantMessage = useCallback((text) => {
  setMessages((prev) => [
    ...prev,
    { id: `assistant-injected-${Date.now()}`, role: 'assistant', text, followups: [] },
  ]);
  if (!isOpenRef.current) setUnreadCount((n) => n + 1);
}, []);

return { ..., injectAssistantMessage };
```

Backend transcript will not record this message (it's client-only) — matches
ADR-0004's "audit trail lives in Stripe + checkout API logs, not in
`chat_messages`" consequence.

## Module 5 — Plumbing `mode` + `feedUuid`

```jsx
// LandingPage.jsx
<ConciergeChat mode="pre_signup" />

// DashboardPage.jsx
<ConciergeChat mode="post_signup" feedUuid={feedUuid} />

// ConciergeChat.jsx
export default function ConciergeChat({ mode = 'pre_signup', feedUuid = null }) {
  const { ..., injectAssistantMessage } = useConciergeChat();
  // ...
  return <ChatPanel mode={mode} feedUuid={feedUuid}
                    onInjectAssistantMessage={injectAssistantMessage} {...rest} />;
}
```

## Test plan (manual)

| # | Mode | Input | Expected |
|---|---|---|---|
| 1 | post_signup | "I want to add credits" | `WalletTopupModal` opens; bot KB reply lands |
| 2 | post_signup | "top up my wallet" | Same as #1 |
| 3 | pre_signup | "buy credits" | No modal; bot explains wallet plans normally |
| 4 | both | "do you offer annual?" | Injected assistant message with mailto; no LLM call |
| 5 | both | "lock my zip" | No modal (deferred); bot replies from KB |
| 6 | both | "how much does pro cost?" | No intent; normal flow |
| 7 | post_signup | "I locked myself out of the app" | `wallet` regex doesn't match; `lock` not detected; normal flow |

## Rollout order

1. Add `intent.js`.
2. Thread `mode` + `feedUuid` props (no behavior change).
3. Add `injectAssistantMessage` to hook.
4. Wire `annual` trigger.
5. Wire `wallet` post_signup trigger (reuse `WalletTopupModal`).
6. Update `docs/chat-launch-checklist.md` §D + `CONTEXT.md` to mark `lock`
   as v2 / deferred.

## Out of scope (tracked for v2)

- `lock` trigger — blocked on backend territory-lock checkout API.
- `unlock` trigger — belongs in dashboard lead-card UI (per checklist).
- Pre_signup wallet purchase path — would require either a guest-checkout
  wallet SKU on the backend or a "signup-then-topup" funnel.
- Backend audit of triggered checkouts in `chat_messages` — ADR-0004 says
  Stripe + checkout API logs are the system of record.
