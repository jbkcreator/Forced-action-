# Chat buying-intent triggers are detected and acted on by the frontend only

The chat backend (`concierge_chat.py`) is a knowledge-grounded, stateless
turn processor. Adding intent classification and Stripe session creation to it
would require database writes, Stripe API calls, and session-state awareness
inside a publicly-facing, unauthenticated endpoint — expanding its blast radius
substantially. Instead, the React frontend detects buying-intent keywords
(`wallet`, `lock`) in the user's outgoing message, fires the appropriate
API call directly (existing `/api/checkout` endpoints), and overlays the
result card in the chat UI. The backend receives and answers the same message
as plain text; it has no awareness of the triggered action.

## Considered options

- **Backend-driven (M5 report Option A):** backend classifies intent, calls
  Stripe, returns a `payment_event` field alongside the text reply. Rejected:
  adds DB + external API calls to the unauthenticated chat endpoint; requires
  coordinated frontend + backend deploy for every new trigger.
- **Hybrid per M5 spec (Option C):** backend emits `payment_event`; frontend
  reads it. Rejected for v1: M5 report documents Bug F3 (frontend ignores
  `payment_event`); fixing both sides simultaneously delays launch.
- **Frontend-only (chosen):** frontend intercepts keywords before the message
  is sent, fires checkout API independently, chat turn proceeds normally.
  Fastest path; backend stays unchanged; intent logic is in one place (React).

## Consequences

Adding a new buying-intent trigger in future requires a frontend deploy only.
The chat transcript will not record that a checkout was triggered — audit trail
lives in Stripe and the checkout API logs, not in `chat_messages`.
