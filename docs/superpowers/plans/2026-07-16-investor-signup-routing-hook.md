# Investor Signup Routing Hook Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the T-B3-02 routing hook now while documenting that the real investor deal-submission destination remains blocked by Block 5 and RESPA clearance.

**Architecture:** Backend free signup accepts `investor` as a signup-only vertical/account type and magic-link verification returns the subscriber vertical. Frontend post-magic-link routing uses a centralized helper so the temporary investor destination can be swapped for the real Block 5 intake route later.

**Tech Stack:** FastAPI/Pydantic/SQLAlchemy backend, React/Vite/Vitest frontend, pytest scenario tests.

## Global Constraints

- Do not build Block 5 deal intake, investor_deals tables, approval workflow, lender matching, or referral/commission logic.
- Investor route destination is a reserved hook only: `/deals/submit?feed_uuid=<feed_uuid>`.
- Existing non-investor free signup and magic-link login must continue routing to `/dashboard/:feedUuid`.
- Preserve unrelated local changes in both git roots.

---

### Task 1: Signup Vertical and Post-Verify Routing Hook

**Files:**
- Modify: `Forced-action-/src/api/deps.py`
- Modify: `Forced-action-/src/api/subscriber_router.py`
- Modify: `Forced-action-ui/src/pages/MagicLinkVerifyPage.jsx`
- Create: `Forced-action-ui/src/utils/postSignupRoute.js`
- Test: `Forced-action-/tests/scenarios/test_subscriber_magic_link_e2e.py`
- Test: `Forced-action-ui/src/pages/MagicLinkVerifyPage.test.jsx`
- Modify: `PENDING_TASKS.md`

**Interfaces:**
- Produces: `SIGNUP_VERTICALS: frozenset[str]` for signup account-type validation.
- Produces: `getPostSignupRoute({ vertical, feedUuid }): string` for frontend routing.
- Produces: magic-link verify JSON containing `vertical`.

- [x] **Step 1: Write failing tests**

Backend: assert `/api/free-signup` accepts `vertical: investor` and returns it.

Frontend: assert magic-link verification routes `vertical: investor` to `/deals/submit?feed_uuid=<feedUuid>` and non-investors to `/dashboard/<feedUuid>`.

- [ ] **Step 2: Verify tests fail before production code**

Run targeted backend and frontend tests. Expected failures: investor rejected by backend validation; frontend still routes investor to dashboard. If sandbox prevents process spawn, rerun with escalation.

- [ ] **Step 3: Implement minimal backend hook**

Add `SIGNUP_VERTICALS = VALID_VERTICALS | {'investor'}` and use it for `/api/free-signup`. Return `vertical` from `/api/subscriber/magic-link/verify`.

- [ ] **Step 4: Implement minimal frontend hook**

Add `getPostSignupRoute`, use it for auto redirect and manual continue button on `MagicLinkVerifyPage`.

- [ ] **Step 5: Document partial status**

Update T-B3-02 in `PENDING_TASKS.md` to mark hook-only scope and deferred Block 5 DoD.

- [ ] **Step 6: Verify**

Run targeted backend/frontend tests. If full backend test runner is unavailable in this environment, report the exact command failure and any successful frontend verification.