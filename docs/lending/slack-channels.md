# Next Deal Lending: Slack channels

The new Next Deal Lending workspace holds only NDL channels. Every post type has one channel setting (a channel
ID, never a name). **There is no fallback between settings**: the old channels are being deleted, so an unset
setting skips that post and logs it; it never lands in another channel.

## Channels

| Channel | Setting | Bot | Posts |
|---|---|---|---|
| #lead-alerts | `LENDING_OPS_CHANNEL` | Lending | LendingFlow "call now" alert (@-mentions `LENDING_CORA_APPROVER_USER_IDS`, on with `LENDING_LEAD_ARRIVAL_ALERT_ENABLED`); 5-minute uncalled/unreached flags; returning-borrower form alert |
| #lead-alerts | `LENDING_REPLIES_CHANNEL` | Lending | Rate/terms and reschedule handoffs, AI-quoted-number alerts, reply-SLA alert |
| #cora | `LENDING_CORA_CHANNEL` | Cora | Chat with Cora; 7:30am digest; inbox digest; 8am routing-hold summary; emergency halt notices; `/lenders` and `/partners` audit posts; pinned callbacks |
| #approvals | `LENDING_CORA_APPROVALS_CHANNEL` | Cora | Approve / Revise / Reject cards. **Required**: Cora does not start without it. A Revise reply typed here needs no @-mention |
| #deals | `LENDING_DEALS_CHANNEL` | Cora | R-1 routing posts (Routed / Held / Declined) with Details, Decline / Approved / Funded and Re-route buttons |
| #deals | `LENDING_PARTNER_CHANNEL` | Lending | Partner file cards; partner notice not delivered |
| #isa-daily | `LENDING_DAVID_CHANNEL` | Lending | David's end-of-day recap only |
| #lending-daily | `LENDING_DAILY_CHANNEL` | Lending | 7:20pm closing scoreboard; hourly scoreboard 10am-6pm ET |
| #dial-tasks | `LENDING_DIAL_TASKS_CHANNEL` | Lending | After-call results; 9am confirmation tasks; missing-disposition and unknown-code alerts; web-lead and LendingFlow sweep alerts; soft-approval cards and PDF; enrichment deal threads |
| (Josh to confirm) | `LENDING_DNC_ALERT_CHANNEL` | Lending | "DNC not propagated" and "DNC removal pending" |
| (Josh to confirm) | `LENDING_OPS_NOTICE_CHANNEL` | Lending | Opt-out mailbox alerts; GHL webhook-failure alerts |

Every alert above that Cora tracks is also raised in her inbox (key `ops`, `ops_notice`, `dnc`, `replies`,
`dial_tasks`, `daily`, `deals`), so a post that is skipped or refused still reaches #cora's digest.

## Slack apps

| | Lending app | Cora app |
|---|---|---|
| Tokens | `LENDING_SLACK_BOT_TOKEN`, `LENDING_SLACK_SIGNING_SECRET` | `LENDING_CORA_SLACK_BOT_TOKEN`, `LENDING_CORA_SLACK_APP_TOKEN` (`connections:write`) |
| Mode | Interactivity on, Request URL `https://<host>/webhooks/lending/slack-interactivity` (soft-approval buttons) | Socket Mode and Interactivity on, no URL |
| Scopes | `chat:write`, `files:write` | `chat:write`, `commands`, `channels:read`, `groups:read`, `channels:history`, `groups:history` |
| Events | none | `message.channels`, `message.groups` |
| Slash commands | none | `/lenders`, `/partners` (approvers only) |
| Invite the bot to | #lead-alerts, #dial-tasks, #isa-daily, #lending-daily, #deals, and the DNC / notice channels | #cora, #approvals, #deals, #lead-alerts, #dial-tasks, #isa-daily, #lending-daily |

Cora's bot needs every channel her inbox routes to: she lists and reads only the channels her bot is in.

## User IDs

They change in the new workspace: `LENDING_CORA_APPROVER_USER_IDS` (Josh; also the "call now" @-mention),
`LENDING_SOFT_APPROVAL_USER_IDS`, `LENDING_CORA_OPERATOR_USER_IDS`.

## Cutover

1. Create the channels, install both apps, add the slash commands, invite the bots.
2. Set every channel ID, token and user ID above. Leaving one unset silences those posts.
3. Deploy, then restart `lending-api`, `fa-lending-cora`, `fa-lending-cdr-poller`, `fa-lending-uncalled-alarm-worker`
   and `fa-lending-reminder-worker` (cron jobs read the settings on their next run).
4. Smoke test one post per channel: a 5-minute flag, a digest, an approval card plus a Revise reply in #approvals,
   a routing post plus a button click in #deals, a partner card, the scoreboard, David's recap, an after-call
   result. Run `/lenders` and `/partners` as Josh (audit posts land in #cora).
5. `@Cora emergency halt`, then `@Cora emergency resume`: the alert lands in #cora.

Do not change the A2P consent text or form labels during the move.
