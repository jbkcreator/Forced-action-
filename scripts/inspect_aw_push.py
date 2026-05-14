"""Throwaway inspector — prints latest accelerated_wallet_push audit rows for sub 4509."""
from src.core.database import Database
from src.core.models import AgentDecision, MessageOutcome, WalletPushOffer
from sqlalchemy import select, desc

with Database().session_scope() as s:
    print("=== agent_decisions (last 3 for accelerated_wallet_push) ===")
    rows = s.execute(
        select(AgentDecision)
        .where(AgentDecision.graph_name == "accelerated_wallet_push")
        .order_by(desc(AgentDecision.started_at))
        .limit(3)
    ).scalars().all()
    for r in rows:
        cost = float(r.cost_usd or 0)
        print(
            f"  decision={str(r.decision_id)[:8]}... "
            f"terminal={r.terminal_status} tokens={r.tokens_used} "
            f"cost=${cost:.5f} summary={(r.summary or {})}"
        )

    print()
    print("=== wallet_push_offers (last 3 for sub 4509) ===")
    rows = s.execute(
        select(WalletPushOffer)
        .where(WalletPushOffer.subscriber_id == 4509)
        .order_by(desc(WalletPushOffer.id))
        .limit(3)
    ).scalars().all()
    for r in rows:
        print(
            f"  id={r.id} status={r.status} framing={r.framing_variant} "
            f"ab={r.ab_variant} tier={r.tier} offered_at={r.offered_at}"
        )

    print()
    print("=== message_outcomes (last 3 for sub 4509) ===")
    rows = s.execute(
        select(MessageOutcome)
        .where(MessageOutcome.subscriber_id == 4509)
        .order_by(desc(MessageOutcome.id))
        .limit(3)
    ).scalars().all()
    for r in rows:
        print(
            f"  id={r.id} message_type={r.message_type} channel={r.channel} "
            f"variant={r.variant_id} template={r.template_id} "
            f"sent_at={r.sent_at}"
        )
