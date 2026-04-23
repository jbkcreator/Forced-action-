"""
Read tools for Cora graphs.

These are the 12 read-only queries graphs use to assemble decision context.
Every tool here is a thin wrapper over existing platform services or a
straightforward Postgres query. No side effects. No external API calls
beyond the database and Redis (which is already platform-shared).

Pattern:
  - Every tool accepts an optional SQLAlchemy Session. If not provided,
	the tool opens its own read-only session via db.session_scope().
  - Every tool returns a dict (or list of dicts) so the shape is stable
	across graph versions. No ORM objects leak out.
  - Every tool handles the "no data yet" case by returning empty dicts /
	sensible defaults rather than raising.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Generator, List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.agents.tools.registry import tool
from src.core.database import db
from src.core.models import (
	DealOutcome,
	LearningCard,
	MessageOutcome,
	Subscriber,
	UserSegment,
	WalletBalance,
	SmsOptIn,
	SmsOptOut,
	AbAssignment,
)


@contextmanager
def _session(provided: Optional[Session]) -> Generator[Session, None, None]:
	"""Use a caller-provided session or open a new scoped one."""
	if provided is not None:
		yield provided
		return
	with db.session_scope() as s:
		yield s


# ──────────────────────────────────────────────────────────────────────────────
# 1. Subscriber profile
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_subscriber_profile(
	subscriber_id: int,
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""Return a subscriber's profile as a flat dict. Empty dict if not found."""
	with _session(session) as s:
		sub = s.query(Subscriber).filter(Subscriber.id == subscriber_id).first()
		if sub is None:
			return {}
		return {
			"id": sub.id,
			"tier": sub.tier,
			"status": sub.status,
			"vertical": sub.vertical,
			"county_id": sub.county_id,
			"founding_member": sub.founding_member,
			"email": sub.email,
			"name": sub.name,
			"has_saved_card": sub.has_saved_card,
			"auto_mode_enabled": sub.auto_mode_enabled,
			"referral_code": sub.referral_code,
			"created_at": sub.created_at.isoformat() if sub.created_at else None,
			"billing_date": sub.billing_date.isoformat() if sub.billing_date else None,
		}


# ──────────────────────────────────────────────────────────────────────────────
# 2. Segment and revenue signal score
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_segment_and_score(
	subscriber_id: int,
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""Return the subscriber's current bucket and 0–100 revenue signal score."""
	with _session(session) as s:
		seg = (
			s.query(UserSegment)
			.filter(UserSegment.subscriber_id == subscriber_id)
			.first()
		)
		if seg is None:
			return {"segment": "new", "revenue_signal_score": 0, "classified_at": None}
		return {
			"segment": seg.segment,
			"revenue_signal_score": int(seg.revenue_signal_score or 0),
			"classified_at": seg.last_classified_at.isoformat() if seg.last_classified_at else None,
			"reason": seg.classification_reason,
		}


# ──────────────────────────────────────────────────────────────────────────────
# 3. Wallet state
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_wallet_state(
	subscriber_id: int,
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""Return the subscriber's wallet tier, balance, usage, and auto-reload state."""
	with _session(session) as s:
		w = (
			s.query(WalletBalance)
			.filter(WalletBalance.subscriber_id == subscriber_id)
			.first()
		)
		if w is None:
			return {
				"enrolled": False,
				"tier": None,
				"credits_remaining": 0,
				"credits_used_total": 0,
				"auto_reload_enabled": False,
				"last_reload_at": None,
			}
		return {
			"enrolled": True,
			"tier": w.wallet_tier,
			"credits_remaining": w.credits_remaining,
			"credits_used_total": w.credits_used_total,
			"auto_reload_enabled": w.auto_reload_enabled,
			"last_reload_at": w.last_reload_at.isoformat() if w.last_reload_at else None,
		}


# ──────────────────────────────────────────────────────────────────────────────
# 4. ZIP activity
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_zip_activity(
	zip_code: str,
	vertical: Optional[str] = None,
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""
	Live activity snapshot for a ZIP — active urgency-window count plus
	recent message-send volume from Cora into this ZIP's subscribers.

	The urgency count comes from Redis (via urgency_engine.get_active_count)
	and degrades gracefully when Redis is down.
	"""
	from src.services import urgency_engine

	active_viewers = urgency_engine.get_active_count(zip_code)

	with _session(session) as s:
		# Recent outbound messages to subscribers who match this ZIP's vertical.
		# This is a coarse signal — enough for graphs to reason about ZIP heat.
		cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
		q = (
			s.query(func.count(MessageOutcome.id))
			.join(Subscriber, Subscriber.id == MessageOutcome.subscriber_id)
			.filter(MessageOutcome.sent_at >= cutoff)
		)
		if vertical:
			q = q.filter(Subscriber.vertical == vertical)
		recent_message_count = int(q.scalar() or 0)

	return {
		"zip": zip_code,
		"vertical": vertical,
		"active_viewers": active_viewers,
		"messages_last_24h": recent_message_count,
	}


# ──────────────────────────────────────────────────────────────────────────────
# 5. Lead pool
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_lead_pool(
	zip_code: str,
	vertical: Optional[str] = None,
	min_score: int = 0,
	limit: int = 25,
	session: Optional[Session] = None,
) -> List[Dict[str, Any]]:
	"""
	Return scored leads available in a ZIP. Filtered by vertical and minimum
	score. Results ordered by score descending.
	"""
	from src.core.models import DistressScore, Property

	with _session(session) as s:
		q = (
			s.query(DistressScore, Property)
			.join(Property, Property.id == DistressScore.property_id)
			.filter(Property.zip == zip_code)
			.filter(DistressScore.final_cds_score >= min_score)
		)
		rows = q.order_by(DistressScore.final_cds_score.desc()).limit(limit).all()

		return [
			{
				"property_id": prop.id,
				"address": prop.address,
				"zip": prop.zip,
				"score": float(score.final_cds_score or 0),
				"tier": score.lead_tier,
				"urgency_level": score.urgency_level,
				"scored_at": score.score_date.isoformat() if score.score_date else None,
			}
			for score, prop in rows
		]


# ──────────────────────────────────────────────────────────────────────────────
# 6. Competition status
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_competition_status(
	zip_code: str,
	vertical: Optional[str] = None,
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""
	Competitive snapshot for a ZIP: current lock holder (if any), active
	wallet users targeting this ZIP, and a heuristic "temperature" value.
	"""
	from src.core.models import ZipTerritory

	with _session(session) as s:
		lock_q = s.query(ZipTerritory).filter(ZipTerritory.zip_code == zip_code)
		if vertical:
			lock_q = lock_q.filter(ZipTerritory.vertical == vertical)
		lock_q = lock_q.filter(ZipTerritory.status == "locked")
		lock = lock_q.first()

		wallet_count_q = (
			s.query(func.count(WalletBalance.id))
			.join(Subscriber, Subscriber.id == WalletBalance.subscriber_id)
			.filter(Subscriber.status == "active")
		)
		if vertical:
			wallet_count_q = wallet_count_q.filter(Subscriber.vertical == vertical)
		active_wallet_count = int(wallet_count_q.scalar() or 0)

	lock_holder_id = getattr(lock, "subscriber_id", None) if lock else None

	return {
		"zip": zip_code,
		"vertical": vertical,
		"lock_holder_subscriber_id": lock_holder_id,
		"is_locked": lock_holder_id is not None,
		"active_wallet_users_in_vertical": active_wallet_count,
	}


# ──────────────────────────────────────────────────────────────────────────────
# 7. Recent messages (to avoid repetition)
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_recent_messages(
	subscriber_id: int,
	hours: int = 72,
	session: Optional[Session] = None,
) -> List[Dict[str, Any]]:
	"""Return recent outbound messages for a subscriber. Used to prevent repetition."""
	with _session(session) as s:
		cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
		rows = (
			s.query(MessageOutcome)
			.filter(MessageOutcome.subscriber_id == subscriber_id)
			.filter(MessageOutcome.sent_at >= cutoff)
			.order_by(MessageOutcome.sent_at.desc())
			.limit(50)
			.all()
		)
		return [
			{
				"id": r.id,
				"message_type": r.message_type,
				"template_id": r.template_id,
				"variant_id": r.variant_id,
				"channel": r.channel,
				"sent_at": r.sent_at.isoformat(),
				"conversion_type": r.conversion_type,
			}
			for r in rows
		]


# ──────────────────────────────────────────────────────────────────────────────
# 8. Latest learning card
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_learning_card(
	card_type: str = "general",
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""
	Return the most recent learning card of the given type. Empty dict if
	no card exists yet (Sunday job hasn't run).

	Card types: message_perf, deal_pattern, ab_result, churn_signal,
	pricing_test, general.
	"""
	with _session(session) as s:
		card = (
			s.query(LearningCard)
			.filter(LearningCard.card_type == card_type)
			.order_by(LearningCard.card_date.desc())
			.first()
		)
		if card is None:
			return {}
		return {
			"card_date": card.card_date.isoformat(),
			"card_type": card.card_type,
			"summary_text": card.summary_text,
			"data": card.data_json or {},
			"action_taken": card.action_taken,
		}


# ──────────────────────────────────────────────────────────────────────────────
# 9. Guardrail lookup
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_guardrail(name: str) -> Dict[str, Any]:
	"""
	Return the numeric bound config for a named guardrail.

	Valid names are the keys in config/cora_guardrails.py GUARDRAILS.
	Returns an empty dict for unknown names (graphs treat absence as
	"fall back to conservative default").
	"""
	from config.cora_guardrails import GUARDRAILS

	return dict(GUARDRAILS.get(name, {}))


# ──────────────────────────────────────────────────────────────────────────────
# 10. A/B variant assignment (read — determinism-dependent lookup)
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_ab_variant(
	subscriber_id: int,
	test_name: str,
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""
	Return the already-assigned A/B variant for this (subscriber, test).
	Empty dict if the subscriber isn't in the test yet. Assignment itself is
	a gating tool — see ab_variant_assign in gating_tools.
	"""
	from src.core.models import AbTest

	with _session(session) as s:
		row = (
			s.query(AbAssignment)
			.join(AbTest, AbTest.id == AbAssignment.test_id)
			.filter(AbAssignment.subscriber_id == subscriber_id)
			.filter(AbTest.test_name == test_name)
			.first()
		)
		if row is None:
			return {}
		return {
			"subscriber_id": subscriber_id,
			"test_name": test_name,
			"variant": row.variant,
			"outcome": row.outcome,
			"assigned_at": row.created_at.isoformat() if row.created_at else None,
		}


# ──────────────────────────────────────────────────────────────────────────────
# 11. Deal history
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def get_deal_history(
	subscriber_id: int,
	limit: int = 20,
	session: Optional[Session] = None,
) -> List[Dict[str, Any]]:
	"""Return the subscriber's reported deals, newest first."""
	with _session(session) as s:
		rows = (
			s.query(DealOutcome)
			.filter(DealOutcome.subscriber_id == subscriber_id)
			.order_by(DealOutcome.created_at.desc())
			.limit(limit)
			.all()
		)
		return [
			{
				"id": r.id,
				"deal_bucket": r.deal_size_bucket,
				"deal_amount": float(r.deal_amount or 0),
				"deal_date": r.deal_date.isoformat() if r.deal_date else None,
				"days_to_close": r.days_to_close,
				"lead_source": r.lead_source,
				"created_at": r.created_at.isoformat() if r.created_at else None,
			}
			for r in rows
		]


# ──────────────────────────────────────────────────────────────────────────────
# 12. Opt-in check
# ──────────────────────────────────────────────────────────────────────────────

@tool(category="read")
def check_opt_in(
	phone: str,
	session: Optional[Session] = None,
) -> Dict[str, Any]:
	"""
	Return TCPA opt-in status for a phone number. Mirrors what the
	compliance gate does, but exposed as a read tool so graphs can decide
	early whether to spend tokens composing a message we cannot send.
	"""
	with _session(session) as s:
		opt_in = (
			s.query(SmsOptIn)
			.filter(SmsOptIn.phone == phone)
			.order_by(SmsOptIn.opted_in_at.desc())
			.first()
		)
		opt_out = (
			s.query(SmsOptOut)
			.filter(SmsOptOut.phone == phone)
			.order_by(SmsOptOut.opted_out_at.desc())
			.first()
		)

		has_optin = opt_in is not None
		has_optout = opt_out is not None
		# Opt-out always wins, regardless of opt-in date.
		can_send = has_optin and not has_optout

		return {
			"phone": phone,
			"has_opt_in": has_optin,
			"has_opt_out": has_optout,
			"can_send_marketing": can_send,
			"opt_in_keyword": opt_in.keyword_used if opt_in else None,
			"opt_in_source": opt_in.source if opt_in else None,
			"opt_in_at": opt_in.opted_in_at.isoformat() if opt_in and opt_in.opted_in_at else None,
			"opt_out_keyword": opt_out.keyword_used if opt_out else None,
			"opt_out_at": opt_out.opted_out_at.isoformat() if opt_out and opt_out.opted_out_at else None,
		}
