"""
Wallet concurrency tests — proves that simultaneous debit() calls on the
same wallet cannot over-spend.

Requires real Postgres (the SELECT FOR UPDATE row lock is a no-op on SQLite).

Run:
    pytest tests/test_wallet_concurrency.py -v
"""
import threading
import uuid

import pytest
from sqlalchemy.orm import sessionmaker

from src.core.models import Subscriber, WalletBalance, WalletTransaction


def _seed_wallet(session, credits: int) -> tuple[int, int]:
    """Create a Subscriber + WalletBalance with the given starting balance.
    Returns (subscriber_id, wallet_id)."""
    uid = uuid.uuid4().hex[:8]
    sub = Subscriber(
        stripe_customer_id=f"cus_concurrency_{uid}",
        tier="starter",
        vertical="roofing",
        county_id="hillsborough",
        event_feed_uuid=f"concurrency-{uid}",
    )
    session.add(sub)
    session.flush()
    wallet = WalletBalance(
        subscriber_id=sub.id,
        wallet_tier="starter_wallet",
        credits_remaining=credits,
        credits_used_total=0,
        auto_reload_enabled=False,
    )
    session.add(wallet)
    session.flush()
    session.commit()
    return sub.id, wallet.id


@pytest.fixture
def isolated_subscriber(pg_engine):
    """Spin up a Subscriber + Wallet in committed state, clean up at the end.

    Concurrency tests can't share the rollback-after-each-test pattern of
    the standard fresh_db fixture — we need committed rows that other
    sessions can SELECT FOR UPDATE.
    """
    if pg_engine is None:
        pytest.skip("DATABASE_URL not configured")
    Session = sessionmaker(bind=pg_engine)
    seed_session = Session()
    sub_id, wallet_id = _seed_wallet(seed_session, credits=10)
    seed_session.close()

    yield sub_id, wallet_id, Session

    cleanup = Session()
    cleanup.query(WalletTransaction).filter_by(subscriber_id=sub_id).delete()
    cleanup.query(WalletBalance).filter_by(id=wallet_id).delete()
    cleanup.query(Subscriber).filter_by(id=sub_id).delete()
    cleanup.commit()
    cleanup.close()


def test_concurrent_debits_never_over_spend(isolated_subscriber):
    """50 concurrent debit() calls against a wallet seeded with 10 credits
    must result in exactly 10 successes, 40 failures, balance=0, never
    negative.
    """
    sub_id, wallet_id, Session = isolated_subscriber
    from src.services.wallet_engine import debit

    workers = 50
    results = [None] * workers

    def worker(i: int):
        session = Session()
        try:
            ok = debit(sub_id, action="lead_unlock", db=session, description=f"concurrent_{i}")
            session.commit()
            results[i] = ok
        except Exception:
            session.rollback()
            results[i] = "error"
        finally:
            session.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    successes = sum(1 for r in results if r is True)
    failures = sum(1 for r in results if r is False)
    errors = sum(1 for r in results if r == "error")

    # Final balance read in a fresh session
    verify = Session()
    final = verify.query(WalletBalance).filter_by(id=wallet_id).one()
    verify.close()

    assert errors == 0, f"{errors} workers errored — wallet contention should not raise"
    assert successes == 10, f"expected 10 successful debits, got {successes}"
    assert failures == 40, f"expected 40 insufficient-credits, got {failures}"
    assert final.credits_remaining == 0, f"final balance {final.credits_remaining} != 0"
    assert final.credits_used_total == 10


def test_check_constraint_rejects_negative_balance(pg_engine):
    """Defense-in-depth: even if a future code path sidesteps the lock, the
    CHECK constraint must refuse a write that would take credits_remaining
    below zero.
    """
    if pg_engine is None:
        pytest.skip("DATABASE_URL not configured")
    from sqlalchemy.exc import IntegrityError
    Session = sessionmaker(bind=pg_engine)
    session = Session()
    uid = uuid.uuid4().hex[:8]
    sub = Subscriber(
        stripe_customer_id=f"cus_check_{uid}",
        tier="starter", vertical="roofing", county_id="hillsborough",
        event_feed_uuid=f"check-{uid}",
    )
    session.add(sub)
    session.flush()
    wallet = WalletBalance(
        subscriber_id=sub.id, wallet_tier="starter_wallet",
        credits_remaining=5, credits_used_total=0,
    )
    session.add(wallet)
    session.flush()
    # Force a negative write — the CHECK should reject the COMMIT.
    wallet.credits_remaining = -1
    with pytest.raises(IntegrityError):
        session.flush()
    session.rollback()
    # Cleanup
    session.query(WalletTransaction).filter_by(subscriber_id=sub.id).delete()
    session.query(WalletBalance).filter_by(subscriber_id=sub.id).delete()
    session.query(Subscriber).filter_by(id=sub.id).delete()
    session.commit()
    session.close()
