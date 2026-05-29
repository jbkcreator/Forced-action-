"""Create a minimal test subscriber and print its feedUuid. Safe to run multiple times."""
import sys, os, uuid
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.database import Database
from src.core.models import Subscriber
from sqlalchemy import select

TEST_EMAIL = "test-concierge@forcedaction.dev"

db = Database()
with db.session_scope() as session:
    existing = session.execute(
        select(Subscriber).where(Subscriber.email == TEST_EMAIL)
    ).scalar_one_or_none()

    if existing:
        print(f"Already exists — feedUuid: {existing.event_feed_uuid}")
    else:
        feed_uuid = str(uuid.uuid4())
        sub = Subscriber(
            stripe_customer_id=f"cus_test_{uuid.uuid4().hex[:16]}",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            status="active",
            event_feed_uuid=feed_uuid,
            email=TEST_EMAIL,
            name="Test Concierge User",
        )
        session.add(sub)
        session.flush()
        print(f"Created — feedUuid: {feed_uuid}")
        print(f"Dashboard URL: http://localhost:5173/dashboard/{feed_uuid}")
