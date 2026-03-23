"""Check what subscribers exist in DB. Delete after use."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.core.database import get_db_context
from src.core.models import Subscriber

with get_db_context() as db:
    subs = db.query(Subscriber).all()
    if not subs:
        print("No subscribers in DB.")
    for s in subs:
        print(f"id={s.id} email={s.email} customer={s.stripe_customer_id} status={s.status} tier={s.tier}")
