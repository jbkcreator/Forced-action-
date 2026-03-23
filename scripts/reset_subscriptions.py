"""Reset all subscription data for retesting. Delete after use."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.core.database import get_db_context
from src.core.models import Subscriber, ZipTerritory, FoundingSubscriberCount

with get_db_context() as db:
    zips   = db.query(ZipTerritory).delete()
    subs   = db.query(Subscriber).delete()
    counts = db.query(FoundingSubscriberCount).delete()
    print(f"Deleted: {subs} subscribers, {zips} zip territories, {counts} founding counts")
    print("Done — ready to retest checkout.")
