import sys; sys.path.insert(0, '.')
from src.core.database import get_db_context
from sqlalchemy import text

with get_db_context() as db:
    r = db.execute(text("SELECT COUNT(*) FROM building_permits WHERE permit_type ILIKE '%roof%'")).scalar()
    print('Roofing permits total:', r)
    r2 = db.execute(text("SELECT COUNT(*) FROM building_permits WHERE permit_type ILIKE '%roof%' AND property_id IS NOT NULL")).scalar()
    print('Roofing permits matched:', r2)
    r3 = db.execute(text("SELECT COUNT(DISTINCT property_id) FROM distress_scores")).scalar()
    print('Total scored properties (distress_scores):', r3)
    r4 = db.execute(text("SELECT COUNT(*) FROM properties")).scalar()
    print('Total properties:', r4)
    r5 = db.execute(text("SELECT COUNT(*) FROM distress_scores WHERE qualified = true")).scalar()
    print('Qualified scored properties:', r5)
