from src.core.database import get_db_context
from sqlalchemy import text

q = """
SELECT table_name,
       pg_total_relation_size(quote_ident(table_name)) AS size_bytes
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN ('manual_action_log', 'human_close_escalations', 'partner_subscriptions')
ORDER BY table_name;
"""

with get_db_context() as db:
    rows = db.execute(text(q)).fetchall()
    if not rows:
        print("NO TABLES FOUND")
    for row in rows:
        print(f"EXISTS: {row[0]}  ({row[1]} bytes)")
