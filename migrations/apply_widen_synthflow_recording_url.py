"""Widen synthflow_calls.recording_url from VARCHAR(500) to VARCHAR(2000).
Signed GCS URLs from Synthflow exceed 500 chars."""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()
cur.execute("""
    ALTER TABLE synthflow_calls
        ALTER COLUMN recording_url TYPE VARCHAR(2000)
""")
conn.commit()
cur.close()
conn.close()
print("done: recording_url widened to VARCHAR(2000)")
