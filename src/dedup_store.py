# src/dedup_store.py
import sqlite3
import os
from datetime import datetime

class DedupStore:
    def __init__(self, db_path="data/dedup.db"):
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        # check_same_thread=False allows usage from multiple threads/coroutines
        self.conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        # durability settings
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=FULL;")
        self._init_table()

    def _init_table(self):
        with self.conn:
            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_events (
                topic TEXT NOT NULL,
                event_id TEXT NOT NULL,
                processed_at TEXT NOT NULL,
                PRIMARY KEY (topic, event_id)
            )""")
            # optional: store processed payloads for GET /events
            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS events_store (
                topic TEXT NOT NULL,
                event_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                source TEXT,
                payload TEXT,
                PRIMARY KEY (topic, event_id)
            )""")

    def mark_processed(self, topic: str, event_id: str, processed_at: str, timestamp_iso: str, source: str, payload_text: str) -> bool:
        """
        Try to insert into processed_events (atomic). If success -> also insert into events_store.
        Returns True if newly processed, False if duplicate.
        """
        try:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO processed_events(topic, event_id, processed_at) VALUES (?, ?, ?)",
                    (topic, event_id, processed_at)
                )
                self.conn.execute(
                    "INSERT OR REPLACE INTO events_store(topic, event_id, timestamp, source, payload) VALUES (?, ?, ?, ?, ?)",
                    (topic, event_id, timestamp_iso, source, payload_text)
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def get_events(self, topic: str = None):
        cur = self.conn.cursor()
        if topic:
            cur.execute("SELECT topic, event_id, timestamp, source, payload FROM events_store WHERE topic = ? ORDER BY timestamp ASC", (topic,))
        else:
            cur.execute("SELECT topic, event_id, timestamp, source, payload FROM events_store ORDER BY timestamp ASC")
        rows = cur.fetchall()
        return rows

    def get_stats(self):
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_events")
        total = cur.fetchone()[0]
        cur.execute("SELECT DISTINCT topic FROM processed_events")
        topics = [r[0] for r in cur.fetchall()]
        return {"unique_processed": total, "topics": topics}
