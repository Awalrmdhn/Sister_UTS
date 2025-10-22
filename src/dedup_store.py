# src/dedup_store.py
import sqlite3
import os
import threading

class DedupStore:
    def __init__(self, db_path="data/dedup.db"):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.lock = threading.Lock()
        self._init_table()

    def _init_table(self):
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    topic TEXT,
                    event_id TEXT,
                    processed_at TEXT,
                    timestamp TEXT,
                    source TEXT,
                    payload TEXT,
                    PRIMARY KEY (topic, event_id)
                )
            """)

    def mark_processed(self, topic, event_id, processed_at, timestamp, source, payload):
        """Return True if new, False if duplicate"""
        with self.lock:
            try:
                with self.conn:
                    self.conn.execute(
                        "INSERT OR IGNORE INTO events (topic, event_id, processed_at, timestamp, source, payload) VALUES (?, ?, ?, ?, ?, ?)",
                        (topic, event_id, processed_at, timestamp, source, payload)
                    )
                    # Cek apakah benar-benar tersimpan (bukan duplikat)
                    cursor = self.conn.execute(
                        "SELECT changes()"
                    )
                    changes = cursor.fetchone()[0]
                    return changes > 0
            except Exception as e:
                print("DB Error:", e)
                return False

    def get_events(self, topic=None):
        query = "SELECT topic, event_id, timestamp, source, payload FROM events"
        params = ()
        if topic:
            query += " WHERE topic = ?"
            params = (topic,)
        cur = self.conn.execute(query, params)
        return cur.fetchall()

    def get_stats(self):
        cur = self.conn.execute("SELECT COUNT(*) FROM events")
        unique_processed = cur.fetchone()[0]
        cur = self.conn.execute("SELECT DISTINCT topic FROM events")
        topics = [r[0] for r in cur.fetchall()]
        return {"unique_processed": unique_processed, "topics": topics}
