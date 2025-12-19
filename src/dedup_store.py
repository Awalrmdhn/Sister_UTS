# src/dedup_store.py
import sqlite3
import os
import threading
                                                             
class DedupStore:
    def __init__(self, db_path="data/dedup.db"):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        # check_same_thread=False diperlukan karena diakses oleh worker async/thread berbeda
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.lock = threading.Lock()
        self._init_table()

    def _init_table(self):
        with self.conn:
            # Menggunakan nama tabel 'processed_events' sesuai saran soal
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_events (
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
        """
        Melakukan Atomic Insert (Transaksi).
        Return True jika event baru (sukses insert), False jika duplikat (ignore).
        """
        with self.lock:  # Serialisasi akses write di level aplikasi (opsional tapi aman untuk SQLite)
            try:
                # 'with self.conn' memulai transaksi SQLite secara otomatis
                with self.conn:
                    # IMPLEMENTASI POIN D: Transaksi & Konkurensi
                    # Menggunakan INSERT OR IGNORE sebagai mekanisme idempotency atomik
                    # setara dengan ON CONFLICT DO NOTHING di Postgres
                    self.conn.execute(
                        "INSERT OR IGNORE INTO processed_events (topic, event_id, processed_at, timestamp, source, payload) VALUES (?, ?, ?, ?, ?, ?)",
                        (topic, event_id, processed_at, timestamp, source, payload)
                    )
                    
                    # Cek apakah baris benar-benar dimasukkan
                    cursor = self.conn.execute("SELECT changes()")
                    changes = cursor.fetchone()[0]
                    return changes > 0
            except Exception as e:
                print("DB Error:", e)
                # Jika error, asumsikan gagal proses (bisa diretry)
                return False

    def get_events(self, topic=None):
        query = "SELECT topic, event_id, timestamp, source, payload FROM processed_events"
        params = ()
        if topic:
            query += " WHERE topic = ?"
            params = (topic,)
        # Tambahkan limit agar tidak crash jika data sangat besar
        query += " LIMIT 1000" 
        cur = self.conn.execute(query, params)
        return cur.fetchall()

    def get_stats(self):
        # Hitung unique processed langsung dari DB (Persisten)
        cur = self.conn.execute("SELECT COUNT(*) FROM processed_events")
        unique_processed = cur.fetchone()[0]
        
        cur = self.conn.execute("SELECT DISTINCT topic FROM processed_events")
        topics = [r[0] for r in cur.fetchall()]
        
        return {"unique_processed": unique_processed, "topics": topics}