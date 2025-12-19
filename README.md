
**Nama:** Awal Ramadhani 
**NIM:** 11221067 
**Program Studi:** Informatika – Institut Teknologi Kalimantan  
**github:** https://github.com/Awalrmdhn/Sister_UTS


Ringkasan Sistem dan Arsitektur  
Sistem ini merupakan implementasi **Distributed Log Aggregator** berbasis arsitektur **Publish–Subscribe (Pub/Sub)** yang dibangun menggunakan **FastAPI** dan **asyncio**.  
Tujuan utama sistem ini adalah menerima event dari banyak sumber (*publisher*), melakukan deduplikasi terhadap event duplikat, serta menjaga *idempotency* agar setiap event dengan kombinasi `(topic, event_id)` hanya diproses satu kali, bahkan jika diterima berulang kali.



Komponen utama sistem adalah sebagai berikut:
| Komponen | Fungsi | Implementasi |
|-----------|---------|--------------|
| **Aggregator** | Menerima event melalui endpoint `/publish`, memvalidasi dan memproses event menggunakan *asynchronous queue*, menyimpan event unik di SQLite, dan menyediakan data statistik melalui endpoint `/stats`. | `src/main.py`, `src/consumer.py`, `src/dedup_store.py` |
| **Publisher** | Mengirim batch event ke aggregator dengan tingkat duplikasi 20% untuk mensimulasikan *at-least-once delivery*. | `src/publisher.py` |



⚙️ Keputusan Desain
Aspek	Deskripsi Keputusan
Idempotency	Setiap event memiliki pasangan kunci (topic, event_id) untuk memastikan hanya diproses satu kali, meskipun dikirim berulang.
Dedup Store	Menggunakan SQLite sebagai basis data lokal tahan restart, sehingga event lama tetap dikenali pasca container restart.
Ordering	Sistem tidak menerapkan total ordering karena setiap event bersifat independen antar-topic; cukup partial ordering berdasarkan timestamp.
Retry Mechanism	Publisher mengirim ulang event duplikat (simulasi at-least-once delivery). Dedup store mencegah reprocessing.
Observability	Endpoint /stats menampilkan metrik seperti received, unique_processed, duplicate_dropped, topics, dan uptime.



📊 Spesifikasi API
Method	Endpoint	Deskripsi	Contoh Respons
POST	/publish	Menerima event tunggal atau batch JSON dan menambahkan ke queue.	{ "status": "ok", "received": 100 }
GET	/events?topic=...	Mengambil daftar event unik berdasarkan topic.	[{"event_id":"abc123", "payload":{...}}]
GET	/stats	Menampilkan statistik pemrosesan event.	{ "received": 5000, "unique_processed": 4000, "duplicate_dropped": 1000 }



instruksi run singkat:
Build: docker build -t uts-aggregator .
Run: docker run -p 8080:8080 uts-aggregator



🧪 Unit Testing
Pengujian dilakukan menggunakan pytest, httpx, dan asgi_lifespan untuk mensimulasikan lifecycle aplikasi.

Jenis Pengujian	Tujuan	Status
Validasi dedup	Memastikan event duplikat tidak dihitung ulang	✅ Passed
Persistensi dedup store	Memastikan SQLite tetap menyimpan status idempotensi setelah restart	✅ Passed
Validasi skema event	Mengecek kesesuaian struktur event JSON (topic, event_id, timestamp)	✅ Passed
Endpoint /stats & /events	Konsistensi data statistik dengan jumlah event sebenarnya	✅ Passed
Stress test (5000 event, 20% duplikasi)	Mengukur responsivitas sistem di bawah beban tinggi	✅ Passed

Semua 5 pengujian lulus dengan hasil:
nginx
Copy code
pytest -v  
5 passed, 0 failed, 1 warning


🧩 Evaluasi Tujuan Pembelajaran
Tujuan	Status	Penjelasan
Memahami karakteristik sistem terdistribusi (Bab 1)	✅	Sistem telah menerapkan prinsip distributed communication antar komponen.
Arsitektur Pub-Sub (Bab 2)	✅	Menggunakan model publish-subscribe dengan queue asinkron.
Komunikasi antar komponen (Bab 3)	✅	Komponen berinteraksi via HTTP async dan internal queue.
Penamaan dan event_id (Bab 4)	✅	Setiap event memiliki topic dan event_id unik untuk dedup.
Ordering dan waktu (Bab 5)	✅	Menggunakan timestamp ISO8601 dan tidak memerlukan total ordering.
Toleransi kegagalan (Bab 6)	✅	SQLite dedup store menjaga idempotency setelah restart.
Konsistensi (Bab 7)	✅	Dedup dan idempotency mendukung eventual consistency.

📚 Referensi
Tanenbaum, A. S., & Van Steen, M. (2017). Distributed Systems: Principles and Paradigms (2nd ed.). Pearson Education.