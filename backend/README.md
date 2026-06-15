# Gambling Blocker — Backend

Backend API **FastAPI** untuk klasifikasi URL judi menggunakan fused ML model (teks + gambar), sistem accountability partner, heartbeat monitoring, dan dashboard admin.

## Tech Stack

| Komponen | Teknologi |
|----------|-----------|
| Bahasa | Python ≥ 3.13 |
| Framework | FastAPI 0.136+ |
| ML Teks | TensorFlow 2.21+ (TF-IDF → Keras Neural Network) |
| ML Gambar | scikit-learn 1.8+ (Random Forest) |
| Cache | Redis 5.3+ |
| Object Storage | MinIO 7.2+ (screenshot) |
| Screenshot | Playwright 1.60+ (headless Chromium) |
| Email | smtplib (Gmail SMTP STARTTLS) |
| Scheduler | APScheduler 3.11+ |
| Database | SQLite (via sqlite3) |
| Pengaturan | JSON file (`settings.json`) |

## Struktur Direktori

```
backend/
├── main.py                 # FastAPI app, 30+ route handlers, APScheduler
├── pyproject.toml          # Project metadata, dependencies, tool config
├── .env                    # Environment variables (gitignored)
├── .env.example            # Template env vars
├── docker-compose.yml      # Redis 7 + MinIO
├── makefile                # Shortcut commands
├── reports.db              # SQLite (partners, heartbeats, reports, lists)
├── settings.json           # Dynamic runtime settings
├── utils/
│   ├── cache.py            # Redis: get, setex, delete, flush, scan, rate limit
│   ├── config.py           # Load env vars via python-dotenv
│   ├── email.py            # SMTP: send password, tamper alert, stale alert
│   ├── extensions.py       # SQLite: partner CRUD, heartbeat, tamper, verify
│   ├── helpers.py          # parse_hostname, is_ip, cache_key, clean_url
│   ├── lists.py            # SQLite: blacklist/whitelist CRUD
│   ├── logger.py           # In-memory ring buffer (max 1000)
│   ├── model.py            # ML inference: text + image + fusion
│   ├── reports.py          # SQLite: false positive reports CRUD
│   ├── settings.py         # JSON file: get/save runtime settings
│   └── storage.py          # MinIO: upload, presigned URL
└── model/
    ├── bin/                # Trained artifacts (.keras, .pkl, .json)
    ├── dataset/            # Training datasets (CSV/JSON)
    └── *.ipynb             # Jupyter notebooks (training, evaluation)
```

## Modul Utilitas

| Modul | Fungsi |
|-------|--------|
| `model.py` | **Inti ML.** Inferensi teks (TF-IDF → Keras), inferensi gambar (Random Forest), fusion skor, multipage crawling, screenshot Playwright |
| `cache.py` | **Redis.** Set/get dengan TTL dari settings, atomic increment untuk rate limiter, scan/flush |
| `extensions.py` | **SQLite Partner.** `setup_partner()` (PBKDF2 SHA-256, 600K iterasi), `record_heartbeat()`, `log_tamper()`, `get_stale_extensions()`, `mark_stale_alerted()` |
| `email.py` | **SMTP.** Tiga template: password akun partner, tamper alert, heartbeat stale alert |
| `lists.py` | **SQLite Blacklist/Whitelist.** Tambah, hapus, cek hostname |
| `reports.py` | **SQLite Reports.** CRUD false positive reports, grouped by hostname, stats |
| `settings.py` | **JSON.** `get()` / `save()` runtime settings, reload tanpa restart |
| `logger.py` | **Ring buffer.** `log(tag, msg)`, filter tag, debug tag suppression |
| `storage.py` | **MinIO.** Upload screenshot PNG, generate presigned URL |
| `helpers.py` | **Utility.** Normalisasi hostname, deteksi IP, cache key generator |

## API Endpoints

### Extension & Partner

| Method | Path | Auth | Deskripsi |
|--------|------|------|-----------|
| POST | `/extension/setup` | ✗ | Daftarkan extension + kirim password ke partner |
| POST | `/extension/heartbeat` | ✗ | Rekam heartbeat |
| POST | `/extension/tamper-alert` | ✗ | Log tamper + email partner |
| POST | `/extension/reset-password` | ✗ | Reset password + email ulang |
| GET | `/extension/status` | Basic | Status ekstensi (heartbeat age, tamper count) |

### Klasifikasi

| Method | Path | Auth | Deskripsi |
|--------|------|------|-----------|
| GET | `/classify/url-fused` | ✗ | Klasifikasi penuh (list → cache → teks → gambar → fusion) |
| GET | `/classify/result` | ✗ | Klasifikasi ringan (list + cache saja) |

### Admin (Basic Auth)

| Method | Path | Deskripsi |
|--------|------|-----------|
| GET/POST/DELETE | `/blacklist` | Manajemen blacklist |
| GET/POST/DELETE | `/whitelist` | Manajemen whitelist |
| GET/POST/DELETE | `/reports` | Manajemen laporan false positive |
| GET/DELETE/DELETE | `/cache` | Manajemen cache Redis |
| GET/PUT | `/settings` | Baca/ubah pengaturan runtime |
| GET/DELETE | `/logs` | Baca/hapus log |

## Cara Menjalankan

```bash
# Prasyarat
cp .env.example .env    # Isi SMTP credentials
docker compose up -d    # Redis + MinIO
uv sync
uv run playwright install chromium

# Development
uv run fastapi dev      # http://localhost:8000

# Production
uv run fastapi run
```

## Environment Variables

| Variabel | Default | Wajib |
|----------|---------|-------|
| `REDIS_HOST` / `REDIS_PORT` | `127.0.0.1:6379` | Ya |
| `DASHBOARD_USERNAME` / `DASHBOARD_PASSWORD` | `admin` / `admin123` | Ya |
| `MINIO_*` | `localhost:9000`, `minioadmin` | Ya |
| `SMTP_HOST` / `SMTP_PORT` | `smtp.gmail.com:587` | Hanya untuk fitur email |
| `SMTP_USER` / `SMTP_PASSWORD` | - | Hanya untuk fitur email |

## Alur Klasifikasi

```
URL masuk → cek whitelist/blacklist (SQLite)
         → cek cache (Redis)
         → inferensi teks (TF-IDF → Keras)
         → [jika root domain] multipage crawling + geometric mean
         → [jika bypass teks tidak aktif] screenshot (Playwright)
         → inferensi gambar (Random Forest)
         → fusion: alpha * text + (1-alpha) * image
         → simpan cache (Redis) + screenshot (MinIO)
         → return JSON
```

## Catatan

- **Read-only GET endpoints** tidak nge-log (mencegah spam ring buffer dari polling dashboard).
- **POST/PUT/DELETE endpoints** selalu nge-log dengan tag sesuai konteks.
- **Ring buffer log** maksimal 1000 entry, hilang saat server restart.
- **APScheduler** untuk stale check: interval bisa diubah via `PUT /settings` tanpa restart.
- **Model** terlatih disimpan di `model/bin/`. Jika tidak ada, endpoint klasifikasi return 503.
