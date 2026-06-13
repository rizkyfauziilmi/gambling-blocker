# Panduan Setup

## Prasyarat

| Tools | Minimal Versi | Catatan |
|-------|---------------|---------|
| Python | 3.13 | Wajib |
| Node.js | 20 LTS | Wajib |
| pnpm | 9+ | `npm install -g pnpm` |
| uv | >= 0.6 | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Docker | latest | Untuk Redis & MinIO |
| Playwright | - | Untuk screenshot |

## 1. Clone Repository

```bash
git clone <repo-url>
cd gambling-blocker
```

## 2. Backend

### 2.1 Environment Variables

```bash
cd backend
cp .env.example .env
```

Edit `.env` — isi variabel berikut:

| Variabel | Deskripsi | Default |
|----------|-----------|---------|
| `REDIS_HOST` | Host Redis | `127.0.0.1` |
| `REDIS_PORT` | Port Redis | `6379` |
| `REDIS_PASSWORD` | Password Redis | (kosong) |
| `DASHBOARD_USERNAME` | Basic Auth username | `admin` |
| `DASHBOARD_PASSWORD` | Basic Auth password | `admin123` |
| `MINIO_ENDPOINT` | Endpoint MinIO | `localhost:9000` |
| `MINIO_ACCESS_KEY` | Access key MinIO | `minioadmin` |
| `MINIO_SECRET_KEY` | Secret key MinIO | `minioadmin` |
| `MINIO_BUCKET` | Bucket screenshot | `screenshots` |
| `SMTP_HOST` | SMTP server | `smtp.gmail.com` |
| `SMTP_PORT` | SMTP port | `587` |
| `SMTP_USER` | Email Gmail pengirim | (isi) |
| `SMTP_PASSWORD` | App Password Gmail | (isi) |
| `SMTP_FROM_NAME` | Nama pengirim | `Gambling Blocker` |

> **SMTP**: Gunakan **App Password** (bukan password biasa). Buat di https://myaccount.google.com/apppasswords

### 2.2 Start Dependencies (Docker)

```bash
# Start Redis + MinIO
docker compose up -d

# Cek status
docker compose ps

# Untuk berhenti
docker compose down
```

### 2.3 Install Python Dependencies

```bash
uv sync
```

### 2.4 Install Playwright Browser

```bash
uv run playwright install chromium
```

### 2.5 Train Model (Opsional)

Model terlatih sudah tersedia di `model/bin/`. Jika ingin melatih ulang:

```bash
make jupyter
# Buka notebook di model/:
# - url_classification.ipynb (model teks)
# - image_classification.ipynb (model gambar)
# - fusion_weight_search.ipynb (bobot fusion)
```

### 2.6 Run Backend

```bash
# Development (auto-reload)
uv run fastapi dev

# Production
uv run fastapi run
```

Backend berjalan di `http://localhost:8000`.

## 3. Browser Extension

### 3.1 Install Dependencies

```bash
cd client/gambling-extension
pnpm install
```

### 3.2 Konfigurasi API Base

Edit `client/gambling-extension/.env`:

```
WXT_API_BASE=http://127.0.0.1:8000
```

### 3.3 Development

```bash
pnpm dev
```

WXT akan membuka browser dengan extension ter-load (HMR aktif).

### 3.4 Build Production

```bash
pnpm build
# Output: .output/chrome-mv3/

pnpm zip
# Output ZIP untuk Chrome Web Store
```

## 4. Dashboard

### 4.1 Install Dependencies

```bash
cd client/dashboard
pnpm install
```

### 4.2 Konfigurasi Environment

```bash
cp .env.example .env
# VITE_API_BASE= (kosong = proxy ke 8000 di dev)
```

### 4.3 Development

```bash
pnpm dev
```

Dashboard berjalan di `http://localhost:5173` dengan proxy ke `http://localhost:8000`.

Dashboard dilindungi **HTTP Basic Auth** dengan kredensial dari `DASHBOARD_USERNAME` / `DASHBOARD_PASSWORD` (default: `admin` / `admin123`).

### 4.4 Build Production

```bash
pnpm build
# Output: dist/
```

## 5. End-to-End Test

```bash
# 1. Pastikan backend running
curl http://localhost:8000/
# → {"service":"url gambling classifier","status":"running"}

# 2. Pastikan Redis running
curl http://localhost:8000/settings
# → (settings JSON, minta basic auth)

# 3. Buka extension di browser
#    - Klik icon extension → popup akan muncul
#    - Buka options → setup partner email
#    - Cek email partner → dapat password
#    - Buka chrome://extensions → harus di-block

# 4. Buka dashboard
# http://localhost:5173
# Login: admin / admin123
```

## Troubleshooting

| Masalah | Solusi |
|---------|--------|
| Redis connection refused | `docker compose up -d` |
| Model not loaded | Cek `model/bin/` ada file `.keras`, `.pkl` |
| Screenshot gagal | `uv run playwright install chromium` |
| Email tidak terkirim | Cek `SMTP_USER` & `SMTP_PASSWORD` di `.env` |
| Extension error `fetch failed` | Cek backend running & port |
| Dashboard 401 | Cek `DASHBOARD_USERNAME` / `DASHBOARD_PASSWORD` |
| `uv` command not found | Install uv: `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Port 8000 sudah dipakai | Matikan proses lain atau ganti port di `fastapi dev --port 8001` |
