# Dokumentasi API

Base URL: `http://localhost:8000`

> Semua endpoint Admin menggunakan **HTTP Basic Auth**. Header: `Authorization: Basic <base64(username:password)>`

---

## Health Check

| Method | Path | Auth | Deskripsi |
|--------|------|------|-----------|
| GET | `/` | ✗ | Cek status server |

**Response:**
```json
{ "service": "url gambling classifier", "status": "running" }
```

---

## Extension & Partner

### Setup Partner

| Method | Path | Auth |
|--------|------|------|
| POST | `/extension/setup` | ✗ |

**Request Body:**
```json
{ "extension_id": "uuid-string", "partner_email": "partner@email.com" }
```

**Response:**
```json
{
  "success": true,
  "password_hash": "pbkdf2-hash",
  "password_salt": "random-salt"
}
```

**Log tag:** `PARTNER` | **Records heartbeat** setelah setup.

### Heartbeat

| Method | Path | Auth |
|--------|------|------|
| POST | `/extension/heartbeat` | ✗ |

**Request Body:**
```json
{ "extension_id": "uuid-string" }
```

**Response:** `{ "ok": true }`

**Log tag:** `HEARTBEAT` (tidak dicatat jika hanya heartbeat dari extension tanpa partner).

### Verify Password

| Method | Path | Auth |
|--------|------|------|
| POST | `/extension/verify` | ✗ |

**Request Body:**
```json
{ "extension_id": "uuid-string", "password": "plain-text-password" }
```

**Response:** `{ "valid": true }`

### Tamper Alert

| Method | Path | Auth |
|--------|------|------|
| POST | `/extension/tamper-alert` | ✗ |

**Request Body:**
```json
{ "extension_id": "uuid-string", "event_type": "extensions_page", "details": "..." }
```

**Response:** `{ "ok": true }`

**Log tag:** `TAMPER` | Mengirim email ke partner.

### Reset Password

| Method | Path | Auth |
|--------|------|------|
| POST | `/extension/reset-password` | ✗ |

**Request Body:**
```json
{ "extension_id": "uuid-string" }
```

**Response:** `{ "success": true }`

### Status Ekstensi

| Method | Path | Auth |
|--------|------|------|
| GET | `/extension/status?extension_id=xxx` | Basic Auth |

**Response:**
```json
{
  "exists": true,
  "partner_email": "partner@email.com",
  "heartbeat_age_hours": 2,
  "tamper_count_1h": 0
}
```

---

## Klasifikasi

### Fused Classification (Penuh)

| Method | Path | Auth |
|--------|------|------|
| GET | `/classify/url-fused?url=<encoded_url>` | ✗ |

Melakukan klasifikasi penuh: cek list → cache → inferensi teks → (opsional multipage) → screenshot → inferensi gambar → fusion.

**Rate limit:** 10 permintaan/menit per hostname (via Redis).

**Response:**
```json
{
  "url": "https://example.com",
  "category": "non-gambling",
  "gambling_score": 0.02,
  "text_score": 0.01,
  "image_score": 0.05,
  "fusion_alpha": 0.6,
  "screenshot_url": "https://minio/presigned/...",
  "screenshot_status": "success",
  "from_cache": false,
  "from_list": null,
  "response_time_ms": 3420
}
```

**Screenshot status:** `success`, `error`, `blocked`, `not_taken`

### Lightweight Classification (Cache + List Only)

| Method | Path | Auth |
|--------|------|------|
| GET | `/classify/result?url=<encoded_url>` | ✗ |

Hanya cek whitelist/blacklist dan cache. Tidak menjalankan inferensi.

**Response:**
```json
{
  "url": "https://example.com",
  "status": "cached",
  "category": "non-gambling",
  "gambling_score": 0.02,
  "text_score": 0.01,
  "image_score": 0.05,
  "screenshot_url": null,
  "screenshot_status": null,
  "from_list": null,
  "from_cache": true
}
```

**Status values:** `cached`, `blacklisted`, `whitelisted`, `not_classified`

---

## False Positive Reports

### Submit Report

| Method | Path | Auth |
|--------|------|------|
| POST | `/report/false-positive` | ✗ |

**Rate limit:** 5/jam per IP.

**Request Body:**
```json
{ "url": "https://example.com", "gambling_score": 0.95 }
```

**Response:** `{ "success": true }`

**Log tag:** `REPORT`

### Get Reports

| Method | Path | Auth |
|--------|------|------|
| GET | `/reports` | Basic Auth |

**Response:**
```json
{
  "groups": [
    {
      "hostname": "example.com",
      "count": 3,
      "avg_score": 0.85,
      "first_reported": "2026-01-01T00:00:00",
      "last_reported": "2026-01-02T00:00:00",
      "reports": [ ... ]
    }
  ],
  "stats": {
    "total": 10,
    "today": 2,
    "unique_hostnames": 5
  }
}
```

### Delete Report by ID

| Method | Path | Auth |
|--------|------|------|
| DELETE | `/reports/{report_id}` | Basic Auth |

**Log tag:** `REPORT`

### Delete Reports by Hostname

| Method | Path | Auth |
|--------|------|------|
| DELETE | `/reports/by-hostname/{hostname}` | Basic Auth |

**Log tag:** `REPORT`

---

## Blacklist

| Method | Path | Auth | Deskripsi |
|--------|------|------|-----------|
| GET | `/blacklist` | Basic Auth | Lihat semua entri |
| POST | `/blacklist` | Basic Auth | Tambah hostname (juga hapus cache & reports) |
| DELETE | `/blacklist/{entry_id}` | Basic Auth | Hapus entri |

**POST Request Body:** `{ "hostname": "example.com" }`

**Log tag:** `LIST`

---

## Whitelist

| Method | Path | Auth | Deskripsi |
|--------|------|------|-----------|
| GET | `/whitelist` | Basic Auth | Lihat semua entri |
| POST | `/whitelist` | Basic Auth | Tambah hostname (juga hapus cache & reports) |
| DELETE | `/whitelist/{entry_id}` | Basic Auth | Hapus entri |

**POST Request Body:** `{ "hostname": "example.com" }`

**Log tag:** `LIST`

---

## Cache

| Method | Path | Auth | Deskripsi |
|--------|------|------|-----------|
| GET | `/cache` | Basic Auth | Lihat cache (limit 1-200, default 50) |
| DELETE | `/cache/{key}` | Basic Auth | Hapus entry tertentu |
| DELETE | `/cache` | Basic Auth | Flush semua cache |

**Log tag:** `CACHE`

---

## Settings

| Method | Path | Auth | Deskripsi |
|--------|------|------|-----------|
| GET | `/settings` | Basic Auth | Baca pengaturan |
| PUT | `/settings` | Basic Auth | Update pengaturan |

**GET Response:**
```json
{
  "bypass_text_enabled": true,
  "multipage_enabled": true,
  "debug_logging_enabled": false,
  "cache_ttl_hours": 24,
  "stale_hours": 2,
  "stale_check_interval_minutes": 30
}
```

**PUT Request Body** (semua field opsional):
```json
{
  "bypass_text_enabled": false,
  "stale_hours": 4,
  "stale_check_interval_minutes": 60
}
```

**Log tag:** `SETTINGS` | Jika `stale_check_interval_minutes` diubah, scheduler di-reschedule tanpa restart.

---

## Logs

| Method | Path | Auth | Deskripsi |
|--------|------|------|-----------|
| GET | `/logs?tag=HEARTBEAT` | Basic Auth | Lihat log (filter tag opsional) |
| DELETE | `/logs` | Basic Auth | Hapus semua log |

**Response:**
```json
{
  "entries": [
    { "timestamp": "16:47:25", "tag": "PARTNER", "message": "partner set up for ..." }
  ]
}
```

**Log tags:** `API`, `PARTNER`, `HEARTBEAT`, `TAMPER`, `CACHE`, `REPORT`, `LIST`, `SETTINGS`, `SCREENSHOT` (debug only), `MULTIPAGE` (debug only), `DBG` (debug only)

---

## Catatan

- **Read-only GET endpoints** tidak dicatat di log untuk mencegah spam (dashboard nge-poll tiap 5-30 detik).
- **Mutation endpoints** (POST/PUT/DELETE) selalu dicatat.
- **Tag debug** (`SCREENSHOT`, `MULTIPAGE`, `DBG`) hanya muncul jika `debug_logging_enabled: true`.
- **Ring buffer** log menyimpan maksimal 1000 entry (in-memory, hilang saat server restart).
