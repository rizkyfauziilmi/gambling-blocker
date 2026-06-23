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

**Log tag:** `PARTNER` | **Records heartbeat** setelah setup (kecuali `auto_heartbeat_on_setup=false`).

**Validasi email:** Format (EmailStr) + MX record (dnspython). Jika gagal → **422**. Jika email gagal dikirim → rollback (`delete_partner`) → **502**.

### Get All Heartbeats

| Method | Path | Auth |
|--------|------|------|
| GET | `/extension/heartbeats` | Basic Auth |

Mengembalikan semua partner dengan status heartbeat terbaru.

**Response:**
```json
{
  "heartbeats": [
    {
      "extension_id": "uuid-string",
      "partner_email": "partner@email.com",
      "stale_alerted_at": null,
      "last_heartbeat_at": "2026-06-15T10:00:00",
      "total_heartbeats": 42,
      "heartbeat_age_hours": 2
    }
  ]
}
```

### Delete Heartbeats

| Method | Path | Auth |
|--------|------|------|
| DELETE | `/extension/heartbeat/{extension_id}` | Basic Auth |

Menghapus semua rekaman heartbeat untuk extension tertentu.

**Response:** `{ "success": true }`

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

### Gambling Alert

| Method | Path | Auth |
|--------|------|------|
| POST | `/extension/gambling-alert` | ✗ |

Mengirim notifikasi ke partner saat user mengunjungi website yang terindikasi judi. Dipanggil oleh content script via background setelah klasifikasi `gambling`.

**Request Body:**
```json
{ "extension_id": "uuid-string", "url": "https://example.com", "gambling_score": 0.95 }
```

**Response:** `{ "ok": true }`

**Log tag:** `GAMBLING` | Hanya mengirim email jika partner terdaftar (tidak log ke `tamper_logs`).

### Reset Password

| Method | Path | Auth |
|--------|------|------|
| POST | `/extension/reset-password` | ✗ |

**Request Body:**
```json
{ "extension_id": "uuid-string" }
```

**Response:**
```json
{
  "success": true,
  "password_hash": "pbkdf2-hash",
  "password_salt": "random-salt"
}
```

Jika email gagal dikirim → rollback (`restore_partner` mengembalikan hash+salt lama) → **502**.

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
  "fusion_alpha": 0.5,
  "screenshot_url": "https://minio/presigned/...",
  "screenshot_object_key": "abc123def456.png",
  "screenshot_status": "screenshot_ok",
  "from_cache": false
}
```

**Screenshot status** (12+ nilai): `screenshot_ok`, `capture_failed`, `blocked`, `blank_screenshot`, `http_error_{code}`, `bypass_list`, `bypass_text_only`, `no_screenshot`, `noise_screenshot`, `extraction_failed`, dll.

### Lightweight Classification (Cache + List Only)

| Method | Path | Auth |
|--------|------|------|
| GET | `/classify/result?url=<encoded_url>` | ✗ |

Hanya cek whitelist/blacklist dan cache. Tidak menjalankan inferensi.

**Response (cache hit):**
```json
{
  "url": "https://example.com",
  "status": "classified",
  "category": "non-gambling",
  "gambling_score": 0.02,
  "text_score": 0.01,
  "image_score": 0.05,
  "fusion_alpha": 0.6,
  "screenshot_url": null,
  "screenshot_status": null,
  "from_list": "",
  "from_cache": true
}
```

**Response (from whitelist/blacklist):**
```json
{
  "url": "https://example.com",
  "status": "classified",
  "category": "non-gambling",
  "gambling_score": 0.0,
  "text_score": 0.0,
  "image_score": null,
  "fusion_alpha": null,
  "screenshot_url": null,
  "screenshot_status": null,
  "from_list": "whitelist",
  "from_cache": false
}
```

**Status:** `classified` (cek `from_cache`, `from_list`), `not_classified`

---

## False Positive Reports

### Submit Report

| Method | Path | Auth |
|--------|------|------|
| POST | `/report` | ✗ |

**Rate limit:** 5/jam per IP.

**Request Body:**
```json
{ "url": "https://example.com", "gambling_score": 0.95 }
```

**Response:** `{ "status": "ok", "message": "Report saved" }`

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
      "report_count": 3,
      "avg_score": 0.85,
      "last_reported": "2026-01-02T00:00:00"
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
  "stale_check_interval_minutes": 30,
  "auto_heartbeat_on_setup": true
}
```

**PUT Request Body** (semua field opsional):
```json
{
  "bypass_text_enabled": false,
  "stale_hours": 4,
  "stale_check_interval_minutes": 60,
  "auto_heartbeat_on_setup": false
}
```

**Log tag:** `SETTINGS` | Jika `stale_check_interval_minutes` diubah, scheduler di-reschedule tanpa restart.

---

## Admin

### Trigger Stale Check

| Method | Path | Auth |
|--------|------|------|
| POST | `/admin/trigger-stale-check` | Basic Auth |

Menjalankan pengecekan heartbeat stale secara manual. Memanggil `_check_stale_heartbeats()` dan mengirim email alert ke partner jika ada extension yang stale.

**Response:**
```json
{
  "success": true,
  "stale_count": 2,
  "alerts_sent": 2
}
```

**Log tag:** `API`

### Trigger Manual Heartbeat

| Method | Path | Auth |
|--------|------|------|
| POST | `/admin/trigger-heartbeat` | Basic Auth |

Mencatat heartbeat untuk extension tertentu secara manual. Berguna untuk testing tanpa menunggu alarm 30 menit.

**Request Body:**
```json
{ "extension_id": "uuid-string" }
```

**Response:**
```json
{
  "success": true,
  "extension_id": "uuid-string"
}
```

**Log tag:** `HEARTBEAT`

### Next Stale Check Countdown

| Method | Path | Auth |
|--------|------|------|
| GET | `/admin/next-stale-check` | Basic Auth |

Mengembalikan jadwal eksekusi `_check_stale_heartbeats` berikutnya.

**Response:**
```json
{
  "next_run": "2026-06-15T12:30:00"
}
```

Jika scheduler tidak aktif: `{ "next_run": null }`.

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

**Log tags:** `API`, `PARTNER`, `HEARTBEAT`, `TAMPER`, `GAMBLING`, `CACHE`, `REPORT`, `LIST`, `SETTINGS`, `SCREENSHOT` (debug only), `MULTIPAGE` (debug only), `DBG` (debug only)

---

## Catatan

- **Read-only GET endpoints** tidak dicatat di log untuk mencegah spam (dashboard nge-poll tiap 5-30 detik).
- **Mutation endpoints** (POST/PUT/DELETE) selalu dicatat.
- **Tag debug** (`SCREENSHOT`, `MULTIPAGE`, `DBG`) hanya muncul jika `debug_logging_enabled: true`.
- **Ring buffer** log menyimpan maksimal 1000 entry (in-memory, hilang saat server restart).
