# Arsitektur Sistem

## Alur Klasifikasi URL

```mermaid
sequenceDiagram
    actor User
    participant CS as Content Script
    participant BG as Background Script
    participant API as Backend API
    participant SQL as SQLite (lists)
    participant Redis as Redis Cache
    participant TXT as Text ML (TF-IDF + Keras)
    participant PW as Playwright
    participant IMG as Image ML (Random Forest)
    participant MQ as MinIO

    User->>CS: Buka website
    CS->>CS: Inject overlay<br/>(block scroll, touch, Escape)
    CS->>BG: classify message (url)
    BG->>API: GET /classify/url-fused?url=...
    API->>SQL: Cek whitelist/blacklist
    alt Ditemukan di list
        SQL-->>API: from_list = "whitelist" | "blacklist"
    else Tidak ada
        API->>Redis: Cek cache
        alt Cache ada
            Redis-->>API: cached result
        else Cache tidak ada
            API->>TXT: Inferensi teks
            TXT-->>API: text_score
            opt Root domain & multipage_enabled
                API->>API: Crawl subpath<br/>sampling + geometric mean
            end
            opt bypass_text_enabled=false atau<br/>skor teks tidak konklusif
                API->>PW: Screenshot (headless Chromium)
                PW-->>API: image bytes
                API->>IMG: Inferensi gambar
                IMG-->>API: image_score
                API->>MQ: Upload screenshot
            end
            API->>API: Fusion: alpha×text + (1-alpha)×image
            API->>Redis: Simpan cache
        end
    end
    API-->>BG: JSON {category, gambling_score, ...}
    alt category = "gambling"
        BG->>User: Redirect ke blocked.html
    else category = "non-gambling"
        BG->>CS: Hapus overlay
    end
```

### Fusion Model

Skor akhir = `fusion_alpha * text_score + (1 - fusion_alpha) * image_score`

- `text_score`: Probabilitas dari *neural network* (TF-IDF → Keras) pada URL yang sudah dibersihkan
- `image_score`: Probabilitas dari *Random Forest* pada ~1500 fitur gambar (histogram warna, edge detection, warm-color ratio, grid cell variance, dll.)
- `fusion_alpha`: Bobot optimal ditemukan via *grid search* pada validation set
- **Bypass teks**: Jika `bypass_text_enabled=true` dan skor teks >= 0.95 atau <= 0.05, gambar dilewati untuk menghemat resource

### Multipage Inference

Untuk URL domain root (path kosong atau "/"), sistem melakukan crawling internal:

1. Ambil halaman root
2. Ekstrak semua link internal (domain yang sama)
3. Filter path dengan kedalaman ≤ 3, panjang ≥ 10 karakter
4. Sample hingga 8 subpath
5. Jalankan inferensi teks pada setiap subpath
6. Agregasi via **geometric mean** untuk akurasi lebih baik

## Sistem Partner Accountability

### Setup

```mermaid
sequenceDiagram
    participant User
    participant Options as Options Page
    participant API as Backend API
    participant Partner as Email Partner

    User->>Options: Masukkan email partner
    Options->>API: POST /extension/setup
    API->>API: Validasi email (format + MX)
    alt Email tidak valid
        API-->>Options: 422 validation error
        Options->>User: Tampilkan "Gagal. Periksa email"
    else Email valid
        API->>API: Generate password (12 char random)
        API->>API: PBKDF2 hash (SHA-256, 600K iter)
        API->>API: INSERT OR REPLACE partner_accounts
        opt auto_heartbeat_on_setup=true
            API->>API: record_heartbeat()
        end
        API->>Partner: Email password
        alt Email gagal
            API->>API: delete_partner() rollback
            API-->>Options: 502 email_failed
            Options->>User: Tampilkan error
        else Email sukses
            API-->>Options: {password_hash, password_salt}
            Options->>User: Simpan hash + salt di storage.local
        end
    end
```

### Password Gate & Bypass

```mermaid
sequenceDiagram
    actor User
    participant Ext as Extension
    participant API as Backend API
    participant Partner as Email Partner

    User->>Ext: Akses settings / chrome://extensions
    Ext->>Ext: Cek session bypass
    alt Bypass valid (< 5 menit)
        Ext->>User: Izinkan akses
    else Bypass tidak ada / expired
        Ext->>User: Minta password
        User->>Ext: Masukkan password
        Ext->>Ext: Verifikasi PBKDF2 lokal
        alt Password benar
            Ext->>Ext: Set session bypass (5 menit)
            Ext->>User: Izinkan akses
        else Password salah
            Ext->>API: POST /extension/tamper-alert
            API->>Partner: Email alert
            Ext->>User: Tampilkan error (1 kesempatan)
        end
    end
```

### Reset Password

```mermaid
sequenceDiagram
    actor User
    participant Page as Options / extensions-blocked
    participant API as Backend API
    participant Partner as Email Partner

    User->>Page: Klik "Forgot password?"
    Page->>API: POST /extension/reset-password
    API->>API: Simpan old_hash, old_salt
    API->>API: Generate password baru
    API->>API: PBKDF2 hash baru (INSERT OR REPLACE)
    API->>Partner: Email password baru
    alt Email gagal
        API->>API: restore_partner(old_hash, old_salt) rollback
        API-->>Page: 502 error
        Page->>User: Tampilkan error
    else Email sukses
        API-->>Page: {password_hash, password_salt}
        Page->>Page: Simpan hash + salt baru di storage.local
        Page->>User: Tampilkan "New password sent to partner email"
    end
```

### Heartbeat & Stale Detection

```mermaid
sequenceDiagram
    participant Ext as Extension
    participant API as Backend API
    participant Partner as Email Partner
    participant Sched as APScheduler

    loop Every 30 menit (hanya jika ada partner)
        Ext->>API: POST /extension/heartbeat
        API->>API: record_heartbeat()<br/>reset stale_alerted_at = NULL
    end

    loop Every N menit (default 30, bisa diubah)
        Sched->>API: Cek ekstensi stale
        API->>API: SELECT partner WHERE<br/>last_heartbeat > stale_hours ago<br/>AND stale_alerted_at IS NULL
        alt Ada yang stale
            API->>Partner: Email stale alert
            API->>API: mark_stale_alerted()
        end
    end
```

## Multi-Browser Extension Guard

`isExtensionsUrl()` mendeteksi URL manajemen ekstensi:

| Browser | URL |
|---------|-----|
| Chrome | `chrome://extensions` |
| Edge | `edge://extensions` |
| Brave | `brave://extensions` |
| Opera | `opera://extensions` |
| Vivaldi | `vivaldi://extensions` |
| Firefox | `about:addons` |

Dua listener di `background.ts`:
- `tabs.onUpdated` — hanya bereaksi pada `changeInfo.url`
- `tabs.onActivated` — menangkap tab yang sudah ada saat bypass kedaluwarsa

## Grace Period

| Periode | Perilaku |
|---------|----------|
| Hari 0–7 | Grace: notifikasi "Set partner dalam X hari", akses extensions diizinkan |
| Hari 7+ | Warn: notifikasi proteksi terkompromi, akses extensions diizinkan |
| Setelah partner di-set | Block: redirect ke halaman password |

## Database SQLite (`app.db`)

| Tabel | Isi |
|-------|-----|
| `reports` | Laporan false positive (url, score, ip, timestamp) |
| `site_lists` | Blacklist & whitelist (hostname, tipe, timestamp) |
| `partner_accounts` | Partner (extension_id, email, password_hash, salt, stale_alerted_at) |
| `heartbeats` | Riwayat heartbeat (extension_id, timestamp, ip_address) |
| `tamper_logs` | Riwayat tamper (extension_id, event_type, details, timestamp) |

## Komponen Ekstensi

| Entrypoint | Fungsi |
|------------|--------|
| `background.ts` | Service worker: routing message, guard extensions, heartbeat alarm |
| `content.ts` | Content script: overlay, klasifikasi, redirect |
| `blocked/App.tsx` | Halaman blokir: skor, report false positive |
| `extensions-blocked/App.tsx` | Halaman password gate + "Forgot password?" (reset) untuk extensions page |
| `options/App.tsx` | Halaman pengaturan: setup partner, password gate dengan "Forgot password?", ganti bahasa |
| `popup/App.tsx` | Popup: status, breakdown, bypass countdown, report |

## Aliran Data Dashboard

```mermaid
flowchart LR
    subgraph DASH["Dashboard (React Query)"]
        R["Reports Tab<br/>(poll 10s)"]
        B["Blacklist Tab<br/>(poll 10s)"]
        W["Whitelist Tab<br/>(poll 10s)"]
        C["Cache Tab<br/>(poll 10s)"]
        S["Settings Tab<br/>(poll 10s)"]
        L["Logs Tab<br/>(poll 5s)"]
        P["Partner Panel<br/>(poll 30s)"]
        H["Heartbeats Tab<br/>(poll 10s)"]
    end
    subgraph API["Backend API (Basic Auth)"]
        direction TB
        REP["GET /reports<br/>DELETE /reports/:id<br/>DELETE /reports/by-hostname/:h"]
        BL["GET /blacklist<br/>POST /blacklist<br/>DELETE /blacklist/:id"]
        WL["GET /whitelist<br/>POST /whitelist<br/>DELETE /whitelist/:id"]
        CACHE["GET /cache<br/>DELETE /cache/:key<br/>DELETE /cache"]
        SET["GET /settings<br/>PUT /settings"]
        LOG["GET /logs?tag=<br/>DELETE /logs"]
        STAT["GET /extension/status"]
        HB["GET /extension/heartbeats<br/>DELETE /extension/heartbeat/:id<br/>POST /admin/trigger-heartbeat"]
    end

    R --> REP
    B --> BL
    W --> WL
    C --> CACHE
    S --> SET
    L --> LOG
    P --> STAT
    H --> HB
```
