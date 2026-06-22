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
    participant IMG as Image DL (MLP)
    participant MINIO as MinIO

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
                API->>MINIO: Upload screenshot
            end
            API->>API: Fusion: alpha×text + (1-alpha)×image
            API->>Redis: Simpan cache
        end
    end
    API-->>BG: JSON {category, gambling_score, ...}
    alt category = "gambling"
        CS->>BG: gambling_alert message (fire-and-forget)
        BG->>API: POST /extension/gambling-alert
        alt Partner terdaftar
            API->>Partner: Email gambling alert
        end
        BG->>User: Redirect ke blocked.html
    else category = "non-gambling"
        BG->>CS: Hapus overlay
    end
```

### Fusion Model

Skor akhir = `fusion_alpha * text_score + (1 - fusion_alpha) * image_score`

- `text_score`: Probabilitas dari *neural network* (TF-IDF → Keras) pada URL yang sudah dibersihkan
- `image_score`: Probabilitas dari *Deep Learning (MLP)* + *StandardScaler* pada **69 fitur** gambar:
  - **Patch features (60)**: Mean & std dari 10 acak patch 16×16 pada 3 kanal warna
  - **Edge density (3)**: Rata-rata, std, dan proporsi piksel di atas threshold gradient magnitude (Sobel)
  - **Color variance 4×4 grid (3)**: Std dari rata-rata warna per kanal pada grid 4×4
  - **Colorfulness index (1)**: Metrik Hasler & Süstrunk (rg/yb)
  - **Brightness distribution (2)**: Rasio piksel gelap (V<0.2) dan terang (V>0.8) di HSV
- `fusion_alpha`: Bobot dari `image_fusion_alpha.json`, ditemukan via *grid search* (alpha=0.5, threshold=0.48, F1=0.9798)
- **Bypass teks**: Jika `bypass_text_enabled=true` dan skor teks >= 0.95 atau <= 0.05, gambar dilewati untuk menghemat resource
- **Threshold akhir**: Menggunakan threshold dari fusion config (0.48) untuk semua keputusan kategori

### Multipage Inference

Untuk URL domain root (path kosong atau "/"), sistem melakukan crawling internal jika `multipage_enabled=true`:

1. Fetch halaman root via HTTP (requests)
2. Parse semua `<a href="...">` — filter same-domain + buang ekstensi file (jpg, png, css, js, pdf, dll.)
3. Filter path depth ≥ 2
4. Stratified sampling: max 2 subpath per direktori pertama, total max 8
5. Jika tidak cukup path depth ≥ 2, fallback include depth 1
6. Jalankan inferensi teks pada setiap subpath
7. Agregasi via **geometric mean** — **hanya subpath** (root score sebagai fallback jika fetch/subpath gagal)

### Tahapan Rinci

Berikut adalah penjelasan langkah demi langkah dari alur klasifikasi URL:

#### A. Injection & Overlay (Content Script)

1. Content script berjalan di `document_start` pada semua URL (`*://*/*`)
2. `shouldSkip()`: Lewati jika protocol `chrome-extension://` / `moz-extension://` atau host `127.0.0.1` / `localhost` / `[::1]` / `0.0.0.0`
3. Inject elemen `<style>` + `<div id="gb-overlay">` dengan:
   - Posisi `fixed; inset: 0; z-index: 2147483647`, background putih, spinner animasi
   - Blokir interaksi: `overflow: hidden`, prevent `Escape` (capture), `wheel`, `touchmove`
4. Kirim `browser.runtime.sendMessage({ type: "classify", url })` ke background script

```mermaid
flowchart TD
    A["User buka website"] --> B{shouldSkip?}
    B -->|"chrome-extension://,<br/>moz-extension://,<br/>localhost, 127.0.0.1"| C["Skip: no overlay"]
    B -->|"Target URL"| D["Inject overlay + blokir input"]
    D --> E["sendMessage({type:'classify', url})"]
```

#### B. Messaging ke Background

1. Background script menerima message `"classify"` dari content script
2. Ekstrak URL, panggil `GET /classify/url-fused?url=<encoded>`
3. Respons JSON dikembalikan ke content script via promise

```mermaid
sequenceDiagram
    participant CS as Content Script
    participant BG as Background Script
    participant API as Backend API
    CS->>BG: runtime.sendMessage({type:"classify", url})
    BG->>API: GET /classify/url-fused?url=...
    API-->>BG: JSON result
    BG-->>CS: Promise resolved
```

#### C. Rate Limiting

1. Backend membuat key Redis `rate:fused:{hostname}` via atomic increment (`cache_incr`)
2. TTL 60 detik, batas 10 permintaan per menit per hostname
3. Jika rate limit terlampaui: coba ambil dari cache dulu, jika tidak ada → return **429**

```mermaid
flowchart TD
    A["Request masuk"] --> B["cache_incr(rate:fused:{host})"]
    B --> C{"count > 10 / menit?"}
    C -->|"Ya"| D{"Cache ada?"}
    D -->|"Ya"| E["Return cached result"]
    D -->|"Tidak"| F["Return 429 rate_limited"]
    C -->|"Tidak"| G["Lanjut ke site list"]
```

#### D. Pengecekan Site List (SQLite)

1. Query tabel `site_lists` untuk `hostname` yang diminta
2. Jika ditemukan sebagai `"whitelist"`:
   - Return segera: `category: "non-gambling"`, `gambling_score: 0.0`, `from_list: "whitelist"`
3. Jika ditemukan sebagai `"blacklist"`:
   - Return segera: `category: "gambling"`, `gambling_score: 1.0`, `from_list: "blacklist"`
4. Jika tidak ada di list: lanjut ke tahap berikutnya

```mermaid
flowchart TD
    A["check_hostname(host)"] --> B{"Ditemukan?"}
    B -->|"whitelist"| C["Return: non-gambling<br/>score=0 | from_list=whitelist"]
    B -->|"blacklist"| D["Return: gambling<br/>score=1 | from_list=blacklist"]
    B -->|"None"| E["Lanjut ke cache"]
```

#### E. Cache Lookup (Redis)

1. Key: `fused:domain:{hostname}`
2. Jika cache ada (`cache_get`): return hasil dengan `from_cache: true`, perbarui `screenshot_url` via presigned MinIO URL
3. TTL cache dari settings (default 24 jam, minimal 1 jam)

```mermaid
flowchart TD
    A["cache_get(fused:domain:{host})"] --> B{"Cache ada?"}
    B -->|"Ya"| C["Return cached result<br/>from_cache=true"]
    B -->|"Tidak"| D["Lanjut ke text inference"]
```

#### F. Text Inference

1. **URL Cleaning** (`clean_url`):
   - Ekstrak `netloc + path` → URL-decode → lowercase
   - Hapus prefix `http://` / `https://`
   - Ganti `-`, `_`, `/` dengan spasi
   - Hapus semua non-alfanumerik/non-spasi
   - Sisipkan spasi antara digit-huruf
   - Collapse spasi ganda
2. **TF-IDF Vectorization**: Transformasi menggunakan `TfidfVectorizer` terlatih dari `text_tfidf_vectorizer.pkl`
3. **Keras Neural Network**: `text_classifier.keras` → output probabilitas sigmoid → `text_score`
4. Threshold teks: 0.66 (dari `text_best_threshold.json`)

```mermaid
flowchart LR
    A["Raw URL"] --> B["clean_url()"]
    B --> C["TF-IDF Vectorizer<br/>(text_tfidf_vectorizer.pkl)"]
    C --> D["Keras Neural Network<br/>(text_classifier.keras)"]
    D --> E["text_score"]
```

#### G. Multipage Inference (Root Domain)

1. Hanya berjalan jika `multipage_enabled=true` dan path adalah root (`""` atau `"/"`)
2. Detail implementasi ada di sub-section **Multipage Inference** di atas
3. Hasil aggregasi menggantikan `text_score` untuk tahap selanjutnya

```mermaid
sequenceDiagram
    participant API as Backend API
    participant HTTP as External HTTP
    participant TXT as Text ML
    API->>API: Root domain detected
    API->>HTTP: GET root page HTML
    HTTP-->>API: HTML
    API->>API: Parse href → same-domain<br/>buang ekstensi file<br/>stratified by dir (max 2/dir, max 8)
    loop For each sampled subpath
        API->>TXT: predict_text(subpath_url)
        TXT-->>API: subpath_score
    end
    API->>API: Geometric mean subpath<br/>(root score sebagai fallback)
```

#### H. Screenshot & Image Inference

1. **Bypass logic**: Jika `bypass_text_enabled=true` DAN (`text_score >= 0.95` ATAU `text_score <= 0.05`) → skip screenshot, `screenshot_status: "bypass_text_only"`, `gambling_score = text_score`
2. **Screenshot** (Playwright):
   - Headless Chromium, viewport 1280×720, user-agent Chrome 125, locale `id-ID`, timezone `Asia/Jakarta`
   - Bypass CSP, ignore HTTPS errors
   - Stealth mode (`playwright_stealth`)
   - Timeout 30s navigasi + 15s networkidle
   - Hapus overlay/modal/cookie popup via JavaScript injection
   - Deteksi halaman terblokir (Cloudflare, 403, akses ditolak, dll.) → `screenshot_status: "blocked"`
   - Screenshot ukuran < 1024 bytes → `screenshot_status: "blank_screenshot"`
   - Screenshot > 100KB dianggap noise → `screenshot_status: "noise_screenshot"`
3. **Feature Extraction** (69 fitur):
   - Resize ke 64×64 (LANCZOS), normalize [0,1]
   - **Patch features (60)**: 10 patch acak 16×16 → mean & std per kanal RGB
   - **Edge density (3)**: Gradient magnitude via Sobel → mean/max, std/max, proporsi > threshold
   - **Color variance 4×4 grid (3)**: Std dari rata-rata warna 16 cell per kanal
   - **Colorfulness index (1)**: `sqrt(rg.std² + yb.std²) + 0.3×sqrt(rg.mean² + yb.mean²)` (Hasler & Süstrunk)
   - **Brightness distribution (2)**: Rasio piksel gelap (V<0.2) dan terang (V>0.8) di HSV
4. **Deep Learning Prediction (MLP)**:
   - Fitur di-scale dengan `StandardScaler` dari `image_scaler.pkl`
   - Prediksi probabilitas kelas gambling via model Deep Learning `image_classifier.keras`
   - Output: `image_score` (float 0–1)
5. **Upload**: Screenshot diupload ke MinIO, `screenshot_url` adalah presigned URL (expired 1 jam)

```mermaid
flowchart TD
    A{"bypass_text_enabled &&<br/>(text >= 0.95 || <= 0.05)"}
    A -->|"Ya"| B["Skip screenshot<br/>screenshot_status = bypass_text_only<br/>gambling_score = text_score"]
    A -->|"Tidak"| C["Playwright: headless Chromium<br/>screenshot halaman"]
    C --> D{"Screenshot OK?"}
    D -->|"blocked"| E["screenshot_status = blocked"]
    D -->|"blank (< 1KB)"| F["screenshot_status = blank_screenshot"]
    D -->|"noise (> 100KB)"| G["screenshot_status = noise_screenshot"]
    D -->|"OK"| H["Extract 69 fitur:<br/>patch(60) edge(3) colorVar(3)<br/>colorfulness(1) brightness(2)"]
    H --> I["StandardScaler<br/>(image_scaler.pkl)"]
    I --> J["Deep Learning MLP<br/>(image_classifier.keras)"]
    J --> K["image_score"]
    D -->|"gagal"| L["screenshot_status = capture_failed<br/>image_score = null"]
    K --> M["Upload ke MinIO"]
    E --> M
    F --> M
    G --> M
```

#### I. Fusion, Caching, & Response

1. **Fusion score**:
   ```
   gambling_score = fusion_alpha × text_score + (1 - fusion_alpha) × image_score
   ```
   - `fusion_alpha`: 0.5 (dari `image_fusion_alpha.json`)
   - Jika image tidak tersedia (bypass/gagal): `gambling_score = text_score`
2. **Kategori**:
   - `gambling_score > fusion_threshold (0.48)` → `category: "gambling"`
   - Selainnya → `category: "non-gambling"`
3. **Cache**: Simpan hasil ke Redis key `fused:domain:{hostname}` dengan TTL dari settings
4. **Response ke background script**: JSON lengkap termasuk semua skor, status screenshot, URL screenshot
5. **Tindakan akhir** (di content script):
   - Jika `category: "gambling"`:
     - Kirim `gambling_alert` (fire-and-forget) → background → `POST /extension/gambling-alert` → email partner (jika terdaftar)
     - Kirim `redirect` → background → `browser.tabs.update()` ke `blocked.html?url=...&score=...&from_list=...`
     - Fallback: navigasi langsung via `window.location.href`
    - Jika `category: "non-gambling"`:
      - `cleanup()`: hapus overlay, restore scroll, remove event listeners

```mermaid
flowchart TD
    X["gambling_score =<br/>alpha × text + (1-alpha) × image"] --> Y{"> 0.48?"}
    Y -->|"Ya"| Z["category = gambling"]
    Y -->|"Tidak"| AA["category = non-gambling"]
    Z --> AB["Cache ke Redis"]
    Z --> AC["Kirim gambling_alert<br/>(fire-and-forget)"]
    Z --> AD["Redirect ke blocked.html"]
    AA --> AE["Cache ke Redis"]
    AA --> AF["cleanup: hapus overlay<br/>restore scroll"]
```

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
| `background.ts` | Service worker: routing message (classify, redirect, **gambling_alert**), guard extensions, heartbeat alarm |
| `content.ts` | Content script: overlay, klasifikasi, redirect, kirim **gambling_alert** ke background jika gambling |
| `blocked/App.tsx` | Halaman blokir: skor, report false positive |
| `extensions-blocked/App.tsx` | Halaman password gate + "Forgot password?" (reset) untuk extensions page |
| `options/App.tsx` | Halaman pengaturan: setup partner, password gate dengan "Forgot password?", ganti bahasa |
| `popup/App.tsx` | Popup: status, breakdown, bypass countdown + Lock Now, report |

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
        STAT["GET /extension/status?extension_id="]
        HB["GET /extension/heartbeats<br/>DELETE /extension/heartbeat/:id<br/>POST /admin/trigger-heartbeat"]
        SC["GET /admin/next-stale-check"]
        STALE["POST /admin/trigger-stale-check"]
    end

    R --> REP
    B --> BL
    W --> WL
    C --> CACHE
    S --> SET
    L --> LOG
    H --> HB
    H -.-> STALE
    S -.-> SC
    S -.-> STALE
```

> **Catatan:** `PartnerPanel` (poll 30s via `GET /extension/status?extension_id=`) adalah komponen yang dirancang untuk digunakan di halaman options/popup ekstensi, **bukan** tab dashboard. Komponen ini ada di `src/components/PartnerPanel.tsx` tapi tidak dirender di `App.tsx`.
>
> Endpoint admin `POST /admin/trigger-stale-check` dan `GET /admin/next-stale-check` digunakan oleh `TriggerStaleCheckButton` di Settings & Heartbeats tab.
