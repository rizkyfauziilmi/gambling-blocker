# Gambling Blocker — Ekstensi Browser

Ekstensi browser untuk deteksi dan pemblokiran situs judi *real-time* dengan sistem **accountability partner** untuk mencegah pengguna mematikan proteksi.

## Tech Stack

| Komponen | Teknologi |
|----------|-----------|
| Framework | WXT 0.20.26 (Vite-based next-gen extension framework) |
| UI | React 19.2 |
| Bahasa | TypeScript 5.9 |
| Styling | Tailwind CSS v4 + Lucide React icons |
| Manajemen package | pnpm |
| Target | Chrome MV3 (juga support Firefox) |

## Struktur Entrypoints

```
entrypoints/
├── background.ts              # Service worker (utama)
├── content.ts                 # Content script (document_start)
├── blocked/
│   └── App.tsx                # Halaman blokir
├── extensions-blocked/
│   └── App.tsx                # Halaman password gate extensions://
├── options/
│   └── App.tsx                # Halaman pengaturan (open_in_tab)
└── popup/
    └── App.tsx                # Popup icon
```

## Penjelasan Entrypoints

### `background.ts` — Service Worker

Mengatur semua logika latar belakang:

- **Message routing**: Meneruskan pesan `classify` ke backend, dan `redirect` untuk navigasi ke halaman blokir
- **Extensions page guard**: Mendeteksi akses ke `chrome://extensions` (dan browser lain) → redirect ke halaman password
- **Heartbeat alarm**: Periodic alarm setiap **30 menit** untuk lapor ke backend (hanya jika partner sudah di-set)
- **Session bypass**: Mengelola bypass 5 menit setelah password berhasil dimasukkan
- **Dua listener**: `tabs.onUpdated` (deteksi navigasi baru) + `tabs.onActivated` (deteksi switch tab ke extensions page)

### `content.ts` — Content Script

Disuntikkan di `document_start` pada semua halaman:

- Inject **overlay full-screen** dengan spinner dan teks "Memeriksa..."
- Blokir semua interaksi (scroll, touch, Escape)
- Kirim URL ke background untuk klasifikasi
- Jika gambling → redirect ke `blocked.html`
- Jika safe → hapus overlay, kembalikan scroll

### `blocked/App.tsx` — Halaman Blokir

Ditampilkan saat situs gambling diblokir:

- Menampilkan URL yang diblokir, skor gambling (persentase + progress bar)
- Indikasi sumber blokir (algoritma vs admin blacklist)
- **Tombol laporkan false positive** (hanya jika bukan dari admin list)
- Status: idle, loading, done, rate_limited, not_classified, listed

### `extensions-blocked/App.tsx` — Password Gate

Ditampilkan saat mencoba akses `chrome://extensions`:

- Form input password partner
- **1 percobaan gagal** → kirim tamper alert ke backend + email ke partner
- Jika berhasil → set session bypass (5 menit) → buka chrome://extensions
- Tidak ada retry — password sekali salah langsung dilaporkan
- **"Forgot password?"** — reset password: backend generate password baru + hash+salt, simpan di local storage, email ke partner

### `options/App.tsx` — Halaman Pengaturan

Tiga halaman alur:
1. **Setup Partner** — input email partner → POST /extension/setup (validasi email format + MX, rollback jika email gagal)
2. **Password Gate** — verifikasi password untuk akses settings, dengan link **"Forgot password?"** untuk reset
3. **Settings** — status partner, pemilih bahasa (EN/ID)

### `popup/App.tsx` — Popup

- Tampilkan status situs saat ini (safe/blocked/error)
- Breakdown skor: text score, image score, fusion alpha
- **Banner partner**: status partner, grace days, atau overdue warning
- **Countdown bypass**: timer real-time MM:SS + tombol "Lock Now"
- Laporkan false positive
- Link ke halaman settings

## Fitur

### Deteksi & Blokir Real-time
- Overlay muncul sebelum halaman dirender (`document_start`)
- Klasifikasi via backend dengan fused ML model (teks + gambar)

### Accountability Partner
- Password 12 karakter random dikirim ke email partner
- Password hash (PBKDF2 SHA-256, 600K iterasi) disimpan lokal
- Akses ke `chrome://extensions` dan settings butuh password
- 1 gagal = tamper alert + email ke partner
- **Bypass 5 menit** setelah password benar (session storage, hilang saat browser restart)
- **"Forgot password?"** — reset password via `POST /extension/reset-password`, hash+salt baru dikembalikan dan disimpan di local storage

### Heartbeat
- Alarm 30 menit → POST `/extension/heartbeat`
- Hanya berjalan jika partner sudah di-set
- Backend deteksi stale → email alert ke partner

### Grace Period
- 7 hari pertama: akses extensions diizinkan, notifikasi pengingat
- 7 hari setelah setup: notifikasi proteksi terkompromi
- Setelah partner di-set: blokir penuh

### Multi-Browser Support
Ekstensi mendeteksi halaman manajemen ekstensi untuk:
Chrome, Edge, Brave, Opera, Vivaldi, Firefox (`about:addons`)

### i18n (Internasionalisasi)
- Bahasa: **English** (default) dan **Bahasa Indonesia**
- Preferensi disimpan di `browser.storage.sync`
- Implementasi custom tanpa library tambahan

## Permissions

| Permission | Kegunaan |
|-----------|----------|
| `tabs` | Navigasi, redirect, buka/tutup tab extensions page |
| `storage` | Simpan password hash (local), preferensi bahasa (sync), session bypass |
| `alarms` | Periodic heartbeat 30 menit |
| `notifications` | Notifikasi grace period & overdue |
| `host_permissions` | Komunikasi dengan backend API |

## Perintah Development

| Perintah | Deskripsi |
|----------|-----------|
| `pnpm dev` | Development server (Chrome, HMR) |
| `pnpm dev:firefox` | Development server (Firefox) |
| `pnpm build` | Build production Chrome MV3 |
| `pnpm build:firefox` | Build production Firefox |
| `pnpm zip` | Buat ZIP untuk Chrome Web Store |
| `pnpm compile` | Type-check (`tsc --noEmit`) |
| `pnpm format` | Format dengan Prettier |

## Konfigurasi

```bash
# client/gambling-extension/.env
WXT_API_BASE=http://127.0.0.1:8000
```

Ikon ekstensi di `public/icon/` (16, 32, 48, 96, 128 px).
