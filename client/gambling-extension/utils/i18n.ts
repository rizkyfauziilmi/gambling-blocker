let _lang = "en"

const messages: Record<string, Record<string, string>> = {
  en: {
    extName: "Gambling Blocker",
    extDescription: "Automatically detects and blocks gambling websites.",
    popup_title: "Gambling Blocker",
    popup_protectionActive: "Protection Active",
    popup_currentTab: "Current Tab",
    popup_scanning: "Scanning...",
    popup_failedToScan: "Failed to scan URL",
    popup_safe: "Safe",
    popup_blocked: "Blocked",
    popup_gamblingScore: "Gambling Score",
    popup_notScannable: "Cannot scan this page",
    popup_notScannableDesc: "Only HTTP(S) websites are checked",
    popup_reportFalsePositive: "Report a False Detection",
    popup_bareIp: "Direct IP \u2014 not scanned",
    popup_bareIpDesc: "Add a path to enable gambling detection",
    popup_blockedByAdmin: "Blocked by admin",
    popup_blockedByAdminDesc:
      "This site was added to the blacklist by an administrator.",
    popup_allowedByAdmin: "Allowed by admin",
    popup_allowedByAdminDesc:
      "This site was added to the whitelist by an administrator.",
    reportListed: "Cannot report \u2014 managed by admin",
    reportListedBlacklistDesc:
      "This URL is on the blacklist. Contact the administrator.",
    reportListedWhitelistDesc:
      "This URL is on the whitelist. Contact the administrator.",
    blocked_title: "This Site Has Been Blocked",
    blocked_description: "Detected as a gambling site",
    blocked_url: "URL",
    blocked_gamblingScore: "Gambling Score",
    blocked_reportFalsePositive: "Report a False Detection",
    reportSending: "Sending...",
    reportSent: "Reported! \u2705",
    reportRateLimited: "Too many reports",
    reportNotClassified: "Visit the site first",
    loading_title: "Checking Site",
    loading_description: "Analyzing website content...",
    popup_notClassified: "Not yet scanned",
    popup_notClassifiedDesc: "Visit the site to trigger classification",
    popup_textScore: "Text Analysis",
    popup_imageScore: "Image Analysis",
    popup_screenshot_ok: "Screenshot captured",
    popup_screenshot_failed: "Screenshot unavailable",
    popup_fusionInfo: "Fusion weight: {alpha}",
  },
  id: {
    extName: "Gambling Blocker",
    extDescription: "Mendeteksi dan memblokir situs perjudian secara otomatis.",
    popup_title: "Gambling Blocker",
    popup_protectionActive: "Perlindungan Aktif",
    popup_currentTab: "Tab Saat Ini",
    popup_scanning: "Memindai...",
    popup_failedToScan: "Gagal memindai URL",
    popup_safe: "Aman",
    popup_blocked: "Diblokir",
    popup_gamblingScore: "Skor Judi",
    popup_notScannable: "Tidak dapat memindai halaman ini",
    popup_notScannableDesc: "Hanya situs HTTP(S) yang diperiksa",
    popup_reportFalsePositive: "Laporkan Salah Deteksi",
    popup_bareIp: "IP Langsung \u2014 tidak dipindai",
    popup_bareIpDesc: "Tambahkan path untuk deteksi judi",
    popup_blockedByAdmin: "Diblokir oleh admin",
    popup_blockedByAdminDesc:
      "Situs ini ditambahkan ke daftar hitam oleh administrator.",
    popup_allowedByAdmin: "Diizinkan oleh admin",
    popup_allowedByAdminDesc:
      "Situs ini ditambahkan ke daftar putih oleh administrator.",
    reportListed: "Tidak bisa melapor \u2014 dikelola admin",
    reportListedBlacklistDesc:
      "URL ini ada di daftar hitam. Hubungi administrator.",
    reportListedWhitelistDesc:
      "URL ini ada di daftar putih. Hubungi administrator.",
    blocked_title: "Situs Ini Diblokir",
    blocked_description: "Terdeteksi sebagai situs perjudian",
    blocked_url: "URL",
    blocked_gamblingScore: "Skor Judi",
    blocked_reportFalsePositive: "Laporkan Salah Deteksi",
    reportSending: "Mengirim...",
    reportSent: "Terkirim! \u2705",
    reportRateLimited: "Terlalu banyak laporan",
    reportNotClassified: "Kunjungi situsnya dulu",
    loading_title: "Memeriksa Situs",
    loading_description: "Menganalisis konten situs...",
    popup_notClassified: "Belum dipindai",
    popup_notClassifiedDesc: "Kunjungi situs untuk memicu pemindaian",
    popup_textScore: "Analisis Teks",
    popup_imageScore: "Analisis Gambar",
    popup_screenshot_ok: "Screenshot berhasil",
    popup_screenshot_failed: "Screenshot tidak tersedia",
    popup_fusionInfo: "Bobot fusion: {alpha}",
  },
}

export async function initLanguage(): Promise<string> {
  try {
    const { language } = await browser.storage.sync.get("language")
    _lang = language === "id" ? "id" : "en"
  } catch {
    _lang = "en"
  }
  return _lang
}

export function setLanguage(lang: string): void {
  _lang = lang === "id" ? "id" : "en"
}

export function getCurrentLanguage(): string {
  return _lang
}

export function t(key: string): string {
  return messages[_lang]?.[key] ?? messages.en[key] ?? key
}
