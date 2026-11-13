import { useCallback, useEffect, useState } from "react"
import {
  Camera,
  CameraOff,
  FileText,
  Flag,
  Image,
  Loader2,
  Scan,
  Settings,
  Shield,
  ShieldCheck,
  ShieldX,
  TriangleAlert,
  UserRoundCheck,
} from "lucide-react"
import { initLanguage, t } from "@/utils/i18n"
import { getPartnerStatus } from "@/utils/password"
import { bypassStorage } from "@/utils/storage"

const API_BASE = import.meta.env.WXT_API_BASE
const EXT_URL = browser.runtime.getURL("")

function shouldSkip(url: string): boolean {
  try {
    const parsed = new URL(url)
    if (
      parsed.protocol === "chrome-extension:" ||
      parsed.protocol === "moz-extension:" ||
      parsed.protocol === "safari-extension:"
    )
      return true
    if (
      parsed.hostname === "127.0.0.1" ||
      parsed.hostname === "localhost" ||
      parsed.hostname === "[::1]" ||
      parsed.hostname === "0.0.0.0"
    )
      return true
    return false
  } catch {
    return true
  }
}

type Status =
  | "idle"
  | "loading"
  | "safe"
  | "gambling"
  | "skipped"
  | "error"
  | "not_classified"

interface Result {
  status: string
  category: string
  gambling_score: number
  text_score: number | null
  image_score: number | null
  fusion_alpha: number | null
  screenshot_url: string | null
  screenshot_status: string | null
  from_list?: string
}

interface TabCheckResult {
  originalUrl: string
  status: "blocked_page" | "normal" | "skipped"
  params?: { gambling_score?: number; from_list?: string }
}

function checkTab(tabUrl: string): TabCheckResult {
  const blockedPath = `${EXT_URL}blocked.html`

  if (tabUrl.startsWith(blockedPath)) {
    const p = new URLSearchParams(new URL(tabUrl).search)
    return {
      originalUrl: p.get("url") || tabUrl,
      status: "blocked_page",
      params: {
        gambling_score: Number(p.get("gambling_score")) || undefined,
        from_list: p.get("from_list") || undefined,
      },
    }
  }

  if (!tabUrl.startsWith("http")) {
    return { originalUrl: tabUrl, status: "skipped" }
  }
  if (shouldSkip(tabUrl)) {
    return { originalUrl: tabUrl, status: "skipped" }
  }

  return { originalUrl: tabUrl, status: "normal" }
}

function App() {
  const [tabUrl, setTabUrl] = useState<string>("")
  const [status, setStatus] = useState<Status>("idle")
  const [result, setResult] = useState<Result | null>(null)
  const [reportState, setReportState] = useState<
    "idle" | "loading" | "done" | "rate_limited" | "not_classified" | "listed"
  >("idle")
  const [partnerInfo, setPartnerInfo] = useState<{
    hasPartner: boolean
    daysSinceInstall: number
  } | null>(null)
  const [bypassRemaining, setBypassRemaining] = useState<number | null>(null)

  useEffect(() => {
    if (bypassRemaining === null) return
    if (bypassRemaining <= 0) return
    const interval = setInterval(() => {
      setBypassRemaining((prev) => (prev !== null ? prev - 1 : null))
    }, 1000)
    return () => clearInterval(interval)
  }, [bypassRemaining])

  useEffect(() => {
    ;(async () => {
      await initLanguage()
      getPartnerStatus().then(setPartnerInfo)
      const session = (await bypassStorage.getSession([
        "extensions_bypass",
        "extensions_bypass_expires_at",
      ])) as {
        extensions_bypass?: boolean
        extensions_bypass_expires_at?: number
      }
      const expiresAt = session.extensions_bypass_expires_at
      if (session.extensions_bypass && expiresAt && expiresAt > Date.now()) {
        setBypassRemaining(Math.round((expiresAt - Date.now()) / 1000))
      }

      try {
        const [tab] = await browser.tabs.query({
          active: true,
          currentWindow: true,
        })

        if (!tab.url) {
          setTabUrl("")
          setStatus("skipped")
          return
        }

        const check = checkTab(tab.url)
        setTabUrl(check.originalUrl)

        switch (check.status) {
          case "blocked_page":
          case "normal":
            break
          case "skipped":
            setStatus("skipped")
            return
        }

        setStatus("loading")
        const params = new URLSearchParams({ url: check.originalUrl })
        const res = await fetch(`${API_BASE}/classify/result?${params}`)
        const data: Result = await res.json()
        if (!data) return

        setResult(data)
        if (data.status === "not_classified") {
          setStatus("not_classified")
        } else if (data.category === "gambling") {
          setStatus("gambling")
        } else {
          setStatus("safe")
        }
      } catch {
        setStatus((prev) => (prev === "skipped" ? prev : "error"))
      }
    })()
  }, [])

  async function handleLockNow() {
    const tabs = await browser.tabs.query({})
    for (const t of tabs) {
      if (t.url && t.id) {
        if (
          t.url.startsWith("chrome://extensions") ||
          t.url.startsWith("edge://extensions") ||
          t.url.startsWith("brave://extensions") ||
          t.url.startsWith("opera://extensions") ||
          t.url.startsWith("vivaldi://extensions") ||
          t.url.startsWith("about:addons")
        ) {
          await browser.tabs.remove(t.id)
        }
      }
    }
    await bypassStorage.clearSession([
      "extensions_bypass",
      "extensions_bypass_expires_at",
    ])
    setBypassRemaining(null)
  }

  async function handleReport() {
    setReportState("loading")
    try {
      const res = await fetch(`${API_BASE}/report`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          url: tabUrl,
          gambling_score: result?.gambling_score,
        }),
      })
      const data = await res.json()
      if (data.status === "ok") {
        setReportState("done")
        setTimeout(() => setReportState("idle"), 3000)
      } else if (data.detail?.error === "not_classified") {
        setReportState("not_classified")
        setTimeout(() => setReportState("idle"), 5000)
      } else if (
        data.detail?.error === "already_blacklisted" ||
        data.detail?.error === "already_whitelisted"
      ) {
        setReportState("listed")
        setTimeout(() => setReportState("idle"), 5000)
      } else {
        setReportState("rate_limited")
        setTimeout(() => setReportState("idle"), 5000)
      }
    } catch {
      setReportState("rate_limited")
      setTimeout(() => setReportState("idle"), 5000)
    }
  }

  const scorePercent = result ? (result.gambling_score * 100).toFixed(1) : null

  function ScreenshotIcon({
    status: s,
  }: {
    status: string | null | undefined
  }) {
    if (!s) return null
    if (s === "screenshot_ok") {
      return (
        <div className="flex items-center gap-1 text-xs text-emerald-600">
          <Camera className="size-3" />
          <span>{t("popup_screenshot_ok")}</span>
        </div>
      )
    }
    let label: string
    if (s.startsWith("http_error_")) {
      const code = s.replace("http_error_", "").replace("_noise", "")
      label = s.endsWith("_noise") ? `Noise (HTTP ${code})` : `HTTP ${code}`
    } else {
      const labels: Record<string, string> = {
        bypass_list: "By List",
        bypass_text_only: "Text Only",
        capture_failed: "Capture Failed",
        extraction_failed: "Extraction Failed",
        noise_screenshot: "Noise",
        no_screenshot: "No Screenshot",
        blocked: "Blocked",
        blank_screenshot: "Blank",
      }
      label = labels[s] ?? s
    }
    return (
      <div className="flex items-center gap-1 text-xs text-gray-400">
        <CameraOff className="size-3" />
        <span>{label}</span>
      </div>
    )
  }

  return (
    <div className="w-90 bg-linear-to-b from-white to-gray-50 p-4">
      <div className="mb-4 flex items-center gap-3">
        <div className="flex size-10 items-center justify-center rounded-xl bg-linear-to-br from-indigo-500 to-purple-600 shadow-lg shadow-indigo-500/30">
          <Shield className="text-lg text-white" size={22} />
        </div>
        <div>
          <h1 className="text-sm font-semibold text-gray-900">
            {t("popup_title")}
          </h1>
          <p className="text-xs text-gray-500">{t("popup_protectionActive")}</p>
        </div>
      </div>

      <div className="mb-3 rounded-xl border border-gray-200 bg-gray-50 p-3">
        <p className="mb-1 text-xs text-gray-500">{t("popup_currentTab")}</p>
        <p className="truncate text-sm text-gray-700">{tabUrl || "—"}</p>
      </div>

      {/* Partner status banner */}
      {partnerInfo && (
        <div
          className={`mb-3 rounded-xl border p-3 ${
            partnerInfo.hasPartner
              ? "border-emerald-200 bg-emerald-50"
              : partnerInfo.daysSinceInstall >= 7
                ? "border-amber-200 bg-amber-50"
                : "border-blue-200 bg-blue-50"
          }`}
        >
          <p
            className={`flex items-center gap-2 text-xs font-medium ${
              partnerInfo.hasPartner
                ? "text-emerald-700"
                : partnerInfo.daysSinceInstall >= 7
                  ? "text-amber-700"
                  : "text-blue-700"
            }`}
          >
            <UserRoundCheck className="size-3" />
            {partnerInfo.hasPartner
              ? t("partner_popupActive")
              : partnerInfo.daysSinceInstall >= 7
                ? t("partner_popupBannerLate")
                : t("partner_popupBanner")}
          </p>
          {!partnerInfo.hasPartner && partnerInfo.daysSinceInstall < 7 && (
            <p className="mt-0.5 text-[11px] text-blue-600/70">
              {t("partner_graceDays").replace(
                "{days}",
                `${7 - partnerInfo.daysSinceInstall}`
              )}
            </p>
          )}
          {!partnerInfo.hasPartner && partnerInfo.daysSinceInstall >= 7 && (
            <p className="mt-0.5 text-[11px] text-amber-600/70">
              {t("partner_daysOverdue")}
            </p>
          )}
        </div>
      )}

      {/* Bypass countdown */}
      {bypassRemaining !== null && bypassRemaining > 0 && (
        <div className="mb-3 rounded-xl border border-indigo-200 bg-indigo-50 p-3">
          <p className="flex items-center gap-2 text-xs font-medium text-indigo-700">
            <Shield className="size-3" />
            {t("partner_popupActive")}
          </p>
          <p className="mt-0.5 text-[11px] text-indigo-600/70">
            {t("bypass_expires_in")
              .replace("{m}", String(Math.floor(bypassRemaining / 60)))
              .replace("{s}", String(bypassRemaining % 60).padStart(2, "0"))}
          </p>
          <button
            onClick={handleLockNow}
            className="mt-2 flex w-full cursor-pointer items-center justify-center gap-2 rounded-lg border border-indigo-300 bg-white px-3 py-1.5 text-xs font-medium text-indigo-700 transition-all hover:bg-indigo-100"
          >
            <ShieldX className="size-3" />
            Lock Now
          </button>
        </div>
      )}
      {bypassRemaining !== null && bypassRemaining <= 0 && (
        <div className="mb-3 rounded-xl border border-amber-200 bg-amber-50 p-3">
          <p className="flex items-center gap-2 text-xs font-medium text-amber-700">
            <ShieldX className="size-3" />
            Bypass expired
          </p>
          <button
            onClick={handleLockNow}
            className="mt-2 flex w-full cursor-pointer items-center justify-center gap-2 rounded-lg border border-amber-300 bg-white px-3 py-1.5 text-xs font-medium text-amber-700 transition-all hover:bg-amber-100"
          >
            <ShieldX className="size-3" />
            Lock Now
          </button>
        </div>
      )}

      {/* Loading state */}
      {status === "loading" && (
        <div className="flex items-center justify-center gap-2 rounded-xl border border-gray-200 bg-gray-50 p-4">
          <Loader2 className="size-4 animate-spin text-indigo-500" />
          <span className="text-sm text-gray-600">{t("popup_scanning")}</span>
        </div>
      )}

      {/* Error state */}
      {status === "error" && (
        <div className="rounded-xl border border-red-200 bg-red-50 p-4 text-center">
          <TriangleAlert className="mx-auto mb-1 size-5 text-red-500" />
          <p className="text-sm text-red-700">{t("popup_failedToScan")}</p>
        </div>
      )}

      {/* Skipped state */}
      {status === "skipped" && (
        <div className="rounded-xl border border-amber-200 bg-amber-50 p-4 text-center">
          <TriangleAlert className="mx-auto mb-1 size-5 text-amber-500" />
          <p className="text-sm font-medium text-amber-800">
            {t("popup_notScannable")}
          </p>
          <p className="mt-1 text-xs text-amber-600/70">
            {t("popup_notScannableDesc")}
          </p>
        </div>
      )}

      {/* Not classified state */}
      {status === "not_classified" && (
        <div className="rounded-xl border border-gray-200 bg-gray-50 p-4 text-center">
          <Scan className="mx-auto mb-1 size-5 text-gray-400" />
          <p className="text-sm font-medium text-gray-700">
            {t("popup_notClassified")}
          </p>
          <p className="mt-1 text-xs text-gray-500">
            {t("popup_notClassifiedDesc")}
          </p>
        </div>
      )}

      {/* Safe state */}
      {status === "safe" && (
        <div className="rounded-xl border border-emerald-200 bg-emerald-50 p-4">
          <div className="mb-2 flex items-center gap-2">
            <ShieldCheck className="size-4 text-emerald-500" />
            <span className="text-sm font-medium text-emerald-700">
              {result?.from_list === "whitelist"
                ? t("popup_allowedByAdmin")
                : t("popup_safe")}
            </span>
          </div>
          {result?.from_list === "whitelist" ? (
            <p className="text-xs text-emerald-600/70">
              {t("popup_allowedByAdminDesc")}
            </p>
          ) : (
            scorePercent && (
              <div>
                <div className="mb-1 flex justify-between text-xs text-gray-500">
                  <span>{t("popup_gamblingScore")}</span>
                  <span>{scorePercent}%</span>
                </div>
                <div className="h-1.5 overflow-hidden rounded-full bg-gray-200">
                  <div
                    className="h-full rounded-full bg-emerald-500 transition-all"
                    style={{ width: `${scorePercent}%` }}
                  />
                </div>
                <div className="mt-3 space-y-1">
                  <div className="flex items-center gap-2 text-xs text-gray-500">
                    <FileText className="size-3" />
                    <span>
                      {t("popup_textScore")}:{" "}
                      {result?.text_score != null
                        ? (result.text_score * 100).toFixed(1) + "%"
                        : "—"}
                    </span>
                  </div>
                  <div className="flex items-center gap-2 text-xs text-gray-500">
                    <Image className="size-3" />
                    <span>
                      {t("popup_imageScore")}:{" "}
                      {result?.image_score != null
                        ? (result.image_score * 100).toFixed(1) + "%"
                        : "—"}
                    </span>
                  </div>
                  <div className="mt-1">
                    <ScreenshotIcon status={result?.screenshot_status} />
                  </div>
                  {result?.fusion_alpha != null && (
                    <div className="mt-1 text-[10px] text-gray-400">
                      {t("popup_fusionInfo").replace(
                        "{alpha}",
                        `${result.fusion_alpha}`
                      )}
                    </div>
                  )}
                </div>
              </div>
            )
          )}
        </div>
      )}

      {/* Gambling state */}
      {status === "gambling" && (
        <div className="rounded-xl border border-red-200 bg-red-50 p-4">
          <div className="mb-2 flex items-center gap-2">
            <ShieldX className="size-4 text-red-500" />
            <span className="text-sm font-medium text-red-700">
              {result?.from_list === "blacklist"
                ? t("popup_blockedByAdmin")
                : t("popup_blocked")}
            </span>
          </div>
          {result?.from_list === "blacklist" ? (
            <p className="text-xs text-red-600/70">
              {t("popup_blockedByAdminDesc")}
            </p>
          ) : (
            scorePercent && (
              <div>
                <div className="mb-1 flex justify-between text-xs text-gray-500">
                  <span>{t("popup_gamblingScore")}</span>
                  <span>{scorePercent}%</span>
                </div>
                <div className="h-1.5 overflow-hidden rounded-full bg-gray-200">
                  <div
                    className="h-full rounded-full bg-red-500 transition-all"
                    style={{ width: `${scorePercent}%` }}
                  />
                </div>
                <div className="mt-3 space-y-1">
                  <div className="flex items-center gap-2 text-xs text-gray-500">
                    <FileText className="size-3" />
                    <span>
                      {t("popup_textScore")}:{" "}
                      {result?.text_score != null
                        ? (result.text_score * 100).toFixed(1) + "%"
                        : "—"}
                    </span>
                  </div>
                  <div className="flex items-center gap-2 text-xs text-gray-500">
                    <Image className="size-3" />
                    <span>
                      {t("popup_imageScore")}:{" "}
                      {result?.image_score != null
                        ? (result.image_score * 100).toFixed(1) + "%"
                        : "—"}
                    </span>
                  </div>
                  <div className="mt-1">
                    <ScreenshotIcon status={result?.screenshot_status} />
                  </div>
                  {result?.fusion_alpha != null && (
                    <div className="mt-1 text-[10px] text-gray-400">
                      {t("popup_fusionInfo").replace(
                        "{alpha}",
                        `${result.fusion_alpha}`
                      )}
                    </div>
                  )}
                </div>
              </div>
            )
          )}
        </div>
      )}

      {/* Report button */}
      {(status === "safe" || status === "gambling") &&
        !result?.from_list &&
        reportState === "idle" && (
          <button
            onClick={handleReport}
            className="mt-3 flex w-full cursor-pointer items-center justify-center gap-2 rounded-xl border border-gray-200 bg-white px-4 py-2.5 text-sm font-medium text-gray-700 shadow-xs transition-all hover:bg-gray-50"
          >
            <Flag className="size-4" />
            {t("popup_reportFalsePositive")}
          </button>
        )}

      {((status === "safe" || status === "gambling") && result?.from_list) ||
      reportState !== "idle" ? (
        <button
          disabled
          className="mt-3 flex w-full cursor-not-allowed items-center justify-center gap-2 rounded-xl border border-gray-200 bg-gray-50 px-4 py-2.5 text-sm font-medium text-gray-400"
        >
          <Flag className="size-4" />
          {reportState === "loading" && t("reportSending")}
          {reportState === "done" && t("reportSent")}
          {reportState === "rate_limited" && t("reportRateLimited")}
          {reportState === "not_classified" && t("reportNotClassified")}
          {reportState === "listed" && t("reportListed")}
          {reportState === "idle" && t("popup_reportFalsePositive")}
        </button>
      ) : null}

      <button
        onClick={() => browser.runtime.openOptionsPage()}
        className="mt-3 flex w-full cursor-pointer items-center justify-center gap-2 rounded-xl border border-gray-200 bg-white px-4 py-2 text-xs font-medium text-gray-500 shadow-xs transition-all hover:bg-gray-50"
      >
        <Settings className="size-3" />
        {t("extName")} Settings
      </button>
    </div>
  )
}

export default App
