import { useEffect, useState } from "react"
import { Flag, ShieldX } from "lucide-react"
import { initLanguage, getCurrentLanguage, t as i18nT } from "@/utils/i18n"

const API_BASE = import.meta.env.WXT_API_BASE

function App() {
  const [, setReady] = useState(false)

  useEffect(() => {
    initLanguage().then(() => setReady(true))
  }, [])

  const t = i18nT
  const params = new URLSearchParams(window.location.search)
  const blockedUrl = params.get("url")
  const gamblingScore = params.get("gambling_score")
  const fromList = params.get("from_list")
  const [reportState, setReportState] = useState<
    "idle" | "loading" | "done" | "rate_limited" | "not_classified" | "listed"
  >("idle")

  async function handleReport() {
    setReportState("loading")
    try {
      const res = await fetch(`${API_BASE}/report`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          url: blockedUrl,
          gambling_score: Number(gamblingScore),
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

  return (
    <div className="flex min-h-screen items-center justify-center bg-linear-to-b from-white to-gray-50 p-4">
      <div className="w-full max-w-md">
        <div className="rounded-2xl border border-gray-200 bg-white p-8 text-center shadow-sm">
          <div className="mx-auto mb-4 flex size-16 items-center justify-center rounded-2xl border border-red-200 bg-red-50">
            <ShieldX className="size-8 text-red-500" />
          </div>
          <h1 className="mb-1 text-2xl font-bold text-gray-900">
            {t("blocked_title")}
          </h1>
          <p className="mb-6 text-sm font-medium text-red-600">
            {fromList ? t("popup_blockedByAdmin") : t("blocked_description")}
          </p>
          {fromList === "blacklist" && (
            <p className="-mt-4 mb-6 text-xs text-red-500/70">
              {t("popup_blockedByAdminDesc")}
            </p>
          )}

          <div className="mb-3 rounded-xl border border-gray-200 bg-gray-50 p-4 text-left">
            <p className="mb-1 text-xs text-gray-500">{t("blocked_url")}</p>
            <p className="text-sm break-all text-gray-700">{blockedUrl}</p>
          </div>

          <div className="mb-6 rounded-xl border border-gray-200 bg-gray-50 p-4 text-left">
            <p className="mb-1 text-xs text-gray-500">
              {t("blocked_gamblingScore")}
            </p>
            <p className="text-lg font-semibold text-gray-900">
              {(Number(gamblingScore) * 100).toFixed(1)}%
            </p>
            <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-gray-200">
              <div
                className="h-full rounded-full bg-red-500"
                style={{
                  width: `${(Number(gamblingScore) * 100).toFixed(1)}%`,
                }}
              />
            </div>
          </div>

          {fromList ? (
            <button
              disabled
              className="flex w-full cursor-not-allowed items-center justify-center gap-2 rounded-xl border border-gray-200 bg-gray-100 px-4 py-2.5 text-sm font-medium text-gray-400"
            >
              <Flag className="size-4" />
              {t("reportListed")}
            </button>
          ) : (
            <button
              onClick={reportState === "idle" ? handleReport : undefined}
              disabled={reportState !== "idle"}
              className="flex w-full cursor-pointer items-center justify-center gap-2 rounded-xl border border-gray-200 bg-white px-4 py-2.5 text-sm font-medium text-gray-700 shadow-xs transition-all hover:bg-gray-50 disabled:cursor-not-allowed disabled:bg-gray-50 disabled:text-gray-400 disabled:hover:bg-gray-50"
            >
              <Flag className="size-4" />
              {reportState === "idle" && t("blocked_reportFalsePositive")}
              {reportState === "loading" && t("reportSending")}
              {reportState === "done" && t("reportSent")}
              {reportState === "rate_limited" && t("reportRateLimited")}
              {reportState === "not_classified" && t("reportNotClassified")}
              {reportState === "listed" && t("reportListed")}
            </button>
          )}
        </div>
      </div>
    </div>
  )
}

export default App
