import { useCallback, useEffect, useState } from "react"
import {
  KeyRound,
  Languages,
  Mail,
  RefreshCw,
  Shield,
  TriangleAlert,
  UserRoundCheck,
} from "lucide-react"
import { initLanguage, t, getCurrentLanguage, setLanguage } from "@/utils/i18n"
import {
  getOrCreateExtensionId,
  getPartnerStatus,
  hasPassword,
  verifyPassword,
} from "@/utils/password"
import { bypassStorage } from "@/utils/storage"

const API_BASE = import.meta.env.WXT_API_BASE
const LANGUAGES = [
  { value: "en", label: "English" },
  { value: "id", label: "Bahasa Indonesia" },
]

type Page = "loading" | "setup_partner" | "password_gate" | "settings"

function App() {
  const [page, setPage] = useState<Page>("loading")
  const [lang, setLangState] = useState("en")
  const [partnerEmail, setPartnerEmail] = useState("")
  const [password, setPassword] = useState("")
  const [setupState, setSetupState] = useState<
    "idle" | "sending" | "success" | "error"
  >("idle")
  const [passwordState, setPasswordState] = useState<
    "idle" | "checking" | "success" | "error"
  >("idle")
  const [resetState, setResetState] = useState<
    "idle" | "sending" | "success" | "error"
  >("idle")
  const [partnerStatus, setPartnerStatus] = useState<{
    hasPartner: boolean
    daysSinceInstall: number
  }>({ hasPartner: false, daysSinceInstall: 0 })

  useEffect(() => {
    ;(async () => {
      await initLanguage()
      setLangState(getCurrentLanguage())
      const status = await getPartnerStatus()
      setPartnerStatus(status)
      if (status.hasPartner) {
        const session = (await bypassStorage.getSession([
          "extensions_bypass",
          "extensions_bypass_expires_at",
        ])) as {
          extensions_bypass?: boolean
          extensions_bypass_expires_at?: number
        }
        if (
          session.extensions_bypass &&
          session.extensions_bypass_expires_at! > Date.now()
        ) {
          setPage("settings")
        } else {
          if (session.extensions_bypass) {
            await bypassStorage.clearSession([
              "extensions_bypass",
              "extensions_bypass_expires_at",
            ])
          }
          setPage("password_gate")
        }
      } else {
        setPage("setup_partner")
      }
    })()
  }, [])

  const handleSetup = useCallback(async () => {
    if (!partnerEmail.trim()) return
    setSetupState("sending")
    try {
      const extensionId = await getOrCreateExtensionId()
      const res = await fetch(`${API_BASE}/extension/setup`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          extension_id: extensionId,
          partner_email: partnerEmail.trim(),
        }),
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data?.detail || "Setup failed")
      setSetupState("success")
      await browser.storage.local.set({
        partner_password: {
          hash: data.password_hash,
          salt: data.password_salt,
        },
      })
    } catch {
      setSetupState("error")
      setTimeout(() => setSetupState("idle"), 3000)
    }
  }, [partnerEmail])

  const handlePasswordSubmit = useCallback(
    async (e: React.FormEvent) => {
      e.preventDefault()
      setPasswordState("checking")
      const ok = await verifyPassword(password)
      if (ok) {
        setPasswordState("success")
        await bypassStorage.setSession({
          extensions_bypass: true,
          extensions_bypass_expires_at: Date.now() + 5 * 60 * 1000,
        })
        setTimeout(() => setPage("settings"), 500)
        return
      }
      setPasswordState("error")
      const extensionId = await getOrCreateExtensionId()
      fetch(`${API_BASE}/extension/tamper-alert`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          extension_id: extensionId,
          event_type: "failed_password",
          details: "Wrong password entered on options page",
        }),
      }).catch(() => {})
    },
    [password]
  )

  const handleResetPassword = useCallback(async () => {
    setResetState("sending")
    try {
      const extensionId = await getOrCreateExtensionId()
      const res = await fetch(`${API_BASE}/extension/reset-password`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ extension_id: extensionId }),
      })
      if (!res.ok) throw new Error("reset failed")
      const data = await res.json()
      await browser.storage.local.set({
        partner_password: {
          hash: data.password_hash,
          salt: data.password_salt,
        },
      })
      setResetState("success")
      setTimeout(() => setResetState("idle"), 4000)
    } catch {
      setResetState("error")
      setTimeout(() => setResetState("idle"), 3000)
    }
  }, [])

  function handleLangChange(value: string) {
    setLanguage(value)
    setLangState(value)
    browser.storage.sync.set({ language: value })
  }

  if (page === "loading") return null

  return (
    <div className="flex min-h-screen items-center justify-center bg-linear-to-b from-white to-gray-50 p-4">
      <div className="w-full max-w-sm">
        <div className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
          <div className="mb-6 flex items-center gap-3">
            <div className="flex size-10 items-center justify-center rounded-xl bg-linear-to-br from-indigo-500 to-purple-600 shadow-lg shadow-indigo-500/30">
              <Shield className="text-lg text-white" size={22} />
            </div>
            <div>
              <h1 className="text-base font-semibold text-gray-900">
                {t("extName")}
              </h1>
              <p className="text-xs text-gray-500">{t("extDescription")}</p>
            </div>
          </div>

          {page === "setup_partner" && (
            <div className="space-y-4">
              <div className="rounded-xl border border-amber-200 bg-amber-50 p-4">
                <h2 className="mb-1 text-sm font-semibold text-amber-800">
                  {t("partner_setupTitle")}
                </h2>
                <p className="text-xs text-amber-700/80">
                  {t("partner_setupDesc")}
                </p>
                {partnerStatus.daysSinceInstall >= 7 && (
                  <p className="mt-2 text-xs font-medium text-amber-700">
                    {t("partner_daysOverdue")}
                  </p>
                )}
                {partnerStatus.daysSinceInstall < 7 && (
                  <p className="mt-2 text-xs text-amber-600/70">
                    {t("partner_graceDays").replace(
                      "{days}",
                      `${7 - partnerStatus.daysSinceInstall}`
                    )}
                  </p>
                )}
              </div>

              <div className="rounded-xl border border-gray-200 bg-gray-50 p-4">
                <label className="mb-2 flex items-center gap-2 text-sm font-medium text-gray-700">
                  <Mail className="size-4" />
                  {t("partner_emailLabel")}
                </label>
                <input
                  type="email"
                  value={partnerEmail}
                  onChange={(e) => setPartnerEmail(e.target.value)}
                  placeholder={t("partner_emailPlaceholder")}
                  className="w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm text-gray-900 outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500"
                />
              </div>

              {setupState === "error" && (
                <div className="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
                  {t("partner_setupFailed")}
                </div>
              )}

              {setupState === "success" ? (
                <div className="rounded-xl border border-emerald-200 bg-emerald-50 p-4 text-center">
                  <UserRoundCheck className="mx-auto mb-2 size-8 text-emerald-500" />
                  <p className="text-sm font-medium text-emerald-700">
                    {t("partner_setupSuccess")}
                  </p>
                </div>
              ) : (
                <button
                  onClick={handleSetup}
                  disabled={setupState === "sending" || !partnerEmail.trim()}
                  className="flex w-full cursor-pointer items-center justify-center gap-2 rounded-xl bg-indigo-500 px-4 py-2.5 text-sm font-medium text-white shadow-sm transition-all hover:bg-indigo-600 disabled:cursor-not-allowed disabled:opacity-50"
                >
                  {setupState === "sending"
                    ? "Sending..."
                    : t("partner_setupButton")}
                </button>
              )}
            </div>
          )}

          {page === "password_gate" && (
            <div className="space-y-4">
              <div className="rounded-xl border border-gray-200 bg-gray-50 p-4">
                <h2 className="mb-1 text-sm font-semibold text-gray-800">
                  {t("partner_passwordTitle")}
                </h2>
                <p className="text-xs text-gray-500">
                  {t("partner_passwordDesc")}
                </p>
              </div>

              <form onSubmit={handlePasswordSubmit} className="space-y-4">
                <div>
                  <label className="mb-2 flex items-center gap-2 text-sm font-medium text-gray-700">
                    <KeyRound className="size-4" />
                    {t("partner_passwordLabel")}
                  </label>
                  <input
                    type="password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    placeholder={t("partner_passwordPlaceholder")}
                    className="w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm text-gray-900 outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500"
                    autoFocus
                  />
                </div>

                {passwordState === "error" && (
                  <div className="flex items-center gap-2 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
                    <TriangleAlert className="size-4 shrink-0" />
                    <span>{t("partner_wrongPassword")}</span>
                  </div>
                )}

                {resetState === "success" && (
                  <div className="flex items-center gap-2 rounded-lg border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">
                    <RefreshCw className="size-4 shrink-0" />
                    <span>{t("partner_resetSuccess")}</span>
                  </div>
                )}

                {resetState === "error" && (
                  <div className="flex items-center gap-2 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
                    <TriangleAlert className="size-4 shrink-0" />
                    <span>{t("partner_resetError")}</span>
                  </div>
                )}

                <button
                  type="submit"
                  disabled={passwordState === "checking" || !password}
                  className="flex w-full cursor-pointer items-center justify-center gap-2 rounded-xl bg-indigo-500 px-4 py-2.5 text-sm font-medium text-white shadow-sm transition-all hover:bg-indigo-600 disabled:cursor-not-allowed disabled:opacity-50"
                >
                  {passwordState === "checking"
                    ? "Checking..."
                    : t("partner_unlockButton")}
                </button>

                <button
                  type="button"
                  onClick={handleResetPassword}
                  disabled={resetState === "sending"}
                  className="flex w-full cursor-pointer items-center justify-center gap-2 text-sm text-gray-500 transition-all hover:text-indigo-600 disabled:cursor-not-allowed disabled:opacity-50"
                >
                  <RefreshCw
                    className={`size-3 ${resetState === "sending" ? "animate-spin" : ""}`}
                  />
                  {resetState === "sending"
                    ? t("partner_resetSending")
                    : t("partner_forgotPassword")}
                </button>
              </form>
            </div>
          )}

          {page === "settings" && (
            <div className="space-y-4">
              <div className="rounded-xl border border-emerald-200 bg-emerald-50 p-3">
                <p className="flex items-center gap-2 text-xs font-medium text-emerald-700">
                  <UserRoundCheck className="size-3" />
                  {t("partner_popupActive")}
                </p>
              </div>

              <div className="rounded-xl border border-gray-200 bg-gray-50 p-4">
                <label className="mb-2 flex items-center gap-2 text-sm font-medium text-gray-700">
                  <Languages className="size-4" />
                  Language / Bahasa
                </label>
                <div className="flex gap-2">
                  {LANGUAGES.map((l) => (
                    <button
                      key={l.value}
                      onClick={() => handleLangChange(l.value)}
                      className={`flex-1 rounded-lg px-4 py-2 text-sm font-medium transition-all ${
                        lang === l.value
                          ? "bg-indigo-500 text-white shadow-sm"
                          : "border border-gray-300 bg-white text-gray-600 hover:bg-gray-100"
                      }`}
                    >
                      {l.label}
                    </button>
                  ))}
                </div>
              </div>

              <p className="text-center text-xs text-gray-400">
                {t("popup_protectionActive")}
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

export default App
