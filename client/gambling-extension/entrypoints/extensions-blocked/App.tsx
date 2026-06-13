import { useEffect, useState } from "react"
import {
  KeyRound,
  Lock,
  Shield,
  ShieldCheck,
  TriangleAlert,
} from "lucide-react"
import { initLanguage, t } from "@/utils/i18n"
import { getOrCreateExtensionId, verifyPassword } from "@/utils/password"

const API_BASE = import.meta.env.WXT_API_BASE

function App() {
  const [ready, setReady] = useState(false)
  const [password, setPassword] = useState("")
  const [state, setState] = useState<"idle" | "checking" | "success" | "error">(
    "idle"
  )

  useEffect(() => {
    initLanguage().then(() => setReady(true))
  }, [])

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setState("checking")

    const ok = await verifyPassword(password)
    if (ok) {
      setState("success")
      await browser.storage.session.set({
        extensions_bypass: true,
        extensions_bypass_expires_at: Date.now() + 5 * 60 * 1000,
      })
      setTimeout(async () => {
        await browser.tabs.create({ url: "chrome://extensions" })
        window.close()
      }, 1500)
      return
    }

    setState("error")
    const extensionId = await getOrCreateExtensionId()
    fetch(`${API_BASE}/extension/tamper-alert`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        extension_id: extensionId,
        event_type: "extensions_page",
        details: "Failed password attempt on extensions-blocked page",
      }),
    }).catch(() => {})
  }

  if (!ready) return null

  return (
    <div className="flex min-h-screen items-center justify-center bg-linear-to-b from-white to-gray-50 p-4">
      <div className="w-full max-w-md">
        <div className="rounded-2xl border border-gray-200 bg-white p-8 text-center shadow-sm">
          <div className="mx-auto mb-4 flex size-16 items-center justify-center rounded-2xl border border-red-200 bg-red-50">
            <Lock className="size-8 text-red-500" />
          </div>

          <h1 className="mb-1 text-2xl font-bold text-gray-900">
            {t("partner_extBlockedTitle")}
          </h1>
          <p className="mb-6 text-sm text-gray-600">
            {t("partner_extBlockedDesc")}
          </p>

          {state === "success" ? (
            <div className="rounded-xl border border-emerald-200 bg-emerald-50 p-6">
              <ShieldCheck className="mx-auto mb-2 size-10 text-emerald-500" />
              <p className="font-medium text-emerald-700">
                {t("partner_extBlockedSuccess")}
              </p>
            </div>
          ) : (
            <form onSubmit={handleSubmit} className="space-y-4">
              <div className="rounded-xl border border-gray-200 bg-gray-50 p-4 text-left">
                <label className="mb-1 block text-xs font-medium text-gray-500">
                  {t("partner_extBlockedForm")}
                </label>
                <div className="relative">
                  <KeyRound className="absolute top-1/2 left-3 size-4 -translate-y-1/2 text-gray-400" />
                  <input
                    type="password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    placeholder={t("partner_passwordPlaceholder")}
                    className="w-full rounded-lg border border-gray-300 bg-white py-2.5 pr-3 pl-10 text-sm text-gray-900 outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500"
                    autoFocus
                  />
                </div>
              </div>

              {state === "error" && (
                <div className="flex items-center gap-2 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-left text-sm text-red-700">
                  <TriangleAlert className="size-4 shrink-0" />
                  <span>{t("partner_wrongPassword")}</span>
                </div>
              )}

              <button
                type="submit"
                disabled={state === "checking" || !password}
                className="flex w-full cursor-pointer items-center justify-center gap-2 rounded-xl bg-indigo-500 px-4 py-2.5 text-sm font-medium text-white shadow-sm transition-all hover:bg-indigo-600 disabled:cursor-not-allowed disabled:opacity-50"
              >
                <Shield className="size-4" />
                {state === "checking"
                  ? "Checking..."
                  : t("partner_unlockButton")}
              </button>
            </form>
          )}
        </div>
      </div>
    </div>
  )
}

export default App
