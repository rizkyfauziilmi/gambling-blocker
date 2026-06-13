import { useEffect, useState } from "react"
import { Languages, Shield } from "lucide-react"
import { initLanguage, t, getCurrentLanguage, setLanguage } from "@/utils/i18n"

const LANGUAGES = [
  { value: "en", label: "English" },
  { value: "id", label: "Bahasa Indonesia" },
]

function App() {
  const [ready, setReady] = useState(false)
  const [lang, setLangState] = useState("en")

  useEffect(() => {
    initLanguage().then(() => {
      setLangState(getCurrentLanguage())
      setReady(true)
    })
  }, [])

  function handleChange(value: string) {
    setLanguage(value)
    setLangState(value)
    browser.storage.sync.set({ language: value })
  }

  if (!ready) return null

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

          <div className="rounded-xl border border-gray-200 bg-gray-50 p-4">
            <label className="mb-2 flex items-center gap-2 text-sm font-medium text-gray-700">
              <Languages className="size-4" />
              Language / Bahasa
            </label>
            <div className="flex gap-2">
              {LANGUAGES.map((l) => (
                <button
                  key={l.value}
                  onClick={() => handleChange(l.value)}
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

          <p className="mt-4 text-center text-xs text-gray-400">
            {t("popup_protectionActive")}
          </p>
        </div>
      </div>
    </div>
  )
}

export default App
