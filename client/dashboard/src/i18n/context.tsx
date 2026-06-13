import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
} from "react"
import en from "./en"
import id from "./id"

type Locale = "en" | "id"
const STORAGE_KEY = "dashboard_lang"

interface I18nContextValue {
  locale: Locale
  setLocale: (locale: Locale) => void
  t: (key: string, params?: Record<string, string | number>) => string
}

const I18nContext = createContext<I18nContextValue | null>(null)

function loadLocale(): Locale {
  try {
    const stored = localStorage.getItem(STORAGE_KEY)
    if (stored === "en" || stored === "id") return stored
  } catch {}
  return "en"
}

const messages: Record<Locale, Record<string, string>> = { en, id }

export function I18nProvider({ children }: { children: React.ReactNode }) {
  const [locale, setLocaleState] = useState<Locale>(loadLocale)

  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, locale)
    } catch {}
  }, [locale])

  const setLocale = useCallback((l: Locale) => {
    setLocaleState(l)
  }, [])

  const t = useCallback(
    (key: string, params?: Record<string, string | number>): string => {
      const msg = messages[locale][key] ?? messages.en[key] ?? key
      if (!params) return msg
      return msg.replace(/\{(\w+)\}/g, (_, k) => String(params[k] ?? `{${k}}`))
    },
    [locale]
  )

  return (
    <I18nContext.Provider value={{ locale, setLocale, t }}>
      {children}
    </I18nContext.Provider>
  )
}

export function useI18n(): I18nContextValue {
  const ctx = useContext(I18nContext)
  if (!ctx) throw new Error("useI18n must be used within I18nProvider")
  return ctx
}
