import { useI18n } from "@/i18n/context"
import { Button } from "@/components/ui/button"

const LANGUAGES = [
  { value: "en", label: "English", flag: "\u{1F1EC}\u{1F1E7}" },
  { value: "id", label: "Bahasa Indonesia", flag: "\u{1F1EE}\u{1F1E9}" },
] as const

export function LanguageSwitcher() {
  const { locale, setLocale } = useI18n()

  return (
    <div className="flex gap-2">
      {LANGUAGES.map((lang) => (
        <Button
          key={lang.value}
          variant={locale === lang.value ? "default" : "outline"}
          size="sm"
          onClick={() => setLocale(lang.value as "en" | "id")}
          className="gap-1.5"
        >
          <span className="text-base leading-none">{lang.flag}</span>
          <span>{lang.label}</span>
        </Button>
      ))}
    </div>
  )
}
