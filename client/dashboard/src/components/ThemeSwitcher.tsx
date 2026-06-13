import { Sun, Moon, Monitor } from "lucide-react"
import { Button } from "@/components/ui/button"
import { useTheme } from "@/components/theme-provider"
import { useI18n } from "@/i18n/context"

const THEMES = [
  { value: "light", icon: Sun, labelKey: "theme_light" },
  { value: "dark", icon: Moon, labelKey: "theme_dark" },
  { value: "system", icon: Monitor, labelKey: "theme_system" },
] as const

export function ThemeSwitcher() {
  const { theme, setTheme } = useTheme()
  const { t } = useI18n()

  return (
    <div className="flex gap-2">
      {THEMES.map(({ value, icon: Icon, labelKey }) => (
        <Button
          key={value}
          variant={theme === value ? "default" : "outline"}
          size="sm"
          onClick={() => setTheme(value)}
          className="gap-1.5"
        >
          <Icon className="h-4 w-4" />
          <span>{t(labelKey)}</span>
        </Button>
      ))}
    </div>
  )
}
