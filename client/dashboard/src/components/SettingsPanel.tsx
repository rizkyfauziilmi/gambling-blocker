import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Switch } from "@/components/ui/switch"
import { Slider } from "@/components/ui/slider"
import { useSettings } from "@/hooks/useSettings"
import { Skeleton } from "@/components/ui/skeleton"
import { LanguageSwitcher } from "@/components/LanguageSwitcher"
import { ThemeSwitcher } from "@/components/ThemeSwitcher"
import { useI18n } from "@/i18n/context"
import { SlidersHorizontal, Database, Activity, Palette } from "lucide-react"
import type { LucideIcon } from "lucide-react"

function SectionHeading({
  icon: Icon,
  children,
}: {
  icon: LucideIcon
  children: React.ReactNode
}) {
  return (
    <h3 className="flex items-center gap-2 text-sm font-semibold tracking-wider text-muted-foreground uppercase">
      <Icon className="h-4 w-4" />
      {children}
    </h3>
  )
}

export function SettingsPanel() {
  const { data: settings, isLoading, update } = useSettings()
  const { t } = useI18n()

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <Skeleton className="h-5 w-48" />
          <Skeleton className="h-4 w-64" />
        </CardHeader>
        <CardContent className="space-y-4">
          <Skeleton className="h-8 w-full" />
        </CardContent>
      </Card>
    )
  }

  if (!settings) {
    return (
      <Card>
        <CardContent className="py-8 text-center text-sm text-muted-foreground">
          {t("failed_load_settings")}
        </CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>{t("settings_title")}</CardTitle>
        <CardDescription>{t("settings_desc")}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <section className="space-y-4 rounded-lg border border-border/50 bg-muted/40 p-4">
          <SectionHeading icon={SlidersHorizontal}>
            {t("section_features")}
          </SectionHeading>

          <div className="flex items-center justify-between gap-4">
            <div className="space-y-0.5">
              <div className="text-sm font-medium">{t("skip_screenshot")}</div>
              <div className="text-sm text-muted-foreground">
                {t("skip_screenshot_desc")}
              </div>
            </div>
            <Switch
              checked={settings.bypass_text_enabled}
              onCheckedChange={(checked) =>
                update.mutate({ bypass_text_enabled: checked })
              }
              disabled={update.isPending}
            />
          </div>

          <div className="flex items-center justify-between gap-4">
            <div className="space-y-0.5">
              <div className="text-sm font-medium">{t("multipage")}</div>
              <div className="text-sm text-muted-foreground">
                {t("multipage_desc")}
              </div>
            </div>
            <Switch
              checked={settings.multipage_enabled}
              onCheckedChange={(checked) =>
                update.mutate({ multipage_enabled: checked })
              }
              disabled={update.isPending}
            />
          </div>

          <div className="flex items-center justify-between gap-4">
            <div className="space-y-0.5">
              <div className="text-sm font-medium">{t("debug_logging")}</div>
              <div className="text-sm text-muted-foreground">
                {t("debug_logging_desc")}
              </div>
            </div>
            <Switch
              checked={settings.debug_logging_enabled}
              onCheckedChange={(checked) =>
                update.mutate({ debug_logging_enabled: checked })
              }
              disabled={update.isPending}
            />
          </div>
        </section>

        <section className="space-y-4 rounded-lg border border-border/50 bg-muted/40 p-4">
          <SectionHeading icon={Database}>{t("section_cache")}</SectionHeading>

          <div className="space-y-2">
            <div className="flex items-center gap-4">
              <Slider
                value={[settings.cache_ttl_hours]}
                onValueChange={([v]) => update.mutate({ cache_ttl_hours: v })}
                min={1}
                max={24}
                step={1}
                disabled={update.isPending}
                className="flex-1"
              />
              <span className="min-w-[3rem] text-right text-sm font-medium tabular-nums">
                {settings.cache_ttl_hours}h
              </span>
            </div>
            <div className="text-sm text-muted-foreground">
              {t("cache_ttl_desc")}
            </div>
          </div>
        </section>

        <section className="space-y-4 rounded-lg border border-border/50 bg-muted/40 p-4">
          <SectionHeading icon={Activity}>
            {t("section_monitoring")}
          </SectionHeading>

          <div className="space-y-2">
            <div className="flex items-center justify-between gap-4">
              <div className="space-y-0.5">
                <div className="text-sm font-medium">
                  {t("stale_threshold")}
                </div>
              </div>
              <span className="text-sm font-medium tabular-nums">
                {settings.stale_hours}h
              </span>
            </div>
            <Slider
              value={[settings.stale_hours]}
              onValueChange={([v]) => update.mutate({ stale_hours: v })}
              min={1}
              max={12}
              step={1}
              disabled={update.isPending}
            />
            <div className="text-sm text-muted-foreground">
              {t("stale_threshold_desc")}
            </div>
          </div>

          <div className="space-y-2">
            <div className="flex items-center justify-between gap-4">
              <div className="space-y-0.5">
                <div className="text-sm font-medium">{t("check_interval")}</div>
              </div>
              <span className="text-sm font-medium tabular-nums">
                {settings.stale_check_interval_minutes}m
              </span>
            </div>
            <Slider
              value={[settings.stale_check_interval_minutes]}
              onValueChange={([v]) =>
                update.mutate({ stale_check_interval_minutes: v })
              }
              min={5}
              max={120}
              step={5}
              disabled={update.isPending}
            />
            <div className="text-sm text-muted-foreground">
              {t("check_interval_desc")}
            </div>
          </div>
        </section>

        <section className="space-y-4 rounded-lg border border-border/50 bg-muted/40 p-4">
          <SectionHeading icon={Palette}>
            {t("section_preferences")}
          </SectionHeading>

          <div className="space-y-2">
            <div className="text-sm font-medium">{t("language")}</div>
            <LanguageSwitcher />
            <div className="text-sm text-muted-foreground">
              {t("language_desc")}
            </div>
          </div>

          <div className="space-y-2">
            <div className="text-sm font-medium">{t("theme_label")}</div>
            <ThemeSwitcher />
            <div className="text-sm text-muted-foreground">
              {t("theme_desc")}
            </div>
          </div>
        </section>
      </CardContent>
    </Card>
  )
}
