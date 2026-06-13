import { useMemo } from "react"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Switch } from "@/components/ui/switch"
import { Separator } from "@/components/ui/separator"
import { DateTimePicker } from "@/components/datetime-picker"
import { useSettings } from "@/hooks/useSettings"
import { Skeleton } from "@/components/ui/skeleton"

function cacheExpiresAtToDate(value: string | null): Date | undefined {
  if (!value) return undefined
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? undefined : d
}

function dateToCacheExpiresAt(d: Date | undefined): string | null {
  return d ? d.toISOString() : null
}

export function SettingsPanel() {
  const { data: settings, isLoading, update } = useSettings()

  const cacheDate = useMemo(
    () => cacheExpiresAtToDate(settings?.cache_expires_at ?? null),
    [settings?.cache_expires_at]
  )

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
          Failed to load settings
        </CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Settings</CardTitle>
        <CardDescription>
          Configure feature flags and cache behavior
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="flex items-center justify-between gap-4">
          <div className="space-y-0.5">
            <div className="text-sm font-medium">Skip Screenshot</div>
            <div className="text-sm text-muted-foreground">
              Skip screenshot capture when text model is already conclusive
              (score &ge; 0.95 or &le; 0.05)
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

        <Separator />

        <div className="flex items-center justify-between gap-4">
          <div className="space-y-0.5">
            <div className="text-sm font-medium">Multipage Inference</div>
            <div className="text-sm text-muted-foreground">
              Run multi-page inference for root domain URLs
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

        <Separator />

        <div className="flex items-center justify-between gap-4">
          <div className="space-y-0.5">
            <div className="text-sm font-medium">Debug Logging</div>
            <div className="text-sm text-muted-foreground">
              Show SCREENSHOT, MULTIPAGE, and DBG tags in logs
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

        <Separator />

        <div className="space-y-2">
          <div className="space-y-0.5">
            <div className="text-sm font-medium">Cache Expiration</div>
            <div className="text-sm text-muted-foreground">
              Set a specific datetime for cache expiration (empty = default 24h
              TTL)
            </div>
          </div>
          <DateTimePicker
            value={cacheDate}
            onChange={(d) =>
              update.mutate({ cache_expires_at: dateToCacheExpiresAt(d) })
            }
            clearable
            disabled={update.isPending}
          />
        </div>
      </CardContent>
    </Card>
  )
}
