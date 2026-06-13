import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Skeleton } from "@/components/ui/skeleton"
import { useExtensionStatus } from "@/hooks/useExtensions"
import { Badge } from "@/components/ui/badge"
import { useI18n } from "@/i18n/context"

interface PartnerPanelProps {
  extensionId: string
}

export function PartnerPanel({ extensionId }: PartnerPanelProps) {
  const { data, isLoading, isError } = useExtensionStatus(extensionId)
  const { t } = useI18n()

  if (isLoading) {
    return (
      <Card>
        <CardHeader className="pb-2">
          <Skeleton className="h-4 w-40" />
        </CardHeader>
        <CardContent>
          <Skeleton className="h-6 w-60" />
        </CardContent>
      </Card>
    )
  }

  if (isError || !data?.exists) {
    return (
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            {t("accountability_partner")}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground">
            {t("no_extension")}
          </p>
        </CardContent>
      </Card>
    )
  }

  const age = data.heartbeat_age_hours
  const isHealthy = age !== null && age !== undefined && age < 24

  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            {t("accountability_partner")}
          </CardTitle>
          <Badge variant={isHealthy ? "default" : "destructive"}>
            {isHealthy ? t("active") : t("stale")}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-2">
        <div className="flex justify-between text-sm">
          <span className="text-muted-foreground">{t("partner_email")}</span>
          <span className="font-medium">{data.partner_email}</span>
        </div>
        <div className="flex justify-between text-sm">
          <span className="text-muted-foreground">{t("last_heartbeat")}</span>
          <span className="font-medium">
            {age !== null && age !== undefined ? t("hours_ago", { age }) : t("never")}
          </span>
        </div>
        <div className="flex justify-between text-sm">
          <span className="text-muted-foreground">{t("tamper_attempts")}</span>
          <span className="font-medium">{data.tamper_count_1h ?? 0}</span>
        </div>
      </CardContent>
    </Card>
  )
}
