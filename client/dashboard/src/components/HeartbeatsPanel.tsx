import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import {
  useHeartbeats,
  useDeleteHeartbeats,
  useTriggerHeartbeat,
} from "@/hooks/useHeartbeats"
import { useTriggerStaleCheck } from "@/hooks/useAdmin"
import { useI18n } from "@/i18n/context"
import { Activity, Loader2, Search, ShieldAlert, Trash2 } from "lucide-react"
import { useState } from "react"

export function HeartbeatsPanel() {
  const { data, isLoading } = useHeartbeats()
  const deleteHb = useDeleteHeartbeats()
  const triggerHb = useTriggerHeartbeat()
  const { t } = useI18n()
  const [search, setSearch] = useState("")
  const staleCheck = useTriggerStaleCheck()

  if (isLoading) {
    return (
      <div className="space-y-3">
        <Skeleton className="h-8 w-full" />
        <Skeleton className="h-8 w-full" />
        <Skeleton className="h-8 w-full" />
      </div>
    )
  }

  if (!data || data.length === 0) {
    return (
      <div className="flex flex-col items-center gap-2 py-12 text-muted-foreground">
        <Activity className="size-8" />
        <p className="text-sm">{t("no_heartbeats")}</p>
      </div>
    )
  }

  const filtered = data.filter(
    (hb) =>
      hb.extension_id.toLowerCase().includes(search.toLowerCase()) ||
      hb.partner_email.toLowerCase().includes(search.toLowerCase())
  )

  function ageBadge(hours: number | null) {
    if (hours === null) return <Badge variant="destructive">{t("never")}</Badge>
    if (hours < 2)
      return <Badge variant="default">{t("hours_ago", { age: hours })}</Badge>
    if (hours < 24)
      return <Badge variant="secondary">{t("hours_ago", { age: hours })}</Badge>
    return <Badge variant="destructive">{t("hours_ago", { age: hours })}</Badge>
  }

  return (
    <div className="space-y-3">
      <Button
        onClick={() => staleCheck.mutate()}
        disabled={staleCheck.isPending}
        variant="outline"
        size="sm"
        className="gap-2"
      >
        {staleCheck.isPending ? (
          <Loader2 className="size-4 animate-spin" />
        ) : (
          <ShieldAlert className="size-4" />
        )}
        {t("trigger_stale_check")}
      </Button>
      <div className="relative">
        <Search className="absolute top-2.5 left-2.5 size-4 text-muted-foreground" />
        <Input
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder={t("search_extension_id")}
          className="pl-8"
        />
      </div>
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>{t("col_extension_id")}</TableHead>
              <TableHead>{t("col_partner_email")}</TableHead>
              <TableHead>{t("col_last_heartbeat")}</TableHead>
              <TableHead>{t("col_age")}</TableHead>
              <TableHead className="text-right">{t("col_total")}</TableHead>
              <TableHead className="text-right">{t("col_actions")}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {filtered.map((hb) => (
              <TableRow key={hb.extension_id}>
                <TableCell className="font-mono text-xs break-all">
                  {hb.extension_id}
                </TableCell>
                <TableCell className="text-sm">{hb.partner_email}</TableCell>
                <TableCell className="text-sm text-muted-foreground">
                  {hb.last_heartbeat_at
                    ? new Date(hb.last_heartbeat_at).toLocaleString()
                    : "-"}
                </TableCell>
                <TableCell>{ageBadge(hb.heartbeat_age_hours)}</TableCell>
                <TableCell className="text-right text-sm">
                  {hb.total_heartbeats}
                </TableCell>
                <TableCell className="text-right">
                  <div className="flex justify-end gap-1">
                    <Button
                      onClick={() => triggerHb.mutate(hb.extension_id)}
                      disabled={triggerHb.isPending}
                      variant="outline"
                      size="sm"
                    >
                      {triggerHb.isPending ? (
                        <Loader2 className="size-3.5 animate-spin" />
                      ) : (
                        <Activity className="size-3.5" />
                      )}
                    </Button>
                    <Button
                      onClick={() => deleteHb.mutate(hb.extension_id)}
                      disabled={deleteHb.isPending}
                      variant="outline"
                      size="sm"
                    >
                      <Trash2 className="size-3.5 text-destructive" />
                    </Button>
                  </div>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </div>
  )
}
