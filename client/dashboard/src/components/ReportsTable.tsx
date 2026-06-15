import { useState } from "react"
import { Ban, ShieldCheck, Trash2 } from "lucide-react"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Skeleton } from "@/components/ui/skeleton"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import type { GroupedReport } from "@/hooks/useReports"
import { useDateLocale } from "@/hooks/useDateLocale"
import { useI18n } from "@/i18n/context"
import { format, parseISO } from "date-fns"

interface ReportsTableProps {
  groups: GroupedReport[] | undefined
  onWhitelist: (hostname: string) => void
  onBlacklist: (hostname: string) => void
  onDeleteByHostname: (hostname: string) => void
  isMutating?: boolean
}

export function ReportsTable({
  groups,
  onWhitelist,
  onBlacklist,
  onDeleteByHostname,
  isMutating,
}: ReportsTableProps) {
  const [search, setSearch] = useState("")
  const { t } = useI18n()
  const dateLocale = useDateLocale()

  const filtered = groups
    ? groups.filter((g) =>
        g.hostname.toLowerCase().includes(search.toLowerCase())
      )
    : []

  return (
    <div className="space-y-4">
      <Input
        placeholder={t("search_hostname")}
        value={search}
        onChange={(e) => setSearch(e.target.value)}
        className="max-w-sm"
      />

      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>{t("hostname")}</TableHead>
              <TableHead className="text-right">{t("reports")}</TableHead>
              <TableHead className="text-right">{t("avg_score")}</TableHead>
              <TableHead>{t("last_reported")}</TableHead>
              <TableHead className="w-28">{t("actions")}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {!groups ? (
              Array.from({ length: 5 }).map((_, i) => (
                <TableRow key={i}>
                  {Array.from({ length: 5 }).map((_, j) => (
                    <TableCell key={j}>
                      <Skeleton className="h-4 w-20" />
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : filtered.length === 0 ? (
              <TableRow>
                <TableCell
                  colSpan={5}
                  className="py-8 text-center text-muted-foreground"
                >
                  {groups.length === 0 ? t("no_reports") : t("no_match")}
                </TableCell>
              </TableRow>
            ) : (
              filtered.map((g) => (
                <TableRow key={g.hostname}>
                  <TableCell
                    className="max-w-xs truncate font-mono text-sm"
                    title={g.hostname}
                  >
                    {g.hostname}
                  </TableCell>
                  <TableCell className="text-right">{g.report_count}</TableCell>
                  <TableCell className="text-right font-mono text-xs">
                    {g.avg_score.toFixed(4)}
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {format(parseISO(g.last_reported), "PPpp", {
                      locale: dateLocale,
                    })}
                  </TableCell>
                  <TableCell>
                    <div className="flex items-center gap-0.5">
                      <AlertDialog>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <AlertDialogTrigger asChild>
                              <Button
                                variant="ghost"
                                size="icon"
                                className="h-8 w-8 text-green-600 hover:bg-green-50 hover:text-green-700"
                                disabled={isMutating}
                              >
                                <ShieldCheck className="h-4 w-4" />
                              </Button>
                            </AlertDialogTrigger>
                          </TooltipTrigger>
                          <TooltipContent>
                            {t("tooltip_whitelist")}
                          </TooltipContent>
                        </Tooltip>
                        <AlertDialogContent>
                          <AlertDialogHeader>
                            <AlertDialogTitle>
                              {t("dialog_whitelist_title", {
                                hostname: g.hostname,
                              })}
                            </AlertDialogTitle>
                            <AlertDialogDescription>
                              {t("dialog_whitelist_desc")}
                            </AlertDialogDescription>
                          </AlertDialogHeader>
                          <AlertDialogFooter>
                            <AlertDialogCancel>{t("cancel")}</AlertDialogCancel>
                            <AlertDialogAction
                              onClick={() => onWhitelist(g.hostname)}
                            >
                              {t("continue_")}
                            </AlertDialogAction>
                          </AlertDialogFooter>
                        </AlertDialogContent>
                      </AlertDialog>

                      <AlertDialog>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <AlertDialogTrigger asChild>
                              <Button
                                variant="ghost"
                                size="icon"
                                className="h-8 w-8 text-red-600 hover:bg-red-50 hover:text-red-700"
                                disabled={isMutating}
                              >
                                <Ban className="h-4 w-4" />
                              </Button>
                            </AlertDialogTrigger>
                          </TooltipTrigger>
                          <TooltipContent>
                            {t("tooltip_blacklist")}
                          </TooltipContent>
                        </Tooltip>
                        <AlertDialogContent>
                          <AlertDialogHeader>
                            <AlertDialogTitle>
                              {t("dialog_blacklist_title", {
                                hostname: g.hostname,
                              })}
                            </AlertDialogTitle>
                            <AlertDialogDescription>
                              {t("dialog_blacklist_desc")}
                            </AlertDialogDescription>
                          </AlertDialogHeader>
                          <AlertDialogFooter>
                            <AlertDialogCancel>{t("cancel")}</AlertDialogCancel>
                            <AlertDialogAction
                              onClick={() => onBlacklist(g.hostname)}
                            >
                              {t("continue_")}
                            </AlertDialogAction>
                          </AlertDialogFooter>
                        </AlertDialogContent>
                      </AlertDialog>

                      <AlertDialog>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <AlertDialogTrigger asChild>
                              <Button
                                variant="ghost"
                                size="icon"
                                className="h-8 w-8 text-muted-foreground hover:text-foreground"
                                disabled={isMutating}
                              >
                                <Trash2 className="h-4 w-4" />
                              </Button>
                            </AlertDialogTrigger>
                          </TooltipTrigger>
                          <TooltipContent>
                            {t("tooltip_delete_reports")}
                          </TooltipContent>
                        </Tooltip>
                        <AlertDialogContent>
                          <AlertDialogHeader>
                            <AlertDialogTitle>
                              {t("dialog_delete_title", {
                                hostname: g.hostname,
                              })}
                            </AlertDialogTitle>
                            <AlertDialogDescription>
                              {t("dialog_delete_desc")}
                            </AlertDialogDescription>
                          </AlertDialogHeader>
                          <AlertDialogFooter>
                            <AlertDialogCancel>{t("cancel")}</AlertDialogCancel>
                            <AlertDialogAction
                              onClick={() => onDeleteByHostname(g.hostname)}
                            >
                              {t("delete")}
                            </AlertDialogAction>
                          </AlertDialogFooter>
                        </AlertDialogContent>
                      </AlertDialog>
                    </div>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      {groups && (
        <p className="text-sm text-muted-foreground">
          {t("hostname_reported", {
            count: groups.length,
            plural: groups.length !== 1 ? t("plural_s") : "",
          })}
        </p>
      )}
    </div>
  )
}
