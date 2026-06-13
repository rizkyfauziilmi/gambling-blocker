import { useState } from "react"
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
import type { ListEntry } from "@/hooks/useLists"
import { useWhitelist } from "@/hooks/useLists"
import { useI18n } from "@/i18n/context"

export function WhitelistPanel() {
  const { data, isLoading, add, remove } = useWhitelist()
  const [hostname, setHostname] = useState("")
  const [filter, setFilter] = useState("")
  const { t } = useI18n()

  const handleAdd = () => {
    const h = hostname.trim().toLowerCase()
    if (!h) return
    add.mutate(h)
    setHostname("")
  }

  const filtered = data?.entries
    ? data.entries.filter((e) =>
        e.hostname.toLowerCase().includes(filter.toLowerCase())
      )
    : []

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        <Input
          placeholder={t("url_to_whitelist")}
          value={hostname}
          onChange={(e) => setHostname(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleAdd()}
          className="max-w-sm"
        />
        <Button
          onClick={handleAdd}
          disabled={!hostname.trim() || add.isPending}
        >
          {add.isPending ? t("adding") : t("add")}
        </Button>
      </div>

      <Input
        placeholder={t("search_hostname")}
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
        className="max-w-sm"
      />

      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>{t("hostname")}</TableHead>
              <TableHead>{t("added")}</TableHead>
              <TableHead className="w-20" />
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              Array.from({ length: 3 }).map((_, i) => (
                <TableRow key={i}>
                  <TableCell>
                    <Skeleton className="h-4 w-40" />
                  </TableCell>
                  <TableCell>
                    <Skeleton className="h-4 w-24" />
                  </TableCell>
                  <TableCell>
                    <Skeleton className="h-4 w-16" />
                  </TableCell>
                </TableRow>
              ))
            ) : !filtered.length ? (
              <TableRow>
                <TableCell
                  colSpan={3}
                  className="py-8 text-center text-muted-foreground"
                >
                  {filter && data?.entries.length
                    ? t("no_match")
                    : t("no_whitelisted_hosts")}
                </TableCell>
              </TableRow>
            ) : (
              filtered.map((entry: ListEntry) => (
                <TableRow key={entry.id}>
                  <TableCell className="font-mono text-sm">
                    {entry.hostname}
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {new Date(entry.created_at + "Z").toLocaleString()}
                  </TableCell>
                  <TableCell>
                    <AlertDialog>
                      <AlertDialogTrigger asChild>
                        <Button
                          variant="outline"
                          size="sm"
                          disabled={remove.isPending}
                          className="text-red-600 hover:text-red-700"
                        >
                          {t("delete")}
                        </Button>
                      </AlertDialogTrigger>
                      <AlertDialogContent>
                        <AlertDialogHeader>
                          <AlertDialogTitle>
                            {t("remove_from_whitelist", {
                              hostname: entry.hostname,
                            })}
                          </AlertDialogTitle>
                          <AlertDialogDescription>
                            {t("will_not_be_allowed")}
                          </AlertDialogDescription>
                        </AlertDialogHeader>
                        <AlertDialogFooter>
                          <AlertDialogCancel>{t("cancel")}</AlertDialogCancel>
                          <AlertDialogAction
                            onClick={() => remove.mutate(entry.id)}
                          >
                            {t("delete")}
                          </AlertDialogAction>
                        </AlertDialogFooter>
                      </AlertDialogContent>
                    </AlertDialog>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      {data?.entries && (
        <p className="text-sm text-muted-foreground">
          {t("hostnames_whitelisted", {
            count: data.entries.length,
            plural: data.entries.length !== 1 ? t("plural_s") : "",
          })}
        </p>
      )}
    </div>
  )
}
