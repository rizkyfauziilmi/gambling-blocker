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

  const filtered = groups
    ? groups.filter((g) =>
        g.hostname.toLowerCase().includes(search.toLowerCase())
      )
    : []

  return (
    <div className="space-y-4">
      <Input
        placeholder="Search by hostname..."
        value={search}
        onChange={(e) => setSearch(e.target.value)}
        className="max-w-sm"
      />

      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Hostname</TableHead>
              <TableHead className="text-right">Reports</TableHead>
              <TableHead className="text-right">Avg Score</TableHead>
              <TableHead>Last Reported</TableHead>
              <TableHead className="w-28">Actions</TableHead>
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
                  {groups.length === 0
                    ? "No reports yet"
                    : "No matching hostnames"}
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
                    {new Date(g.last_reported + "Z").toLocaleString()}
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
                          <TooltipContent>Add to whitelist</TooltipContent>
                        </Tooltip>
                        <AlertDialogContent>
                          <AlertDialogHeader>
                            <AlertDialogTitle>
                              Whitelist {g.hostname}?
                            </AlertDialogTitle>
                            <AlertDialogDescription>
                              Reports for this hostname will also be deleted.
                            </AlertDialogDescription>
                          </AlertDialogHeader>
                          <AlertDialogFooter>
                            <AlertDialogCancel>Cancel</AlertDialogCancel>
                            <AlertDialogAction
                              onClick={() => onWhitelist(g.hostname)}
                            >
                              Continue
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
                          <TooltipContent>Add to blacklist</TooltipContent>
                        </Tooltip>
                        <AlertDialogContent>
                          <AlertDialogHeader>
                            <AlertDialogTitle>
                              Blacklist {g.hostname}?
                            </AlertDialogTitle>
                            <AlertDialogDescription>
                              Reports for this hostname will also be deleted.
                            </AlertDialogDescription>
                          </AlertDialogHeader>
                          <AlertDialogFooter>
                            <AlertDialogCancel>Cancel</AlertDialogCancel>
                            <AlertDialogAction
                              onClick={() => onBlacklist(g.hostname)}
                            >
                              Continue
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
                          <TooltipContent>Delete all reports</TooltipContent>
                        </Tooltip>
                        <AlertDialogContent>
                          <AlertDialogHeader>
                            <AlertDialogTitle>
                              Delete all reports for {g.hostname}?
                            </AlertDialogTitle>
                            <AlertDialogDescription>
                              This action cannot be undone.
                            </AlertDialogDescription>
                          </AlertDialogHeader>
                          <AlertDialogFooter>
                            <AlertDialogCancel>Cancel</AlertDialogCancel>
                            <AlertDialogAction
                              onClick={() => onDeleteByHostname(g.hostname)}
                            >
                              Delete
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
          {groups.length} hostname{groups.length !== 1 && "s"} reported
        </p>
      )}
    </div>
  )
}
