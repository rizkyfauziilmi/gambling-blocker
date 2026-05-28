import { useState } from "react"
import { Ban, ShieldCheck, Trash2 } from "lucide-react"
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
import type { Report } from "@/hooks/useReports"

interface ReportsTableProps {
  reports: Report[] | undefined
  onWhitelist: (hostname: string) => void
  onBlacklist: (hostname: string) => void
  onDelete: (id: number) => void
  onDeleteByHostname: (hostname: string) => void
  isMutating?: boolean
}

export function ReportsTable({
  reports,
  onWhitelist,
  onBlacklist,
  onDelete,
  onDeleteByHostname,
  isMutating,
}: ReportsTableProps) {
  const [search, setSearch] = useState("")

  const filtered = reports
    ? reports.filter(
        (r) =>
          r.hostname.toLowerCase().includes(search.toLowerCase()) ||
          r.url.toLowerCase().includes(search.toLowerCase())
      )
    : []

  return (
    <div className="space-y-4">
      <Input
        placeholder="Search by hostname or URL..."
        value={search}
        onChange={(e) => setSearch(e.target.value)}
        className="max-w-sm"
      />

      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>URL</TableHead>
              <TableHead className="text-right">Score</TableHead>
              <TableHead>Reporter</TableHead>
              <TableHead>Time</TableHead>
              <TableHead className="w-28">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {!reports ? (
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
                <TableCell colSpan={5} className="py-8 text-center text-muted-foreground">
                  {reports.length === 0
                    ? "No reports yet"
                    : "No matching reports"}
                </TableCell>
              </TableRow>
            ) : (
              filtered.map((r) => (
                <TableRow key={r.id}>
                  <TableCell className="max-w-xs truncate" title={r.url}>
                    {r.hostname}
                  </TableCell>
                  <TableCell className="text-right font-mono text-xs">
                    {r.gambling_score.toFixed(4)}
                  </TableCell>
                  <TableCell className="font-mono text-xs text-muted-foreground">
                    {r.reporter_ip}
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {new Date(r.created_at + "Z").toLocaleString()}
                  </TableCell>
                  <TableCell>
                    <div className="flex items-center gap-0.5">
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-8 w-8 text-green-600 hover:text-green-700 hover:bg-green-50"
                            disabled={isMutating}
                            onClick={() => { onWhitelist(r.hostname); onDeleteByHostname(r.hostname) }}
                          >
                            <ShieldCheck className="h-4 w-4" />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent>Add to whitelist</TooltipContent>
                      </Tooltip>

                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-8 w-8 text-red-600 hover:text-red-700 hover:bg-red-50"
                            disabled={isMutating}
                            onClick={() => { onBlacklist(r.hostname); onDeleteByHostname(r.hostname) }}
                          >
                            <Ban className="h-4 w-4" />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent>Add to blacklist</TooltipContent>
                      </Tooltip>

                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-8 w-8 text-muted-foreground hover:text-foreground"
                            disabled={isMutating}
                            onClick={() => onDelete(r.id)}
                          >
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent>Delete report</TooltipContent>
                      </Tooltip>
                    </div>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      {reports && (
        <p className="text-sm text-muted-foreground">
          Showing {filtered.length} of {reports.length} reports
        </p>
      )}
    </div>
  )
}
