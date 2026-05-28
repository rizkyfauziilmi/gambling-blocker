import { useState } from "react"
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
import type { Report } from "@/hooks/useReports"

interface ReportsTableProps {
  reports: Report[] | undefined
}

export function ReportsTable({ reports }: ReportsTableProps) {
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
            </TableRow>
          </TableHeader>
          <TableBody>
            {!reports ? (
              Array.from({ length: 5 }).map((_, i) => (
                <TableRow key={i}>
                  {Array.from({ length: 4 }).map((_, j) => (
                    <TableCell key={j}>
                      <Skeleton className="h-4 w-20" />
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : filtered.length === 0 ? (
              <TableRow>
                <TableCell colSpan={4} className="py-8 text-center text-muted-foreground">
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
