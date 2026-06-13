import { useEffect, useMemo, useRef, useState } from "react"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Badge } from "@/components/ui/badge"
import { Trash2 } from "lucide-react"
import { useLogs, type LogEntry } from "@/hooks/useLogs"
import { format } from "date-fns"

const TAG_COLORS: Record<string, string> = {
  API: "bg-blue-500",
  WARN: "bg-yellow-500",
  INFO: "bg-green-500",
  SCREENSHOT: "bg-purple-500",
  MULTIPAGE: "bg-orange-500",
  DBG: "bg-gray-500",
}

function tagColor(tag: string): string {
  return TAG_COLORS[tag] ?? "bg-slate-500"
}

function LogRow({ entry }: { entry: LogEntry }) {
  return (
    <tr className="border-b border-border text-xs">
      <td className="px-2 py-1 font-mono whitespace-nowrap text-muted-foreground">
        {format(new Date(entry.time), "HH:mm:ss")}
      </td>
      <td className="px-2 py-1">
        <Badge
          className={`${tagColor(entry.tag)} text-white hover:${tagColor(entry.tag)}`}
        >
          {entry.tag}
        </Badge>
      </td>
      <td className="px-2 py-1 font-mono break-all whitespace-pre-wrap">
        {entry.message}
      </td>
    </tr>
  )
}

export function LogsPanel() {
  const [tagFilter, setTagFilter] = useState<string | undefined>(undefined)
  const { data, isLoading, clear } = useLogs(tagFilter)
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" })
  }, [data])

  const allTags = useMemo(() => {
    if (!data?.entries) return []
    const tags = new Set(data.entries.map((e) => e.tag))
    return Array.from(tags).sort()
  }, [data])

  const tagCounts = useMemo(() => {
    if (!data?.entries) return {} as Record<string, number>
    return data.entries.reduce(
      (acc, e) => {
        acc[e.tag] = (acc[e.tag] ?? 0) + 1
        return acc
      },
      {} as Record<string, number>
    )
  }, [data])

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <div>
          <CardTitle>API Logs</CardTitle>
          <CardDescription>
            Recent backend logs (last 1000 entries)
          </CardDescription>
        </div>
        <Button
          variant="destructive"
          size="sm"
          onClick={() => clear.mutate()}
          disabled={clear.isPending || !data?.entries?.length}
        >
          <Trash2 className="mr-1 size-3" />
          Clear
        </Button>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="flex flex-wrap gap-1">
          <Badge
            variant={tagFilter === undefined ? "default" : "outline"}
            className="cursor-pointer"
            onClick={() => setTagFilter(undefined)}
          >
            All {data?.entries ? `(${data.entries.length})` : ""}
          </Badge>
          {allTags.map((tag) => (
            <Badge
              key={tag}
              variant={tagFilter === tag ? "default" : "outline"}
              className={`cursor-pointer ${tagFilter !== tag ? "" : tagColor(tag) + " text-white"}`}
              onClick={() => setTagFilter(tag === tagFilter ? undefined : tag)}
            >
              {tag} ({tagCounts[tag]})
            </Badge>
          ))}
        </div>

        {isLoading ? (
          <div className="py-8 text-center text-sm text-muted-foreground">
            Loading logs...
          </div>
        ) : !data?.entries?.length ? (
          <div className="py-8 text-center text-sm text-muted-foreground">
            No logs yet
          </div>
        ) : (
          <ScrollArea className="h-[500px] rounded-md border">
            <table className="w-full table-fixed">
              <thead>
                <tr className="border-b border-border text-xs text-muted-foreground">
                  <th className="w-20 px-2 py-1 text-left font-medium">Time</th>
                  <th className="w-24 px-2 py-1 text-left font-medium">Tag</th>
                  <th className="px-2 py-1 text-left font-medium">Message</th>
                </tr>
              </thead>
              <tbody>
                {data.entries.map((entry, i) => (
                  <LogRow key={i} entry={entry} />
                ))}
              </tbody>
              <tfoot>
                <tr>
                  <td colSpan={3}>
                    <div ref={bottomRef} />
                  </td>
                </tr>
              </tfoot>
            </table>
          </ScrollArea>
        )}
      </CardContent>
    </Card>
  )
}
