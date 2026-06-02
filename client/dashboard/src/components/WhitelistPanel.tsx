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

export function WhitelistPanel() {
  const { data, isLoading, add, remove } = useWhitelist()
  const [hostname, setHostname] = useState("")
  const [filter, setFilter] = useState("")

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
          placeholder="URL to whitelist..."
          value={hostname}
          onChange={(e) => setHostname(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleAdd()}
          className="max-w-sm"
        />
        <Button onClick={handleAdd} disabled={!hostname.trim() || add.isPending}>
          {add.isPending ? "Adding..." : "Add"}
        </Button>
      </div>

      <Input
        placeholder="Search by hostname..."
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
        className="max-w-sm"
      />

      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Hostname</TableHead>
              <TableHead>Added</TableHead>
              <TableHead className="w-20" />
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              Array.from({ length: 3 }).map((_, i) => (
                <TableRow key={i}>
                  <TableCell><Skeleton className="h-4 w-40" /></TableCell>
                  <TableCell><Skeleton className="h-4 w-24" /></TableCell>
                  <TableCell><Skeleton className="h-4 w-16" /></TableCell>
                </TableRow>
              ))
            ) : !filtered.length ? (
              <TableRow>
                <TableCell colSpan={3} className="py-8 text-center text-muted-foreground">
                  {filter && data?.entries.length
                    ? "No matching hostnames"
                    : "No whitelisted hosts"}
                </TableCell>
              </TableRow>
            ) : (
              filtered.map((entry: ListEntry) => (
                <TableRow key={entry.id}>
                  <TableCell className="font-mono text-sm">{entry.hostname}</TableCell>
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
                          Delete
                        </Button>
                      </AlertDialogTrigger>
                      <AlertDialogContent>
                        <AlertDialogHeader>
                          <AlertDialogTitle>Remove {entry.hostname} from whitelist?</AlertDialogTitle>
                          <AlertDialogDescription>
                            This hostname will no longer be allowed.
                          </AlertDialogDescription>
                        </AlertDialogHeader>
                        <AlertDialogFooter>
                          <AlertDialogCancel>Cancel</AlertDialogCancel>
                          <AlertDialogAction onClick={() => remove.mutate(entry.id)}>
                            Delete
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
          {data.entries.length} hostname{data.entries.length !== 1 && "s"} whitelisted
        </p>
      )}
    </div>
  )
}
