import { Trash2 } from "lucide-react"
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
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
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
import { useCache } from "@/hooks/useCache"

export function CachePanel() {
  const { data, isLoading, remove, flush } = useCache()

  const categoryVariant: Record<string, "destructive" | "secondary" | "outline"> = {
    gambling: "destructive",
    "non-gambling": "secondary",
    "bare-ip": "outline",
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-sm text-muted-foreground">
          Recent classifications from Redis cache
        </p>

        {data?.entries.length ? (
          <AlertDialog>
            <AlertDialogTrigger asChild>
              <Button variant="destructive" size="sm" disabled={flush.isPending}>
                {flush.isPending ? "Deleting..." : "Delete All Cache"}
              </Button>
            </AlertDialogTrigger>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>Delete all cache?</AlertDialogTitle>
                <AlertDialogDescription>
                  This will remove all {data.entries.length} cached classifications.
                  URLs will be re-classified when visited again.
                </AlertDialogDescription>
              </AlertDialogHeader>
              <AlertDialogFooter>
                <AlertDialogCancel>Cancel</AlertDialogCancel>
                <AlertDialogAction onClick={() => flush.mutate()}>
                  Delete All
                </AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>
        ) : null}
      </div>

      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>URL</TableHead>
              <TableHead>Category</TableHead>
              <TableHead className="text-right">Score</TableHead>
              <TableHead className="w-12" />
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              Array.from({ length: 5 }).map((_, i) => (
                <TableRow key={i}>
                  <TableCell><Skeleton className="h-4 w-60" /></TableCell>
                  <TableCell><Skeleton className="h-5 w-20" /></TableCell>
                  <TableCell><Skeleton className="h-4 w-16 ml-auto" /></TableCell>
                  <TableCell><Skeleton className="h-4 w-8" /></TableCell>
                </TableRow>
              ))
            ) : !data?.entries.length ? (
              <TableRow>
                <TableCell colSpan={4} className="py-8 text-center text-muted-foreground">
                  No cached classifications
                </TableCell>
              </TableRow>
            ) : (
              data.entries.map((entry) => (
                <TableRow key={entry.cache_key}>
                  <TableCell className="max-w-md truncate" title={entry.url}>
                    {entry.url}
                  </TableCell>
                  <TableCell>
                    <Badge variant={categoryVariant[entry.category] ?? "outline"}>
                      {entry.category}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-right font-mono text-xs">
                    {entry.gambling_score.toFixed(4)}
                  </TableCell>
                  <TableCell>
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-8 w-8 text-muted-foreground hover:text-foreground"
                          disabled={remove.isPending}
                          onClick={() => remove.mutate(entry.cache_key)}
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </TooltipTrigger>
                      <TooltipContent>Delete cache entry</TooltipContent>
                    </Tooltip>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      {data?.entries && (
        <p className="text-sm text-muted-foreground">
          {data.entries.length} cached entr{data.entries.length === 1 ? "y" : "ies"}
        </p>
      )}
    </div>
  )
}
