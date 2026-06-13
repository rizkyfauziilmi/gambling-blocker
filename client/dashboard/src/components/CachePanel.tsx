import { useState } from "react"
import { Ban, Camera, CameraOff, ShieldCheck, Trash2 } from "lucide-react"
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
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
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
import { useCache } from "@/hooks/useCache"
import { useBlacklist, useWhitelist } from "@/hooks/useLists"
import { hostnameFromUrl } from "@/utils/url"

export function CachePanel() {
  const { data, isLoading, remove, flush } = useCache()
  const { add: addBlacklist } = useBlacklist()
  const { add: addWhitelist } = useWhitelist()
  const [filter, setFilter] = useState("")

  const isMutating =
    remove.isPending || addBlacklist.isPending || addWhitelist.isPending

  const categoryVariant: Record<
    string,
    "destructive" | "secondary" | "outline"
  > = {
    gambling: "destructive",
    "non-gambling": "secondary",
    "bare-ip": "outline",
  }

  const filtered = data?.entries
    ? data.entries.filter((e) => {
        const q = filter.toLowerCase()
        return (
          e.url.toLowerCase().includes(q) ||
          hostnameFromUrl(e.url).toLowerCase().includes(q)
        )
      })
    : []

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-sm text-muted-foreground">
          Recent classifications from Redis cache
        </p>

        {data?.entries.length ? (
          <AlertDialog>
            <AlertDialogTrigger asChild>
              <Button
                variant="destructive"
                size="sm"
                disabled={flush.isPending}
              >
                {flush.isPending ? "Deleting..." : "Delete All Cache"}
              </Button>
            </AlertDialogTrigger>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>Delete all cache?</AlertDialogTitle>
                <AlertDialogDescription>
                  This will remove all {data.entries.length} cached
                  classifications. URLs will be re-classified when visited
                  again.
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

      <Input
        placeholder="Search by URL or hostname..."
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
        className="max-w-sm"
      />

      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>URL</TableHead>
              <TableHead>Hostname</TableHead>
              <TableHead>Category</TableHead>
              <TableHead className="text-right">Fused Score</TableHead>
              <TableHead className="text-right">Text</TableHead>
              <TableHead className="text-right">Image</TableHead>
              <TableHead>Screenshot</TableHead>
              <TableHead className="w-36">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoading ? (
              Array.from({ length: 5 }).map((_, i) => (
                <TableRow key={i}>
                  <TableCell>
                    <Skeleton className="h-5 w-20" />
                  </TableCell>
                  <TableCell>
                    <Skeleton className="h-4 w-60" />
                  </TableCell>
                  <TableCell>
                    <Skeleton className="h-5 w-20" />
                  </TableCell>
                  <TableCell>
                    <Skeleton className="ml-auto h-4 w-16" />
                  </TableCell>
                  <TableCell>
                    <Skeleton className="ml-auto h-4 w-12" />
                  </TableCell>
                  <TableCell>
                    <Skeleton className="ml-auto h-4 w-12" />
                  </TableCell>
                  <TableCell>
                    <Skeleton className="h-5 w-20" />
                  </TableCell>
                  <TableCell>
                    <Skeleton className="h-4 w-24" />
                  </TableCell>
                </TableRow>
              ))
            ) : !filtered.length ? (
              <TableRow>
                <TableCell
                  colSpan={8}
                  className="py-8 text-center text-muted-foreground"
                >
                  {filter && data?.entries.length
                    ? "No matching entries"
                    : "No cached classifications"}
                </TableCell>
              </TableRow>
            ) : (
              filtered.map((entry) => {
                const hostname = hostnameFromUrl(entry.url)
                const pct = (v: number | null | undefined) =>
                  v != null ? (v * 100).toFixed(1) + "%" : "—"

                const screenshotLabel = (s: string | null | undefined) => {
                  if (!s) return null
                  if (s === "screenshot_ok")
                    return {
                      label: "OK",
                      icon: Camera,
                      variant: "success" as const,
                    }
                  if (s === "noise_screenshot")
                    return {
                      label: "Noise",
                      icon: CameraOff,
                      variant: "warning" as const,
                    }
                  if (s.startsWith("http_error_")) {
                    const code = s
                      .replace("http_error_", "")
                      .replace("_noise", "")
                    const isNoise = s.endsWith("_noise")
                    return {
                      label: isNoise ? `Noise (HTTP ${code})` : `HTTP ${code}`,
                      icon: CameraOff,
                      variant: isNoise
                        ? ("warning" as const)
                        : ("destructive" as const),
                    }
                  }
                  const labels: Record<
                    string,
                    {
                      label: string
                      variant: "default" | "destructive" | "warning" | "success"
                    }
                  > = {
                    bypass_list: { label: "By List", variant: "default" },
                    bypass_bare_ip: { label: "Bare IP", variant: "default" },
                    bypass_text_only: {
                      label: "Text Only",
                      variant: "default",
                    },
                    no_screenshot: {
                      label: "No Screenshot",
                      variant: "default",
                    },
                    capture_failed: {
                      label: "Capture Failed",
                      variant: "destructive",
                    },
                    extraction_failed: {
                      label: "Extraction Failed",
                      variant: "destructive",
                    },
                  }
                  const known = labels[s]
                  if (known) return { ...known, icon: CameraOff }
                  return {
                    label: s,
                    icon: CameraOff,
                    variant: "destructive" as const,
                  }
                }

                const ss = screenshotLabel(entry.screenshot_status)

                return (
                  <TableRow
                    key={entry.cache_key}
                    className={entry.is_fused ? "" : "opacity-60"}
                  >
                    <TableCell className="max-w-sm truncate" title={entry.url}>
                      {entry.url}
                    </TableCell>
                    <TableCell className="font-mono text-sm">
                      {hostname}
                    </TableCell>
                    <TableCell>
                      <Badge
                        variant={categoryVariant[entry.category] ?? "outline"}
                      >
                        {entry.category}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-right font-mono text-xs">
                      {entry.gambling_score.toFixed(4)}
                    </TableCell>
                    <TableCell className="text-right font-mono text-xs text-muted-foreground">
                      {pct(entry.text_score)}
                    </TableCell>
                    <TableCell className="text-right font-mono text-xs text-muted-foreground">
                      {pct(entry.image_score)}
                    </TableCell>
                    <TableCell>
                      {ss ? (
                        entry.screenshot_url ? (
                          <Dialog>
                            <DialogTrigger asChild>
                              <Button
                                variant="ghost"
                                size="sm"
                                className="h-7 gap-1 px-2 text-xs"
                              >
                                <ss.icon className="h-3 w-3" />
                                {ss.label}
                              </Button>
                            </DialogTrigger>
                            <DialogContent className="max-w-3xl">
                              <DialogHeader>
                                <DialogTitle>Screenshot</DialogTitle>
                                <DialogDescription>
                                  {hostname}
                                </DialogDescription>
                              </DialogHeader>
                              <div className="flex items-center justify-center">
                                <img
                                  src={entry.screenshot_url!}
                                  alt="Screenshot"
                                  className="max-h-[70vh] rounded border object-contain"
                                />
                              </div>
                            </DialogContent>
                          </Dialog>
                        ) : (
                          <span className="inline-flex items-center gap-1 text-xs text-muted-foreground">
                            <ss.icon className="h-3 w-3" />
                            {ss.label}
                          </span>
                        )
                      ) : (
                        <span className="text-xs text-muted-foreground">—</span>
                      )}
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
                                Whitelist {hostname}?
                              </AlertDialogTitle>
                              <AlertDialogDescription>
                                Reports for this hostname will also be deleted.
                              </AlertDialogDescription>
                            </AlertDialogHeader>
                            <AlertDialogFooter>
                              <AlertDialogCancel>Cancel</AlertDialogCancel>
                              <AlertDialogAction
                                onClick={() => addWhitelist.mutate(entry.url)}
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
                                Blacklist {hostname}?
                              </AlertDialogTitle>
                              <AlertDialogDescription>
                                Reports for this hostname will also be deleted.
                              </AlertDialogDescription>
                            </AlertDialogHeader>
                            <AlertDialogFooter>
                              <AlertDialogCancel>Cancel</AlertDialogCancel>
                              <AlertDialogAction
                                onClick={() => addBlacklist.mutate(entry.url)}
                              >
                                Continue
                              </AlertDialogAction>
                            </AlertDialogFooter>
                          </AlertDialogContent>
                        </AlertDialog>

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
                      </div>
                    </TableCell>
                  </TableRow>
                )
              })
            )}
          </TableBody>
        </Table>
      </div>

      {data?.entries && (
        <p className="text-sm text-muted-foreground">
          {data.entries.length} cached entr
          {data.entries.length === 1 ? "y" : "ies"}
        </p>
      )}
    </div>
  )
}
