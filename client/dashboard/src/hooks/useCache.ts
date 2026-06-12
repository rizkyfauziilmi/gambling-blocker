import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { toast } from "sonner"

export interface CacheEntry {
  url: string
  category: string
  gambling_score: number
  cache_key: string
  text_score?: number | null
  image_score?: number | null
  fusion_alpha?: number | null
  screenshot_url?: string | null
  screenshot_status?: string | null
  from_list?: string | null
  is_fused?: boolean
}

interface CacheResponse {
  entries: CacheEntry[]
}

async function fetchCache(): Promise<CacheResponse> {
  const base = import.meta.env.VITE_API_BASE ?? ""
  const res = await fetch(`${base}/cache`)
  if (!res.ok) throw new Error("Failed to fetch cache")
  return res.json()
}

async function deleteCacheEntry(key: string): Promise<void> {
  const base = import.meta.env.VITE_API_BASE ?? ""
  const res = await fetch(`${base}/cache/${encodeURIComponent(key)}`, {
    method: "DELETE",
  })
  if (!res.ok) throw new Error("Failed to delete cache entry")
}

async function flushCache(): Promise<void> {
  const base = import.meta.env.VITE_API_BASE ?? ""
  const res = await fetch(`${base}/cache`, { method: "DELETE" })
  if (!res.ok) throw new Error("Failed to flush cache")
}

export function useCache() {
  const queryClient = useQueryClient()

  const query = useQuery<CacheResponse>({
    queryKey: ["cache"],
    queryFn: fetchCache,
  })

  const remove = useMutation({
    mutationFn: deleteCacheEntry,
    onSuccess: () => {
      toast.success("Cache entry deleted")
      queryClient.invalidateQueries({ queryKey: ["cache"] })
    },
    onError: () => toast.error("Failed to delete cache entry"),
  })

  const flush = useMutation({
    mutationFn: flushCache,
    onSuccess: () => {
      toast.success("All cache deleted")
      queryClient.invalidateQueries({ queryKey: ["cache"] })
    },
    onError: () => toast.error("Failed to flush cache"),
  })

  return { ...query, remove, flush }
}
