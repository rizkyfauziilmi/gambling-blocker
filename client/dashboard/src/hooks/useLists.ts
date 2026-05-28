import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { toast } from "sonner"

export interface ListEntry {
  id: number
  hostname: string
  list_type: "blacklist" | "whitelist"
  created_at: string
}

interface ListResponse {
  entries: ListEntry[]
}

async function fetchList(type: string): Promise<ListResponse> {
  const base = import.meta.env.VITE_API_BASE ?? ""
  const res = await fetch(`${base}/${type}`)
  if (!res.ok) throw new Error(`Failed to fetch ${type}`)
  return res.json()
}

async function addToList(type: string, hostname: string) {
  const base = import.meta.env.VITE_API_BASE ?? ""
  const res = await fetch(`${base}/${type}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ hostname }),
  })
  if (!res.ok) {
    const data = await res.json()
    throw new Error(data.detail || "Failed to add")
  }
  return res.json()
}

async function removeFromList(type: string, id: number) {
  const base = import.meta.env.VITE_API_BASE ?? ""
  const res = await fetch(`${base}/${type}/${id}`, { method: "DELETE" })
  if (!res.ok) throw new Error("Failed to remove")
  return res.json()
}

export function useBlacklist() {
  const queryClient = useQueryClient()

  const query = useQuery<ListResponse>({
    queryKey: ["blacklist"],
    queryFn: () => fetchList("blacklist"),
  })

  const add = useMutation({
    mutationFn: (hostname: string) => addToList("blacklist", hostname),
    onSuccess: (data) => {
      toast.success(`${data.entry?.hostname || "Hostname"} blacklisted`)
      queryClient.invalidateQueries({ queryKey: ["blacklist"] })
      queryClient.invalidateQueries({ queryKey: ["reports"] })
      queryClient.invalidateQueries({ queryKey: ["cache"] })
    },
    onError: (err) => toast.error(err.message || "Failed to add to blacklist"),
  })

  const remove = useMutation({
    mutationFn: (id: number) => removeFromList("blacklist", id),
    onSuccess: () => {
      toast.success("Removed from blacklist")
      queryClient.invalidateQueries({ queryKey: ["blacklist"] })
      queryClient.invalidateQueries({ queryKey: ["cache"] })
    },
    onError: () => toast.error("Failed to remove from blacklist"),
  })

  return { ...query, add, remove }
}

export function useWhitelist() {
  const queryClient = useQueryClient()

  const query = useQuery<ListResponse>({
    queryKey: ["whitelist"],
    queryFn: () => fetchList("whitelist"),
  })

  const add = useMutation({
    mutationFn: (hostname: string) => addToList("whitelist", hostname),
    onSuccess: (data) => {
      toast.success(`${data.entry?.hostname || "Hostname"} whitelisted`)
      queryClient.invalidateQueries({ queryKey: ["whitelist"] })
      queryClient.invalidateQueries({ queryKey: ["reports"] })
      queryClient.invalidateQueries({ queryKey: ["cache"] })
    },
    onError: (err) => toast.error(err.message || "Failed to add to whitelist"),
  })

  const remove = useMutation({
    mutationFn: (id: number) => removeFromList("whitelist", id),
    onSuccess: () => {
      toast.success("Removed from whitelist")
      queryClient.invalidateQueries({ queryKey: ["whitelist"] })
      queryClient.invalidateQueries({ queryKey: ["cache"] })
    },
    onError: () => toast.error("Failed to remove from whitelist"),
  })

  return { ...query, add, remove }
}
