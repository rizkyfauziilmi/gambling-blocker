import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"

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
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["blacklist"] }),
  })

  const remove = useMutation({
    mutationFn: (id: number) => removeFromList("blacklist", id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["blacklist"] }),
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
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["whitelist"] }),
  })

  const remove = useMutation({
    mutationFn: (id: number) => removeFromList("whitelist", id),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["whitelist"] }),
  })

  return { ...query, add, remove }
}
