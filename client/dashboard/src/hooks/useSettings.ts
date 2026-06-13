import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { toast } from "sonner"

export interface Settings {
  bypass_text_enabled: boolean
  multipage_enabled: boolean
  debug_logging_enabled: boolean
  cache_expires_at: string | null
}

async function fetchSettings(): Promise<Settings> {
  const base = import.meta.env.VITE_API_BASE ?? ""
  const res = await fetch(`${base}/settings`)
  if (!res.ok) throw new Error("Failed to fetch settings")
  return res.json()
}

async function updateSettings(updates: Partial<Settings>): Promise<Settings> {
  const base = import.meta.env.VITE_API_BASE ?? ""
  const res = await fetch(`${base}/settings`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(updates),
  })
  if (!res.ok) throw new Error("Failed to update settings")
  return res.json()
}

export function useSettings() {
  const queryClient = useQueryClient()

  const query = useQuery<Settings>({
    queryKey: ["settings"],
    queryFn: fetchSettings,
  })

  const update = useMutation({
    mutationFn: updateSettings,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["settings"] })
    },
    onError: () => toast.error("Failed to update settings"),
  })

  return { ...query, update }
}
