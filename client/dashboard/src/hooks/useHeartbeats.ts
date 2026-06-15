import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { toast } from "sonner"

export interface HeartbeatEntry {
  extension_id: string
  partner_email: string
  stale_alerted_at: string | null
  last_heartbeat_at: string | null
  total_heartbeats: number
  heartbeat_age_hours: number | null
}

interface HeartbeatsResponse {
  heartbeats: HeartbeatEntry[]
}

async function fetchHeartbeats(): Promise<HeartbeatEntry[]> {
  const base = import.meta.env.VITE_API_BASE ?? ""
  const res = await fetch(`${base}/extension/heartbeats`)
  if (!res.ok) throw new Error("Failed to fetch heartbeats")
  const data: HeartbeatsResponse = await res.json()
  return data.heartbeats
}

export function useHeartbeats() {
  return useQuery({
    queryKey: ["heartbeats"],
    queryFn: fetchHeartbeats,
    refetchInterval: 10000,
  })
}

async function deleteHeartbeats(extensionId: string): Promise<void> {
  const base = import.meta.env.VITE_API_BASE ?? ""
  const res = await fetch(`${base}/extension/heartbeat/${extensionId}`, {
    method: "DELETE",
  })
  if (!res.ok) throw new Error("Failed to delete heartbeats")
}

export function useDeleteHeartbeats() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: deleteHeartbeats,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["heartbeats"] })
      toast.success("Heartbeats deleted")
    },
    onError: () => toast.error("Failed to delete heartbeats"),
  })
}

async function triggerHeartbeat(extensionId: string): Promise<void> {
  const base = import.meta.env.VITE_API_BASE ?? ""
  const res = await fetch(`${base}/admin/trigger-heartbeat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ extension_id: extensionId }),
  })
  if (!res.ok) throw new Error("Failed to trigger heartbeat")
}

export function useTriggerHeartbeat() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: triggerHeartbeat,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["heartbeats"] })
      toast.success("Heartbeat triggered")
    },
    onError: () => toast.error("Failed to trigger heartbeat"),
  })
}
