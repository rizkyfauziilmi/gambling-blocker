import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { toast } from "sonner"

export interface LogEntry {
  time: string
  tag: string
  message: string
}

interface LogsResponse {
  entries: LogEntry[]
}

async function fetchLogs(tag?: string): Promise<LogsResponse> {
  const base = import.meta.env.VITE_API_BASE ?? ""
  const params = tag ? `?tag=${encodeURIComponent(tag)}` : ""
  const res = await fetch(`${base}/logs${params}`)
  if (!res.ok) throw new Error("Failed to fetch logs")
  return res.json()
}

async function clearLogs(): Promise<void> {
  const base = import.meta.env.VITE_API_BASE ?? ""
  const res = await fetch(`${base}/logs`, { method: "DELETE" })
  if (!res.ok) throw new Error("Failed to clear logs")
}

export function useLogs(tag?: string) {
  const queryClient = useQueryClient()

  const query = useQuery<LogsResponse>({
    queryKey: ["logs", tag],
    queryFn: () => fetchLogs(tag),
    refetchInterval: 5000,
  })

  const clear = useMutation({
    mutationFn: clearLogs,
    onSuccess: () => {
      toast.success("Logs cleared")
      queryClient.invalidateQueries({ queryKey: ["logs"] })
    },
    onError: () => toast.error("Failed to clear logs"),
  })

  return { ...query, clear }
}
