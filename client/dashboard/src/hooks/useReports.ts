import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { toast } from "sonner"

export interface GroupedReport {
  hostname: string
  report_count: number
  avg_score: number
  last_reported: string
}

interface Stats {
  total: number
  today: number
  unique_hostnames: number
}

interface ReportsResponse {
  groups: GroupedReport[]
  stats: Stats
}

async function fetchReports(): Promise<ReportsResponse> {
  const base = import.meta.env.VITE_API_BASE ?? ""
  const res = await fetch(`${base}/reports`)
  if (!res.ok) throw new Error("Failed to fetch reports")
  return res.json()
}

async function deleteReport(id: number): Promise<void> {
  const base = import.meta.env.VITE_API_BASE ?? ""
  const res = await fetch(`${base}/reports/${id}`, { method: "DELETE" })
  if (!res.ok) throw new Error("Failed to delete report")
}

async function deleteByHostname(hostname: string): Promise<void> {
  const base = import.meta.env.VITE_API_BASE ?? ""
  const res = await fetch(
    `${base}/reports/by-hostname/${encodeURIComponent(hostname)}`,
    { method: "DELETE" }
  )
  if (!res.ok) throw new Error("Failed to delete reports")
}

export function useReports() {
  const queryClient = useQueryClient()

  const query = useQuery<ReportsResponse>({
    queryKey: ["reports"],
    queryFn: fetchReports,
  })

  const remove = useMutation({
    mutationFn: deleteReport,
    onSuccess: () => {
      toast.success("Report deleted")
      queryClient.invalidateQueries({ queryKey: ["reports"] })
    },
    onError: () => toast.error("Failed to delete report"),
  })

  const removeByHostname = useMutation({
    mutationFn: deleteByHostname,
    onSuccess: () => {
      toast.success("Reports deleted")
      queryClient.invalidateQueries({ queryKey: ["reports"] })
    },
    onError: () => toast.error("Failed to delete reports"),
  })

  return { ...query, remove, removeByHostname }
}
