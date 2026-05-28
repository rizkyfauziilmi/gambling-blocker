import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"

export interface Report {
  id: number
  url: string
  hostname: string
  gambling_score: number
  reporter_ip: string
  created_at: string
}

interface Stats {
  total: number
  today: number
  unique_hostnames: number
}

interface ReportsResponse {
  reports: Report[]
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
  const res = await fetch(`${base}/reports/by-hostname/${encodeURIComponent(hostname)}`, { method: "DELETE" })
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
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["reports"] }),
  })

  const removeByHostname = useMutation({
    mutationFn: deleteByHostname,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["reports"] }),
  })

  return { ...query, remove, removeByHostname }
}
