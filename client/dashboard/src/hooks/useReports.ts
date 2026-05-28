import { useQuery } from "@tanstack/react-query"

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

export function useReports() {
  return useQuery<ReportsResponse>({
    queryKey: ["reports"],
    queryFn: fetchReports,
  })
}
