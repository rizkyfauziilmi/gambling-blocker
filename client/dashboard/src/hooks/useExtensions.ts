import { useQuery } from "@tanstack/react-query"

export interface ExtensionStatus {
  exists: boolean
  partner_email?: string
  heartbeat_age_hours?: number | null
  tamper_count_1h?: number
}

async function fetchExtensionStatus(id: string): Promise<ExtensionStatus> {
  const base = import.meta.env.VITE_API_BASE ?? ""
  const res = await fetch(
    `${base}/extension/status?extension_id=${encodeURIComponent(id)}`
  )
  if (!res.ok) throw new Error("Failed to fetch extension status")
  return res.json()
}

export function useExtensionStatus(extensionId: string | null) {
  return useQuery<ExtensionStatus>({
    queryKey: ["extension-status", extensionId],
    queryFn: () => fetchExtensionStatus(extensionId!),
    enabled: !!extensionId,
    refetchInterval: 30000,
  })
}
