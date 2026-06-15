import { useMutation } from "@tanstack/react-query"
import { toast } from "sonner"

interface TriggerResult {
  success: boolean
  stale_count: number
  alerts_sent: number
}

async function triggerStaleCheck(): Promise<TriggerResult> {
  const base = import.meta.env.VITE_API_BASE ?? ""
  const res = await fetch(`${base}/admin/trigger-stale-check`, {
    method: "POST",
  })
  if (!res.ok) throw new Error("Failed to trigger stale check")
  return res.json()
}

export function useTriggerStaleCheck() {
  return useMutation({
    mutationFn: triggerStaleCheck,
    onSuccess: (data) => {
      if (data.alerts_sent > 0) {
        toast.success(`${data.alerts_sent} alert(s) sent to partner(s)`)
      } else {
        toast.info("No stale extensions found")
      }
    },
    onError: () => toast.error("Failed to trigger stale check"),
  })
}
