import { Button } from "@/components/ui/button"
import { useNextStaleCheck, useTriggerStaleCheck } from "@/hooks/useAdmin"
import { useI18n } from "@/i18n/context"
import { Loader2, type LucideIcon, ShieldAlert } from "lucide-react"
import { useEffect, useState } from "react"

interface Props {
  icon?: LucideIcon
  showLoadingText?: boolean
  variant?:
    | "default"
    | "destructive"
    | "outline"
    | "secondary"
    | "ghost"
    | "link"
  size?: "default" | "sm" | "lg" | "icon"
  className?: string
}

function formatCountdown(ms: number): string {
  const total = Math.max(0, Math.floor(ms / 1000))
  const m = Math.floor(total / 60)
  const s = total % 60
  if (m === 0) return `${s}s`
  return `${m}m ${s}s`
}

export function TriggerStaleCheckButton({
  icon: Icon = ShieldAlert,
  showLoadingText = false,
  variant = "outline",
  size = "sm",
  className,
}: Props) {
  const staleCheck = useTriggerStaleCheck()
  const { data: nextCheck } = useNextStaleCheck()
  const { t } = useI18n()
  const [countdown, setCountdown] = useState("")

  useEffect(() => {
    if (!nextCheck?.next_run) {
      setCountdown("")
      return
    }
    const id = setInterval(() => {
      const diff = new Date(nextCheck.next_run).getTime() - Date.now()
      setCountdown(diff > 0 ? formatCountdown(diff) : "")
    }, 1000)
    return () => clearInterval(id)
  }, [nextCheck?.next_run])

  return (
    <div className="flex items-center gap-3">
      <Button
        onClick={() => staleCheck.mutate()}
        disabled={staleCheck.isPending}
        variant={variant}
        size={size}
        className={className}
      >
        {staleCheck.isPending ? (
          <Loader2 className="mr-1 size-4 animate-spin" />
        ) : (
          <Icon className="mr-1 size-4" />
        )}
        {staleCheck.isPending && showLoadingText
          ? t("checking")
          : t("trigger_stale_check")}
      </Button>
      {countdown && (
        <span className="text-xs text-muted-foreground">
          {t("next_check_in", { time: countdown })}
        </span>
      )}
    </div>
  )
}
