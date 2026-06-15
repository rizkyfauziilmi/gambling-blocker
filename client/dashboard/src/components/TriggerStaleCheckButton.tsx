import { Button } from "@/components/ui/button"
import { useTriggerStaleCheck } from "@/hooks/useAdmin"
import { useI18n } from "@/i18n/context"
import { Loader2, type LucideIcon, ShieldAlert } from "lucide-react"

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

export function TriggerStaleCheckButton({
  icon: Icon = ShieldAlert,
  showLoadingText = false,
  variant = "outline",
  size = "sm",
  className,
}: Props) {
  const staleCheck = useTriggerStaleCheck()
  const { t } = useI18n()

  return (
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
  )
}
