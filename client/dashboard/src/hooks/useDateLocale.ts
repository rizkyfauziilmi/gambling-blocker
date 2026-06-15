import { enUS, id } from "date-fns/locale"
import { useI18n } from "@/i18n/context"

export function useDateLocale() {
  const { locale } = useI18n()
  return locale === "id" ? id : enUS
}
