import { SummaryCards } from "@/components/SummaryCards"
import { ReportsTable } from "@/components/ReportsTable"
import { useReports } from "@/hooks/useReports"
import { useBlacklist, useWhitelist } from "@/hooks/useLists"

export function ReportsContent() {
  const { data, removeByHostname } = useReports()
  const { add: addBlacklist } = useBlacklist()
  const { add: addWhitelist } = useWhitelist()

  const isMutating =
    removeByHostname.isPending ||
    addBlacklist.isPending ||
    addWhitelist.isPending

  return (
    <>
      <SummaryCards stats={data?.stats} />
      <ReportsTable
        groups={data?.groups}
        onWhitelist={(hostname) => addWhitelist.mutate(hostname)}
        onBlacklist={(hostname) => addBlacklist.mutate(hostname)}
        onDeleteByHostname={(hostname) => removeByHostname.mutate(hostname)}
        isMutating={isMutating}
      />
    </>
  )
}
