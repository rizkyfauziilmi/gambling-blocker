import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { TooltipProvider } from "@/components/ui/tooltip"
import { SummaryCards } from "@/components/SummaryCards"
import { ReportsTable } from "@/components/ReportsTable"
import { BlacklistPanel } from "@/components/BlacklistPanel"
import { WhitelistPanel } from "@/components/WhitelistPanel"
import { useReports } from "@/hooks/useReports"
import { useBlacklist, useWhitelist } from "@/hooks/useLists"

export function App() {
  const { data, remove, removeByHostname } = useReports()
  const { add: addBlacklist } = useBlacklist()
  const { add: addWhitelist } = useWhitelist()

  const isMutating = remove.isPending || addBlacklist.isPending || addWhitelist.isPending

  return (
    <TooltipProvider>
      <div className="mx-auto max-w-5xl space-y-8 p-6">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Gambling Blocker Reports</h1>
          <p className="text-sm text-muted-foreground">
            False positive reports submitted by users
          </p>
        </div>

        <Tabs defaultValue="reports">
          <TabsList>
            <TabsTrigger value="reports">Reports</TabsTrigger>
            <TabsTrigger value="blacklist">Blacklist</TabsTrigger>
            <TabsTrigger value="whitelist">Whitelist</TabsTrigger>
          </TabsList>
          <TabsContent value="reports" className="space-y-8">
            <SummaryCards reports={data?.reports} />
            <ReportsTable
              reports={data?.reports}
              onWhitelist={(hostname) => addWhitelist.mutate(hostname)}
              onBlacklist={(hostname) => addBlacklist.mutate(hostname)}
              onDelete={(id) => remove.mutate(id)}
              onDeleteByHostname={(hostname) => removeByHostname.mutate(hostname)}
              isMutating={isMutating}
            />
          </TabsContent>
          <TabsContent value="blacklist">
            <BlacklistPanel />
          </TabsContent>
          <TabsContent value="whitelist">
            <WhitelistPanel />
          </TabsContent>
        </Tabs>
      </div>
    </TooltipProvider>
  )
}

export default App
