import { Toaster } from "@/components/ui/sonner"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { TooltipProvider } from "@/components/ui/tooltip"
import { CachePanel } from "@/components/CachePanel"
import { SummaryCards } from "@/components/SummaryCards"
import { ReportsTable } from "@/components/ReportsTable"
import { BlacklistPanel } from "@/components/BlacklistPanel"
import { WhitelistPanel } from "@/components/WhitelistPanel"
import { useReports } from "@/hooks/useReports"
import { useBlacklist, useWhitelist } from "@/hooks/useLists"

export function App() {
  const { data, removeByHostname } = useReports()
  const { add: addBlacklist } = useBlacklist()
  const { add: addWhitelist } = useWhitelist()

  const isMutating = removeByHostname.isPending || addBlacklist.isPending || addWhitelist.isPending

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
          <TabsTrigger value="cache">Cache</TabsTrigger>
          </TabsList>
          <TabsContent value="reports" className="space-y-8">
            <SummaryCards stats={data?.stats} />
            <ReportsTable
              groups={data?.groups}
              onWhitelist={(hostname) => addWhitelist.mutate(hostname)}
              onBlacklist={(hostname) => addBlacklist.mutate(hostname)}
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
        <TabsContent value="cache">
          <CachePanel />
        </TabsContent>
      </Tabs>
    </div>
    <Toaster />
  </TooltipProvider>
  )
}

export default App
