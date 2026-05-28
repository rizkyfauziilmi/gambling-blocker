import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { SummaryCards } from "@/components/SummaryCards"
import { ReportsTable } from "@/components/ReportsTable"
import { BlacklistPanel } from "@/components/BlacklistPanel"
import { WhitelistPanel } from "@/components/WhitelistPanel"
import { useReports } from "@/hooks/useReports"

export function App() {
  const { data } = useReports()

  return (
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
          <ReportsTable reports={data?.reports} />
        </TabsContent>
        <TabsContent value="blacklist">
          <BlacklistPanel />
        </TabsContent>
        <TabsContent value="whitelist">
          <WhitelistPanel />
        </TabsContent>
      </Tabs>
    </div>
  )
}

export default App
