import { Toaster } from "@/components/ui/sonner"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { TooltipProvider } from "@/components/ui/tooltip"
import { CachePanel } from "@/components/CachePanel"
import { BlacklistPanel } from "@/components/BlacklistPanel"
import { WhitelistPanel } from "@/components/WhitelistPanel"
import { ReportsContent } from "./components/ReportsContent"
import { useState } from "react"

export function App() {
  type TabKey = "reports" | "blacklist" | "whitelist" | "cache"

  const [activeTabs, setActiveTabs] = useState<TabKey>("reports")

  const tabMeta: Record<
    TabKey,
    {
      title: string
      description: string
    }
  > = {
    reports: {
      title: "Reports Management",
      description: "False detection reports submitted by users",
    },
    blacklist: {
      title: "Blacklist Management",
      description: "Manage blocked domains and URLs",
    },
    whitelist: {
      title: "Whitelist Management",
      description: "Manage allowed domains and URLs",
    },
    cache: {
      title: "Cache Management",
      description: "View and clear application cache",
    },
  }

  return (
    <TooltipProvider>
      <div className="mx-auto max-w-5xl space-y-8 p-6">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">
            {tabMeta[activeTabs].title}
          </h1>
          <p className="text-sm text-muted-foreground">
            {tabMeta[activeTabs].description}
          </p>
        </div>

        <Tabs
          value={activeTabs}
          onValueChange={(value) => setActiveTabs(value as TabKey)}
        >
          <TabsList>
            <TabsTrigger value="reports">Reports</TabsTrigger>
            <TabsTrigger value="blacklist">Blacklist</TabsTrigger>
            <TabsTrigger value="whitelist">Whitelist</TabsTrigger>
            <TabsTrigger value="cache">Cache</TabsTrigger>
          </TabsList>
          <TabsContent value="reports" className="space-y-8">
            <ReportsContent />
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
