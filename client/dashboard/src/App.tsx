import { Toaster } from "@/components/ui/sonner"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { TooltipProvider } from "@/components/ui/tooltip"
import { CachePanel } from "@/components/CachePanel"
import { BlacklistPanel } from "@/components/BlacklistPanel"
import { WhitelistPanel } from "@/components/WhitelistPanel"
import { ReportsContent } from "./components/ReportsContent"
import { SettingsPanel } from "@/components/SettingsPanel"
import { LogsPanel } from "@/components/LogsPanel"
import { useState } from "react"

export function App() {
  type TabKey = "reports" | "blacklist" | "whitelist" | "cache" | "settings" | "logs"

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
    settings: {
      title: "Settings",
      description: "Application configuration and feature flags",
    },
    logs: {
      title: "Logs",
      description: "Backend API logs (last 1000 entries)",
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
            <TabsTrigger value="settings">Settings</TabsTrigger>
            <TabsTrigger value="logs">Logs</TabsTrigger>
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
          <TabsContent value="settings">
            <SettingsPanel />
          </TabsContent>
          <TabsContent value="logs">
            <LogsPanel />
          </TabsContent>
        </Tabs>
      </div>
      <Toaster />
    </TooltipProvider>
  )
}

export default App
