import { Toaster } from "@/components/ui/sonner"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { TooltipProvider } from "@/components/ui/tooltip"
import { BlacklistPanel } from "@/components/BlacklistPanel"
import { CachePanel } from "@/components/CachePanel"
import { HeartbeatsPanel } from "@/components/HeartbeatsPanel"
import { LogsPanel } from "@/components/LogsPanel"
import { ReportsContent } from "./components/ReportsContent"
import { SettingsPanel } from "@/components/SettingsPanel"
import { WhitelistPanel } from "@/components/WhitelistPanel"
import { useState } from "react"
import { useI18n } from "@/i18n/context"

export function App() {
  type TabKey =
    | "reports"
    | "blacklist"
    | "whitelist"
    | "heartbeats"
    | "cache"
    | "settings"
    | "logs"

  const [activeTabs, setActiveTabs] = useState<TabKey>("reports")
  const { t } = useI18n()

  const tabMeta: Record<
    TabKey,
    {
      title: string
      description: string
    }
  > = {
    reports: {
      title: t("tab_reports_title"),
      description: t("tab_reports_desc"),
    },
    blacklist: {
      title: t("tab_blacklist_title"),
      description: t("tab_blacklist_desc"),
    },
    whitelist: {
      title: t("tab_whitelist_title"),
      description: t("tab_whitelist_desc"),
    },
    heartbeats: {
      title: t("tab_heartbeats_title"),
      description: t("tab_heartbeats_desc"),
    },
    cache: {
      title: t("tab_cache_title"),
      description: t("tab_cache_desc"),
    },
    settings: {
      title: t("tab_settings_title"),
      description: t("tab_settings_desc"),
    },
    logs: {
      title: t("tab_logs_title"),
      description: t("tab_logs_desc"),
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
            <TabsTrigger value="reports">{t("tab_reports")}</TabsTrigger>
            <TabsTrigger value="blacklist">{t("tab_blacklist")}</TabsTrigger>
            <TabsTrigger value="whitelist">{t("tab_whitelist")}</TabsTrigger>
            <TabsTrigger value="heartbeats">{t("tab_heartbeats")}</TabsTrigger>
            <TabsTrigger value="cache">{t("tab_cache")}</TabsTrigger>
            <TabsTrigger value="settings">{t("tab_settings")}</TabsTrigger>
            <TabsTrigger value="logs">{t("tab_logs")}</TabsTrigger>
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
          <TabsContent value="heartbeats">
            <HeartbeatsPanel />
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
