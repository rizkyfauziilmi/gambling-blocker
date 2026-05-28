import { SummaryCards } from "@/components/SummaryCards"
import { ReportsTable } from "@/components/ReportsTable"
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

      <SummaryCards reports={data?.reports} />
      <ReportsTable reports={data?.reports} />
    </div>
  )
}

export default App
