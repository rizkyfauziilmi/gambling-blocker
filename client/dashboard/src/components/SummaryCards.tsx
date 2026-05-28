import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Skeleton } from "@/components/ui/skeleton"
import type { Report } from "@/hooks/useReports"

interface SummaryCardsProps {
  reports: Report[] | undefined
}

export function SummaryCards({ reports }: SummaryCardsProps) {
  if (!reports) {
    return (
      <div className="grid gap-4 sm:grid-cols-3">
        {Array.from({ length: 3 }).map((_, i) => (
          <Card key={i}>
            <CardHeader className="pb-2">
              <Skeleton className="h-4 w-24" />
            </CardHeader>
            <CardContent>
              <Skeleton className="h-8 w-16" />
            </CardContent>
          </Card>
        ))}
      </div>
    )
  }

  const today = reports.filter(
    (r) =>
      new Date(r.created_at + "Z").toDateString() === new Date().toDateString()
  ).length
  const uniqueHostnames = new Set(reports.map((r) => r.hostname)).size

  const cards = [
    { label: "Total Reports", value: reports.length },
    { label: "Reports Today", value: today },
    { label: "Unique Hostnames", value: uniqueHostnames },
  ]

  return (
    <div className="grid gap-4 sm:grid-cols-3">
      {cards.map((card) => (
        <Card key={card.label}>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              {card.label}
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-bold">{card.value}</p>
          </CardContent>
        </Card>
      ))}
    </div>
  )
}
