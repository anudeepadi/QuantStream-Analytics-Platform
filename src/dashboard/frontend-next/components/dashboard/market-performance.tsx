"use client"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { BarChart } from "@/components/charts/bar-chart"
import { useSectorPerformance } from "@/lib/hooks/use-market-data"
import { MOCK_SECTOR_PERFORMANCE } from "@/lib/mock-data/market"

export function MarketPerformance() {
  const { data: sectors } = useSectorPerformance()
  const raw = sectors ?? MOCK_SECTOR_PERFORMANCE

  const chartData = raw.map((s) => ({
    sector: s.sector.replace(" Services", "").replace("Consumer ", ""),
    change: s.change_percent,
  }))

  const positive = raw.filter((s) => s.change_percent >= 0).length
  const negative = raw.filter((s) => s.change_percent < 0).length

  return (
    <Card>
      <CardHeader className="pb-2 pt-5 px-5">
        <div className="flex items-start justify-between">
          <div>
            <CardTitle className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
              Sector Performance
            </CardTitle>
            <div className="flex items-center gap-3 mt-1.5">
              <span className="flex items-center gap-1.5 text-[11px] font-medium text-positive">
                <span className="h-1.5 w-1.5 rounded-full bg-positive inline-block" />
                {positive} advancing
              </span>
              <span className="flex items-center gap-1.5 text-[11px] font-medium text-negative">
                <span className="h-1.5 w-1.5 rounded-full bg-negative inline-block" />
                {negative} declining
              </span>
            </div>
          </div>
        </div>
      </CardHeader>
      <CardContent className="px-2 pb-3">
        <BarChart
          data={chartData}
          dataKey="change"
          xAxisKey="sector"
          height={248}
          colorByValue
        />
      </CardContent>
    </Card>
  )
}
