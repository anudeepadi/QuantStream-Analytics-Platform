"use client"

import { Card, CardContent, CardHeader } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { AreaChart } from "@/components/charts/area-chart"
import { TimeFilter } from "@/components/shared/time-filter"
import { MOCK_OVERVIEW_CHART } from "@/lib/mock-data/dashboard"
import { usePortfolioSummary } from "@/lib/hooks/use-portfolio"
import { formatCurrency } from "@/lib/utils/format"

export function OverviewChart() {
  const { data: summary } = usePortfolioSummary()
  const totalValue = summary?.total_value ?? 284520.75
  const dailyPnl = summary?.daily_pnl_percent ?? 8.4
  const isPositive = dailyPnl >= 0

  return (
    <Card>
      <CardHeader className="pb-0 pt-5 px-5">
        <div className="flex items-start justify-between gap-4">
          {/* Headline numbers — mirrors the reference's big profit number */}
          <div>
            <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-1.5">
              Total Portfolio Value
            </p>
            <div className="flex items-baseline gap-2.5">
              <span className="text-3xl font-bold tracking-tight">
                {formatCurrency(totalValue)}
              </span>
              <Badge
                className={`text-xs font-semibold px-2 py-0.5 rounded-full border-0 ${
                  isPositive
                    ? "bg-positive/10 text-positive"
                    : "bg-negative/10 text-negative"
                }`}
              >
                {isPositive ? "+" : ""}{dailyPnl.toFixed(1)}%
              </Badge>
            </div>
            <div className="flex items-center gap-4 mt-2">
              <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
                <span className="h-2.5 w-2.5 rounded-full bg-[oklch(0.54_0.185_142)] inline-block" />
                Portfolio
              </span>
              <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
                <span className="h-2.5 w-2.5 rounded-full bg-[oklch(0.58_0.150_250)] inline-block" />
                S&amp;P 500
              </span>
            </div>
          </div>
          <div className="shrink-0 pt-1">
            <TimeFilter />
          </div>
        </div>
      </CardHeader>
      <CardContent className="px-2 pb-2 pt-4">
        <AreaChart
          data={MOCK_OVERVIEW_CHART}
          dataKeys={[
            { key: "portfolio", color: "oklch(0.54 0.185 142)", name: "Portfolio" },
            { key: "benchmark", color: "oklch(0.58 0.150 250)", name: "S&P 500" },
          ]}
          height={280}
        />
      </CardContent>
    </Card>
  )
}
