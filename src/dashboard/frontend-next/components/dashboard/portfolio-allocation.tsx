"use client"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { DonutChart } from "@/components/charts/donut-chart"
import { useAllocation, usePortfolioSummary } from "@/lib/hooks/use-portfolio"
import { MOCK_ALLOCATION } from "@/lib/mock-data/portfolio"
import { formatCompactNumber, formatCurrency } from "@/lib/utils/format"

export function PortfolioAllocation() {
  const { data: allocation } = useAllocation()
  const { data: summary } = usePortfolioSummary()

  const items = allocation ?? MOCK_ALLOCATION
  const totalValue = summary?.total_value ?? 284520.75

  const chartData = items.map((a) => ({
    name: a.category,
    value: a.percentage,
    color: a.color,
  }))

  return (
    <Card>
      <CardHeader className="pb-1 pt-5 px-5">
        <div className="flex items-center justify-between">
          <CardTitle className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
            Allocation
          </CardTitle>
          <span className="text-xs font-medium text-muted-foreground">
            {formatCurrency(totalValue)} total
          </span>
        </div>
      </CardHeader>
      <CardContent className="px-5 pb-5">
        {/* Donut chart */}
        <DonutChart
          data={chartData}
          centerLabel="Total"
          centerValue={formatCompactNumber(totalValue)}
          height={200}
        />

        {/* Segmented bar — like the reference's Sales Overview bar */}
        <div className="flex h-2 w-full overflow-hidden rounded-full mt-3 mb-4">
          {items.map((a) => (
            <div
              key={a.category}
              style={{ width: `${a.percentage}%`, backgroundColor: a.color }}
              className="transition-all first:rounded-l-full last:rounded-r-full"
            />
          ))}
        </div>

        {/* Legend rows — matches reference's product/percentage/value list */}
        <div className="space-y-2.5">
          {items.map((a) => (
            <div
              key={a.category}
              className="flex items-center justify-between"
            >
              <div className="flex items-center gap-2.5">
                <div
                  className="h-2.5 w-2.5 rounded-full shrink-0"
                  style={{ backgroundColor: a.color }}
                />
                <span className="text-[13px] text-muted-foreground">{a.category}</span>
              </div>
              <div className="flex items-center gap-3">
                <div className="w-16 h-1.5 rounded-full bg-muted overflow-hidden">
                  <div
                    className="h-full rounded-full"
                    style={{
                      width: `${a.percentage}%`,
                      backgroundColor: a.color,
                    }}
                  />
                </div>
                <span className="text-[13px] font-semibold w-9 text-right">
                  {a.percentage}%
                </span>
              </div>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  )
}
