"use client"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { CardSkeleton, TableSkeleton } from "@/components/shared/card-skeleton"
import { useAllocation, usePortfolioSummary } from "@/lib/hooks/use-portfolio"
import { MOCK_ALLOCATION, MOCK_PORTFOLIO_SUMMARY } from "@/lib/mock-data/portfolio"
import { formatCurrency } from "@/lib/utils/format"

const SECTOR_COLORS = [
  "oklch(0.54 0.185 142)",
  "oklch(0.65 0.12 250)",
  "oklch(0.72 0.18 60)",
  "oklch(0.62 0.22 25)",
  "oklch(0.70 0.10 300)",
]

export default function AllocationPage() {
  const { data: allocationData, isLoading: isAllocationLoading } = useAllocation()
  const { data: summaryData, isLoading: isSummaryLoading } = usePortfolioSummary()

  const allocation = allocationData ?? MOCK_ALLOCATION
  const summary = summaryData ?? MOCK_PORTFOLIO_SUMMARY

  const isLoading = isAllocationLoading || isSummaryLoading
  const total = summary.total_value

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Allocation</h1>
        <p className="text-sm text-muted-foreground mt-1">Portfolio breakdown by asset class and sector.</p>
      </div>

      <div className="grid gap-5 lg:grid-cols-2">
        {/* Segmented bar + summary */}
        {isLoading ? (
          <>
            <CardSkeleton lines={6} />
            <CardSkeleton lines={6} />
          </>
        ) : (
          <>
            <Card>
              <CardHeader className="pb-2 pt-5 px-5">
                <CardTitle className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
                  Allocation Overview
                </CardTitle>
              </CardHeader>
              <CardContent className="px-5 pb-5">
                {/* Segmented bar */}
                <div className="h-3 flex rounded-full overflow-hidden mb-6 mt-2">
                  {allocation.map((a, i) => (
                    <div
                      key={a.category}
                      style={{
                        width: `${a.percentage}%`,
                        backgroundColor: SECTOR_COLORS[i % SECTOR_COLORS.length],
                      }}
                    />
                  ))}
                </div>

                {/* Legend rows */}
                <div className="space-y-3">
                  {allocation.map((a, i) => (
                    <div key={a.category} className="flex items-center justify-between">
                      <div className="flex items-center gap-2.5">
                        <span
                          className="h-2.5 w-2.5 rounded-full flex-shrink-0"
                          style={{ backgroundColor: SECTOR_COLORS[i % SECTOR_COLORS.length] }}
                        />
                        <span className="text-[13px] font-medium">{a.category}</span>
                      </div>
                      <div className="flex items-center gap-4">
                        <div className="w-24 h-1.5 rounded-full bg-muted overflow-hidden">
                          <div
                            className="h-full rounded-full"
                            style={{
                              width: `${a.percentage}%`,
                              backgroundColor: SECTOR_COLORS[i % SECTOR_COLORS.length],
                            }}
                          />
                        </div>
                        <span className="text-[13px] font-semibold w-10 text-right">{a.percentage.toFixed(1)}%</span>
                      </div>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>

            {/* Value breakdown */}
            <Card>
              <CardHeader className="pb-2 pt-5 px-5">
                <CardTitle className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
                  Value Breakdown
                </CardTitle>
              </CardHeader>
              <CardContent className="px-5 pb-5">
                <div className="space-y-4 mt-2">
                  {allocation.map((a, i) => (
                    <div key={a.category}>
                      <div className="flex items-center justify-between mb-1.5">
                        <div className="flex items-center gap-2">
                          <span
                            className="h-2 w-2 rounded-full"
                            style={{ backgroundColor: SECTOR_COLORS[i % SECTOR_COLORS.length] }}
                          />
                          <span className="text-[13px] font-medium">{a.category}</span>
                        </div>
                        <span className="text-[13px] font-semibold">{formatCurrency(a.value)}</span>
                      </div>
                      <div className="h-1.5 rounded-full bg-muted overflow-hidden">
                        <div
                          className="h-full rounded-full transition-all"
                          style={{
                            width: `${a.percentage}%`,
                            backgroundColor: SECTOR_COLORS[i % SECTOR_COLORS.length],
                            opacity: 0.75,
                          }}
                        />
                      </div>
                    </div>
                  ))}
                </div>

                <div className="mt-6 pt-4 border-t border-border/60 flex items-center justify-between">
                  <span className="text-[13px] font-semibold text-muted-foreground">Total Value</span>
                  <span className="text-base font-bold">{formatCurrency(total)}</span>
                </div>
              </CardContent>
            </Card>
          </>
        )}
      </div>

      {/* Allocation table */}
      {isAllocationLoading ? (
        <TableSkeleton rows={5} cols={4} />
      ) : (
        <Card>
          <CardHeader className="pb-2 pt-5 px-5">
            <CardTitle className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
              Detailed Breakdown
            </CardTitle>
          </CardHeader>
          <CardContent className="px-5 pb-5">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border/60">
                  {["Category", "Value", "Weight", "vs Target"].map((h) => (
                    <th key={h} className="pb-3 text-left text-[11px] font-semibold uppercase tracking-wider text-muted-foreground first:pl-0 px-3">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-border/40">
                {allocation.map((a, i) => {
                  const target = 25
                  const diff = a.percentage - target
                  return (
                    <tr key={a.category} className="hover:bg-muted/40 transition-colors">
                      <td className="py-3 pl-0 pr-3">
                        <div className="flex items-center gap-2.5">
                          <span
                            className="h-2.5 w-2.5 rounded-full flex-shrink-0"
                            style={{ backgroundColor: SECTOR_COLORS[i % SECTOR_COLORS.length] }}
                          />
                          <span className="text-[13px] font-medium">{a.category}</span>
                        </div>
                      </td>
                      <td className="py-3 px-3 text-[13px] font-semibold">{formatCurrency(a.value)}</td>
                      <td className="py-3 px-3 text-[13px]">{a.percentage.toFixed(1)}%</td>
                      <td className="py-3 pl-3 pr-0">
                        <span className={`text-[12px] font-medium px-2 py-0.5 rounded-full ${
                          Math.abs(diff) < 5
                            ? "bg-positive/10 text-positive"
                            : diff > 0
                            ? "bg-yellow-500/10 text-yellow-600"
                            : "bg-muted text-muted-foreground"
                        }`}>
                          {diff > 0 ? "+" : ""}{diff.toFixed(1)}%
                        </span>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
