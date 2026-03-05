"use client"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { CardSkeleton, TableSkeleton } from "@/components/shared/card-skeleton"
import { usePortfolioSummary, usePositions } from "@/lib/hooks/use-portfolio"
import { MOCK_PORTFOLIO_SUMMARY, MOCK_POSITIONS } from "@/lib/mock-data/portfolio"
import { formatCurrency, formatPercent } from "@/lib/utils/format"
import { cn } from "@/lib/utils"
import { TrendingUp, TrendingDown, DollarSign, Briefcase } from "lucide-react"

export default function PortfolioPage() {
  const { data: summaryData, isLoading: isSummaryLoading } = usePortfolioSummary()
  const { data: positionsData, isLoading: isPositionsLoading } = usePositions()

  const summary = summaryData ?? MOCK_PORTFOLIO_SUMMARY
  const positions = positionsData ?? MOCK_POSITIONS

  const isLoading = isSummaryLoading || isPositionsLoading

  const SUMMARY_CARDS = [
    {
      label: "Portfolio Value",
      value: formatCurrency(summary.total_value),
      sub: `Cash: ${formatCurrency(summary.cash)}`,
      icon: DollarSign,
      positive: null,
    },
    {
      label: "Total P&L",
      value: formatCurrency(summary.total_pnl),
      sub: formatPercent(summary.total_pnl_percent) + " all-time",
      icon: TrendingUp,
      positive: true,
    },
    {
      label: "Daily P&L",
      value: formatCurrency(summary.daily_pnl),
      sub: formatPercent(summary.daily_pnl_percent) + " today",
      icon: TrendingUp,
      positive: true,
    },
    {
      label: "Positions",
      value: positions.length.toString(),
      sub: `Buying power: ${formatCurrency(summary.buying_power)}`,
      icon: Briefcase,
      positive: null,
    },
  ]

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Portfolio</h1>
        <p className="text-sm text-muted-foreground mt-1">Current positions and holdings.</p>
      </div>

      {/* Summary KPIs */}
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {isLoading
          ? Array.from({ length: 4 }, (_, i) => <CardSkeleton key={i} />)
          : SUMMARY_CARDS.map((card) => (
              <Card key={card.label}>
                <CardContent className="p-5">
                  <div className="flex items-start justify-between mb-3">
                    <p className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
                      {card.label}
                    </p>
                    <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-primary/10">
                      <card.icon className="h-4 w-4 text-primary" />
                    </div>
                  </div>
                  <p
                    className={cn(
                      "text-2xl font-bold",
                      card.positive === true && "text-positive",
                      card.positive === false && "text-negative",
                    )}
                  >
                    {card.value}
                  </p>
                  <p className="text-xs text-muted-foreground mt-1">{card.sub}</p>
                </CardContent>
              </Card>
            ))}
      </div>

      {/* Positions Table */}
      {isPositionsLoading ? (
        <TableSkeleton rows={8} cols={7} />
      ) : (
        <Card>
          <CardHeader className="pb-2 pt-5 px-5">
            <CardTitle className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
              Open Positions
            </CardTitle>
          </CardHeader>
          <CardContent className="px-5 pb-5">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border/60">
                    {["Symbol", "Qty", "Avg Cost", "Price", "Market Value", "Unrealized P&L", "Weight"].map((h) => (
                      <th
                        key={h}
                        className="pb-3 text-left text-[11px] font-semibold uppercase tracking-wider text-muted-foreground first:pl-0 last:pr-0 px-3"
                      >
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-border/40">
                  {positions.map((pos) => {
                    const isPositive = pos.unrealized_pnl >= 0
                    return (
                      <tr key={pos.symbol} className="hover:bg-muted/40 transition-colors">
                        <td className="py-3.5 pl-0 pr-3">
                          <div className="flex items-center gap-2.5">
                            <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-muted text-[11px] font-bold text-muted-foreground">
                              {pos.symbol.slice(0, 2)}
                            </div>
                            <div>
                              <p className="text-[13px] font-semibold leading-none">{pos.symbol}</p>
                              <p className="text-[10px] text-muted-foreground mt-0.5 leading-none truncate max-w-[100px]">
                                {pos.name}
                              </p>
                            </div>
                          </div>
                        </td>
                        <td className="py-3.5 px-3 text-[13px]">{pos.quantity}</td>
                        <td className="py-3.5 px-3 text-[13px] text-muted-foreground">{formatCurrency(pos.avg_cost)}</td>
                        <td className="py-3.5 px-3 text-[13px] font-medium">{formatCurrency(pos.current_price)}</td>
                        <td className="py-3.5 px-3 text-[13px] font-semibold">{formatCurrency(pos.market_value)}</td>
                        <td className="py-3.5 px-3">
                          <div className="flex items-center gap-1">
                            {isPositive ? (
                              <TrendingUp className="h-3 w-3 text-positive" />
                            ) : (
                              <TrendingDown className="h-3 w-3 text-negative" />
                            )}
                            <span className={cn("text-[13px] font-semibold", isPositive ? "text-positive" : "text-negative")}>
                              {formatCurrency(pos.unrealized_pnl)}
                            </span>
                            <span className={cn("text-[11px] ml-1", isPositive ? "text-positive/70" : "text-negative/70")}>
                              ({formatPercent(pos.unrealized_pnl_percent)})
                            </span>
                          </div>
                        </td>
                        <td className="py-3.5 pl-3 pr-0">
                          <div className="flex items-center gap-2">
                            <div className="h-1.5 w-16 rounded-full bg-muted overflow-hidden">
                              <div
                                className="h-full rounded-full bg-primary/60"
                                style={{ width: `${Math.min(pos.weight, 100)}%` }}
                              />
                            </div>
                            <span className="text-[12px] text-muted-foreground">{pos.weight.toFixed(1)}%</span>
                          </div>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
