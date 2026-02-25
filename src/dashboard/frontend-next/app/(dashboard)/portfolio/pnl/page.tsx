"use client"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { MOCK_PERFORMANCE, MOCK_PORTFOLIO_SUMMARY } from "@/lib/mock-data/portfolio"
import { formatCurrency, formatPercent } from "@/lib/utils/format"
import { cn } from "@/lib/utils"

// Horizontal bar chart for return comparison
function ReturnBar({ portfolio, benchmark, period }: { portfolio: number; benchmark: number; period: string }) {
  const max = Math.max(Math.abs(portfolio), Math.abs(benchmark), 1)
  return (
    <div className="flex items-center gap-3">
      <span className="w-6 text-[11px] font-semibold text-muted-foreground text-right">{period}</span>
      <div className="flex-1 space-y-1">
        <div className="flex items-center gap-2">
          <div className="w-20 text-right">
            <span className={cn("text-[11px] font-semibold", portfolio >= 0 ? "text-positive" : "text-negative")}>
              {formatPercent(portfolio)}
            </span>
          </div>
          <div className="flex-1 h-2 rounded-full bg-muted overflow-hidden">
            <div
              className="h-full rounded-full bg-primary"
              style={{ width: `${(Math.abs(portfolio) / max) * 100}%` }}
            />
          </div>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-20 text-right">
            <span className="text-[11px] font-medium text-muted-foreground">{formatPercent(benchmark)}</span>
          </div>
          <div className="flex-1 h-2 rounded-full bg-muted overflow-hidden">
            <div
              className="h-full rounded-full bg-blue-400/50"
              style={{ width: `${(Math.abs(benchmark) / max) * 100}%` }}
            />
          </div>
        </div>
      </div>
    </div>
  )
}

export default function PnlPage() {
  const totalPnl = MOCK_PORTFOLIO_SUMMARY.total_pnl
  const dailyPnl = MOCK_PORTFOLIO_SUMMARY.daily_pnl

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Profit &amp; Loss</h1>
        <p className="text-sm text-muted-foreground mt-1">Track performance across all time periods.</p>
      </div>

      {/* Top KPIs */}
      <div className="grid gap-4 sm:grid-cols-3">
        <Card>
          <CardContent className="p-5">
            <p className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground mb-3">Total P&L</p>
            <p className={cn("text-3xl font-bold", totalPnl >= 0 ? "text-positive" : "text-negative")}>
              {formatCurrency(totalPnl)}
            </p>
            <p className="text-xs text-muted-foreground mt-1">
              {formatPercent(MOCK_PORTFOLIO_SUMMARY.total_pnl_percent)} on cost basis
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-5">
            <p className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground mb-3">Daily P&L</p>
            <p className={cn("text-3xl font-bold", dailyPnl >= 0 ? "text-positive" : "text-negative")}>
              {formatCurrency(dailyPnl)}
            </p>
            <p className="text-xs text-muted-foreground mt-1">
              {formatPercent(MOCK_PORTFOLIO_SUMMARY.daily_pnl_percent)} today
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-5">
            <p className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground mb-3">Cost Basis</p>
            <p className="text-3xl font-bold">{formatCurrency(MOCK_PORTFOLIO_SUMMARY.total_cost)}</p>
            <p className="text-xs text-muted-foreground mt-1">Total invested capital</p>
          </CardContent>
        </Card>
      </div>

      {/* Return comparison chart */}
      <Card>
        <CardHeader className="pb-2 pt-5 px-5">
          <div className="flex items-center justify-between">
            <CardTitle className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
              Return Comparison
            </CardTitle>
            <div className="flex items-center gap-4 text-[11px]">
              <span className="flex items-center gap-1.5 font-medium">
                <span className="h-1.5 w-1.5 rounded-full bg-primary inline-block" />
                Portfolio
              </span>
              <span className="flex items-center gap-1.5 font-medium text-muted-foreground">
                <span className="h-1.5 w-1.5 rounded-full bg-blue-400/60 inline-block" />
                Benchmark
              </span>
            </div>
          </div>
        </CardHeader>
        <CardContent className="px-5 pb-5">
          <div className="space-y-4 mt-2">
            {MOCK_PERFORMANCE.map((p) => (
              <ReturnBar
                key={p.period}
                period={p.period}
                portfolio={p.return_percent}
                benchmark={p.benchmark_return_percent}
              />
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Detailed metrics table */}
      <Card>
        <CardHeader className="pb-2 pt-5 px-5">
          <CardTitle className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
            Risk Metrics
          </CardTitle>
        </CardHeader>
        <CardContent className="px-5 pb-5">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border/60">
                  {["Period", "Alpha", "Sharpe Ratio", "Volatility", "Max Drawdown"].map((h) => (
                    <th key={h} className="pb-3 text-left text-[11px] font-semibold uppercase tracking-wider text-muted-foreground first:pl-0 last:pr-0 px-3">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-border/40">
                {MOCK_PERFORMANCE.map((p) => (
                  <tr key={p.period} className="hover:bg-muted/40 transition-colors">
                    <td className="py-3 pl-0 pr-3 font-semibold text-[13px]">{p.period}</td>
                    <td className={cn("py-3 px-3 text-[13px] font-medium", p.alpha >= 0 ? "text-positive" : "text-negative")}>
                      {p.alpha >= 0 ? "+" : ""}{p.alpha.toFixed(2)}%
                    </td>
                    <td className="py-3 px-3 text-[13px]">{p.sharpe_ratio.toFixed(2)}</td>
                    <td className="py-3 px-3 text-[13px]">{p.volatility.toFixed(1)}%</td>
                    <td className="py-3 pl-3 pr-0 text-[13px] text-negative font-medium">
                      {p.max_drawdown.toFixed(2)}%
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
