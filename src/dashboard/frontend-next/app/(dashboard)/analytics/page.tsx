"use client"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { AreaChart } from "@/components/charts/area-chart"
import { CardSkeleton, TableSkeleton } from "@/components/shared/card-skeleton"
import { usePerformance } from "@/lib/hooks/use-portfolio"
import { MOCK_PERFORMANCE } from "@/lib/mock-data/portfolio"
import { cn } from "@/lib/utils"
import { TrendingUp, Activity, BarChart3, AlertTriangle } from "lucide-react"

const KPI_CARDS = [
  {
    label: "Total Return",
    value: "+42.26%",
    sub: "vs +28.00% benchmark",
    positive: true,
    icon: TrendingUp,
  },
  {
    label: "Sharpe Ratio",
    value: "1.42",
    sub: "annualised, risk-adjusted",
    positive: true,
    icon: Activity,
  },
  {
    label: "Volatility",
    value: "22.4%",
    sub: "annualised std deviation",
    positive: null,
    icon: BarChart3,
  },
  {
    label: "Max Drawdown",
    value: "-12.50%",
    sub: "worst peak-to-trough",
    positive: false,
    icon: AlertTriangle,
  },
]

// Generate 12-month chart data
const CHART_DATA = Array.from({ length: 12 }, (_, i) => {
  const month = new Date(2025, i, 1).toLocaleString("en-US", { month: "short" })
  const base = 200000
  const portfolio = base * (1 + (i * 0.034) + Math.sin(i * 0.8) * 0.02)
  const benchmark = base * (1 + (i * 0.022) + Math.cos(i * 0.6) * 0.015)
  return { month, portfolio: Math.round(portfolio), benchmark: Math.round(benchmark) }
})

const CHART_KEYS = [
  { key: "portfolio", color: "oklch(0.54 0.185 142)", name: "Portfolio" },
  { key: "benchmark", color: "oklch(0.65 0.12 250)", name: "S&P 500" },
]

export default function AnalyticsPage() {
  const { data: perfData, isLoading } = usePerformance()
  const performance = perfData ?? MOCK_PERFORMANCE

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Analytics</h1>
        <p className="text-sm text-muted-foreground mt-1">
          Deep-dive performance analysis and risk metrics.
        </p>
      </div>

      {/* KPI Cards */}
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {isLoading
          ? Array.from({ length: 4 }, (_, i) => <CardSkeleton key={i} />)
          : KPI_CARDS.map((kpi) => (
              <Card key={kpi.label}>
                <CardContent className="p-5">
                  <div className="flex items-start justify-between mb-3">
                    <p className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
                      {kpi.label}
                    </p>
                    <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-primary/10">
                      <kpi.icon className="h-4 w-4 text-primary" />
                    </div>
                  </div>
                  <p
                    className={cn(
                      "text-2xl font-bold",
                      kpi.positive === true && "text-positive",
                      kpi.positive === false && "text-negative",
                    )}
                  >
                    {kpi.value}
                  </p>
                  <p className="text-xs text-muted-foreground mt-1">{kpi.sub}</p>
                </CardContent>
              </Card>
            ))}
      </div>

      {/* Portfolio vs Benchmark Chart */}
      <Card>
        <CardHeader className="pb-2 pt-5 px-5">
          <div className="flex items-start justify-between">
            <div>
              <CardTitle className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
                Portfolio vs Benchmark
              </CardTitle>
              <p className="text-xs text-muted-foreground mt-1">12-month performance comparison</p>
            </div>
            <div className="flex items-center gap-4 text-[11px]">
              <span className="flex items-center gap-1.5 font-medium">
                <span className="h-1.5 w-1.5 rounded-full bg-primary inline-block" />
                Portfolio
              </span>
              <span className="flex items-center gap-1.5 font-medium text-muted-foreground">
                <span className="h-1.5 w-1.5 rounded-full bg-blue-400 inline-block" />
                S&P 500
              </span>
            </div>
          </div>
        </CardHeader>
        <CardContent className="px-2 pb-3">
          <AreaChart
            data={CHART_DATA}
            dataKeys={CHART_KEYS}
            xAxisKey="month"
            height={260}
          />
        </CardContent>
      </Card>

      {/* Performance by Period */}
      {isLoading ? (
        <TableSkeleton rows={5} cols={7} />
      ) : (
        <Card>
          <CardHeader className="pb-2 pt-5 px-5">
            <CardTitle className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
              Performance by Period
            </CardTitle>
          </CardHeader>
          <CardContent className="px-5 pb-5">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border/60">
                    {["Period", "Return", "Benchmark", "Alpha", "Sharpe", "Volatility", "Max DD"].map((h) => (
                      <th key={h} className="pb-3 text-left text-[11px] font-semibold uppercase tracking-wider text-muted-foreground first:pl-0 last:pr-0 px-3">
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-border/40">
                  {performance.map((p) => (
                    <tr key={p.period} className="hover:bg-muted/40 transition-colors">
                      <td className="py-3 pl-0 pr-3 font-semibold text-[13px]">{p.period}</td>
                      <td className={cn("py-3 px-3 text-[13px] font-semibold", p.return_percent >= 0 ? "text-positive" : "text-negative")}>
                        {p.return_percent >= 0 ? "+" : ""}{p.return_percent.toFixed(2)}%
                      </td>
                      <td className="py-3 px-3 text-[13px] text-muted-foreground">
                        +{p.benchmark_return_percent.toFixed(2)}%
                      </td>
                      <td className={cn("py-3 px-3 text-[13px] font-medium", p.alpha >= 0 ? "text-positive" : "text-negative")}>
                        {p.alpha >= 0 ? "+" : ""}{p.alpha.toFixed(2)}%
                      </td>
                      <td className="py-3 px-3 text-[13px]">{p.sharpe_ratio.toFixed(2)}</td>
                      <td className="py-3 px-3 text-[13px]">{p.volatility.toFixed(1)}%</td>
                      <td className="py-3 pl-3 pr-0 text-[13px] text-negative">{p.max_drawdown.toFixed(2)}%</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
