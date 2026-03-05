"use client"

import { useState, useMemo } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { CardSkeleton, TableSkeleton } from "@/components/shared/card-skeleton"
import { AreaChart } from "@/components/charts/area-chart"
import { MOCK_MARKET_OVERVIEW } from "@/lib/mock-data/market"
import { useHistoricalData, useMarketOverview } from "@/lib/hooks/use-market-data"
import { formatCurrency } from "@/lib/utils/format"
import { cn } from "@/lib/utils"

const PERIODS = ["1W", "1M", "3M", "6M", "1Y"] as const
type Period = (typeof PERIODS)[number]

const PERIOD_DAYS: Record<Period, number> = { "1W": 7, "1M": 30, "3M": 90, "6M": 180, "1Y": 365 }

export default function HistoricalPage() {
  const [symbol, setSymbol] = useState("AAPL")
  const [period, setPeriod] = useState<Period>("3M")

  const { data: overviewData } = useMarketOverview()
  const overview = overviewData ?? MOCK_MARKET_OVERVIEW
  const symbols = useMemo(() => overview.map((m) => m.symbol), [overview])

  const { data: histData, isLoading } = useHistoricalData(symbol, PERIOD_DAYS[period])

  const chartData = useMemo(
    () => histData?.map((d) => ({ date: d.date, price: d.close })) ?? [],
    [histData],
  )

  const current = overview.find((m) => m.symbol === symbol)
  const first = chartData[0]?.price ?? 0
  const last = chartData[chartData.length - 1]?.price ?? 0
  const periodReturn = first > 0 ? ((last - first) / first) * 100 : 0
  const isPositive = periodReturn >= 0

  const chartKeys = [
    { key: "price", color: isPositive ? "oklch(0.54 0.185 142)" : "oklch(0.62 0.22 25)", name: "Price" },
  ]

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Historical Data</h1>
        <p className="text-sm text-muted-foreground mt-1">Price history for any tracked symbol.</p>
      </div>

      {/* Controls */}
      <div className="flex flex-wrap items-center gap-3">
        {/* Symbol selector */}
        <div className="flex items-center gap-1.5 flex-wrap">
          {symbols.map((s) => (
            <button
              key={s}
              onClick={() => setSymbol(s)}
              className={cn(
                "rounded-xl px-3 py-1.5 text-xs font-semibold transition-colors",
                symbol === s
                  ? "bg-primary text-primary-foreground"
                  : "bg-muted text-muted-foreground hover:text-foreground",
              )}
            >
              {s}
            </button>
          ))}
        </div>

        <div className="ml-auto flex items-center gap-1">
          {PERIODS.map((p) => (
            <button
              key={p}
              onClick={() => setPeriod(p)}
              className={cn(
                "rounded-lg px-3 py-1.5 text-xs font-semibold transition-colors",
                period === p
                  ? "bg-primary/15 text-primary"
                  : "text-muted-foreground hover:text-foreground",
              )}
            >
              {p}
            </button>
          ))}
        </div>
      </div>

      {/* Price summary */}
      {current && (
        isLoading ? (
          <div className="grid gap-4 sm:grid-cols-3">
            <CardSkeleton lines={2} />
            <CardSkeleton lines={2} />
            <CardSkeleton lines={2} />
          </div>
        ) : (
          <div className="grid gap-4 sm:grid-cols-3">
            <Card>
              <CardContent className="p-5">
                <p className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground mb-2">
                  {symbol} · Current
                </p>
                <p className="text-2xl font-bold">{formatCurrency(current.price)}</p>
                <p className={cn("text-sm font-medium mt-1", current.change_percent >= 0 ? "text-positive" : "text-negative")}>
                  {current.change_percent >= 0 ? "+" : ""}{current.change_percent.toFixed(2)}% today
                </p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-5">
                <p className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground mb-2">
                  {period} Return
                </p>
                <p className={cn("text-2xl font-bold", isPositive ? "text-positive" : "text-negative")}>
                  {isPositive ? "+" : ""}{periodReturn.toFixed(2)}%
                </p>
                <p className="text-sm text-muted-foreground mt-1">
                  {formatCurrency(first)} &rarr; {formatCurrency(last)}
                </p>
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-5">
                <p className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground mb-2">
                  Name
                </p>
                <p className="text-lg font-bold truncate">{current.name}</p>
                <p className="text-sm text-muted-foreground mt-1">{current.sector}</p>
              </CardContent>
            </Card>
          </div>
        )
      )}

      {/* Chart */}
      {isLoading ? (
        <TableSkeleton rows={6} cols={4} />
      ) : (
        <Card>
          <CardHeader className="pb-2 pt-5 px-5">
            <CardTitle className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
              {symbol} Price &mdash; {period}
            </CardTitle>
          </CardHeader>
          <CardContent className="px-2 pb-3">
            <AreaChart
              data={chartData}
              dataKeys={chartKeys}
              xAxisKey="date"
              height={300}
              gradientOpacity={0.25}
            />
          </CardContent>
        </Card>
      )}
    </div>
  )
}
