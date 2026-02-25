"use client"

import { useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { AreaChart } from "@/components/charts/area-chart"
import { MOCK_MARKET_OVERVIEW } from "@/lib/mock-data/market"
import { formatCurrency } from "@/lib/utils/format"
import { cn } from "@/lib/utils"

const PERIODS = ["1W", "1M", "3M", "6M", "1Y"] as const
type Period = (typeof PERIODS)[number]

const PERIOD_POINTS: Record<Period, number> = { "1W": 7, "1M": 30, "3M": 90, "6M": 180, "1Y": 365 }

function generateHistorical(symbol: string, period: Period) {
  const n = PERIOD_POINTS[period]
  const base = MOCK_MARKET_OVERVIEW.find((m) => m.symbol === symbol)?.price ?? 200
  const data: { date: string; price: number; volume: number }[] = []
  let price = base * 0.75
  for (let i = 0; i < n; i++) {
    price = price * (1 + (Math.random() - 0.45) * 0.03)
    const d = new Date()
    d.setDate(d.getDate() - (n - i))
    const label =
      period === "1Y"
        ? d.toLocaleDateString("en-US", { month: "short", day: "numeric" }).replace(/\s\d+$/, "")
        : d.toLocaleDateString("en-US", { month: "short", day: "numeric" })
    data.push({
      date: label,
      price: Math.round(price * 100) / 100,
      volume: Math.round(Math.random() * 50000000 + 5000000),
    })
  }
  // Thin the data for display
  const step = Math.max(1, Math.floor(n / 40))
  return data.filter((_, i) => i % step === 0)
}

const SYMBOLS = MOCK_MARKET_OVERVIEW.map((m) => m.symbol)

export default function HistoricalPage() {
  const [symbol, setSymbol] = useState("AAPL")
  const [period, setPeriod] = useState<Period>("3M")

  const chartData = generateHistorical(symbol, period)
  const current = MOCK_MARKET_OVERVIEW.find((m) => m.symbol === symbol)
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
          {SYMBOLS.map((s) => (
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
                {formatCurrency(first)} → {formatCurrency(last)}
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
      )}

      {/* Chart */}
      <Card>
        <CardHeader className="pb-2 pt-5 px-5">
          <CardTitle className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
            {symbol} Price — {period}
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
    </div>
  )
}
