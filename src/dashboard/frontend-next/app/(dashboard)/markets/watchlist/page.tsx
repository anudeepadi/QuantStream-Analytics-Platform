"use client"

import { Card, CardContent } from "@/components/ui/card"
import { CardSkeleton, TableSkeleton } from "@/components/shared/card-skeleton"
import { MOCK_MARKET_OVERVIEW, MOCK_TOP_GAINERS, MOCK_TOP_LOSERS } from "@/lib/mock-data/market"
import { useMarketOverview, useTopGainers, useTopLosers } from "@/lib/hooks/use-market-data"
import { formatCurrency } from "@/lib/utils/format"
import { cn } from "@/lib/utils"
import { TrendingUp, TrendingDown, Star } from "lucide-react"

// Mini sparkline using inline SVG
function Sparkline({ positive }: { readonly positive: boolean }) {
  const points = positive
    ? [40, 35, 42, 38, 50, 45, 60, 55, 70, 65, 78]
    : [70, 65, 60, 68, 55, 50, 45, 52, 40, 35, 30]
  const max = Math.max(...points)
  const min = Math.min(...points)
  const range = max - min || 1
  const h = 36
  const w = 80
  const pts = points
    .map((v, i) => {
      const x = (i / (points.length - 1)) * w
      const y = h - ((v - min) / range) * h
      return `${x},${y}`
    })
    .join(" ")

  return (
    <svg width={w} height={h} viewBox={`0 0 ${w} ${h}`} className="overflow-visible">
      <polyline
        points={pts}
        fill="none"
        stroke={positive ? "oklch(0.54 0.185 142)" : "oklch(0.62 0.22 25)"}
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  )
}

export default function WatchlistPage() {
  const { data: gainersData, isLoading: gainersLoading } = useTopGainers()
  const { data: losersData, isLoading: losersLoading } = useTopLosers()
  const { data: overviewData, isLoading: overviewLoading } = useMarketOverview()

  const gainers = gainersData ?? MOCK_TOP_GAINERS
  const losers = losersData ?? MOCK_TOP_LOSERS
  const overview = overviewData ?? MOCK_MARKET_OVERVIEW

  // Combine gainers + losers for watchlist items
  const watchlist = [...gainers, ...losers]
  const watchlistLoading = gainersLoading || losersLoading

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Watchlist</h1>
          <p className="text-sm text-muted-foreground mt-1">Your tracked symbols and price alerts.</p>
        </div>
        <button className="flex items-center gap-1.5 rounded-xl bg-primary/10 px-3 py-2 text-xs font-semibold text-primary hover:bg-primary/20 transition-colors">
          <Star className="h-3.5 w-3.5" />
          Add Symbol
        </button>
      </div>

      {/* Watchlist grid */}
      {watchlistLoading ? (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
          {Array.from({ length: 8 }, (_, i) => (
            <CardSkeleton key={i} lines={2} />
          ))}
        </div>
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
          {watchlist.map((m) => {
            const isPositive = m.change_percent >= 0
            return (
              <Card key={m.symbol} className="hover:shadow-md transition-shadow cursor-pointer">
                <CardContent className="p-5">
                  <div className="flex items-start justify-between mb-3">
                    <div className="flex items-center gap-2.5">
                      <div className="flex h-9 w-9 items-center justify-center rounded-xl bg-muted text-[11px] font-bold text-muted-foreground">
                        {m.symbol.slice(0, 2)}
                      </div>
                      <div>
                        <p className="text-[13px] font-bold leading-none">{m.symbol}</p>
                        <p className="text-[10px] text-muted-foreground mt-0.5 leading-none truncate max-w-[80px]">
                          {m.name}
                        </p>
                      </div>
                    </div>
                    <Star className="h-3.5 w-3.5 text-yellow-400 fill-yellow-400" />
                  </div>

                  <div className="flex items-end justify-between">
                    <div>
                      <p className="text-lg font-bold leading-none">{formatCurrency(m.price)}</p>
                      <div className="flex items-center gap-1 mt-1">
                        {isPositive ? (
                          <TrendingUp className="h-3 w-3 text-positive" />
                        ) : (
                          <TrendingDown className="h-3 w-3 text-negative" />
                        )}
                        <span className={cn("text-[12px] font-semibold", isPositive ? "text-positive" : "text-negative")}>
                          {isPositive ? "+" : ""}{m.change_percent.toFixed(2)}%
                        </span>
                      </div>
                    </div>
                    <Sparkline positive={isPositive} />
                  </div>
                </CardContent>
              </Card>
            )
          })}
        </div>
      )}

      {/* Market snapshot */}
      {overviewLoading ? (
        <TableSkeleton rows={4} cols={2} />
      ) : (
        <Card>
          <CardContent className="px-5 py-5">
            <p className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground mb-4">
              Market Snapshot
            </p>
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
              {overview.map((m) => {
                const isPositive = m.change_percent >= 0
                return (
                  <div key={m.symbol} className="flex items-center justify-between rounded-xl border border-border/60 bg-muted/30 px-4 py-3">
                    <div>
                      <p className="text-[12px] font-bold">{m.symbol}</p>
                      <p className="text-[11px] text-muted-foreground">{formatCurrency(m.price)}</p>
                    </div>
                    <span className={cn("text-[12px] font-semibold", isPositive ? "text-positive" : "text-negative")}>
                      {isPositive ? "+" : ""}{m.change_percent.toFixed(2)}%
                    </span>
                  </div>
                )
              })}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
