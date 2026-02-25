"use client"

import { useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { MOCK_MARKET_OVERVIEW } from "@/lib/mock-data/market"
import { formatCurrency, formatCompactNumber } from "@/lib/utils/format"
import { cn } from "@/lib/utils"
import { TrendingUp, TrendingDown, ChevronUp, ChevronDown } from "lucide-react"
import type { MarketOverview } from "@/lib/types/market-data"

type SortKey = keyof MarketOverview
type SortDir = "asc" | "desc"

const SECTORS = ["All", ...Array.from(new Set(MOCK_MARKET_OVERVIEW.map((m) => m.sector)))]

export default function MarketsPage() {
  const [sector, setSector] = useState("All")
  const [sortKey, setSortKey] = useState<SortKey>("market_cap")
  const [sortDir, setSortDir] = useState<SortDir>("desc")

  const filtered = MOCK_MARKET_OVERVIEW.filter((m) => sector === "All" || m.sector === sector)
  const sorted = [...filtered].sort((a, b) => {
    const av = a[sortKey]
    const bv = b[sortKey]
    if (typeof av === "number" && typeof bv === "number") {
      return sortDir === "asc" ? av - bv : bv - av
    }
    return sortDir === "asc"
      ? String(av).localeCompare(String(bv))
      : String(bv).localeCompare(String(av))
  })

  const handleSort = (key: SortKey) => {
    if (key === sortKey) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"))
    } else {
      setSortKey(key)
      setSortDir("desc")
    }
  }

  const advancing = MOCK_MARKET_OVERVIEW.filter((m) => m.change_percent >= 0).length
  const declining = MOCK_MARKET_OVERVIEW.filter((m) => m.change_percent < 0).length

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Live Markets</h1>
        <p className="text-sm text-muted-foreground mt-1">Real-time quotes across major equities.</p>
      </div>

      {/* Status bar */}
      <div className="flex flex-wrap items-center gap-4">
        <span className="flex items-center gap-1.5 text-sm font-medium text-positive">
          <span className="h-2 w-2 rounded-full bg-positive animate-pulse" />
          {advancing} advancing
        </span>
        <span className="flex items-center gap-1.5 text-sm font-medium text-negative">
          <span className="h-2 w-2 rounded-full bg-negative" />
          {declining} declining
        </span>
        <div className="ml-auto flex items-center gap-1.5">
          {SECTORS.map((s) => (
            <button
              key={s}
              onClick={() => setSector(s)}
              className={cn(
                "rounded-full px-3 py-1 text-xs font-medium transition-colors",
                sector === s
                  ? "bg-primary text-primary-foreground"
                  : "bg-muted text-muted-foreground hover:text-foreground",
              )}
            >
              {s}
            </button>
          ))}
        </div>
      </div>

      {/* Market table */}
      <Card>
        <CardHeader className="pb-2 pt-5 px-5">
          <CardTitle className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
            Market Overview
          </CardTitle>
        </CardHeader>
        <CardContent className="px-5 pb-5">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border/60">
                  {[
                    { label: "Symbol", key: "symbol" as SortKey },
                    { label: "Price", key: "price" as SortKey },
                    { label: "Change", key: "change_percent" as SortKey },
                    { label: "Volume", key: "volume" as SortKey },
                    { label: "Market Cap", key: "market_cap" as SortKey },
                    { label: "Sector", key: "sector" as SortKey },
                  ].map(({ label, key }) => (
                    <th
                      key={key}
                      onClick={() => handleSort(key)}
                      className="pb-3 text-left text-[11px] font-semibold uppercase tracking-wider text-muted-foreground first:pl-0 last:pr-0 px-3 cursor-pointer select-none hover:text-foreground transition-colors"
                    >
                      <span className="flex items-center gap-1">
                        {label}
                        {sortKey === key ? (
                          sortDir === "asc" ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />
                        ) : null}
                      </span>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-border/40">
                {sorted.map((m) => {
                  const isPositive = m.change_percent >= 0
                  return (
                    <tr key={m.symbol} className="hover:bg-muted/40 transition-colors">
                      <td className="py-3.5 pl-0 pr-3">
                        <div className="flex items-center gap-2.5">
                          <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-muted text-[11px] font-bold text-muted-foreground">
                            {m.symbol.slice(0, 2)}
                          </div>
                          <div>
                            <p className="text-[13px] font-semibold leading-none">{m.symbol}</p>
                            <p className="text-[10px] text-muted-foreground mt-0.5 leading-none truncate max-w-[100px]">
                              {m.name}
                            </p>
                          </div>
                        </div>
                      </td>
                      <td className="py-3.5 px-3 text-[13px] font-semibold">{formatCurrency(m.price)}</td>
                      <td className="py-3.5 px-3">
                        <div className="flex items-center gap-1">
                          {isPositive ? (
                            <TrendingUp className="h-3 w-3 text-positive" />
                          ) : (
                            <TrendingDown className="h-3 w-3 text-negative" />
                          )}
                          <span className={cn("text-[13px] font-semibold", isPositive ? "text-positive" : "text-negative")}>
                            {isPositive ? "+" : ""}{m.change_percent.toFixed(2)}%
                          </span>
                          <span className={cn("text-[11px] ml-1", isPositive ? "text-positive/60" : "text-negative/60")}>
                            ({isPositive ? "+" : ""}{m.change.toFixed(2)})
                          </span>
                        </div>
                      </td>
                      <td className="py-3.5 px-3 text-[13px] text-muted-foreground">
                        {formatCompactNumber(m.volume)}
                      </td>
                      <td className="py-3.5 px-3 text-[13px] font-medium">
                        ${formatCompactNumber(m.market_cap)}
                      </td>
                      <td className="py-3.5 pl-3 pr-0">
                        <span className="rounded-full bg-primary/10 px-2 py-0.5 text-[11px] font-medium text-primary">
                          {m.sector}
                        </span>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
