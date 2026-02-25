"use client"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { cn } from "@/lib/utils"
import { useTopGainers, useTopLosers } from "@/lib/hooks/use-market-data"
import { MOCK_TOP_GAINERS, MOCK_TOP_LOSERS } from "@/lib/mock-data/market"
import { TrendingUp, TrendingDown } from "lucide-react"
import { formatCurrency } from "@/lib/utils/format"
import type { TopMover } from "@/lib/types/market-data"

function MoverRow({ mover }: { readonly mover: TopMover }) {
  const isPositive = mover.change_percent >= 0
  return (
    <div className="flex items-center justify-between py-2.5 group">
      {/* Symbol badge */}
      <div className="flex items-center gap-3">
        <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-muted text-[11px] font-bold text-muted-foreground group-hover:bg-primary/10 group-hover:text-primary transition-colors">
          {mover.symbol.slice(0, 2)}
        </div>
        <div>
          <p className="text-[13px] font-semibold leading-none">{mover.symbol}</p>
          <p className="text-[11px] text-muted-foreground mt-0.5 leading-none truncate max-w-[90px]">
            {mover.name}
          </p>
        </div>
      </div>

      {/* Price + change */}
      <div className="text-right">
        <p className="text-[13px] font-semibold leading-none">
          {formatCurrency(mover.price)}
        </p>
        <div className="flex items-center justify-end gap-1 mt-0.5">
          {isPositive ? (
            <TrendingUp className="h-3 w-3 text-positive" />
          ) : (
            <TrendingDown className="h-3 w-3 text-negative" />
          )}
          <span
            className={cn(
              "text-[11px] font-semibold",
              isPositive ? "text-positive" : "text-negative",
            )}
          >
            {isPositive ? "+" : ""}{mover.change_percent.toFixed(2)}%
          </span>
        </div>
      </div>
    </div>
  )
}

export function TopMovers() {
  const { data: gainers } = useTopGainers()
  const { data: losers } = useTopLosers()

  const topGainers = gainers ?? MOCK_TOP_GAINERS
  const topLosers = losers ?? MOCK_TOP_LOSERS

  return (
    <Card>
      <CardHeader className="pb-0 pt-5 px-5">
        <CardTitle className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
          Top Movers
        </CardTitle>
      </CardHeader>
      <CardContent className="px-5 pb-5">
        <Tabs defaultValue="gainers" className="mt-3">
          <TabsList className="w-full h-8 rounded-xl bg-muted p-0.5">
            <TabsTrigger
              value="gainers"
              className="flex-1 h-7 text-xs font-medium rounded-lg data-[state=active]:bg-background data-[state=active]:shadow-sm"
            >
              <TrendingUp className="mr-1.5 h-3 w-3" />
              Gainers
            </TabsTrigger>
            <TabsTrigger
              value="losers"
              className="flex-1 h-7 text-xs font-medium rounded-lg data-[state=active]:bg-background data-[state=active]:shadow-sm"
            >
              <TrendingDown className="mr-1.5 h-3 w-3" />
              Losers
            </TabsTrigger>
          </TabsList>
          <TabsContent value="gainers" className="mt-2">
            <div className="divide-y divide-border/60">
              {topGainers.map((m) => (
                <MoverRow key={m.symbol} mover={m} />
              ))}
            </div>
          </TabsContent>
          <TabsContent value="losers" className="mt-2">
            <div className="divide-y divide-border/60">
              {topLosers.map((m) => (
                <MoverRow key={m.symbol} mover={m} />
              ))}
            </div>
          </TabsContent>
        </Tabs>
      </CardContent>
    </Card>
  )
}
