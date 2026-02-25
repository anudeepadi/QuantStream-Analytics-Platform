"use client"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { cn } from "@/lib/utils"
import { useSectorPerformance } from "@/lib/hooks/use-market-data"
import { MOCK_SECTOR_PERFORMANCE } from "@/lib/mock-data/market"

export function SectorPerformance() {
  const { data: sectors } = useSectorPerformance()
  const sorted = [...(sectors ?? MOCK_SECTOR_PERFORMANCE)].sort(
    (a, b) => b.change_percent - a.change_percent,
  )

  const maxAbs = Math.max(...sorted.map((s) => Math.abs(s.change_percent)), 1)

  return (
    <Card>
      <CardHeader className="pb-2 pt-5 px-5">
        <div className="flex items-center justify-between">
          <CardTitle className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
            Sectors
          </CardTitle>
          <span className="text-[11px] text-muted-foreground">% change</span>
        </div>
      </CardHeader>
      <CardContent className="px-5 pb-5">
        <div className="space-y-3">
          {sorted.map((sector) => {
            const isPositive = sector.change_percent >= 0
            const barWidth = (Math.abs(sector.change_percent) / maxAbs) * 100

            return (
              <div key={sector.sector} className="space-y-1">
                <div className="flex items-center justify-between">
                  <span className="text-[13px] font-medium truncate pr-2">
                    {sector.sector}
                  </span>
                  <span
                    className={cn(
                      "text-[12px] font-semibold shrink-0",
                      isPositive ? "text-positive" : "text-negative"
                    )}
                  >
                    {isPositive ? "+" : ""}
                    {sector.change_percent.toFixed(2)}%
                  </span>
                </div>
                {/* Progress bar — mirrors the reference's percentage bars */}
                <div className="h-1.5 w-full rounded-full bg-muted overflow-hidden">
                  <div
                    className={cn(
                      "h-full rounded-full transition-all",
                      isPositive ? "bg-positive" : "bg-negative"
                    )}
                    style={{ width: `${barWidth}%` }}
                  />
                </div>
              </div>
            )
          })}
        </div>
      </CardContent>
    </Card>
  )
}
