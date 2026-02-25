"use client"

import { Button } from "@/components/ui/button"
import { useAppStore } from "@/lib/store/app-store"
import { cn } from "@/lib/utils"

const FILTERS = [
  { value: "today", label: "Today" },
  { value: "week", label: "7D" },
  { value: "month", label: "30D" },
  { value: "year", label: "1Y" },
] as const

export function TimeFilter() {
  const { timeFilter, setTimeFilter } = useAppStore()

  return (
    <div className="flex items-center gap-1 rounded-lg bg-muted p-1">
      {FILTERS.map((filter) => (
        <Button
          key={filter.value}
          variant="ghost"
          size="sm"
          className={cn(
            "h-7 px-3 text-xs font-medium",
            timeFilter === filter.value &&
              "bg-background shadow-sm"
          )}
          onClick={() => setTimeFilter(filter.value)}
        >
          {filter.label}
        </Button>
      ))}
    </div>
  )
}
