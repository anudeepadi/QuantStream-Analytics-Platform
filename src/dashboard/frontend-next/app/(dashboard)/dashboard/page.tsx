"use client"

import { useAuth } from "@/lib/auth/auth-context"
import { KpiCard } from "@/components/dashboard/kpi-card"
import { OverviewChart } from "@/components/dashboard/overview-chart"
import { MarketPerformance } from "@/components/dashboard/market-performance"
import { PortfolioAllocation } from "@/components/dashboard/portfolio-allocation"
import { TopMovers } from "@/components/dashboard/top-movers"
import { RecentActivity } from "@/components/dashboard/recent-activity"
import { SectorPerformance } from "@/components/dashboard/sector-performance"
import { MOCK_KPI_CARDS } from "@/lib/mock-data/dashboard"

export default function DashboardPage() {
  const { user } = useAuth()
  const firstName = user?.full_name?.split(" ")[0] ?? user?.username ?? "Analyst"

  return (
    <div className="space-y-6">
      {/* Welcome section — mirrors the reference's greeting header */}
      <div>
        <h1 className="text-2xl font-bold tracking-tight">
          Welcome, {firstName} 👋
        </h1>
        <p className="text-sm text-muted-foreground mt-1">
          An overview of portfolio performance and market analytics.
        </p>
      </div>

      {/* KPI Cards */}
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {MOCK_KPI_CARDS.map((kpi) => (
          <KpiCard key={kpi.title} data={kpi} />
        ))}
      </div>

      {/* Main Charts Row: overview (2/3) + allocation (1/3) */}
      <div className="grid gap-5 lg:grid-cols-3">
        <div className="lg:col-span-2">
          <OverviewChart />
        </div>
        <PortfolioAllocation />
      </div>

      {/* Secondary Row: market + movers + sectors */}
      <div className="grid gap-5 lg:grid-cols-3">
        <MarketPerformance />
        <TopMovers />
        <SectorPerformance />
      </div>

      {/* Activity Row */}
      <div className="grid gap-5 lg:grid-cols-2">
        <RecentActivity />
      </div>
    </div>
  )
}
