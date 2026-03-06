"use client";

import { useAuth } from "@/lib/auth/auth-context";
import { KpiCard } from "@/components/dashboard/kpi-card";
import { OverviewChart } from "@/components/dashboard/overview-chart";
import { MarketPerformance } from "@/components/dashboard/market-performance";
import { PortfolioAllocation } from "@/components/dashboard/portfolio-allocation";
import { TopMovers } from "@/components/dashboard/top-movers";
import { RecentActivity } from "@/components/dashboard/recent-activity";
import { SectorPerformance } from "@/components/dashboard/sector-performance";
import { CardSkeleton } from "@/components/shared/card-skeleton";
import { usePortfolioSummary, usePositions } from "@/lib/hooks/use-portfolio";
import { MOCK_KPI_CARDS } from "@/lib/mock-data/dashboard";
import type { KpiCardData } from "@/lib/types";

function buildKpiCards(
  totalValue: number,
  dailyPnl: number,
  dailyPnlPct: number,
  winRate: number,
  positionCount: number,
): readonly KpiCardData[] {
  return [
    {
      title: "Portfolio Value",
      value: `$${totalValue.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
      change: dailyPnlPct,
      changeLabel: "vs last month",
      icon: "DollarSign",
      trend: MOCK_KPI_CARDS[0].trend,
    },
    {
      title: "Daily P&L",
      value: `${dailyPnl >= 0 ? "+" : ""}$${Math.abs(dailyPnl).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
      change: dailyPnlPct,
      changeLabel: "vs yesterday",
      icon: "TrendingUp",
      trend: MOCK_KPI_CARDS[1].trend,
    },
    {
      title: "Win Rate",
      value: `${winRate.toFixed(1)}%`,
      change: -1.2,
      changeLabel: "vs last week",
      icon: "Target",
      trend: MOCK_KPI_CARDS[2].trend,
    },
    {
      title: "Active Positions",
      value: `${positionCount}`,
      change: 3,
      changeLabel: "new this week",
      icon: "BarChart3",
      trend: MOCK_KPI_CARDS[3].trend,
    },
  ];
}

export default function DashboardPage() {
  const { user } = useAuth();
  const firstName =
    user?.full_name?.split(" ")[0] ?? user?.username ?? "Analyst";

  const { data: summary, isLoading: summaryLoading } = usePortfolioSummary();
  const { data: positions } = usePositions();

  const kpiCards = summary
    ? buildKpiCards(
        summary.total_value,
        summary.daily_pnl,
        summary.daily_pnl_percent,
        68.4,
        positions?.length ?? 12,
      )
    : MOCK_KPI_CARDS;

  return (
    <div className="space-y-6">
      {/* Welcome section */}
      <div>
        <h1 className="text-2xl font-bold tracking-tight">
          Welcome, {firstName}
        </h1>
        <p className="text-sm text-muted-foreground mt-1">
          An overview of portfolio performance and market analytics.
        </p>
      </div>

      {/* KPI Cards */}
      {summaryLoading && !summary ? (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <CardSkeleton key={i} />
          ))}
        </div>
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          {kpiCards.map((kpi) => (
            <KpiCard key={kpi.title} data={kpi} />
          ))}
        </div>
      )}

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

      {/* Activity — full-width, content-rich enough to own the row */}
      <RecentActivity />
    </div>
  );
}
