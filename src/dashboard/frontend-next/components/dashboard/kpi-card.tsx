"use client";

import { Card, CardContent } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import { DollarSign, TrendingUp, Target, BarChart3 } from "lucide-react";
import type { KpiCardData } from "@/lib/types";

const ICON_MAP: Record<string, React.ElementType> = {
  DollarSign,
  TrendingUp,
  Target,
  BarChart3,
};

interface KpiCardProps {
  readonly data: KpiCardData;
}

export function KpiCard({ data }: KpiCardProps) {
  const Icon = ICON_MAP[data.icon] ?? DollarSign;
  const isPositive = data.change >= 0;

  return (
    <Card className="relative overflow-hidden">
      <CardContent className="p-5">
        {/* Top row: label + icon badge */}
        <div className="flex items-start justify-between mb-3">
          <p className="text-sm font-medium text-muted-foreground leading-none">
            {data.title}
          </p>
          <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-xl bg-primary/10">
            <Icon className="h-4 w-4 text-primary" />
          </div>
        </div>

        {/* Value */}
        <p className="text-[1.6rem] font-bold tracking-tight leading-none mb-2">
          {data.value}
        </p>

        {/* Sparkline */}
        <Sparkline data={data.trend} positive={isPositive} />

        {/* Bottom row: change indicator */}
        <div className="flex items-center gap-1.5 mt-3">
          <span
            className={cn(
              "inline-flex items-center gap-0.5 text-xs font-semibold",
              isPositive ? "text-positive" : "text-negative",
            )}
          >
            {isPositive ? "▲" : "▼"} {Math.abs(data.change)}%
          </span>
          <span className="text-xs text-muted-foreground">
            {data.changeLabel}
          </span>
        </div>
      </CardContent>
    </Card>
  );
}

function Sparkline({
  data,
  positive,
}: {
  readonly data: readonly number[];
  readonly positive: boolean;
}) {
  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = max - min || 1;
  const height = 36;
  const width = 100;

  const points = data
    .map((v, i) => {
      const x = (i / (data.length - 1)) * width;
      const y = height - ((v - min) / range) * (height * 0.85);
      return `${x},${y}`;
    })
    .join(" ");

  const areaPoints = `0,${height} ${points} ${width},${height}`;

  return (
    <svg
      width={width}
      height={height}
      className={cn(
        "overflow-visible w-full",
        positive ? "text-positive" : "text-negative",
      )}
      preserveAspectRatio="none"
    >
      <defs>
        <linearGradient id={`grad-${positive}`} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="currentColor" stopOpacity="0.15" />
          <stop offset="100%" stopColor="currentColor" stopOpacity="0.02" />
        </linearGradient>
      </defs>
      {/* Area fill */}
      <polygon points={areaPoints} fill={`url(#grad-${positive})`} />
      {/* Line */}
      <polyline
        points={points}
        fill="none"
        stroke="currentColor"
        strokeWidth={1.5}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
