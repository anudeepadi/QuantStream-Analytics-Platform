"use client";

import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from "recharts";

interface DonutChartProps {
  readonly data: readonly { name: string; value: number; color: string }[];
  readonly height?: number;
  readonly innerRadius?: number;
  readonly outerRadius?: number;
  readonly centerLabel?: string;
  readonly centerValue?: string;
}

export function DonutChart({
  data,
  height = 250,
  innerRadius = 60,
  outerRadius = 90,
  centerLabel,
  centerValue,
}: DonutChartProps) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <PieChart>
        <Pie
          data={[...data]}
          cx="50%"
          cy="50%"
          innerRadius={innerRadius}
          outerRadius={outerRadius}
          paddingAngle={2}
          dataKey="value"
          strokeWidth={0}
        >
          {[...data].map((entry, i) => (
            <Cell key={i} fill={entry.color} />
          ))}
        </Pie>
        <Tooltip
          contentStyle={{
            backgroundColor: "hsl(var(--popover))",
            border: "1px solid hsl(var(--border))",
            borderRadius: "8px",
            fontSize: "12px",
          }}
          formatter={(value) => [`${Number(value).toFixed(1)}%`, ""]}
        />
        {centerLabel && (
          <text
            x="50%"
            y="46%"
            textAnchor="middle"
            className="fill-muted-foreground text-xs"
          >
            {centerLabel}
          </text>
        )}
        {centerValue && (
          <text
            x="50%"
            y="56%"
            textAnchor="middle"
            className="fill-foreground text-lg font-bold"
          >
            {centerValue}
          </text>
        )}
      </PieChart>
    </ResponsiveContainer>
  );
}
