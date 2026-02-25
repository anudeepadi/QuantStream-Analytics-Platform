"use client"

import { AreaChart as RechartsAreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts"

interface AreaChartProps {
  readonly data: readonly Record<string, string | number>[]
  readonly dataKeys: readonly { key: string; color: string; name: string }[]
  readonly xAxisKey?: string
  readonly height?: number
  readonly showGrid?: boolean
  readonly showTooltip?: boolean
  readonly gradientOpacity?: number
}

export function AreaChart({
  data,
  dataKeys,
  xAxisKey = "date",
  height = 300,
  showGrid = true,
  showTooltip = true,
  gradientOpacity = 0.3,
}: AreaChartProps) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <RechartsAreaChart data={[...data]} margin={{ top: 5, right: 10, left: 10, bottom: 0 }}>
        <defs>
          {dataKeys.map((dk) => (
            <linearGradient key={dk.key} id={`gradient-${dk.key}`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor={dk.color} stopOpacity={gradientOpacity} />
              <stop offset="95%" stopColor={dk.color} stopOpacity={0} />
            </linearGradient>
          ))}
        </defs>
        {showGrid && <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />}
        <XAxis
          dataKey={xAxisKey}
          tick={{ fontSize: 12 }}
          tickLine={false}
          axisLine={false}
          className="fill-muted-foreground"
        />
        <YAxis
          tick={{ fontSize: 12 }}
          tickLine={false}
          axisLine={false}
          className="fill-muted-foreground"
          tickFormatter={(v: number) => `$${(v / 1000).toFixed(0)}k`}
        />
        {showTooltip && (
          <Tooltip
            contentStyle={{
              backgroundColor: "hsl(var(--popover))",
              border: "1px solid hsl(var(--border))",
              borderRadius: "8px",
              fontSize: "12px",
            }}
          />
        )}
        {dataKeys.map((dk) => (
          <Area
            key={dk.key}
            type="monotone"
            dataKey={dk.key}
            name={dk.name}
            stroke={dk.color}
            fill={`url(#gradient-${dk.key})`}
            strokeWidth={2}
          />
        ))}
      </RechartsAreaChart>
    </ResponsiveContainer>
  )
}
