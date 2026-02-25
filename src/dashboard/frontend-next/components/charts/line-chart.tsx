"use client"

import { LineChart as RechartsLineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts"

interface LineChartProps {
  readonly data: readonly Record<string, string | number>[]
  readonly dataKeys: readonly { key: string; color: string; name: string; dashed?: boolean }[]
  readonly xAxisKey?: string
  readonly height?: number
  readonly showGrid?: boolean
}

export function LineChart({
  data,
  dataKeys,
  xAxisKey = "date",
  height = 300,
  showGrid = true,
}: LineChartProps) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <RechartsLineChart data={[...data]} margin={{ top: 5, right: 10, left: 10, bottom: 0 }}>
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
        />
        <Tooltip
          contentStyle={{
            backgroundColor: "hsl(var(--popover))",
            border: "1px solid hsl(var(--border))",
            borderRadius: "8px",
            fontSize: "12px",
          }}
        />
        {dataKeys.map((dk) => (
          <Line
            key={dk.key}
            type="monotone"
            dataKey={dk.key}
            name={dk.name}
            stroke={dk.color}
            strokeWidth={2}
            strokeDasharray={dk.dashed ? "5 5" : undefined}
            dot={false}
            activeDot={{ r: 4 }}
          />
        ))}
      </RechartsLineChart>
    </ResponsiveContainer>
  )
}
