"use client"

import { BarChart as RechartsBarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell } from "recharts"

interface BarChartProps {
  readonly data: readonly Record<string, string | number>[]
  readonly dataKey: string
  readonly xAxisKey?: string
  readonly height?: number
  readonly color?: string
  readonly showGrid?: boolean
  readonly colorByValue?: boolean
  readonly layout?: "horizontal" | "vertical"
}

export function BarChart({
  data,
  dataKey,
  xAxisKey = "date",
  height = 300,
  color = "hsl(var(--chart-1))",
  showGrid = true,
  colorByValue = false,
  layout = "horizontal",
}: BarChartProps) {
  const isVertical = layout === "vertical"

  return (
    <ResponsiveContainer width="100%" height={height}>
      <RechartsBarChart
        data={[...data]}
        layout={isVertical ? "vertical" : "horizontal"}
        margin={{ top: 5, right: 10, left: 10, bottom: 0 }}
      >
        {showGrid && <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />}
        {isVertical ? (
          <>
            <XAxis type="number" tick={{ fontSize: 12 }} tickLine={false} axisLine={false} className="fill-muted-foreground" />
            <YAxis
              dataKey={xAxisKey}
              type="category"
              tick={{ fontSize: 12 }}
              tickLine={false}
              axisLine={false}
              className="fill-muted-foreground"
              width={100}
            />
          </>
        ) : (
          <>
            <XAxis dataKey={xAxisKey} tick={{ fontSize: 12 }} tickLine={false} axisLine={false} className="fill-muted-foreground" />
            <YAxis tick={{ fontSize: 12 }} tickLine={false} axisLine={false} className="fill-muted-foreground" />
          </>
        )}
        <Tooltip
          contentStyle={{
            backgroundColor: "hsl(var(--popover))",
            border: "1px solid hsl(var(--border))",
            borderRadius: "8px",
            fontSize: "12px",
          }}
        />
        <Bar dataKey={dataKey} radius={[4, 4, 0, 0]} maxBarSize={40}>
          {colorByValue
            ? [...data].map((entry, i) => (
                <Cell
                  key={i}
                  fill={
                    Number(entry[dataKey]) >= 0
                      ? "hsl(var(--positive))"
                      : "hsl(var(--negative))"
                  }
                />
              ))
            : [...data].map((_, i) => <Cell key={i} fill={color} />)}
        </Bar>
      </RechartsBarChart>
    </ResponsiveContainer>
  )
}
