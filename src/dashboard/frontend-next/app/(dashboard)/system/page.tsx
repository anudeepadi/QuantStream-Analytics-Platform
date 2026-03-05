"use client";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { CardSkeleton, TableSkeleton } from "@/components/shared/card-skeleton";
import { useSystemHealth } from "@/lib/hooks/use-system";
import { useSystemWebSocket } from "@/lib/hooks/use-websocket";
import type { SystemMetrics, ServiceStatus } from "@/lib/types/system";
import {
  Activity,
  Cpu,
  HardDrive,
  Wifi,
  Server,
  Zap,
  Radio,
} from "lucide-react";
import { cn } from "@/lib/utils";

const FALLBACK_SERVICES: readonly ServiceStatus[] = [
  { name: "FastAPI Backend", status: "healthy", latency_ms: 12, last_check: new Date().toISOString(), message: null },
  { name: "PostgreSQL", status: "healthy", latency_ms: 3, last_check: new Date().toISOString(), message: null },
  { name: "Redis Cache", status: "healthy", latency_ms: 1, last_check: new Date().toISOString(), message: null },
  { name: "Kafka Broker", status: "degraded", latency_ms: 85, last_check: new Date().toISOString(), message: "High consumer lag detected" },
  { name: "Spark Cluster", status: "healthy", latency_ms: 45, last_check: new Date().toISOString(), message: null },
  { name: "Market Data Feed", status: "healthy", latency_ms: 28, last_check: new Date().toISOString(), message: null },
];

const FALLBACK_METRICS: SystemMetrics = {
  cpu_usage: 42.5,
  memory_usage: 68.2,
  disk_usage: 55.8,
  network_in: 125.4,
  network_out: 89.2,
  uptime_seconds: 1728000,
  active_connections: 156,
  requests_per_second: 342,
};

const STATUS_STYLES = {
  healthy: {
    dot: "bg-positive",
    badge: "bg-positive/10 text-positive",
    label: "Healthy",
  },
  degraded: {
    dot: "bg-yellow-400",
    badge: "bg-yellow-500/10 text-yellow-600",
    label: "Degraded",
  },
  down: {
    dot: "bg-negative",
    badge: "bg-negative/10 text-negative",
    label: "Down",
  },
};

function buildMetricCards(m: SystemMetrics) {
  const networkTotal = m.network_in + m.network_out;
  return [
    { label: "CPU Usage", value: `${m.cpu_usage.toFixed(1)}%`, pct: m.cpu_usage, icon: Cpu },
    { label: "Memory", value: `${m.memory_usage.toFixed(1)}%`, pct: m.memory_usage, icon: Server },
    { label: "Disk", value: `${m.disk_usage.toFixed(1)}%`, pct: m.disk_usage, icon: HardDrive },
    { label: "Network I/O", value: `${networkTotal.toFixed(1)} MB/s`, pct: Math.min((networkTotal / 500) * 100, 100), icon: Wifi },
    { label: "Connections", value: `${m.active_connections}`, pct: Math.min((m.active_connections / 300) * 100, 100), icon: Radio },
    { label: "Requests/sec", value: `${m.requests_per_second}`, pct: Math.min((m.requests_per_second / 1000) * 100, 100), icon: Zap },
  ];
}

export default function SystemPage() {
  const { data: health, isLoading } = useSystemHealth();
  const { metrics: wsMetrics, isConnected } = useSystemWebSocket();

  const services = health?.services ?? FALLBACK_SERVICES;
  const restMetrics = health?.metrics ?? FALLBACK_METRICS;
  const metrics = wsMetrics ?? restMetrics;
  const metricCards = buildMetricCards(metrics);

  const healthy = services.filter((s) => s.status === "healthy").length;
  const degraded = services.filter((s) => s.status === "degraded").length;
  const down = services.filter((s) => s.status === "down").length;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">System Health</h1>
          <p className="text-sm text-muted-foreground mt-1">
            Monitor infrastructure status and service performance.
          </p>
        </div>
        {isConnected && (
          <span className="flex items-center gap-1.5 text-[11px] text-positive font-semibold">
            <span className="h-1.5 w-1.5 rounded-full bg-positive animate-pulse" />
            Live
          </span>
        )}
      </div>

      {/* Overall status banner */}
      {isLoading && !health ? (
        <CardSkeleton />
      ) : (
        <div
          className={cn(
            "rounded-2xl border p-4 flex items-center gap-3",
            degraded > 0 || down > 0
              ? "border-yellow-400/30 bg-yellow-500/5"
              : "border-positive/30 bg-positive/5",
          )}
        >
          <div
            className={cn(
              "h-2.5 w-2.5 rounded-full animate-pulse",
              degraded > 0 || down > 0 ? "bg-yellow-400" : "bg-positive",
            )}
          />
          <div>
            <p
              className={cn(
                "text-sm font-semibold",
                degraded > 0 || down > 0 ? "text-yellow-700" : "text-positive",
              )}
            >
              {down > 0
                ? `${down} service${down > 1 ? "s" : ""} down`
                : degraded > 0
                  ? `${degraded} service${degraded > 1 ? "s" : ""} degraded`
                  : "All systems operational"}
            </p>
            <p className="text-[11px] text-muted-foreground">
              {healthy} healthy · {degraded} degraded · {down} down
            </p>
          </div>
          <div className="ml-auto text-[11px] text-muted-foreground">
            Last checked: {health?.last_updated ? new Date(health.last_updated).toLocaleTimeString() : "just now"}
          </div>
        </div>
      )}

      {/* Metrics Grid */}
      {isLoading && !health ? (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {Array.from({ length: 6 }).map((_, i) => (
            <CardSkeleton key={i} />
          ))}
        </div>
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {metricCards.map((metric) => (
            <Card key={metric.label}>
              <CardContent className="p-5">
                <div className="flex items-start justify-between mb-3">
                  <p className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
                    {metric.label}
                  </p>
                  <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-primary/10">
                    <metric.icon className="h-4 w-4 text-primary" />
                  </div>
                </div>
                <p
                  className={cn(
                    "text-2xl font-bold",
                    metric.pct > 80
                      ? "text-negative"
                      : metric.pct > 60
                        ? "text-yellow-600"
                        : "",
                  )}
                >
                  {metric.value}
                </p>
                <div className="mt-3 h-1.5 rounded-full bg-muted overflow-hidden">
                  <div
                    className={cn(
                      "h-full rounded-full transition-all",
                      metric.pct > 80
                        ? "bg-negative"
                        : metric.pct > 60
                          ? "bg-yellow-400"
                          : "bg-primary",
                    )}
                    style={{ width: `${metric.pct}%` }}
                  />
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* Service Status */}
      <Card>
        <CardHeader className="pb-2 pt-5 px-5">
          <CardTitle className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground flex items-center gap-2">
            <Activity className="h-4 w-4" />
            Service Status
          </CardTitle>
        </CardHeader>
        <CardContent className="px-5 pb-5">
          {isLoading && !health ? (
            <TableSkeleton rows={6} />
          ) : (
            <div className="space-y-2">
              {services.map((service) => {
                const style = STATUS_STYLES[service.status];
                return (
                  <div
                    key={service.name}
                    className="flex items-center justify-between rounded-xl border border-border/60 bg-muted/30 px-4 py-3"
                  >
                    <div className="flex items-center gap-3">
                      <div className={cn("h-2 w-2 rounded-full", style.dot)} />
                      <div>
                        <p className="text-[13px] font-semibold">
                          {service.name}
                        </p>
                        <p className="text-[11px] text-muted-foreground">
                          {service.message ?? `Last check: ${new Date(service.last_check).toLocaleTimeString()}`}
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-3">
                      <span className="text-[12px] text-muted-foreground font-mono">
                        {service.latency_ms}ms
                      </span>
                      <span
                        className={cn(
                          "rounded-full px-2.5 py-0.5 text-[10px] font-semibold",
                          style.badge,
                        )}
                      >
                        {style.label}
                      </span>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
