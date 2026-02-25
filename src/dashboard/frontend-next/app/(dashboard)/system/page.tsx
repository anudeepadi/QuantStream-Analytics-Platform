"use client";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Activity,
  Cpu,
  HardDrive,
  Wifi,
  Server,
  Database,
  Zap,
  Radio,
} from "lucide-react";
import { cn } from "@/lib/utils";

type ServiceStatus = "healthy" | "degraded" | "down";

const MOCK_SERVICES: {
  name: string;
  status: ServiceStatus;
  latency: number;
  uptime: string;
}[] = [
  { name: "FastAPI Backend", status: "healthy", latency: 12, uptime: "99.98%" },
  { name: "PostgreSQL", status: "healthy", latency: 3, uptime: "99.99%" },
  { name: "Redis Cache", status: "healthy", latency: 1, uptime: "100%" },
  { name: "Kafka Broker", status: "degraded", latency: 85, uptime: "97.2%" },
  { name: "Spark Cluster", status: "healthy", latency: 45, uptime: "99.5%" },
  { name: "Market Data Feed", status: "healthy", latency: 28, uptime: "99.9%" },
];

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

const METRICS = [
  { label: "CPU Usage", value: "42.5%", pct: 42.5, icon: Cpu },
  { label: "Memory", value: "68.2%", pct: 68.2, icon: Server },
  { label: "Disk", value: "55.8%", pct: 55.8, icon: HardDrive },
  { label: "Network I/O", value: "214.6 MB/s", pct: 65, icon: Wifi },
  { label: "Connections", value: "156", pct: 52, icon: Radio },
  { label: "Requests/sec", value: "342", pct: 34, icon: Zap },
];

const healthy = MOCK_SERVICES.filter((s) => s.status === "healthy").length;
const degraded = MOCK_SERVICES.filter((s) => s.status === "degraded").length;
const down = MOCK_SERVICES.filter((s) => s.status === "down").length;

export default function SystemPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">System Health</h1>
        <p className="text-sm text-muted-foreground mt-1">
          Monitor infrastructure status and service performance.
        </p>
      </div>

      {/* Overall status banner */}
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
            {degraded > 0
              ? `${degraded} service degraded`
              : "All systems operational"}
          </p>
          <p className="text-[11px] text-muted-foreground">
            {healthy} healthy · {degraded} degraded · {down} down
          </p>
        </div>
        <div className="ml-auto text-[11px] text-muted-foreground">
          Last checked: just now
        </div>
      </div>

      {/* Metrics Grid */}
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {METRICS.map((metric) => (
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

      {/* Service Status */}
      <Card>
        <CardHeader className="pb-2 pt-5 px-5">
          <CardTitle className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground flex items-center gap-2">
            <Activity className="h-4 w-4" />
            Service Status
          </CardTitle>
        </CardHeader>
        <CardContent className="px-5 pb-5">
          <div className="space-y-2">
            {MOCK_SERVICES.map((service) => {
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
                        Uptime: {service.uptime}
                      </p>
                    </div>
                  </div>
                  <div className="flex items-center gap-3">
                    <span className="text-[12px] text-muted-foreground font-mono">
                      {service.latency}ms
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
        </CardContent>
      </Card>
    </div>
  );
}
