"use client";

import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  MOCK_ALERTS,
  MOCK_ALERT_RULES,
  MOCK_ALERT_STATISTICS,
} from "@/lib/mock-data/alerts";
import { cn } from "@/lib/utils";
import {
  Bell,
  AlertTriangle,
  Info,
  CheckCircle,
  XCircle,
  Plus,
} from "lucide-react";
import { formatDate, formatTime } from "@/lib/utils/format";
import type { Alert } from "@/lib/types/alerts";

const SEVERITY_CONFIG = {
  critical: {
    label: "Critical",
    bg: "bg-negative/10",
    text: "text-negative",
    icon: XCircle,
  },
  warning: {
    label: "Warning",
    bg: "bg-yellow-500/10",
    text: "text-yellow-600",
    icon: AlertTriangle,
  },
  info: {
    label: "Info",
    bg: "bg-primary/10",
    text: "text-primary",
    icon: Info,
  },
};

const STATUS_CONFIG: Record<
  string,
  { label: string; bg: string; text: string }
> = {
  active: { label: "Active", bg: "bg-positive/10", text: "text-positive" },
  acknowledged: {
    label: "Acknowledged",
    bg: "bg-yellow-500/10",
    text: "text-yellow-600",
  },
  resolved: {
    label: "Resolved",
    bg: "bg-muted",
    text: "text-muted-foreground",
  },
  dismissed: {
    label: "Dismissed",
    bg: "bg-muted",
    text: "text-muted-foreground",
  },
};

function AlertRow({ alert }: { readonly alert: Alert }) {
  const sev = SEVERITY_CONFIG[alert.severity];
  const status = STATUS_CONFIG[alert.status];
  const SevIcon = sev.icon;
  return (
    <div className="flex items-start gap-3 py-4 border-b border-border/50 last:border-0">
      <div
        className={cn(
          "flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-xl",
          sev.bg,
        )}
      >
        <SevIcon className={cn("h-4 w-4", sev.text)} />
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-start justify-between gap-2">
          <div>
            <p className="text-[13px] font-semibold leading-none">
              {alert.title}
            </p>
            {alert.symbol && (
              <span className="inline-block mt-1 text-[10px] font-bold px-1.5 py-0.5 rounded bg-muted text-muted-foreground">
                {alert.symbol}
              </span>
            )}
          </div>
          <div className="flex flex-shrink-0 items-center gap-2">
            <span
              className={cn(
                "rounded-full px-2 py-0.5 text-[10px] font-semibold",
                sev.bg,
                sev.text,
              )}
            >
              {sev.label}
            </span>
            <span
              className={cn(
                "rounded-full px-2 py-0.5 text-[10px] font-semibold",
                status.bg,
                status.text,
              )}
            >
              {status.label}
            </span>
          </div>
        </div>
        <p className="text-[12px] text-muted-foreground mt-1">
          {alert.message}
        </p>
        <p className="text-[10px] text-muted-foreground/60 mt-1">
          {alert.triggered_at
            ? `Triggered ${formatDate(alert.triggered_at)} at ${formatTime(alert.triggered_at)}`
            : `Created ${formatDate(alert.created_at)}`}
        </p>
      </div>
    </div>
  );
}

export default function AlertsPage() {
  const [tab, setTab] = useState<"active" | "all">("active");

  const displayed =
    tab === "active"
      ? MOCK_ALERTS.filter(
          (a) => a.status === "active" || a.status === "acknowledged",
        )
      : MOCK_ALERTS;

  const stats = MOCK_ALERT_STATISTICS;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Alerts</h1>
          <p className="text-sm text-muted-foreground mt-1">
            Monitor and manage your alert rules.
          </p>
        </div>
        <button className="flex items-center gap-1.5 rounded-xl bg-primary px-4 py-2 text-xs font-semibold text-primary-foreground hover:bg-primary/90 transition-colors">
          <Plus className="h-3.5 w-3.5" />
          New Alert
        </button>
      </div>

      {/* Stats row */}
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {[
          {
            label: "Total Alerts",
            value: stats.total_alerts,
            icon: Bell,
            sub: "all time",
          },
          {
            label: "Active",
            value: stats.active_alerts,
            icon: AlertTriangle,
            sub: "currently active",
            accent: true,
          },
          {
            label: "Triggered Today",
            value: stats.triggered_today,
            icon: CheckCircle,
            sub: "since midnight",
          },
          {
            label: "Critical",
            value: stats.critical_count,
            icon: XCircle,
            sub: "need attention",
            danger: true,
          },
        ].map((s) => (
          <Card key={s.label}>
            <CardContent className="p-5">
              <div className="flex items-start justify-between mb-3">
                <p className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
                  {s.label}
                </p>
                <div
                  className={cn(
                    "flex h-8 w-8 items-center justify-center rounded-xl",
                    s.danger
                      ? "bg-negative/10"
                      : s.accent
                        ? "bg-yellow-500/10"
                        : "bg-primary/10",
                  )}
                >
                  <s.icon
                    className={cn(
                      "h-4 w-4",
                      s.danger
                        ? "text-negative"
                        : s.accent
                          ? "text-yellow-600"
                          : "text-primary",
                    )}
                  />
                </div>
              </div>
              <p
                className={cn(
                  "text-2xl font-bold",
                  s.danger && s.value > 0 ? "text-negative" : "",
                )}
              >
                {s.value}
              </p>
              <p className="text-xs text-muted-foreground mt-1">{s.sub}</p>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Alert feed */}
      <Card>
        <CardHeader className="pb-0 pt-5 px-5">
          <div className="flex items-center justify-between">
            <CardTitle className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
              Alert Feed
            </CardTitle>
            <div className="flex items-center gap-1">
              {(["active", "all"] as const).map((t) => (
                <button
                  key={t}
                  onClick={() => setTab(t)}
                  className={cn(
                    "rounded-lg px-3 py-1.5 text-xs font-semibold capitalize transition-colors",
                    tab === t
                      ? "bg-primary/15 text-primary"
                      : "text-muted-foreground hover:text-foreground",
                  )}
                >
                  {t === "active" ? "Active" : "All"}
                </button>
              ))}
            </div>
          </div>
        </CardHeader>
        <CardContent className="px-5 pb-2">
          {displayed.length === 0 ? (
            <p className="py-8 text-center text-sm text-muted-foreground">
              No alerts to display.
            </p>
          ) : (
            displayed.map((a) => <AlertRow key={a.id} alert={a} />)
          )}
        </CardContent>
      </Card>

      {/* Alert Rules */}
      <Card>
        <CardHeader className="pb-2 pt-5 px-5">
          <CardTitle className="text-[13px] font-semibold uppercase tracking-wider text-muted-foreground">
            Alert Rules
          </CardTitle>
        </CardHeader>
        <CardContent className="px-5 pb-5">
          <div className="space-y-3">
            {MOCK_ALERT_RULES.map((rule) => (
              <div
                key={rule.id}
                className="flex items-center justify-between rounded-xl border border-border/60 bg-muted/30 px-4 py-3"
              >
                <div className="flex items-center gap-3">
                  <div
                    className={cn(
                      "h-2 w-2 rounded-full",
                      rule.enabled ? "bg-positive" : "bg-muted-foreground",
                    )}
                  />
                  <div>
                    <p className="text-[13px] font-semibold">{rule.name}</p>
                    <p className="text-[11px] text-muted-foreground mt-0.5">
                      {rule.condition} · threshold: {rule.threshold}
                      {rule.symbol ? ` · ${rule.symbol}` : ""}
                    </p>
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  <span
                    className={cn(
                      "rounded-full px-2 py-0.5 text-[10px] font-semibold capitalize",
                      rule.type === "price"
                        ? "bg-primary/10 text-primary"
                        : rule.type === "volume"
                          ? "bg-yellow-500/10 text-yellow-600"
                          : "bg-muted text-muted-foreground",
                    )}
                  >
                    {rule.type}
                  </span>
                  <div
                    className={cn(
                      "h-5 w-9 rounded-full flex items-center transition-colors",
                      rule.enabled ? "bg-primary" : "bg-muted",
                    )}
                  >
                    <div
                      className={cn(
                        "h-4 w-4 rounded-full bg-white shadow transition-transform mx-0.5",
                        rule.enabled ? "translate-x-4" : "translate-x-0",
                      )}
                    />
                  </div>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
