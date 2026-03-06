"use client";

import { useState } from "react";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import { cn } from "@/lib/utils";
import { User, Bell, Key, Palette, Save, Eye, EyeOff } from "lucide-react";
import { useAuth } from "@/lib/auth/auth-context";

const NOTIFICATION_SETTINGS = [
  {
    id: "email_alerts",
    label: "Email Alerts",
    desc: "Receive alert notifications via email",
  },
  {
    id: "push_alerts",
    label: "Push Notifications",
    desc: "Browser push notifications for critical alerts",
  },
  {
    id: "price_alerts",
    label: "Price Alerts",
    desc: "Notify when price targets are hit",
  },
  {
    id: "volume_alerts",
    label: "Volume Spikes",
    desc: "Notify on unusual trading volume",
  },
  {
    id: "portfolio_daily",
    label: "Daily Portfolio Summary",
    desc: "Daily email with portfolio performance",
  },
  {
    id: "market_open",
    label: "Market Open/Close",
    desc: "Notification at market open and close",
  },
];

export default function SettingsPage() {
  const { user } = useAuth();
  const [notifications, setNotifications] = useState<Record<string, boolean>>({
    email_alerts: true,
    push_alerts: false,
    price_alerts: true,
    volume_alerts: true,
    portfolio_daily: true,
    market_open: false,
  });
  const [showApiKey, setShowApiKey] = useState(false);
  const [theme, setTheme] = useState<"light" | "dark" | "system">("light");
  const [saved, setSaved] = useState(false);

  const toggleNotification = (id: string) => {
    setNotifications((prev) => ({ ...prev, [id]: !prev[id] }));
  };

  const handleSave = () => {
    setSaved(true);
    setTimeout(() => setSaved(false), 2000);
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Settings</h1>
        <p className="text-sm text-muted-foreground mt-1">
          Manage your account and platform preferences.
        </p>
      </div>

      {/* Profile */}
      <Card>
        <CardHeader className="pt-5 px-5 pb-4">
          <div className="flex items-center gap-2">
            <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-primary/10">
              <User className="h-4 w-4 text-primary" />
            </div>
            <div>
              <CardTitle className="text-[14px] font-semibold">
                Profile
              </CardTitle>
              <CardDescription className="text-[12px]">
                Update your personal information
              </CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="px-5 pb-5">
          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-1.5">
              <Label htmlFor="full-name" className="text-[12px] font-medium">
                Full Name
              </Label>
              <Input
                id="full-name"
                defaultValue={user?.full_name ?? "Admin User"}
                className="h-9 rounded-xl text-sm"
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="username" className="text-[12px] font-medium">
                Username
              </Label>
              <Input
                id="username"
                defaultValue={user?.username ?? "admin"}
                className="h-9 rounded-xl text-sm"
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="email" className="text-[12px] font-medium">
                Email
              </Label>
              <Input
                id="email"
                type="email"
                defaultValue={user?.email ?? "admin@quantstream.io"}
                className="h-9 rounded-xl text-sm"
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="role" className="text-[12px] font-medium">
                Role
              </Label>
              <Input
                id="role"
                defaultValue={user?.role ?? "Portfolio Manager"}
                className="h-9 rounded-xl text-sm"
              />
            </div>
          </div>
          <Button
            onClick={handleSave}
            className="mt-4 h-9 rounded-xl text-sm"
            size="sm"
          >
            {saved ? (
              "Saved!"
            ) : (
              <>
                <Save className="h-3.5 w-3.5 mr-1.5" />
                Save changes
              </>
            )}
          </Button>
        </CardContent>
      </Card>

      {/* Notifications */}
      <Card>
        <CardHeader className="pt-5 px-5 pb-4">
          <div className="flex items-center gap-2">
            <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-primary/10">
              <Bell className="h-4 w-4 text-primary" />
            </div>
            <div>
              <CardTitle className="text-[14px] font-semibold">
                Notifications
              </CardTitle>
              <CardDescription className="text-[12px]">
                Choose what you want to be notified about
              </CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="px-5 pb-5">
          <div className="divide-y divide-border/50">
            {NOTIFICATION_SETTINGS.map((setting) => (
              <div
                key={setting.id}
                className="flex items-center justify-between py-3"
              >
                <div>
                  <p className="text-[13px] font-medium">{setting.label}</p>
                  <p className="text-[11px] text-muted-foreground mt-0.5">
                    {setting.desc}
                  </p>
                </div>
                <Switch
                  checked={notifications[setting.id] ?? false}
                  onCheckedChange={() => toggleNotification(setting.id)}
                  size="sm"
                  aria-label={setting.label}
                />
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* API Key */}
      <Card>
        <CardHeader className="pt-5 px-5 pb-4">
          <div className="flex items-center gap-2">
            <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-primary/10">
              <Key className="h-4 w-4 text-primary" />
            </div>
            <div>
              <CardTitle className="text-[14px] font-semibold">
                API Access
              </CardTitle>
              <CardDescription className="text-[12px]">
                Manage your API keys for programmatic access
              </CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="px-5 pb-5">
          <div className="space-y-3">
            <div className="space-y-1.5">
              <Label className="text-[12px] font-medium">API Key</Label>
              <div className="flex gap-2">
                <Input
                  readOnly
                  type={showApiKey ? "text" : "password"}
                  value="qs_live_4x8k2m9p1q3r7s5t6u0v"
                  className="h-9 rounded-xl text-sm font-mono flex-1"
                />
                <button
                  onClick={() => setShowApiKey((v) => !v)}
                  className="flex h-9 w-9 items-center justify-center rounded-xl border border-border hover:bg-muted transition-colors"
                >
                  {showApiKey ? (
                    <EyeOff className="h-4 w-4 text-muted-foreground" />
                  ) : (
                    <Eye className="h-4 w-4 text-muted-foreground" />
                  )}
                </button>
              </div>
            </div>
            <p className="text-[11px] text-muted-foreground">
              Keep your API key secure. Do not share it publicly.
            </p>
            <div className="flex gap-2">
              <Button
                variant="outline"
                size="sm"
                className="h-8 rounded-xl text-xs"
              >
                Regenerate Key
              </Button>
              <Button
                variant="outline"
                size="sm"
                className="h-8 rounded-xl text-xs"
              >
                View Docs
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Appearance */}
      <Card>
        <CardHeader className="pt-5 px-5 pb-4">
          <div className="flex items-center gap-2">
            <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-primary/10">
              <Palette className="h-4 w-4 text-primary" />
            </div>
            <div>
              <CardTitle className="text-[14px] font-semibold">
                Appearance
              </CardTitle>
              <CardDescription className="text-[12px]">
                Customize how the platform looks
              </CardDescription>
            </div>
          </div>
        </CardHeader>
        <CardContent className="px-5 pb-5">
          <div className="space-y-3">
            <Label className="text-[12px] font-medium">Theme</Label>
            <div className="flex gap-2">
              {(["light", "dark", "system"] as const).map((t) => (
                <button
                  key={t}
                  onClick={() => setTheme(t)}
                  className={cn(
                    "flex-1 rounded-xl border py-3 text-[12px] font-semibold capitalize transition-all active:scale-[0.97]",
                    theme === t
                      ? "border-primary bg-primary/10 text-primary"
                      : "border-border text-muted-foreground hover:text-foreground hover:bg-muted",
                  )}
                >
                  {t}
                </button>
              ))}
            </div>
            <p className="text-[11px] text-muted-foreground">
              Theme preference is saved locally in your browser.
            </p>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
