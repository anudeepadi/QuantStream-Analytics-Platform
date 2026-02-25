"use client"

import Link from "next/link"
import { usePathname } from "next/navigation"
import { cn } from "@/lib/utils"
import { useAppStore } from "@/lib/store/app-store"
import { useAuth } from "@/lib/auth/auth-context"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import { Avatar, AvatarFallback } from "@/components/ui/avatar"
import {
  Activity,
  LayoutDashboard,
  BarChart3,
  Eye,
  Clock,
  Briefcase,
  TrendingUp,
  PieChart,
  LineChart,
  Bell,
  Monitor,
  Settings,
  ChevronLeft,
  ChevronRight,
} from "lucide-react"
import type { LucideIcon } from "lucide-react"

interface NavItem {
  readonly title: string
  readonly href: string
  readonly icon: LucideIcon
}

interface NavGroup {
  readonly label: string
  readonly items: readonly NavItem[]
}

const NAVIGATION: readonly NavGroup[] = [
  {
    label: "Main",
    items: [
      { title: "Dashboard", href: "/dashboard", icon: LayoutDashboard },
      { title: "Analytics", href: "/analytics", icon: BarChart3 },
    ],
  },
  {
    label: "Markets",
    items: [
      { title: "Live Data", href: "/markets", icon: Activity },
      { title: "Watchlist", href: "/markets/watchlist", icon: Eye },
      { title: "Historical", href: "/markets/historical", icon: Clock },
    ],
  },
  {
    label: "Portfolio",
    items: [
      { title: "Positions", href: "/portfolio", icon: Briefcase },
      { title: "P&L", href: "/portfolio/pnl", icon: TrendingUp },
      { title: "Allocation", href: "/portfolio/allocation", icon: PieChart },
    ],
  },
  {
    label: "Analysis",
    items: [
      { title: "Technical", href: "/analysis", icon: LineChart },
    ],
  },
  {
    label: "Alerts",
    items: [{ title: "Active Alerts", href: "/alerts", icon: Bell }],
  },
  {
    label: "System",
    items: [
      { title: "Metrics", href: "/system", icon: Monitor },
      { title: "Settings", href: "/settings", icon: Settings },
    ],
  },
]

function NavLink({
  item,
  isActive,
  collapsed,
}: {
  readonly item: NavItem
  readonly isActive: boolean
  readonly collapsed: boolean
}) {
  const Icon = item.icon

  const link = (
    <Link
      href={item.href}
      className={cn(
        "flex items-center gap-3 rounded-xl px-3 py-2 text-sm font-medium transition-all duration-150",
        isActive
          ? "bg-primary text-primary-foreground shadow-sm"
          : "text-muted-foreground hover:bg-accent hover:text-foreground"
      )}
    >
      <Icon className="h-4 w-4 shrink-0" />
      {!collapsed && <span>{item.title}</span>}
    </Link>
  )

  if (collapsed) {
    return (
      <Tooltip delayDuration={0}>
        <TooltipTrigger asChild>{link}</TooltipTrigger>
        <TooltipContent side="right">
          {item.title}
        </TooltipContent>
      </Tooltip>
    )
  }

  return link
}

export function Sidebar() {
  const pathname = usePathname()
  const collapsed = useAppStore((s) => s.sidebarCollapsed)
  const toggleSidebar = useAppStore((s) => s.toggleSidebar)
  const { user } = useAuth()

  const initials = user?.full_name
    ? user.full_name.split(" ").map((n) => n[0]).join("").toUpperCase().slice(0, 2)
    : user?.username?.slice(0, 2).toUpperCase() ?? "QS"

  return (
    <aside
      className={cn(
        "flex h-screen flex-col bg-sidebar border-r border-sidebar-border transition-all duration-300",
        collapsed ? "w-16" : "w-60"
      )}
    >
      {/* Logo */}
      <div className={cn(
        "flex h-16 shrink-0 items-center border-b border-sidebar-border",
        collapsed ? "justify-center px-0" : "gap-3 px-5"
      )}>
        <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-xl bg-primary shadow-sm">
          <Activity className="h-4 w-4 text-primary-foreground" />
        </div>
        {!collapsed && (
          <span className="text-[15px] font-bold tracking-tight text-foreground">
            QuantStream
          </span>
        )}
      </div>

      {/* Navigation */}
      <ScrollArea className="flex-1 py-4">
        <nav className={cn("space-y-0.5", collapsed ? "px-2" : "px-3")}>
          {NAVIGATION.map((group, groupIdx) => (
            <div key={group.label} className={groupIdx > 0 ? "mt-4" : ""}>
              {!collapsed && (
                <p className="mb-1 px-3 text-[10px] font-semibold uppercase tracking-widest text-muted-foreground/70">
                  {group.label}
                </p>
              )}
              <div className="space-y-0.5">
                {group.items.map((item) => (
                  <NavLink
                    key={item.href}
                    item={item}
                    isActive={pathname === item.href}
                    collapsed={collapsed}
                  />
                ))}
              </div>
            </div>
          ))}
        </nav>
      </ScrollArea>

      {/* Footer: user + collapse */}
      <div className="shrink-0 border-t border-sidebar-border p-3">
        {!collapsed && (
          <div className="mb-2 flex items-center gap-3 rounded-xl px-2 py-2 hover:bg-accent transition-colors cursor-pointer">
            <Avatar className="h-7 w-7 shrink-0">
              <AvatarFallback className="text-[10px] bg-primary/10 text-primary font-semibold">
                {initials}
              </AvatarFallback>
            </Avatar>
            <div className="min-w-0 flex-1">
              <p className="truncate text-[13px] font-semibold leading-tight">
                {user?.full_name ?? user?.username ?? "Demo User"}
              </p>
              <p className="truncate text-[11px] text-muted-foreground leading-tight">
                {user?.role ?? "analyst"}
              </p>
            </div>
          </div>
        )}
        <button
          onClick={toggleSidebar}
          className={cn(
            "flex w-full items-center justify-center rounded-xl p-2 text-muted-foreground hover:bg-accent hover:text-foreground transition-all duration-150",
            collapsed && "aspect-square"
          )}
          aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
        >
          {collapsed ? (
            <ChevronRight className="h-4 w-4" />
          ) : (
            <div className="flex items-center gap-2 text-xs font-medium">
              <ChevronLeft className="h-4 w-4" />
              <span>Collapse</span>
            </div>
          )}
        </button>
      </div>
    </aside>
  )
}
