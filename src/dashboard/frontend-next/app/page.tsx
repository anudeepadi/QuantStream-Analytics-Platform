import Link from "next/link"
import { Activity, BarChart3, TrendingUp, Shield, Globe, Briefcase, LineChart, FileText, Zap, ArrowRight } from "lucide-react"

const STATS = [
  { label: "Assets Tracked", value: "2,400+" },
  { label: "Daily Volume", value: "$1.2B" },
  { label: "Data Latency", value: "12ms" },
  { label: "Uptime", value: "99.9%" },
]

const FEATURES = [
  { icon: BarChart3, title: "Real-time Analytics", desc: "Stream live market data with sub-second latency across all major exchanges." },
  { icon: TrendingUp, title: "Portfolio Tracking", desc: "Monitor P&L, positions, and allocation with institutional-grade precision." },
  { icon: Shield, title: "Risk Management", desc: "Set alerts and thresholds to protect against unexpected market moves." },
  { icon: Globe, title: "Global Markets", desc: "Access equities, ETFs, forex, and commodities from a single dashboard." },
  { icon: Briefcase, title: "Multi-Asset Support", desc: "Manage diversified portfolios across all asset classes seamlessly." },
  { icon: LineChart, title: "Technical Analysis", desc: "RSI, MACD, Bollinger Bands and 50+ indicators at your fingertips." },
  { icon: FileText, title: "Smart Reporting", desc: "Generate performance reports with one click for any date range." },
  { icon: Zap, title: "Instant Alerts", desc: "Get notified on price targets, volume spikes, and technical signals." },
]

const PREVIEW_KPIS = [
  { label: "Portfolio Value", value: "$284,521", change: "+1.15% today" },
  { label: "Total P&L", value: "$84,521", change: "+42.26% all-time" },
  { label: "Daily Gain", value: "$3,241", change: "↑ outperforming" },
  { label: "Open Positions", value: "8 active", change: "4 sectors" },
]

const PREVIEW_BARS = [62, 45, 78, 30, 55, 48, 35, 68]

export default function LandingPage() {
  return (
    <div className="min-h-screen bg-background flex flex-col">
      {/* Nav */}
      <header className="sticky top-0 z-50 w-full border-b border-border/60 bg-background/95 backdrop-blur">
        <div className="mx-auto flex h-14 max-w-6xl items-center justify-between px-6">
          <div className="flex items-center gap-2.5">
            <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-primary">
              <Activity className="h-4 w-4 text-primary-foreground" />
            </div>
            <span className="text-base font-bold">QuantStream</span>
          </div>
          <nav className="hidden md:flex items-center gap-6 text-sm text-muted-foreground">
            <a href="#features" className="hover:text-foreground transition-colors">Features</a>
            <a href="#stats" className="hover:text-foreground transition-colors">Platform</a>
            <a href="#" className="hover:text-foreground transition-colors">Docs</a>
          </nav>
          <div className="flex items-center gap-2">
            <Link
              href="/login"
              className="text-sm font-medium text-muted-foreground hover:text-foreground transition-colors px-3 py-1.5"
            >
              Sign in
            </Link>
            <Link
              href="/dashboard"
              className="rounded-xl bg-primary px-4 py-1.5 text-sm font-semibold text-primary-foreground hover:bg-primary/90 transition-colors"
            >
              Get started
            </Link>
          </div>
        </div>
      </header>

      <main className="flex-1">
        {/* Hero */}
        <section className="mx-auto max-w-6xl px-6 py-20 md:py-28">
          <div className="grid gap-12 lg:grid-cols-2 lg:items-center">
            {/* Copy */}
            <div className="space-y-6">
              <div className="inline-flex items-center rounded-full border border-primary/20 bg-primary/8 px-3 py-1 text-xs font-medium text-primary">
                Real-time · Institutional-grade · Multi-asset
              </div>
              <h1 className="text-4xl font-bold tracking-tight lg:text-5xl leading-tight">
                Analytics for<br />every market.
              </h1>
              <p className="text-lg text-muted-foreground leading-relaxed max-w-md">
                Monitor portfolios, track performance, and make data-driven decisions with professional tools built for serious investors.
              </p>
              <div className="flex flex-wrap items-center gap-3">
                <Link
                  href="/dashboard"
                  className="inline-flex items-center gap-2 rounded-xl bg-primary px-6 py-3 text-sm font-semibold text-primary-foreground hover:bg-primary/90 transition-colors"
                >
                  Open Dashboard
                  <ArrowRight className="h-4 w-4" />
                </Link>
                <Link
                  href="/login"
                  className="inline-flex items-center gap-2 rounded-xl border border-border px-6 py-3 text-sm font-semibold hover:bg-muted transition-colors"
                >
                  Sign in
                </Link>
              </div>
            </div>

            {/* Mini dashboard preview */}
            <div className="rounded-2xl border border-border/50 bg-card p-5 shadow-xl">
              <div className="mb-4 flex items-center gap-1.5">
                <div className="h-2.5 w-2.5 rounded-full bg-red-400/70" />
                <div className="h-2.5 w-2.5 rounded-full bg-yellow-400/70" />
                <div className="h-2.5 w-2.5 rounded-full bg-positive/70" />
                <div className="ml-auto text-[10px] text-muted-foreground font-mono">quantstream · dashboard</div>
              </div>
              <div className="grid grid-cols-2 gap-2.5 mb-3">
                {PREVIEW_KPIS.map((kpi) => (
                  <div key={kpi.label} className="rounded-xl border border-border/60 bg-background p-3">
                    <p className="text-[9px] text-muted-foreground uppercase tracking-wider font-semibold">{kpi.label}</p>
                    <p className="text-sm font-bold mt-0.5">{kpi.value}</p>
                    <p className="text-[10px] text-positive font-medium mt-0.5">{kpi.change}</p>
                  </div>
                ))}
              </div>
              <div className="rounded-xl border border-border/40 bg-background/50 p-3">
                <p className="text-[9px] text-muted-foreground font-semibold uppercase tracking-wider mb-2">Sector Performance</p>
                <div className="flex items-end gap-1 h-14">
                  {PREVIEW_BARS.map((h, i) => (
                    <div
                      key={i}
                      className="flex-1 rounded-t-sm transition-all"
                      style={{
                        height: `${h}%`,
                        backgroundColor: h > 50
                          ? "oklch(0.54 0.185 142)"
                          : "oklch(0.62 0.22 25)",
                      }}
                    />
                  ))}
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* Stats strip */}
        <section id="stats" className="border-y border-border/50 bg-card">
          <div className="mx-auto max-w-6xl px-6 py-10">
            <div className="grid grid-cols-2 gap-8 md:grid-cols-4">
              {STATS.map((stat) => (
                <div key={stat.label} className="text-center">
                  <p className="text-3xl font-bold">{stat.value}</p>
                  <p className="text-sm text-muted-foreground mt-1">{stat.label}</p>
                </div>
              ))}
            </div>
          </div>
        </section>

        {/* Features */}
        <section id="features" className="mx-auto max-w-6xl px-6 py-20">
          <div className="text-center mb-12">
            <h2 className="text-3xl font-bold tracking-tight">Everything you need</h2>
            <p className="text-muted-foreground mt-2 max-w-md mx-auto">
              A complete analytics suite for monitoring, analyzing, and optimizing your investments.
            </p>
          </div>
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            {FEATURES.map((f) => (
              <div
                key={f.title}
                className="rounded-2xl border border-border/60 bg-card p-5 hover:border-primary/40 hover:shadow-md transition-all"
              >
                <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-primary/10 mb-3">
                  <f.icon className="h-5 w-5 text-primary" />
                </div>
                <h3 className="font-semibold text-sm mb-1">{f.title}</h3>
                <p className="text-xs text-muted-foreground leading-relaxed">{f.desc}</p>
              </div>
            ))}
          </div>
        </section>

        {/* CTA */}
        <section className="bg-primary">
          <div className="mx-auto max-w-6xl px-6 py-16 text-center">
            <h2 className="text-3xl font-bold text-primary-foreground">Ready to get started?</h2>
            <p className="mt-2 max-w-sm mx-auto" style={{ color: "oklch(1 0 0 / 0.65)" }}>
              Access the full dashboard with demo credentials — no signup required.
            </p>
            <div className="flex items-center justify-center gap-3 mt-6">
              <Link
                href="/dashboard"
                className="inline-flex items-center gap-2 rounded-xl bg-white px-6 py-3 text-sm font-semibold text-primary hover:bg-white/90 transition-colors"
              >
                Open Dashboard
                <ArrowRight className="h-4 w-4" />
              </Link>
            </div>
            <p className="text-xs mt-4" style={{ color: "oklch(1 0 0 / 0.45)" }}>
              Demo credentials: admin / admin123
            </p>
          </div>
        </section>
      </main>

      {/* Footer */}
      <footer className="border-t border-border/50">
        <div className="mx-auto max-w-6xl px-6 py-6 flex items-center justify-between text-xs text-muted-foreground">
          <span>© 2026 QuantStream Analytics</span>
          <div className="flex items-center gap-4">
            <a href="#" className="hover:text-foreground transition-colors">Privacy</a>
            <a href="#" className="hover:text-foreground transition-colors">Terms</a>
          </div>
        </div>
      </footer>
    </div>
  )
}
