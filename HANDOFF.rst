QuantStream Analytics Platform — Agent Handoff Document
=======================================================

Date: 2026-03-07
Repo: https://github.com/anudeepadi/QuantStream-Analytics-Platform.git
Branch: master (latest commit: 3f1a766)

1. What This Project Is
-----------------------

A production-grade financial analytics platform (Databricks portfolio piece) with:

- Real-time market data ingestion via Kafka + WebSocket
- Delta Lake medallion architecture (Bronze > Silver > Gold)
- Spark stream processing for technical indicators
- MLflow for anomaly detection model lifecycle
- FastAPI backend with JWT auth, PostgreSQL, Redis
- Next.js 16 frontend (React 19, TypeScript, TailwindCSS 4, shadcn/ui)
- Docker Compose with 8 services (API, Frontend, Postgres, Redis, Kafka, Zookeeper, Prometheus, Grafana)

2. Current State of Progress
-----------------------------

DONE (Frontend + Backend API layer):

- FastAPI Backend: Auth (JWT), Market Data, Portfolio, Alerts, System Health, WebSocket endpoints
- Next.js Frontend: 14 pages, all wired to live API hooks with mock fallbacks
- Auth System: Login page, JWT flow, AuthProvider context, demo users (admin/admin123, analyst/analyst123, trader/trader123)
- Dashboard: KPI cards (live from usePortfolioSummary), charts, market perf, top movers, sectors, activity feed
- Markets Pages: Live data, watchlist, historical — all hooked to API
- Portfolio Pages: Positions, P&L, allocation — all hooked to API
- Alerts Page: Alert feed, rules, stats — live hooks + Radix Switch
- Analysis Page: 8 technical indicators (RSI, MACD, SMA, Bollinger, ADX, CCI, Stoch), symbol selector
- System Page: Service health, metrics grid, WebSocket live indicator
- Settings Page: Profile, notifications (Switch), API key, theme selector
- Design System: Sage-green oklch theme, .interface-design/system.md extracted
- WebSocket Hooks: useMarketWebSocket(symbols), useSystemWebSocket() with exponential backoff reconnect
- Loading States: CardSkeleton, TableSkeleton on all pages
- React Query: All hooks with refetchInterval (10-60s), enabled flags, mock fallbacks

NOT DONE (Backend data pipeline + infrastructure):

All 20 Task Master tasks are in pending status. The major unfinished work:

- Tasks 1: Project Foundation & Infrastructure Setup (Docker networking, CI/CD) — HIGH
- Tasks 2-4: Data Ingestion Pipeline (REST connectors, WebSocket feeds, Kafka producers) — HIGH
- Tasks 5-6: Kafka Infrastructure + Delta Lake Bronze Layer — HIGH
- Tasks 7-9: Stream Processing + Silver/Gold ETL layers — HIGH/MEDIUM
- Tasks 10-11: Feature Engineering + Feature Store — MEDIUM
- Tasks 12-14: MLflow + Anomaly Detection Model + Model Serving — MEDIUM
- Tasks 15-16: Monitoring Stack + Security & Compliance — HIGH
- Tasks 17-20: Performance Tuning, Testing Suite, Dashboard integration, Docs — MEDIUM

SUMMARY: The frontend is complete and polished. The backend API serves data (with mock/seed data). The actual data pipeline (Kafka > Spark > Delta Lake > ML) has not been implemented yet.

3. Architecture
---------------

::

  FRONTEND (Next.js 16)
    React 19 + TypeScript + TailwindCSS 4 + shadcn/ui + Recharts
    Port 3000 (docker: 13000)
    React Query hooks > /api/* proxy > FastAPI
    WebSocket hooks > ws://localhost:18000/ws/*
           |
           | HTTP + WebSocket
           v
  BACKEND (FastAPI + Uvicorn)
    JWT Auth, REST endpoints, WebSocket broadcast
    Port 8000 (docker: 18000)
    Services: auth_service, database_service, redis_service
    |              |                |
    PostgreSQL     Redis            Kafka (not wired yet)
    Port 5432      Port 6379        Port 9092
    (15432)        (16379)          (19092)
           |
           v (NOT YET BUILT)
  DATA PIPELINE (Spark + Delta Lake)
    Bronze > Silver > Gold layers
    Feature Store > MLflow > Anomaly Detection
    Prometheus + Grafana monitoring

4. How to Run
-------------

Prerequisites: Docker & Docker Compose, Node.js 18+, Python 3.11+

Option A: Docker Compose (full stack)::

  git clone https://github.com/anudeepadi/QuantStream-Analytics-Platform.git
  cd QuantStream-Analytics-Platform
  cp .env.example .env
  docker compose up -d

Service URLs:
  Frontend:   http://localhost:13000
  API:        http://localhost:18000
  API Docs:   http://localhost:18000/docs
  Grafana:    http://localhost:13001 (admin/admin)
  Prometheus: http://localhost:19090

Option B: Local development::

  # Terminal 1 — Backend
  cd QuantStream-Analytics-Platform
  pip install -r requirements.txt
  docker compose up -d postgres redis
  uvicorn src.dashboard.backend.api.main:app --host 0.0.0.0 --port 8000 --reload

  # Terminal 2 — Frontend
  cd src/dashboard/frontend-next
  npm install
  npx next start  # or: npx next dev (for hot reload)

Demo Login Credentials:
  admin    / admin123    (Administrator)
  analyst  / analyst123  (Analyst)
  trader   / trader123   (Trader)

5. Key File Paths
-----------------

Frontend (src/dashboard/frontend-next/)::

  app/
    layout.tsx                    Root layout (providers, fonts)
    globals.css                   Design tokens (oklch colors)
    page.tsx                      Landing > redirects to /dashboard
    login/page.tsx                Auth page
    (dashboard)/
      layout.tsx                  Sidebar + header shell
      dashboard/page.tsx          Main dashboard (KPI, charts, activity)
      markets/page.tsx            Live market data
      markets/watchlist/page.tsx
      markets/historical/page.tsx
      portfolio/page.tsx          Positions table
      portfolio/pnl/page.tsx
      portfolio/allocation/page.tsx
      alerts/page.tsx             Alert feed + rules
      analysis/page.tsx           Technical indicators
      analytics/page.tsx          Analytics overview
      settings/page.tsx           User preferences
      system/page.tsx             Infrastructure health

  components/
    ui/                           shadcn primitives (button, card, switch, etc.)
    layout/sidebar.tsx            Collapsible sidebar (w-60/w-16)
    layout/header.tsx             Top bar (search, theme, user menu)
    dashboard/kpi-card.tsx        KPI card with sparkline SVG
    dashboard/overview-chart.tsx  Portfolio vs benchmark chart
    dashboard/market-performance.tsx
    dashboard/top-movers.tsx
    dashboard/sector-performance.tsx
    dashboard/portfolio-allocation.tsx
    dashboard/recent-activity.tsx
    charts/area-chart.tsx         Recharts wrapper
    shared/card-skeleton.tsx      Loading skeletons
    auth/auth-guard.tsx           Route protection
    providers/query-provider.tsx  React Query setup

  lib/
    api/                          API client functions
      client.ts                   Base fetch with JWT Bearer header
      market-data.ts, portfolio.ts, alerts.ts, system.ts
    hooks/                        React Query hooks
      use-market-data.ts          useMarketOverview, useTechnicalIndicators, etc.
      use-portfolio.ts            usePortfolioSummary, usePositions, etc.
      use-alerts.ts               useAlerts, useAlertRules, useAlertStatistics
      use-system.ts               useSystemHealth
      use-websocket.ts            useMarketWebSocket, useSystemWebSocket
    auth/auth-context.tsx         AuthProvider + useAuth hook
    store/app-store.ts            Zustand store (sidebar state)
    mock-data/                    Fallback data for all pages
    types/                        TypeScript interfaces
    utils/format.ts               formatCurrency, formatDate, etc.

Backend (src/dashboard/backend/)::

  api/
    main.py                       FastAPI app, lifespan, CORS, demo user seeding
    endpoints/
      auth.py                     /api/auth/* (login, register, refresh, me)
      market_data.py              /api/market/* (current, historical, indicators)
      portfolio.py                /api/portfolio/* (positions, pnl, allocation)
      alerts.py                   /api/alerts/* (CRUD)
      system_metrics.py           /api/system/* (health, metrics)
      websocket_manager.py        /ws/* (market-data, alerts)
  services/
    auth_service.py               JWT, bcrypt, user auth
    database_service.py           AsyncPG pool, CRUD
    redis_service.py              Redis caching
  models/                         Pydantic models
  websocket/                      WebSocket connection manager

Config::

  docker-compose.yml              8 services
  Dockerfile                      Python backend image
  .env / .env.example             Environment variables
  config/prometheus.yml           Prometheus scrape config
  .interface-design/system.md     UI design system tokens
  .taskmaster/tasks/tasks.json    20 pending pipeline tasks

6. Design System
----------------

- Theme: Sage green (oklch hue 142) — white cards floating on sage background
- Colors: oklch color space, light + dark mode
- Typography: Plus Jakarta Sans (body), Geist Mono (data/code)
- Body text: text-[13px] dominant (79 occurrences)
- Radius: rounded-xl for cards/buttons, rounded-full for badges/dots
- Depth: Borders (107x) define structure, shadows only on cards
- Spacing: 4px base, p-5 card padding, space-y-6 section gaps, gap-4 grids
- Interactions: All buttons have active:scale-[0.97] press feedback
- Financial: --positive (green), --negative (red) semantic colors
- Full spec: .interface-design/system.md

7. What to Do Next
------------------

Immediate priorities (in dependency order):

1. Data Pipeline Foundation (Tasks 1-2)
   - Wire Kafka producers to ingest real market data (yfinance is already a dependency)
   - Set up Delta Lake Bronze layer tables
   - Connect API endpoints to real DB queries instead of seed data

2. Stream Processing (Tasks 5-7)
   - Kafka consumer > Spark Structured Streaming > Delta Lake writes
   - Technical indicator calculations in Spark (replace frontend mock math)

3. Silver/Gold ETL (Tasks 8-9)
   - Data quality, deduplication, aggregation layers

4. ML Pipeline (Tasks 12-14)
   - MLflow experiment tracking
   - Anomaly detection model training + serving

5. Testing (Task 18)
   - Backend: pytest with 80% coverage
   - Frontend: component tests, E2E flows
   - Currently zero test coverage

6. Monitoring (Task 15)
   - Wire Prometheus metrics from backend
   - Grafana dashboards for pipeline health

8. Tech Stack
-------------

::

  Frontend:       Next.js 16.1.6, React 19.2.3
  Styling:        TailwindCSS 4, shadcn/ui + Radix
  State:          Zustand 5.0.11
  Data Fetching:  TanStack React Query 5.90.21
  Charts:         Recharts 3.7.0
  Backend:        FastAPI 0.104.1
  Auth:           PyJWT 2.8.0 + passlib (bcrypt)
  Database:       PostgreSQL 15 (asyncpg 0.29.0)
  Cache:          Redis 7 (redis-py 5.0.1)
  Streaming:      Kafka (Confluent 7.4.0)
  Processing:     PySpark
  Storage:        Delta Lake
  ML:             MLflow + scikit-learn + XGBoost
  Monitoring:     Prometheus + Grafana
  Languages:      TypeScript 5 / Python 3.11

9. Known Issues
---------------

1. Docker daemon must be running for docker compose up
2. API proxy: Frontend uses Next.js rewrites (/api/* > http://api:8000/api/*) in Docker; locally uses NEXT_PUBLIC_API_BASE_URL=http://localhost:8000
3. Mock data fallback: Every frontend page uses `const data = apiData ?? MOCK_FALLBACK`
4. WebSocket reconnect: Exponential backoff with 30s cap, no auth token on WS yet
5. No tests exist yet — zero coverage on both frontend and backend
6. Task Master tasks are all pending — generated from PRD but not started
7. Old Streamlit frontend at src/dashboard/frontend/ — legacy, ignore it
8. A PreToolUse hook blocks creation of .md/.txt files that are not README/CLAUDE/AGENTS/CONTRIBUTING

10. Commit History
------------------

::

  3f1a766 refactor: design critique — craft, composition, and structure improvements
  c65fcd6 feat: wire all dashboard pages to live API hooks with WebSocket streaming
  2eec6f0 feat: complete Next.js frontend + backend overhaul
  b432a0b fs (initial commit)
