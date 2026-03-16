# QuantStream Analytics Platform

**Real-time financial analytics dashboard with live market data, portfolio tracking, and technical analysis.**

**[Live Demo](https://frontend-next-sooty.vercel.app)** | **[API Docs](https://api-production-b5c3.up.railway.app/docs)** | **[API Health](https://api-production-b5c3.up.railway.app/health)**

> Demo credentials: `admin` / `admin123`

---

## What It Does

QuantStream is a full-stack financial analytics platform that streams real-time stock, crypto, and forex data into an interactive dashboard. Built for portfolio managers, quant analysts, and serious investors who need institutional-grade tools without the Bloomberg terminal price tag.

### Key Features

- **Live Market Data** — Real-time stock quotes from Finnhub API with WebSocket streaming
- **Historical Charts** — OHLCV candlestick data via Yahoo Finance with interactive time-range selection
- **Technical Indicators** — RSI, SMA, EMA, MACD, Bollinger Bands computed from real market data
- **Portfolio Tracking** — Positions, P&L analysis, allocation breakdown with live price updates
- **Multi-Asset Coverage** — US equities (AAPL, TSLA, NVDA...), crypto (BTC, ETH, SOL), forex (EUR/USD, GBP/USD)
- **Alert System** — Price targets, volume anomalies, RSI signals, technical breakouts
- **System Monitoring** — Real-time CPU, memory, network metrics via WebSocket
- **Auth System** — JWT-based authentication with role-based access (admin, analyst, trader)

## Architecture

```
                    +-----------------+
                    |   Vercel CDN    |
                    |  (Next.js 16)   |
                    +--------+--------+
                             |
                    REST + WebSocket
                             |
              +--------------+--------------+
              |      Railway (Backend)      |
              |   FastAPI + Uvicorn (4w)    |
              +---+----------+----------+---+
                  |          |          |
           +------+   +-----+-----+   +--------+
           |Finnhub|   |PostgreSQL |   |  Redis  |
           |  API  |   | (Railway) |   |(Railway)|
           +---+---+   +-----------+   +---------+
               |
          +----+----+
          |  Yahoo  |
          | Finance |
          +---------+
```

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Frontend** | Next.js 16, React 19, Tailwind CSS 4, Recharts | Dashboard UI with real-time updates |
| **State** | TanStack Query, Zustand | Server state caching + client state |
| **Backend** | FastAPI, Uvicorn, asyncpg, httpx | REST API + WebSocket server |
| **Data** | Finnhub (quotes), Yahoo Finance (history) | Real-time + historical market data |
| **Database** | PostgreSQL | Users, positions, transactions, alerts |
| **Cache** | Redis | API response caching, session tokens |
| **Auth** | JWT (PyJWT), bcrypt | Token-based authentication |
| **Deploy** | Vercel (frontend), Railway (backend + DB + Redis) | Production hosting |

## Live Demo

**Frontend:** https://frontend-next-sooty.vercel.app

| Page | What You'll See |
|------|----------------|
| `/` | Landing page with platform overview |
| `/login` | Sign in (admin / admin123) |
| `/dashboard` | KPIs, portfolio chart, sector performance, top movers |
| `/markets` | Live stock table with real-time Finnhub quotes |
| `/markets/historical` | Interactive OHLCV charts for any symbol |
| `/markets/watchlist` | Curated watchlist with alerts |
| `/portfolio` | Current positions with live P&L |
| `/portfolio/pnl` | Profit & loss analysis |
| `/portfolio/allocation` | Asset allocation breakdown |
| `/analysis` | Technical indicators (RSI, MACD, Bollinger) |
| `/alerts` | Active alert rules and history |
| `/system` | Server metrics (CPU, RAM, network) |
| `/settings` | User preferences |

**Backend API:** https://api-production-b5c3.up.railway.app

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/auth/login` | POST | JWT authentication |
| `/api/v1/market-data/overview` | GET | Live quotes for 8 major stocks |
| `/api/v1/market-data/historical` | POST | Historical OHLCV candles |
| `/api/v1/market-data/current/{symbol}` | GET | Current price + technical indicators |
| `/api/v1/portfolio/summary` | GET | Portfolio value, P&L, positions |
| `/api/v1/portfolio/positions` | GET | All open positions |
| `/api/v1/alerts/` | GET | Active alerts |
| `/api/v1/system/metrics` | GET | Server health metrics |
| `/ws/market-data` | WS | Real-time trade stream |
| `/docs` | GET | Swagger UI |

## Tech Stack

**Frontend**
- Next.js 16 (App Router, static export)
- React 19 with Server Components
- Tailwind CSS 4 with oklch color space
- Recharts for data visualization
- TanStack Query v5 for data fetching
- Zustand for client state
- Radix UI primitives (shadcn/ui)

**Backend**
- Python 3.11 + FastAPI
- asyncpg (async PostgreSQL)
- Redis for caching (30s quote TTL)
- Finnhub API (real-time quotes + WebSocket)
- Yahoo Finance v8 (historical candles)
- NumPy for technical indicator computation
- JWT authentication with bcrypt

**Infrastructure**
- Vercel (frontend CDN + edge)
- Railway (backend + PostgreSQL + Redis)
- Docker multi-stage builds
- GitHub Actions CI/CD

## Design System

Custom design system built for financial data density:

- **Color:** oklch-based sage green palette (hue 142) with semantic positive/negative
- **Typography:** Plus Jakarta Sans (body) + Geist Mono (data)
- **Density:** Compact 13px body text, 20px card padding, 4px base unit
- **Depth:** Borders define structure, cards float with subtle shadows
- **Components:** 20+ custom components (KPI cards, data tables, chart wrappers, status indicators)

## Quick Start (Local Development)

```bash
# Clone
git clone https://github.com/anudeepadi/QuantStream-Analytics-Platform.git
cd QuantStream-Analytics-Platform

# Backend
pip install -r requirements.txt
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/quantstream
export REDIS_URL=redis://localhost:6379
export FINNHUB_API_KEY=your_key_here
python -m uvicorn src.dashboard.backend.api.main:app --reload

# Frontend
cd src/dashboard/frontend-next
npm install
echo "NEXT_PUBLIC_API_BASE_URL=http://localhost:8000" > .env.local
npm run dev
```

## API Examples

```bash
# Login
TOKEN=$(curl -s localhost:8000/api/v1/auth/login \
  -X POST -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}' \
  | jq -r '.access_token')

# Get live quotes
curl -s localhost:8000/api/v1/market-data/overview \
  -H "Authorization: Bearer $TOKEN" | jq '.[0]'
# → {"symbol":"AAPL","price":253.30,"change_percent":1.27,...}

# Get historical candles
curl -s localhost:8000/api/v1/market-data/historical \
  -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"symbol":"AAPL","start_date":"2025-12-01T00:00:00","end_date":"2026-03-16T00:00:00","interval":"1d"}'
# → [{"date":"2025-12-01","open":280.15,"close":274.11,...}, ...]

# Get technical indicators
curl -s localhost:8000/api/v1/market-data/current/AAPL?include_indicators=true \
  -H "Authorization: Bearer $TOKEN" | jq '.indicators'
# → [{"name":"RSI_14","value":23.69,"signal":"buy"}, ...]
```

## Project Structure

```
QuantStream-Analytics-Platform/
├── src/dashboard/
│   ├── frontend-next/           # Next.js 16 frontend
│   │   ├── app/                 # App Router pages (16 routes)
│   │   ├── components/          # UI components (37 files)
│   │   │   ├── ui/              # shadcn/ui primitives
│   │   │   ├── charts/          # Recharts wrappers
│   │   │   ├── dashboard/       # Dashboard widgets
│   │   │   └── layout/          # Sidebar, header
│   │   └── lib/                 # API clients, hooks, types, store
│   └── backend/                 # FastAPI backend
│       ├── api/
│       │   ├── main.py          # App entry + lifespan
│       │   └── endpoints/       # Route handlers
│       ├── services/
│       │   ├── finnhub_service.py   # Finnhub + Yahoo Finance client
│       │   ├── database_service.py  # PostgreSQL operations
│       │   ├── redis_service.py     # Redis caching
│       │   └── auth_service.py      # JWT auth
│       ├── models/              # Pydantic schemas
│       └── websocket/           # WebSocket manager
├── Dockerfile                   # Multi-stage Docker build
├── railway.toml                 # Railway deployment config
├── requirements.txt             # Python dependencies
└── .interface-design/system.md  # Design system documentation
```

## License

MIT
