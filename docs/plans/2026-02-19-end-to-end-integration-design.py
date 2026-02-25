# QuantStream Analytics Platform - End-to-End Integration Design
# Date: 2026-02-19
# Status: Approved

"""
PHASE 1 — Authentication (Backend + Frontend)
=============================================
Backend:
- Add auth router: POST /api/v1/auth/login, POST /register, GET /me, POST /refresh, POST /logout
- Add missing DatabaseService methods: get_user_by_username, get_user_by_id, create_user, update_last_login
- Wire AuthService.set_services(db_service, redis_service) in main.py lifespan
- Seed default admin user on first startup
- Add get_current_user dependency for protected routes

Frontend:
- Login page at /login with email/password form
- AuthProvider context with token storage (localStorage)
- Add Authorization header to API client
- Middleware to redirect unauthenticated users to /login
- User info display in header from /me endpoint

PHASE 2 — Backend Real Data
============================
- Portfolio endpoints: query portfolio_positions + transactions tables
- Alerts endpoints: CRUD to alerts + alert_history tables
- System endpoints: store and query system_metrics table
- Add new backend routes frontend expects: /overview, /sectors, /top-gainers, /top-losers
- Fix Redis service wiring in market_data.py
- SQL seed script with initial portfolio positions + sample alerts
- Fix market-status endpoint timezone handling

PHASE 3 — Frontend Alignment & Pages
=====================================
- Fix type shape mismatches (frontend types ↔ backend response shapes)
- Build Markets page with live symbol search + price display
- Build Portfolio page with positions table + allocation chart
- Build Alerts page with alert list + create/acknowledge actions
- Build Analytics page with technical indicators charts
- Build Settings page with user preferences
- Connect system page to real API hooks
- Switch NEXT_PUBLIC_USE_MOCK_DATA=false

PHASE 4 — Docker End-to-End
============================
- Fix docker-compose.yml: correct CMD path, remove broken mounts
- Create scripts/sql/init.sql with schema + seed data
- Add frontend-next service to docker-compose
- Create Dockerfile.frontend for Next.js
- Create .env with working defaults for all services
- Add nginx reverse proxy for unified entry point
- Health check on all containers
- docker-compose up should bring entire platform online
"""
