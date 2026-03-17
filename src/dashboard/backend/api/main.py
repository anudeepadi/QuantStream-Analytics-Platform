"""
FastAPI Backend Main Application

Main entry point for the QuantStream Dashboard backend API.
Provides REST endpoints for market data, portfolio management, alerts, and system metrics.
"""

from fastapi import FastAPI, HTTPException, Depends, Security, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse, PlainTextResponse
import asyncio
import uvicorn
from contextlib import asynccontextmanager
from datetime import datetime
import os
import logging

# Import API modules
from .endpoints import market_data, portfolio, alerts, system_metrics, websocket_manager, auth
from ..services.auth_service import AuthService
from ..services.database_service import DatabaseService
from ..services.redis_service import RedisService
from ..services.finnhub_service import FinnhubService
from ..models.responses import HealthResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global services
auth_service = AuthService()
db_service = DatabaseService()
redis_service = RedisService()
finnhub_service = FinnhubService()
security = HTTPBearer()

async def _connect_with_retry():
    """Background task: retry DB+Redis until both are reachable, then seed data."""
    delay = 5
    seeded = False
    while True:
        try:
            if not db_service.pool:
                await db_service.initialize()
                logger.info("Database connected (retry succeeded)")
        except Exception as e:
            logger.warning("DB retry failed, next in %ds: %s", delay, e)

        try:
            if not redis_service.redis_client:
                await redis_service.initialize()
                logger.info("Redis connected (retry succeeded)")
                # Re-initialize Finnhub with Redis now available
                finnhub_service.redis = redis_service
        except Exception as e:
            logger.warning("Redis retry failed, next in %ds: %s", delay, e)

        if db_service.pool and not seeded:
            await _seed_default_users(db_service, auth_service)
            await _seed_demo_portfolio(db_service)
            seeded = True

        if db_service.pool and redis_service.redis_client:
            logger.info("All services connected")
            return

        await asyncio.sleep(delay)
        delay = min(delay * 2, 60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler"""
    # Startup
    logger.info("Starting QuantStream Dashboard API...")

    # Wire service dependencies first (before any connection attempts)
    auth_service.set_services(db_service, redis_service)
    auth.set_services(auth_service, db_service)
    portfolio.set_services(db_service)
    alerts.set_services(db_service)
    market_data.set_services(finnhub_service)

    # Try initial connection — non-fatal if DB/Redis not yet available
    try:
        await db_service.initialize()
    except Exception as e:
        logger.warning("Database unavailable at startup (retrying in background): %s", e)

    try:
        await redis_service.initialize()
    except Exception as e:
        logger.warning("Redis unavailable at startup (retrying in background): %s", e)

    await finnhub_service.initialize(redis_service if redis_service.redis_client else None)

    # Seed if DB connected on first try
    if db_service.pool:
        await _seed_default_users(db_service, auth_service)
        await _seed_demo_portfolio(db_service)

    # Background retry loop — keeps trying until all services are up
    asyncio.create_task(_connect_with_retry())

    # Start background tasks — Finnhub WS feeds trade data to our WS manager
    finnhub_service.on_trade(websocket_manager.on_finnhub_trades)
    await finnhub_service.start_ws()
    asyncio.create_task(websocket_manager.start_data_streaming())

    logger.info("API startup complete")

    yield

    # Shutdown
    logger.info("Shutting down QuantStream Dashboard API...")

    # Cleanup services
    await finnhub_service.close()
    await db_service.close()
    await redis_service.close()
    await websocket_manager.stop_data_streaming()
    
    logger.info("API shutdown complete")

async def _seed_default_users(db: DatabaseService, auth_svc: AuthService):
    """Seed demo users on first startup if the users table is empty."""
    try:
        async with db.get_connection() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM users")
        if count > 0:
            return

        demo_users = [
            {"username": "admin", "email": "admin@quantstream.ai", "password": "admin123", "role": "administrator", "full_name": "System Administrator"},
            {"username": "analyst", "email": "analyst@quantstream.ai", "password": "analyst123", "role": "analyst", "full_name": "Financial Analyst"},
            {"username": "trader", "email": "trader@quantstream.ai", "password": "trader123", "role": "trader", "full_name": "Quantitative Trader"},
        ]
        for u in demo_users:
            await db.create_user({
                "username": u["username"],
                "email": u["email"],
                "password_hash": auth_svc.hash_password(u["password"]),
                "role": u["role"],
                "full_name": u["full_name"],
            })
        logger.info("Seeded %d demo users", len(demo_users))
    except Exception as e:
        logger.warning("Could not seed users (DB may be unavailable): %s", e)


async def _seed_demo_portfolio(db: DatabaseService):
    """Seed demo portfolio positions and transactions for the admin user (id=1)."""
    try:
        async with db.get_connection() as conn:
            pos_count = await conn.fetchval("SELECT COUNT(*) FROM portfolio_positions")
            txn_count = await conn.fetchval("SELECT COUNT(*) FROM transactions")

        if pos_count == 0:
            positions = [
                ("AAPL", 100, 150.00),
                ("GOOGL", 50, 2800.00),
                ("MSFT", 75, 380.00),
                ("AMZN", 30, 3200.00),
                ("TSLA", 25, 220.00),
                ("NVDA", 40, 450.00),
                ("META", 60, 310.00),
                ("JPM", 45, 155.00),
            ]
            for symbol, shares, avg_cost in positions:
                await db.update_portfolio_position(1, symbol, shares, avg_cost)
            logger.info("Seeded %d demo portfolio positions", len(positions))

        if txn_count == 0:
            async with db.get_connection() as conn:
                txns = [
                    (1, "AAPL", "BUY", 100, 150.00, 15000.00, datetime(2024, 6, 15, 9, 45)),
                    (1, "GOOGL", "BUY", 50, 2800.00, 140000.00, datetime(2024, 6, 20, 14, 15)),
                    (1, "MSFT", "BUY", 75, 380.00, 28500.00, datetime(2024, 7, 10, 11, 20)),
                    (1, "TSLA", "BUY", 25, 220.00, 5500.00, datetime(2024, 7, 25, 10, 30)),
                    (1, "NVDA", "BUY", 40, 450.00, 18000.00, datetime(2024, 8, 1, 13, 0)),
                    (1, "META", "BUY", 60, 310.00, 18600.00, datetime(2024, 8, 15, 15, 0)),
                ]
                for uid, sym, ttype, qty, price, total, dt in txns:
                    await conn.execute(
                        """INSERT INTO transactions (user_id, symbol, transaction_type, quantity, price, total_amount, transaction_date)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT DO NOTHING""",
                        uid, sym, ttype, qty, price, total, dt,
                    )
            logger.info("Seeded %d demo transactions", len(txns))
    except Exception as e:
        logger.warning("Could not seed portfolio (DB may be unavailable): %s", e)


# Create FastAPI application
app = FastAPI(
    title="QuantStream Dashboard API",
    description="Backend API for QuantStream Analytics Dashboard",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure based on environment
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Authentication dependency
async def get_current_user(credentials: HTTPAuthorizationCredentials = Security(security)):
    """Verify JWT token and return current user"""
    
    token = credentials.credentials
    user = await auth_service.verify_token(token)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return user

# Health check endpoint
@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    
    # Check service health
    db_healthy = await db_service.health_check()
    redis_healthy = await redis_service.health_check()
    
    overall_status = "healthy" if db_healthy and redis_healthy else "unhealthy"
    
    return HealthResponse(
        status=overall_status,
        timestamp=datetime.now(),
        services={
            "database": "healthy" if db_healthy else "unhealthy",
            "redis": "healthy" if redis_healthy else "unhealthy",
            "websocket": "healthy"  # TODO: Add actual websocket health check
        },
        version="1.0.0"
    )

# Metrics endpoint for Prometheus
@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    """Prometheus metrics endpoint"""
    return generate_latest().decode('utf-8')

# Auth router (public — no auth dependency)
app.include_router(
    auth.router,
    prefix="/api/v1/auth",
    tags=["Authentication"],
)

# Include API routers
app.include_router(
    market_data.router,
    prefix="/api/v1/market-data",
    tags=["Market Data"],
    dependencies=[Depends(get_current_user)]
)

app.include_router(
    portfolio.router,
    prefix="/api/v1/portfolio",
    tags=["Portfolio"],
    dependencies=[Depends(get_current_user)]
)

app.include_router(
    alerts.router,
    prefix="/api/v1/alerts",
    tags=["Alerts"],
    dependencies=[Depends(get_current_user)]
)

app.include_router(
    system_metrics.router,
    prefix="/api/v1/system",
    tags=["System Metrics"],
    dependencies=[Depends(get_current_user)]
)

# WebSocket endpoint (no auth dependency for real-time data)
app.include_router(
    websocket_manager.router,
    prefix="/ws",
    tags=["WebSocket"]
)

# Root endpoint
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "QuantStream Dashboard API",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs",
        "health": "/health"
    }

# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Handle HTTP exceptions"""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.now().isoformat()
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle general exceptions"""
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "status_code": 500,
            "timestamp": datetime.now().isoformat()
        }
    )

if __name__ == "__main__":
    # Development server
    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "0.0.0.0")
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=True if os.getenv("ENVIRONMENT") == "development" else False,
        log_level="info"
    )