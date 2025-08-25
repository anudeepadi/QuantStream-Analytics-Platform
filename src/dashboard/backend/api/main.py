"""
FastAPI Backend Main Application

Main entry point for the QuantStream Dashboard backend API.
Provides REST endpoints for market data, portfolio management, alerts, and system metrics.
"""

from fastapi import FastAPI, HTTPException, Depends, Security, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import PlainTextResponse
import asyncio
import uvicorn
from contextlib import asynccontextmanager
from datetime import datetime
import os
import logging

# Import API modules
from .endpoints import market_data, portfolio, alerts, system_metrics, websocket_manager
from ..services.auth_service import AuthService
from ..services.database_service import DatabaseService
from ..services.redis_service import RedisService
from ..models.responses import HealthResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global services
auth_service = AuthService()
db_service = DatabaseService()
redis_service = RedisService()
security = HTTPBearer()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler"""
    # Startup
    logger.info("Starting QuantStream Dashboard API...")
    
    # Initialize services
    await db_service.initialize()
    await redis_service.initialize()
    
    # Start background tasks
    asyncio.create_task(websocket_manager.start_data_streaming())
    
    logger.info("API startup complete")
    
    yield
    
    # Shutdown
    logger.info("Shutting down QuantStream Dashboard API...")
    
    # Cleanup services
    await db_service.close()
    await redis_service.close()
    await websocket_manager.stop_data_streaming()
    
    logger.info("API shutdown complete")

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
    return {
        "error": exc.detail,
        "status_code": exc.status_code,
        "timestamp": datetime.now().isoformat()
    }

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle general exceptions"""
    logger.error(f"Unhandled exception: {str(exc)}")
    return {
        "error": "Internal server error",
        "status_code": 500,
        "timestamp": datetime.now().isoformat()
    }

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