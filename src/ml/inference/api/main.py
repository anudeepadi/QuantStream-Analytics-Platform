"""
FastAPI Main Application for Anomaly Detection Model Serving

This module provides the main FastAPI application with comprehensive features including
authentication, rate limiting, health checks, and detailed OpenAPI documentation.
"""

import logging
import time
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager
from datetime import datetime

# FastAPI imports
try:
    from fastapi import FastAPI, HTTPException, Depends, Request, status
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.middleware.trustedhost import TrustedHostMiddleware
    from fastapi.responses import JSONResponse
    from fastapi.openapi.utils import get_openapi
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False

# Rate limiting
try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.util import get_remote_address
    from slowapi.errors import RateLimitExceeded
    HAS_SLOWAPI = True
except ImportError:
    HAS_SLOWAPI = False

# Authentication
try:
    import jwt
    HAS_JWT = True
except ImportError:
    HAS_JWT = False

import os
from pathlib import Path

from ..engine import RealTimeInferenceEngine, InferenceConfig
from .models import *

logger = logging.getLogger(__name__)

# Global inference engine instance
inference_engine: Optional[RealTimeInferenceEngine] = None


class AuthenticationError(Exception):
    """Custom authentication error."""
    pass


class RateLimitError(Exception):
    """Custom rate limit error."""
    pass


# Rate limiter setup
if HAS_SLOWAPI:
    limiter = Limiter(key_func=get_remote_address)
else:
    limiter = None


# Security scheme
security = HTTPBearer(auto_error=False) if HAS_FASTAPI else None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    # Startup
    logger.info("Starting up anomaly detection API...")
    
    global inference_engine
    
    # Initialize inference engine
    config = InferenceConfig(
        batch_size=int(os.getenv("INFERENCE_BATCH_SIZE", "32")),
        max_batch_wait_time=float(os.getenv("INFERENCE_MAX_WAIT_TIME", "1.0")),
        num_workers=int(os.getenv("INFERENCE_WORKERS", "4")),
        enable_caching=os.getenv("INFERENCE_ENABLE_CACHE", "true").lower() == "true",
        cache_ttl=int(os.getenv("INFERENCE_CACHE_TTL", "300")),
        performance_monitoring=True
    )
    
    inference_engine = RealTimeInferenceEngine(
        config=config,
        redis_host=os.getenv("REDIS_HOST", "localhost"),
        redis_port=int(os.getenv("REDIS_PORT", "6379")),
        redis_db=int(os.getenv("REDIS_DB", "0"))
    )
    
    # Load default model if specified
    default_model_path = os.getenv("DEFAULT_MODEL_PATH")
    if default_model_path and Path(default_model_path).exists():
        try:
            inference_engine.load_model(default_model_path, "default_model")
            logger.info(f"Loaded default model from {default_model_path}")
        except Exception as e:
            logger.error(f"Failed to load default model: {e}")
    
    # Start inference engine
    inference_engine.start()
    
    # Setup alert callback
    def alert_callback(alert: Dict[str, Any]):
        logger.warning(f"Alert triggered: {alert}")
    
    inference_engine.add_alert_callback(alert_callback)
    
    logger.info("API startup completed")
    
    yield
    
    # Shutdown
    logger.info("Shutting down anomaly detection API...")
    
    if inference_engine:
        inference_engine.stop()
    
    logger.info("API shutdown completed")


def create_app() -> FastAPI:
    """Create and configure FastAPI application."""
    if not HAS_FASTAPI:
        raise ImportError("FastAPI is required for the API server")
    
    app = FastAPI(
        title="QuantStream Anomaly Detection API",
        description="""
        Production-ready REST API for anomaly detection in financial time-series data.
        
        This API provides real-time and batch anomaly detection capabilities using
        multiple machine learning models including Isolation Forest, LSTM Autoencoders,
        Statistical methods, and Ensemble approaches.
        
        ## Features
        - Real-time anomaly detection
        - Batch processing for high throughput
        - Multiple model support
        - Intelligent caching
        - Performance monitoring
        - Health checks and metrics
        - Authentication and rate limiting
        - Comprehensive error handling
        
        ## Model Types Supported
        - **Isolation Forest**: Unsupervised outlier detection
        - **LSTM Autoencoder**: Deep learning for time-series anomalies
        - **Statistical Methods**: Z-score, IQR, seasonal decomposition
        - **Ensemble Methods**: Combining multiple detection approaches
        """,
        version="1.0.0",
        openapi_tags=[
            {
                "name": "prediction",
                "description": "Anomaly prediction endpoints"
            },
            {
                "name": "models",
                "description": "Model management endpoints"
            },
            {
                "name": "monitoring", 
                "description": "Health checks and performance monitoring"
            },
            {
                "name": "admin",
                "description": "Administrative endpoints"
            }
        ],
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json"
    )
    
    # Add middleware
    
    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE"],
        allow_headers=["*"],
    )
    
    # Trusted host middleware
    trusted_hosts = os.getenv("TRUSTED_HOSTS", "*").split(",")
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=trusted_hosts
    )
    
    # Rate limiting middleware
    if HAS_SLOWAPI and limiter:
        app.state.limiter = limiter
        app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    
    # Custom middleware for request logging and timing
    @app.middleware("http")
    async def logging_middleware(request: Request, call_next):
        """Log requests and measure response time."""
        start_time = time.time()
        
        # Log request
        logger.info(f"Request: {request.method} {request.url}")
        
        # Process request
        response = await call_next(request)
        
        # Calculate response time
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        
        # Log response
        logger.info(f"Response: {response.status_code} in {process_time:.4f}s")
        
        return response
    
    return app


def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> Optional[str]:
    """Verify JWT token for authentication."""
    if not os.getenv("ENABLE_AUTH", "false").lower() == "true":
        return None  # Authentication disabled
    
    if not HAS_JWT:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="JWT authentication not available"
        )
    
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication credentials required",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    try:
        secret_key = os.getenv("JWT_SECRET_KEY")
        if not secret_key:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="JWT secret key not configured"
            )
        
        payload = jwt.decode(
            credentials.credentials,
            secret_key,
            algorithms=[os.getenv("JWT_ALGORITHM", "HS256")]
        )
        
        user_id = payload.get("sub")
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token: missing user ID"
            )
        
        return user_id
    
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"}
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"}
        )


def get_inference_engine() -> RealTimeInferenceEngine:
    """Get the global inference engine instance."""
    if inference_engine is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Inference engine not available"
        )
    return inference_engine


# Create the FastAPI app
if HAS_FASTAPI:
    app = create_app()
    
    # Include routers
    from .endpoints import prediction_router, model_router, monitoring_router, admin_router
    
    app.include_router(prediction_router, prefix="/api/v1", tags=["prediction"])
    app.include_router(model_router, prefix="/api/v1", tags=["models"])
    app.include_router(monitoring_router, prefix="/api/v1", tags=["monitoring"])
    app.include_router(admin_router, prefix="/api/v1", tags=["admin"])
    
    # Root endpoint
    @app.get("/", include_in_schema=False)
    async def root():
        """Root endpoint with API information."""
        return {
            "name": "QuantStream Anomaly Detection API",
            "version": "1.0.0",
            "description": "Production-ready anomaly detection for financial time-series data",
            "docs_url": "/docs",
            "health_url": "/api/v1/health",
            "models_url": "/api/v1/models",
            "predict_url": "/api/v1/predict"
        }
    
    # Custom OpenAPI schema
    def custom_openapi():
        """Generate custom OpenAPI schema."""
        if app.openapi_schema:
            return app.openapi_schema
        
        openapi_schema = get_openapi(
            title="QuantStream Anomaly Detection API",
            version="1.0.0",
            description=app.description,
            routes=app.routes,
        )
        
        # Add security definitions
        openapi_schema["components"]["securitySchemes"] = {
            "BearerAuth": {
                "type": "http",
                "scheme": "bearer",
                "bearerFormat": "JWT"
            }
        }
        
        # Add common responses
        openapi_schema["components"]["responses"] = {
            "AuthenticationError": {
                "description": "Authentication credentials required or invalid",
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/ErrorResponse"}
                    }
                }
            },
            "RateLimitError": {
                "description": "Rate limit exceeded",
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/ErrorResponse"}
                    }
                }
            },
            "ValidationError": {
                "description": "Request validation error",
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/ErrorResponse"}
                    }
                }
            },
            "InternalServerError": {
                "description": "Internal server error",
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/ErrorResponse"}
                    }
                }
            }
        }
        
        app.openapi_schema = openapi_schema
        return app.openapi_schema
    
    app.openapi = custom_openapi
    
    # Global exception handlers
    @app.exception_handler(AuthenticationError)
    async def authentication_exception_handler(request: Request, exc: AuthenticationError):
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={
                "error": "AuthenticationError",
                "message": str(exc),
                "timestamp": datetime.now().isoformat()
            }
        )
    
    @app.exception_handler(RateLimitError)
    async def rate_limit_exception_handler(request: Request, exc: RateLimitError):
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={
                "error": "RateLimitError",
                "message": str(exc),
                "timestamp": datetime.now().isoformat()
            }
        )
    
    @app.exception_handler(ValueError)
    async def value_error_exception_handler(request: Request, exc: ValueError):
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={
                "error": "ValidationError",
                "message": str(exc),
                "timestamp": datetime.now().isoformat()
            }
        )
    
    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        logger.error(f"Unhandled exception: {exc}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": "InternalServerError",
                "message": "An internal server error occurred",
                "timestamp": datetime.now().isoformat()
            }
        )

else:
    app = None
    logger.error("FastAPI not available - API server cannot be created")


# Development server runner
if __name__ == "__main__":
    if not HAS_FASTAPI:
        print("FastAPI not installed. Please install with: pip install fastapi uvicorn")
        exit(1)
    
    try:
        import uvicorn
        
        # Configuration from environment
        host = os.getenv("API_HOST", "0.0.0.0")
        port = int(os.getenv("API_PORT", "8000"))
        reload = os.getenv("API_RELOAD", "false").lower() == "true"
        log_level = os.getenv("API_LOG_LEVEL", "info")
        
        logger.info(f"Starting development server on {host}:{port}")
        
        uvicorn.run(
            "main:app",
            host=host,
            port=port,
            reload=reload,
            log_level=log_level,
            access_log=True
        )
        
    except ImportError:
        print("Uvicorn not installed. Please install with: pip install uvicorn")
        exit(1)