"""
API Endpoints for Anomaly Detection Model Serving

This module implements all the REST API endpoints for the anomaly detection service
including prediction, model management, monitoring, and administrative endpoints.
"""

import logging
import uuid
from typing import List, Dict, Any, Optional
from datetime import datetime

# FastAPI imports
try:
    from fastapi import APIRouter, HTTPException, Depends, Request, status, BackgroundTasks
    from fastapi.responses import JSONResponse
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False

# Rate limiting
try:
    from slowapi import Limiter
    from slowapi.util import get_remote_address
    HAS_SLOWAPI = True
except ImportError:
    HAS_SLOWAPI = False

import numpy as np
from pathlib import Path

from .models import *
from .main import verify_token, get_inference_engine, limiter

logger = logging.getLogger(__name__)

# Create routers
if HAS_FASTAPI:
    prediction_router = APIRouter()
    model_router = APIRouter() 
    monitoring_router = APIRouter()
    admin_router = APIRouter()
else:
    # Dummy routers for when FastAPI is not available
    class DummyRouter:
        def get(self, *args, **kwargs): pass
        def post(self, *args, **kwargs): pass
        def put(self, *args, **kwargs): pass
        def delete(self, *args, **kwargs): pass
    
    prediction_router = DummyRouter()
    model_router = DummyRouter()
    monitoring_router = DummyRouter()
    admin_router = DummyRouter()


# ============================================================================
# PREDICTION ENDPOINTS
# ============================================================================

if HAS_FASTAPI:
    @prediction_router.post(
        "/predict",
        response_model=PredictionResponse,
        summary="Single Prediction",
        description="Perform anomaly detection on a single sample or batch of samples",
        responses={
            200: {"description": "Successful prediction"},
            401: {"$ref": "#/components/responses/AuthenticationError"},
            422: {"$ref": "#/components/responses/ValidationError"},
            429: {"$ref": "#/components/responses/RateLimitError"},
            500: {"$ref": "#/components/responses/InternalServerError"}
        }
    )
    async def predict_endpoint(
        request: PredictionRequest,
        user_id: Optional[str] = Depends(verify_token),
        engine = Depends(get_inference_engine),
        req: Request = None
    ) -> PredictionResponse:
        """
        Perform anomaly detection on input features.
        
        This endpoint accepts feature vectors and returns anomaly predictions
        with confidence scores and detailed metadata.
        """
        if HAS_SLOWAPI and limiter:
            # Apply rate limiting
            await limiter.limit("100/minute")(req)
        
        try:
            # Convert features to numpy array
            features = np.array(request.features)
            
            # Generate request ID if not provided
            request_id = request.request_id or f"pred_{uuid.uuid4().hex[:8]}"
            
            # Add user info to metadata
            metadata = request.metadata or {}
            if user_id:
                metadata["user_id"] = user_id
            metadata["endpoint"] = "predict"
            
            # Make prediction
            response = engine.predict(
                features=features,
                model_name=request.model_name,
                request_id=request_id,
                metadata=metadata
            )
            
            return response
        
        except Exception as e:
            logger.error(f"Prediction failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Prediction failed: {str(e)}"
            )

    
    @prediction_router.post(
        "/predict/batch",
        response_model=BatchPredictionResponse,
        summary="Batch Prediction",
        description="Perform anomaly detection on multiple samples in a single request for improved throughput",
        responses={
            200: {"description": "Successful batch prediction"},
            401: {"$ref": "#/components/responses/AuthenticationError"},
            422: {"$ref": "#/components/responses/ValidationError"},
            429: {"$ref": "#/components/responses/RateLimitError"},
            500: {"$ref": "#/components/responses/InternalServerError"}
        }
    )
    async def predict_batch_endpoint(
        request: BatchPredictionRequest,
        user_id: Optional[str] = Depends(verify_token),
        engine = Depends(get_inference_engine),
        req: Request = None
    ) -> BatchPredictionResponse:
        """
        Perform batch anomaly detection for improved throughput.
        
        This endpoint is optimized for processing multiple samples
        simultaneously, reducing per-sample latency.
        """
        if HAS_SLOWAPI and limiter:
            # Apply stricter rate limiting for batch requests
            await limiter.limit("20/minute")(req)
        
        try:
            start_time = datetime.now()
            
            # Convert features to numpy array
            features_batch = [np.array(features) for features in request.features_batch]
            
            # Generate request IDs if not provided
            if request.request_ids:
                request_ids = request.request_ids
            else:
                batch_id = uuid.uuid4().hex[:8]
                request_ids = [f"batch_{batch_id}_{i}" for i in range(len(features_batch))]
            
            # Prepare metadata batch
            metadata_batch = request.metadata_batch or [{}] * len(features_batch)
            
            # Add user info to metadata
            if user_id:
                for metadata in metadata_batch:
                    metadata["user_id"] = user_id
                    metadata["endpoint"] = "predict_batch"
            
            # Make batch prediction
            predictions = engine.predict_batch(
                features_batch=np.array(features_batch),
                model_name=request.model_name,
                request_ids=request_ids,
                metadata_batch=metadata_batch
            )
            
            # Calculate total latency
            total_latency_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            return BatchPredictionResponse(
                predictions=predictions,
                batch_size=len(predictions),
                total_latency_ms=total_latency_ms,
                timestamp=datetime.now()
            )
        
        except Exception as e:
            logger.error(f"Batch prediction failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Batch prediction failed: {str(e)}"
            )


# ============================================================================
# MODEL MANAGEMENT ENDPOINTS
# ============================================================================

if HAS_FASTAPI:
    @model_router.get(
        "/models",
        response_model=ModelListResponse,
        summary="List Models",
        description="Get information about all available models",
        responses={
            200: {"description": "Successful model list retrieval"},
            401: {"$ref": "#/components/responses/AuthenticationError"},
            500: {"$ref": "#/components/responses/InternalServerError"}
        }
    )
    async def list_models_endpoint(
        user_id: Optional[str] = Depends(verify_token),
        engine = Depends(get_inference_engine)
    ) -> ModelListResponse:
        """
        List all available models with their information and status.
        
        Returns detailed information about each loaded model including
        type, version, performance metrics, and current status.
        """
        try:
            # Get model information from cache
            model_info_list = []
            
            for model_name, model in engine.model_cache.cache.items():
                # Get model info
                info = model.get_model_info() if hasattr(model, 'get_model_info') else {}
                
                model_info = ModelInfo(
                    name=model_name,
                    type=getattr(model, 'model_type', 'unknown'),
                    version=info.get('metadata', {}).get('version', '1.0.0'),
                    is_loaded=True,
                    is_default=(model_name == engine.default_model_name),
                    created_at=datetime.fromisoformat(
                        info.get('metadata', {}).get('creation_date', datetime.now().isoformat())
                    ),
                    performance_metrics=info.get('validation_scores', {})
                )
                
                model_info_list.append(model_info)
            
            return ModelListResponse(
                models=model_info_list,
                total_count=len(model_info_list),
                default_model=engine.default_model_name
            )
        
        except Exception as e:
            logger.error(f"Failed to list models: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to list models: {str(e)}"
            )

    
    @model_router.get(
        "/models/{model_name}",
        response_model=ModelInfo,
        summary="Get Model Info",
        description="Get detailed information about a specific model",
        responses={
            200: {"description": "Successful model info retrieval"},
            401: {"$ref": "#/components/responses/AuthenticationError"},
            404: {"description": "Model not found"},
            500: {"$ref": "#/components/responses/InternalServerError"}
        }
    )
    async def get_model_endpoint(
        model_name: str,
        user_id: Optional[str] = Depends(verify_token),
        engine = Depends(get_inference_engine)
    ) -> ModelInfo:
        """
        Get detailed information about a specific model.
        
        Returns comprehensive information including model type, version,
        performance metrics, and current status.
        """
        try:
            model = engine.model_cache.get(model_name)
            if model is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Model '{model_name}' not found"
                )
            
            # Get model info
            info = model.get_model_info() if hasattr(model, 'get_model_info') else {}
            
            return ModelInfo(
                name=model_name,
                type=getattr(model, 'model_type', 'unknown'),
                version=info.get('metadata', {}).get('version', '1.0.0'),
                is_loaded=True,
                is_default=(model_name == engine.default_model_name),
                created_at=datetime.fromisoformat(
                    info.get('metadata', {}).get('creation_date', datetime.now().isoformat())
                ),
                performance_metrics=info.get('validation_scores', {})
            )
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to get model info: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get model info: {str(e)}"
            )

    
    @model_router.post(
        "/models/load",
        response_model=ModelLoadResponse,
        summary="Load Model",
        description="Load a new model from file or MLflow registry",
        responses={
            200: {"description": "Model loaded successfully"},
            401: {"$ref": "#/components/responses/AuthenticationError"},
            422: {"$ref": "#/components/responses/ValidationError"},
            500: {"$ref": "#/components/responses/InternalServerError"}
        }
    )
    async def load_model_endpoint(
        request: ModelLoadRequest,
        background_tasks: BackgroundTasks,
        user_id: Optional[str] = Depends(verify_token),
        engine = Depends(get_inference_engine)
    ) -> ModelLoadResponse:
        """
        Load a new model from file system or MLflow model registry.
        
        Supports loading from various sources including:
        - Local file paths (pickle, joblib, etc.)
        - MLflow model URIs (models:/ scheme)
        - Remote URLs (http/https)
        """
        try:
            # Load model (this might take time, so consider background task for large models)
            model_name = engine.load_model(
                model_or_path=request.model_path,
                model_name=request.model_name
            )
            
            # Set as default if requested
            if request.set_as_default:
                engine.set_default_model(model_name)
            
            # Log model loading event
            logger.info(f"Model loaded: {model_name} from {request.model_path} by user {user_id}")
            
            return ModelLoadResponse(
                model_name=model_name,
                model_path=request.model_path,
                is_default=request.set_as_default or (model_name == engine.default_model_name),
                loaded_at=datetime.now()
            )
        
        except FileNotFoundError:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Model file not found: {request.model_path}"
            )
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to load model: {str(e)}"
            )

    
    @model_router.put(
        "/models/{model_name}/default",
        summary="Set Default Model",
        description="Set a model as the default for predictions",
        responses={
            200: {"description": "Default model set successfully"},
            401: {"$ref": "#/components/responses/AuthenticationError"},
            404: {"description": "Model not found"},
            500: {"$ref": "#/components/responses/InternalServerError"}
        }
    )
    async def set_default_model_endpoint(
        model_name: str,
        user_id: Optional[str] = Depends(verify_token),
        engine = Depends(get_inference_engine)
    ) -> Dict[str, Any]:
        """
        Set the specified model as the default for predictions.
        
        The default model is used when no specific model is requested
        in prediction endpoints.
        """
        try:
            # Check if model exists
            if engine.model_cache.get(model_name) is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Model '{model_name}' not found"
                )
            
            engine.set_default_model(model_name)
            
            logger.info(f"Default model set to {model_name} by user {user_id}")
            
            return {
                "message": f"Default model set to '{model_name}'",
                "model_name": model_name,
                "timestamp": datetime.now().isoformat()
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to set default model: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to set default model: {str(e)}"
            )

    
    @model_router.delete(
        "/models/{model_name}",
        summary="Unload Model",
        description="Unload a model from memory",
        responses={
            200: {"description": "Model unloaded successfully"},
            401: {"$ref": "#/components/responses/AuthenticationError"},
            404: {"description": "Model not found"},
            400: {"description": "Cannot unload default model"},
            500: {"$ref": "#/components/responses/InternalServerError"}
        }
    )
    async def unload_model_endpoint(
        model_name: str,
        user_id: Optional[str] = Depends(verify_token),
        engine = Depends(get_inference_engine)
    ) -> Dict[str, Any]:
        """
        Unload a model from memory to free up resources.
        
        Note: The default model cannot be unloaded. Set a different
        default model first if you need to unload the current default.
        """
        try:
            # Check if model exists
            if engine.model_cache.get(model_name) is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Model '{model_name}' not found"
                )
            
            # Don't allow unloading the default model
            if model_name == engine.default_model_name:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Cannot unload default model. Set a different default first."
                )
            
            # Remove from cache
            engine.model_cache.remove(model_name)
            
            logger.info(f"Model {model_name} unloaded by user {user_id}")
            
            return {
                "message": f"Model '{model_name}' unloaded successfully",
                "model_name": model_name,
                "timestamp": datetime.now().isoformat()
            }
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to unload model: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to unload model: {str(e)}"
            )


# ============================================================================
# MONITORING ENDPOINTS
# ============================================================================

if HAS_FASTAPI:
    @monitoring_router.get(
        "/health",
        response_model=HealthResponse,
        summary="Health Check",
        description="Get the current health status of the API and inference engine",
        responses={
            200: {"description": "Health check successful"},
            503: {"description": "Service unhealthy"}
        }
    )
    async def health_check_endpoint(engine = Depends(get_inference_engine)) -> HealthResponse:
        """
        Comprehensive health check of the anomaly detection service.
        
        Returns detailed health information including:
        - Overall service status
        - Performance statistics
        - Resource utilization
        - Any detected issues
        """
        try:
            health_data = engine.health_check()
            
            # Map status to enum
            status_map = {
                'healthy': HealthStatus.HEALTHY,
                'degraded': HealthStatus.DEGRADED,
                'unhealthy': HealthStatus.UNHEALTHY
            }
            
            health_status = status_map.get(health_data['status'], HealthStatus.UNHEALTHY)
            
            # Extract performance stats
            perf_stats = {k: v for k, v in health_data.items() 
                         if k not in ['status', 'timestamp', 'issues']}
            
            response = HealthResponse(
                status=health_status,
                timestamp=datetime.fromisoformat(health_data['timestamp']),
                version="1.0.0",
                issues=health_data.get('issues', []),
                performance_stats=perf_stats
            )
            
            # Return appropriate HTTP status
            if health_status == HealthStatus.UNHEALTHY:
                return JSONResponse(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    content=response.dict()
                )
            
            return response
        
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={
                    "status": "unhealthy",
                    "timestamp": datetime.now().isoformat(),
                    "version": "1.0.0",
                    "issues": [f"Health check failed: {str(e)}"],
                    "performance_stats": {}
                }
            )

    
    @monitoring_router.get(
        "/metrics",
        response_model=PerformanceStats,
        summary="Performance Metrics",
        description="Get detailed performance metrics and statistics",
        responses={
            200: {"description": "Metrics retrieved successfully"},
            401: {"$ref": "#/components/responses/AuthenticationError"},
            500: {"$ref": "#/components/responses/InternalServerError"}
        }
    )
    async def metrics_endpoint(
        user_id: Optional[str] = Depends(verify_token),
        engine = Depends(get_inference_engine)
    ) -> PerformanceStats:
        """
        Get detailed performance metrics for monitoring and optimization.
        
        Returns comprehensive statistics including:
        - Request rates and latency percentiles
        - Error rates and cache performance
        - Queue sizes and resource utilization
        """
        try:
            stats = engine.get_performance_stats()
            
            return PerformanceStats(
                requests_per_minute=stats.get('requests_per_minute', 0.0),
                avg_latency_ms=stats.get('avg_latency_ms', 0.0),
                p95_latency_ms=stats.get('p95_latency_ms', 0.0),
                p99_latency_ms=stats.get('p99_latency_ms', 0.0),
                error_rate=stats.get('error_rate', 0.0),
                cache_hit_rate=stats.get('cache_hit_rate', 0.0),
                queue_size=stats.get('queue_size', 0),
                total_requests=stats.get('total_requests', 0)
            )
        
        except Exception as e:
            logger.error(f"Failed to get metrics: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get metrics: {str(e)}"
            )


# ============================================================================
# ADMINISTRATIVE ENDPOINTS
# ============================================================================

if HAS_FASTAPI:
    @admin_router.get(
        "/config",
        response_model=APIConfigResponse,
        summary="Get API Configuration",
        description="Get current API configuration and settings",
        responses={
            200: {"description": "Configuration retrieved successfully"},
            401: {"$ref": "#/components/responses/AuthenticationError"},
            500: {"$ref": "#/components/responses/InternalServerError"}
        }
    )
    async def get_config_endpoint(
        user_id: Optional[str] = Depends(verify_token),
        engine = Depends(get_inference_engine)
    ) -> APIConfigResponse:
        """
        Get current API configuration and inference engine settings.
        
        This endpoint provides visibility into the current configuration
        for debugging and monitoring purposes.
        """
        try:
            import os
            
            config = engine.config
            
            return APIConfigResponse(
                version="1.0.0",
                inference_config={
                    "batch_size": config.batch_size,
                    "max_batch_wait_time": config.max_batch_wait_time,
                    "max_queue_size": config.max_queue_size,
                    "num_workers": config.num_workers,
                    "enable_caching": config.enable_caching,
                    "cache_ttl": config.cache_ttl,
                    "performance_monitoring": config.performance_monitoring
                },
                rate_limits={
                    "predict_per_minute": 100,
                    "batch_predict_per_minute": 20,
                    "burst_limit": 10
                },
                authentication_enabled=os.getenv("ENABLE_AUTH", "false").lower() == "true",
                cache_enabled=config.enable_caching
            )
        
        except Exception as e:
            logger.error(f"Failed to get config: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get config: {str(e)}"
            )

    
    @admin_router.post(
        "/cache/clear",
        summary="Clear Cache",
        description="Clear all cached predictions and features",
        responses={
            200: {"description": "Cache cleared successfully"},
            401: {"$ref": "#/components/responses/AuthenticationError"},
            500: {"$ref": "#/components/responses/InternalServerError"}
        }
    )
    async def clear_cache_endpoint(
        user_id: Optional[str] = Depends(verify_token),
        engine = Depends(get_inference_engine)
    ) -> Dict[str, Any]:
        """
        Clear all cached predictions and features.
        
        This can be useful for debugging or when you want to ensure
        fresh predictions without cached results.
        """
        try:
            # Clear local caches
            engine.feature_cache.local_cache.clear()
            engine.feature_cache.local_access_times.clear()
            engine.prediction_cache.local_cache.clear()
            engine.prediction_cache.local_access_times.clear()
            
            # Clear Redis caches if available
            if engine.feature_cache.redis_client:
                # Note: This clears all keys - in production you might want to be more selective
                try:
                    engine.feature_cache.redis_client.flushdb()
                except Exception as e:
                    logger.warning(f"Failed to clear Redis cache: {e}")
            
            logger.info(f"Caches cleared by user {user_id}")
            
            return {
                "message": "Caches cleared successfully",
                "timestamp": datetime.now().isoformat()
            }
        
        except Exception as e:
            logger.error(f"Failed to clear cache: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to clear cache: {str(e)}"
            )