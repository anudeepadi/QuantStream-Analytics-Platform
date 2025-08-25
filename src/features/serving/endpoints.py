"""
FastAPI Endpoints for Feature Serving

High-performance REST API endpoints with comprehensive feature serving capabilities.
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import logging

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
import uvicorn

from .api_models import (
    FeatureRequest,
    FeatureResponse,
    BatchFeatureRequest,
    BatchFeatureResponse,
    FeatureComputeRequest,
    FeatureComputeResponse,
    FeatureSearchRequest,
    FeatureSearchResponse,
    FeatureStatsRequest,
    FeatureStatsResponse,
    FeatureLineageRequest,
    FeatureLineageResponse,
    CacheInvalidateRequest,
    CacheInvalidateResponse,
    HealthCheckResponse,
    ServingMetrics,
    ServingConfig
)
from .feature_server import FeatureServer
from ..store.feature_store import FeatureStore
from ..store.feature_registry import FeatureSearchFilter


logger = logging.getLogger(__name__)


def create_feature_serving_app(
    feature_server: FeatureServer,
    feature_store: FeatureStore,
    config: ServingConfig,
    title: str = "QuantStream Feature Store API",
    version: str = "1.0.0"
) -> FastAPI:
    """
    Create FastAPI application for feature serving.
    
    Args:
        feature_server: Feature server instance
        feature_store: Feature store instance
        config: Serving configuration
        title: API title
        version: API version
        
    Returns:
        Configured FastAPI application
    """
    
    app = FastAPI(
        title=title,
        version=version,
        description="High-performance feature store API with sub-50ms serving latency",
        docs_url="/docs",
        redoc_url="/redoc"
    )
    
    # Add middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure appropriately for production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    
    # Dependency injection
    def get_feature_server() -> FeatureServer:
        return feature_server
    
    def get_feature_store() -> FeatureStore:
        return feature_store
    
    def get_config() -> ServingConfig:
        return config
    
    # Health and status endpoints
    @app.get("/health", response_model=HealthCheckResponse)
    async def health_check(
        server: FeatureServer = Depends(get_feature_server)
    ) -> HealthCheckResponse:
        """Health check endpoint."""
        try:
            health_data = await server.health_check()
            metrics = await server.get_metrics()
            
            return HealthCheckResponse(
                status=health_data["status"],
                timestamp=datetime.fromisoformat(health_data["timestamp"]),
                components=health_data.get("components", {}),
                metrics=metrics,
                version=version
            )
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            raise HTTPException(status_code=503, detail="Health check failed")
    
    @app.get("/metrics", response_model=ServingMetrics)
    async def get_metrics(
        server: FeatureServer = Depends(get_feature_server)
    ) -> ServingMetrics:
        """Get serving metrics."""
        return await server.get_metrics()
    
    # Core feature serving endpoints
    @app.post("/features/serve", response_model=FeatureResponse)
    async def serve_features(
        request: FeatureRequest,
        server: FeatureServer = Depends(get_feature_server)
    ) -> FeatureResponse:
        """
        Serve features for a single entity with sub-50ms latency target.
        """
        start_time = time.time()
        
        try:
            response = await server.serve_features(request)
            
            # Log slow requests
            if response.response_time_ms > config.latency_threshold_ms:
                logger.warning(
                    f"Slow request: {response.response_time_ms:.2f}ms for entity {request.entity_id}"
                )
            
            return response
            
        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            logger.error(f"Error serving features: {e}")
            
            return FeatureResponse(
                success=False,
                entity_id=request.entity_id,
                error_message=str(e),
                response_time_ms=response_time_ms
            )
    
    @app.post("/features/serve/batch", response_model=BatchFeatureResponse)
    async def serve_batch_features(
        request: BatchFeatureRequest,
        server: FeatureServer = Depends(get_feature_server)
    ) -> BatchFeatureResponse:
        """
        Serve features for multiple entities in batch.
        """
        try:
            return await server.serve_batch_features(request)
            
        except Exception as e:
            logger.error(f"Error serving batch features: {e}")
            return BatchFeatureResponse(
                success=False,
                error_message=str(e),
                total_entities=len(request.entity_ids) if hasattr(request, 'entity_ids') else 0
            )
    
    @app.post("/features/serve/set/{set_name}", response_model=FeatureResponse)
    async def serve_feature_set(
        set_name: str,
        entity_id: str = Query(..., description="Entity identifier"),
        timestamp: Optional[datetime] = Query(default=None, description="Point-in-time timestamp"),
        server: FeatureServer = Depends(get_feature_server)
    ) -> FeatureResponse:
        """
        Serve a precomputed feature set.
        """
        try:
            return await server.serve_feature_set(set_name, entity_id, timestamp)
            
        except Exception as e:
            logger.error(f"Error serving feature set {set_name}: {e}")
            return FeatureResponse(
                success=False,
                entity_id=entity_id,
                error_message=str(e)
            )
    
    # Feature computation endpoints
    @app.post("/features/compute", response_model=List[FeatureComputeResponse])
    async def compute_features(
        request: FeatureComputeRequest,
        background_tasks: BackgroundTasks,
        store: FeatureStore = Depends(get_feature_store)
    ) -> List[FeatureComputeResponse]:
        """
        Compute features on-demand from input data.
        """
        try:
            # Convert input data to DataFrame
            import pandas as pd
            input_df = pd.DataFrame(request.input_data)
            
            if request.computation_mode == "async":
                # Asynchronous computation
                background_tasks.add_task(
                    _compute_features_background,
                    store,
                    request.feature_ids,
                    input_df,
                    request.entity_ids,
                    request.store_result
                )
                
                return [
                    FeatureComputeResponse(
                        success=True,
                        feature_id=fid,
                        computation_time_ms=0,
                        records_processed=len(input_df)
                    )
                    for fid in request.feature_ids
                ]
            
            else:
                # Synchronous computation
                results = await store.batch_compute_features(
                    feature_ids=request.feature_ids,
                    input_data=input_df,
                    parallel=True
                )
                
                return [
                    FeatureComputeResponse(
                        success=result.success,
                        feature_id=result.feature_id,
                        computation_time_ms=result.computation_time_ms,
                        records_processed=result.metadata.get("output_records", 0) if result.success else 0,
                        error_message=result.error_message
                    )
                    for result in results.values()
                ]
                
        except Exception as e:
            logger.error(f"Error computing features: {e}")
            return [
                FeatureComputeResponse(
                    success=False,
                    feature_id=fid,
                    computation_time_ms=0,
                    error_message=str(e)
                )
                for fid in request.feature_ids
            ]
    
    # Feature discovery and metadata endpoints
    @app.post("/features/search", response_model=FeatureSearchResponse)
    async def search_features(
        request: FeatureSearchRequest,
        store: FeatureStore = Depends(get_feature_store)
    ) -> FeatureSearchResponse:
        """
        Search and discover features.
        """
        start_time = time.time()
        
        try:
            # Build search filter
            filter_criteria = FeatureSearchFilter(
                namespace=request.namespace,
                category=request.category,
                tags=request.tags
            )
            
            # Search features
            features = await store.search_features(
                query=request.query,
                filters=filter_criteria,
                limit=request.limit
            )
            
            # Format response
            feature_data = []
            for feature in features:
                if request.include_metadata:
                    feature_data.append(feature.dict())
                else:
                    feature_data.append({
                        "feature_id": feature.feature_id,
                        "name": feature.name,
                        "category": feature.category,
                        "description": feature.description
                    })
            
            response_time_ms = (time.time() - start_time) * 1000
            
            return FeatureSearchResponse(
                success=True,
                features=feature_data,
                total_matches=len(features),
                response_time_ms=response_time_ms
            )
            
        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            logger.error(f"Error searching features: {e}")
            
            return FeatureSearchResponse(
                success=False,
                error_message=str(e),
                response_time_ms=response_time_ms
            )
    
    @app.get("/features/{feature_id}/metadata")
    async def get_feature_metadata(
        feature_id: str,
        store: FeatureStore = Depends(get_feature_store)
    ) -> Dict[str, Any]:
        """
        Get detailed metadata for a specific feature.
        """
        try:
            metadata = await store.registry.get_feature(feature_id)
            if not metadata:
                raise HTTPException(status_code=404, detail="Feature not found")
            
            return metadata.dict()
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting feature metadata: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.post("/features/{feature_id}/stats", response_model=FeatureStatsResponse)
    async def get_feature_stats(
        feature_id: str,
        request: FeatureStatsRequest,
        store: FeatureStore = Depends(get_feature_store)
    ) -> FeatureStatsResponse:
        """
        Get statistical summary of feature data.
        """
        try:
            stats = await store.get_feature_statistics(
                feature_id=feature_id,
                start_time=request.start_time,
                end_time=request.end_time
            )
            
            if stats is None:
                return FeatureStatsResponse(
                    success=False,
                    feature_id=feature_id,
                    error_message="Feature not found or no data available"
                )
            
            return FeatureStatsResponse(
                success=True,
                feature_id=feature_id,
                statistics=stats
            )
            
        except Exception as e:
            logger.error(f"Error getting feature stats: {e}")
            return FeatureStatsResponse(
                success=False,
                feature_id=feature_id,
                error_message=str(e)
            )
    
    @app.post("/features/{feature_id}/lineage", response_model=FeatureLineageResponse)
    async def get_feature_lineage(
        feature_id: str,
        request: FeatureLineageRequest,
        store: FeatureStore = Depends(get_feature_store)
    ) -> FeatureLineageResponse:
        """
        Get feature lineage information.
        """
        try:
            lineage = await store.get_feature_lineage(
                feature_id=feature_id,
                depth=request.depth
            )
            
            if lineage is None:
                return FeatureLineageResponse(
                    success=False,
                    feature_id=feature_id,
                    error_message="Feature lineage not available"
                )
            
            return FeatureLineageResponse(
                success=True,
                feature_id=feature_id,
                lineage=lineage
            )
            
        except Exception as e:
            logger.error(f"Error getting feature lineage: {e}")
            return FeatureLineageResponse(
                success=False,
                feature_id=feature_id,
                error_message=str(e)
            )
    
    # Cache management endpoints
    @app.post("/cache/invalidate", response_model=CacheInvalidateResponse)
    async def invalidate_cache(
        request: CacheInvalidateRequest,
        store: FeatureStore = Depends(get_feature_store)
    ) -> CacheInvalidateResponse:
        """
        Invalidate cached feature data.
        """
        try:
            if request.invalidate_all:
                success = await store.invalidate_cache()
                keys_invalidated = -1  # Unknown count for full invalidation
            else:
                success = await store.invalidate_cache(
                    feature_id=request.feature_id,
                    entity_ids=request.entity_ids
                )
                keys_invalidated = 1  # Simplified count
            
            return CacheInvalidateResponse(
                success=success,
                keys_invalidated=keys_invalidated
            )
            
        except Exception as e:
            logger.error(f"Error invalidating cache: {e}")
            return CacheInvalidateResponse(
                success=False,
                error_message=str(e)
            )
    
    @app.post("/cache/warm")
    async def warm_cache(
        feature_ids: List[str] = Query(..., description="Feature IDs to warm"),
        entity_ids: List[str] = Query(..., description="Entity IDs to warm"),
        batch_size: int = Query(default=100, description="Batch size"),
        background_tasks: BackgroundTasks = BackgroundTasks(),
        server: FeatureServer = Depends(get_feature_server)
    ) -> Dict[str, Any]:
        """
        Warm the cache with popular features and entities.
        """
        try:
            # Run cache warming in background
            background_tasks.add_task(
                server.warm_cache,
                feature_ids,
                entity_ids,
                batch_size
            )
            
            return {
                "success": True,
                "message": f"Cache warming started for {len(feature_ids)} features and {len(entity_ids)} entities"
            }
            
        except Exception as e:
            logger.error(f"Error starting cache warming: {e}")
            return {"success": False, "error": str(e)}
    
    # Administrative endpoints
    @app.post("/admin/precompute-sets")
    async def precompute_feature_sets(
        feature_sets: Dict[str, List[str]],
        background_tasks: BackgroundTasks,
        server: FeatureServer = Depends(get_feature_server)
    ) -> Dict[str, Any]:
        """
        Precompute feature sets for faster serving.
        """
        try:
            background_tasks.add_task(
                server.precompute_feature_sets,
                feature_sets
            )
            
            return {
                "success": True,
                "message": f"Precomputing {len(feature_sets)} feature sets"
            }
            
        except Exception as e:
            logger.error(f"Error precomputing feature sets: {e}")
            return {"success": False, "error": str(e)}
    
    # Exception handlers
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request, exc):
        return JSONResponse(
            status_code=exc.status_code,
            content={"success": False, "error": exc.detail}
        )
    
    @app.exception_handler(Exception)
    async def general_exception_handler(request, exc):
        logger.error(f"Unexpected error: {exc}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": "Internal server error"}
        )
    
    return app


async def _compute_features_background(
    store: FeatureStore,
    feature_ids: List[str],
    input_data,
    entity_ids: Optional[List[str]],
    store_result: bool
) -> None:
    """Background task for feature computation."""
    try:
        await store.batch_compute_features(
            feature_ids=feature_ids,
            input_data=input_data,
            parallel=True
        )
        logger.info(f"Background computation completed for {len(feature_ids)} features")
        
    except Exception as e:
        logger.error(f"Background computation failed: {e}")


def run_feature_server(
    feature_server: FeatureServer,
    feature_store: FeatureStore,
    config: ServingConfig,
    host: str = "0.0.0.0",
    port: int = 8000,
    workers: int = 1
) -> None:
    """
    Run the feature serving API server.
    
    Args:
        feature_server: Feature server instance
        feature_store: Feature store instance
        config: Serving configuration
        host: Server host
        port: Server port
        workers: Number of worker processes
    """
    app = create_feature_serving_app(feature_server, feature_store, config)
    
    uvicorn.run(
        app,
        host=host,
        port=port,
        workers=workers,
        access_log=True,
        log_level="info"
    )