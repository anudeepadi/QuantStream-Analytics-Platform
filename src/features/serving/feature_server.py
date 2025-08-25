"""
High-Performance Feature Server

Provides optimized feature serving with caching and sub-50ms latency.
"""

import asyncio
import time
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
import json
from collections import defaultdict, deque
import statistics

import redis.asyncio as redis
import pandas as pd
from fastapi import HTTPException

from .api_models import (
    FeatureRequest,
    FeatureResponse,
    BatchFeatureRequest,
    BatchFeatureResponse,
    FeatureVector,
    ServingMetrics,
    ServingConfig
)
from ..store.feature_store import FeatureStore


logger = logging.getLogger(__name__)


class PerformanceTracker:
    """Track performance metrics for feature serving."""
    
    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        self.response_times = deque(maxlen=window_size)
        self.request_count = 0
        self.success_count = 0
        self.error_count = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.features_served = 0
        self.unique_features = set()
        
    def record_request(
        self,
        response_time_ms: float,
        success: bool,
        cache_hit: bool,
        features_count: int,
        feature_ids: List[str]
    ) -> None:
        """Record request metrics."""
        self.request_count += 1
        self.response_times.append(response_time_ms)
        
        if success:
            self.success_count += 1
        else:
            self.error_count += 1
        
        if cache_hit:
            self.cache_hits += 1
        else:
            self.cache_misses += 1
        
        self.features_served += features_count
        self.unique_features.update(feature_ids)
    
    def get_metrics(self) -> ServingMetrics:
        """Get current performance metrics."""
        if self.response_times:
            avg_response_time = statistics.mean(self.response_times)
            sorted_times = sorted(self.response_times)
            p95_response_time = sorted_times[int(0.95 * len(sorted_times))] if sorted_times else 0
            p99_response_time = sorted_times[int(0.99 * len(sorted_times))] if sorted_times else 0
        else:
            avg_response_time = p95_response_time = p99_response_time = 0
        
        return ServingMetrics(
            total_requests=self.request_count,
            successful_requests=self.success_count,
            failed_requests=self.error_count,
            cache_hits=self.cache_hits,
            cache_misses=self.cache_misses,
            avg_response_time_ms=avg_response_time,
            p95_response_time_ms=p95_response_time,
            p99_response_time_ms=p99_response_time,
            features_served=self.features_served,
            unique_features=len(self.unique_features)
        )


class FeatureServer:
    """
    High-performance feature server with caching and optimizations.
    
    Provides:
    - Sub-50ms feature serving with Redis caching
    - Batch processing for efficiency
    - Connection pooling and async processing
    - Performance monitoring and alerting
    """
    
    def __init__(
        self,
        feature_store: FeatureStore,
        config: ServingConfig,
        cache_prefix: str = "feature_cache"
    ):
        self.feature_store = feature_store
        self.config = config
        self.cache_prefix = cache_prefix
        
        # Performance tracking
        self.metrics = PerformanceTracker()
        
        # Request rate limiting
        self.request_counts = defaultdict(deque)  # Per-entity request tracking
        
        # Precomputed feature sets for faster serving
        self.feature_set_cache: Dict[str, List[str]] = {}
        
        # Async semaphore for concurrency control
        self.semaphore = asyncio.Semaphore(config.max_concurrent_requests)
    
    async def serve_features(self, request: FeatureRequest) -> FeatureResponse:
        """
        Serve features for a single entity with optimized performance.
        
        Args:
            request: Feature request
            
        Returns:
            Feature response with sub-50ms latency target
        """
        start_time = time.time()
        
        async with self.semaphore:
            try:
                # Rate limiting check
                if not await self._check_rate_limit(request.entity_id):
                    raise HTTPException(
                        status_code=429,
                        detail="Rate limit exceeded"
                    )
                
                # Get feature vector
                feature_vector = await self.feature_store.get_feature_vector(
                    feature_ids=request.feature_ids,
                    entity_id=request.entity_id,
                    timestamp=request.timestamp,
                    entity_column=request.entity_column,
                    use_cache=request.use_cache
                )
                
                response_time_ms = (time.time() - start_time) * 1000
                
                # Check if we hit latency SLA
                if response_time_ms > self.config.latency_threshold_ms:
                    logger.warning(
                        f"Latency SLA miss: {response_time_ms:.2f}ms > {self.config.latency_threshold_ms}ms"
                    )
                
                # Build response
                if feature_vector:
                    response = FeatureResponse(
                        success=True,
                        entity_id=request.entity_id,
                        features=feature_vector,
                        timestamp=request.timestamp or datetime.now(timezone.utc),
                        cache_hit=True,  # TODO: track actual cache hits from feature store
                        response_time_ms=response_time_ms
                    )
                else:
                    response = FeatureResponse(
                        success=False,
                        entity_id=request.entity_id,
                        error_message="No features found for entity",
                        response_time_ms=response_time_ms
                    )
                
                # Record metrics
                self.metrics.record_request(
                    response_time_ms=response_time_ms,
                    success=response.success,
                    cache_hit=response.cache_hit,
                    features_count=len(response.features) if response.features else 0,
                    feature_ids=request.feature_ids
                )
                
                return response
                
            except Exception as e:
                response_time_ms = (time.time() - start_time) * 1000
                
                self.metrics.record_request(
                    response_time_ms=response_time_ms,
                    success=False,
                    cache_hit=False,
                    features_count=0,
                    feature_ids=request.feature_ids
                )
                
                logger.error(f"Error serving features for entity {request.entity_id}: {e}")
                return FeatureResponse(
                    success=False,
                    entity_id=request.entity_id,
                    error_message=str(e),
                    response_time_ms=response_time_ms
                )
    
    async def serve_batch_features(
        self, 
        request: BatchFeatureRequest
    ) -> BatchFeatureResponse:
        """
        Serve features for multiple entities in batch.
        
        Args:
            request: Batch feature request
            
        Returns:
            Batch feature response
        """
        start_time = time.time()
        
        async with self.semaphore:
            try:
                # Validate batch size
                if len(request.entity_ids) > self.config.max_batch_size:
                    raise ValueError(f"Batch size {len(request.entity_ids)} exceeds maximum {self.config.max_batch_size}")
                
                # Get features for all entities
                feature_data = await self.feature_store.get_features(
                    feature_ids=request.feature_ids,
                    entities=request.entity_ids,
                    timestamp=request.timestamp,
                    entity_column=request.entity_column,
                    use_cache=request.use_cache
                )
                
                # Build feature vectors
                feature_vectors = []
                failed_entities = []
                cache_stats = {"hits": 0, "misses": 0}
                
                for entity_id in request.entity_ids:
                    entity_features = {}
                    has_data = False
                    
                    for feature_id, feature_df in feature_data.items():
                        if not feature_df.empty:
                            entity_data = feature_df[
                                feature_df[request.entity_column] == entity_id
                            ]
                            
                            if not entity_data.empty:
                                has_data = True
                                # Extract feature values (excluding metadata columns)
                                value_cols = [
                                    col for col in entity_data.columns 
                                    if not col.startswith('_') and 
                                    col not in [request.entity_column, 'timestamp']
                                ]
                                
                                if len(value_cols) == 1:
                                    entity_features[feature_id] = entity_data.iloc[0][value_cols[0]]
                                else:
                                    entity_features[feature_id] = {
                                        col: entity_data.iloc[0][col] for col in value_cols
                                    }
                    
                    if has_data:
                        feature_vectors.append(FeatureVector(
                            entity_id=entity_id,
                            features=entity_features,
                            timestamp=request.timestamp or datetime.now(timezone.utc)
                        ))
                        cache_stats["hits"] += 1
                    else:
                        failed_entities.append(entity_id)
                        cache_stats["misses"] += 1
                
                response_time_ms = (time.time() - start_time) * 1000
                
                response = BatchFeatureResponse(
                    success=True,
                    feature_vectors=feature_vectors,
                    failed_entities=failed_entities,
                    cache_stats=cache_stats,
                    response_time_ms=response_time_ms,
                    total_entities=len(request.entity_ids)
                )
                
                # Record metrics
                self.metrics.record_request(
                    response_time_ms=response_time_ms,
                    success=True,
                    cache_hit=len(feature_vectors) > 0,
                    features_count=len(feature_vectors) * len(request.feature_ids),
                    feature_ids=request.feature_ids
                )
                
                return response
                
            except Exception as e:
                response_time_ms = (time.time() - start_time) * 1000
                
                self.metrics.record_request(
                    response_time_ms=response_time_ms,
                    success=False,
                    cache_hit=False,
                    features_count=0,
                    feature_ids=request.feature_ids
                )
                
                logger.error(f"Error serving batch features: {e}")
                return BatchFeatureResponse(
                    success=False,
                    error_message=str(e),
                    response_time_ms=response_time_ms,
                    total_entities=len(request.entity_ids) if hasattr(request, 'entity_ids') else 0
                )
    
    async def precompute_feature_sets(
        self,
        feature_set_configs: Dict[str, List[str]]
    ) -> None:
        """
        Precompute popular feature combinations for faster serving.
        
        Args:
            feature_set_configs: Dictionary mapping set names to feature ID lists
        """
        try:
            for set_name, feature_ids in feature_set_configs.items():
                # Validate all features exist
                valid_features = []
                for feature_id in feature_ids:
                    metadata = await self.feature_store.registry.get_feature(feature_id)
                    if metadata and metadata.is_active:
                        valid_features.append(feature_id)
                    else:
                        logger.warning(f"Feature {feature_id} not found or inactive in set {set_name}")
                
                if valid_features:
                    self.feature_set_cache[set_name] = valid_features
                    logger.info(f"Cached feature set '{set_name}' with {len(valid_features)} features")
                
        except Exception as e:
            logger.error(f"Error precomputing feature sets: {e}")
    
    async def serve_feature_set(
        self,
        set_name: str,
        entity_id: str,
        timestamp: Optional[datetime] = None
    ) -> FeatureResponse:
        """
        Serve a precomputed feature set.
        
        Args:
            set_name: Feature set name
            entity_id: Entity identifier
            timestamp: Point-in-time timestamp
            
        Returns:
            Feature response
        """
        if set_name not in self.feature_set_cache:
            return FeatureResponse(
                success=False,
                entity_id=entity_id,
                error_message=f"Feature set '{set_name}' not found"
            )
        
        request = FeatureRequest(
            feature_ids=self.feature_set_cache[set_name],
            entity_id=entity_id,
            timestamp=timestamp
        )
        
        return await self.serve_features(request)
    
    async def warm_cache(
        self,
        feature_ids: List[str],
        entity_ids: List[str],
        batch_size: int = 100
    ) -> Dict[str, int]:
        """
        Warm the cache with popular features and entities.
        
        Args:
            feature_ids: Features to cache
            entity_ids: Entities to cache
            batch_size: Batch size for warming
            
        Returns:
            Cache warming statistics
        """
        stats = {"batches_processed": 0, "entities_cached": 0, "errors": 0}
        
        try:
            # Process entities in batches
            for i in range(0, len(entity_ids), batch_size):
                batch_entities = entity_ids[i:i + batch_size]
                
                try:
                    # Request features for batch (this will populate cache)
                    await self.feature_store.get_features(
                        feature_ids=feature_ids,
                        entities=batch_entities,
                        use_cache=True
                    )
                    
                    stats["batches_processed"] += 1
                    stats["entities_cached"] += len(batch_entities)
                    
                except Exception as e:
                    logger.error(f"Error warming cache for batch {i//batch_size + 1}: {e}")
                    stats["errors"] += 1
            
            logger.info(f"Cache warming completed: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error during cache warming: {e}")
            stats["errors"] += 1
            return stats
    
    async def _check_rate_limit(self, entity_id: str) -> bool:
        """Check if entity is within rate limits."""
        if not self.config.enable_per_entity_limits:
            return True
        
        try:
            now = time.time()
            minute_ago = now - 60
            
            # Clean old requests
            entity_requests = self.request_counts[entity_id]
            while entity_requests and entity_requests[0] < minute_ago:
                entity_requests.popleft()
            
            # Check rate limit
            if len(entity_requests) >= self.config.per_entity_rpm:
                return False
            
            # Record current request
            entity_requests.append(now)
            return True
            
        except Exception as e:
            logger.error(f"Error checking rate limit: {e}")
            return True  # Allow request if rate limiting fails
    
    async def get_metrics(self) -> ServingMetrics:
        """Get current serving metrics."""
        return self.metrics.get_metrics()
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on feature server."""
        health = {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "components": {},
            "metrics": self.metrics.get_metrics().dict()
        }
        
        try:
            # Check feature store health
            store_health = await self.feature_store.health_check()
            health["components"]["feature_store"] = store_health["overall"]
            
            # Check performance thresholds
            metrics = self.metrics.get_metrics()
            
            if metrics.avg_response_time_ms > self.config.latency_threshold_ms:
                health["status"] = "degraded"
                health["warnings"] = health.get("warnings", [])
                health["warnings"].append(f"High latency: {metrics.avg_response_time_ms:.2f}ms")
            
            if metrics.cache_hit_rate < self.config.cache_hit_rate_threshold:
                health["status"] = "degraded"
                health["warnings"] = health.get("warnings", [])
                health["warnings"].append(f"Low cache hit rate: {metrics.cache_hit_rate:.2%}")
            
            error_rate = 1 - metrics.success_rate if metrics.total_requests > 0 else 0
            if error_rate > self.config.error_rate_threshold:
                health["status"] = "unhealthy"
                health["errors"] = health.get("errors", [])
                health["errors"].append(f"High error rate: {error_rate:.2%}")
        
        except Exception as e:
            health["status"] = "unhealthy"
            health["errors"] = [f"Health check failed: {e}"]
        
        return health