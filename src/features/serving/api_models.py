"""
API Models for Feature Serving

Defines request/response models for the feature serving API.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from pydantic import BaseModel, Field, validator
from enum import Enum


class ServingMode(str, Enum):
    """Feature serving modes."""
    LATEST = "latest"
    POINT_IN_TIME = "point_in_time"
    BATCH = "batch"


class FeatureRequest(BaseModel):
    """Single feature request."""
    
    feature_ids: List[str] = Field(..., description="List of feature IDs to retrieve")
    entity_id: str = Field(..., description="Entity identifier")
    timestamp: Optional[datetime] = Field(default=None, description="Point-in-time timestamp")
    use_cache: bool = Field(default=True, description="Whether to use cache")
    entity_column: str = Field(default="entity_id", description="Entity column name")
    
    @validator('feature_ids')
    def validate_feature_ids(cls, v):
        if not v:
            raise ValueError("feature_ids cannot be empty")
        return v


class BatchFeatureRequest(BaseModel):
    """Batch feature request for multiple entities."""
    
    feature_ids: List[str] = Field(..., description="List of feature IDs to retrieve")
    entity_ids: List[str] = Field(..., description="List of entity identifiers")
    timestamp: Optional[datetime] = Field(default=None, description="Point-in-time timestamp")
    use_cache: bool = Field(default=True, description="Whether to use cache")
    entity_column: str = Field(default="entity_id", description="Entity column name")
    max_entities: int = Field(default=1000, description="Maximum entities per request")
    
    @validator('entity_ids')
    def validate_entity_ids(cls, v):
        if not v:
            raise ValueError("entity_ids cannot be empty")
        if len(v) > 1000:  # Default max
            raise ValueError("Too many entities in single request")
        return v


class FeatureVector(BaseModel):
    """Feature vector for a single entity."""
    
    entity_id: str = Field(..., description="Entity identifier")
    features: Dict[str, Any] = Field(..., description="Feature values")
    timestamp: Optional[datetime] = Field(default=None, description="Feature timestamp")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


class FeatureResponse(BaseModel):
    """Single feature response."""
    
    success: bool = Field(..., description="Whether request was successful")
    entity_id: str = Field(..., description="Entity identifier")
    features: Dict[str, Any] = Field(default_factory=dict, description="Feature values")
    timestamp: Optional[datetime] = Field(default=None, description="Feature timestamp")
    error_message: Optional[str] = Field(default=None, description="Error message if failed")
    cache_hit: bool = Field(default=False, description="Whether result came from cache")
    response_time_ms: float = Field(default=0.0, description="Response time in milliseconds")


class BatchFeatureResponse(BaseModel):
    """Batch feature response."""
    
    success: bool = Field(..., description="Whether request was successful")
    feature_vectors: List[FeatureVector] = Field(default_factory=list, description="Feature vectors")
    failed_entities: List[str] = Field(default_factory=list, description="Entities that failed")
    error_message: Optional[str] = Field(default=None, description="Error message if failed")
    cache_stats: Dict[str, int] = Field(default_factory=dict, description="Cache hit/miss stats")
    response_time_ms: float = Field(default=0.0, description="Total response time in milliseconds")
    total_entities: int = Field(default=0, description="Total entities processed")


class FeatureComputeRequest(BaseModel):
    """Request to compute features on-demand."""
    
    feature_ids: List[str] = Field(..., description="Features to compute")
    input_data: Dict[str, Any] = Field(..., description="Input market data")
    entity_ids: Optional[List[str]] = Field(default=None, description="Specific entities to compute")
    store_result: bool = Field(default=True, description="Whether to store computed results")
    computation_mode: str = Field(default="sync", description="Computation mode (sync/async)")


class FeatureComputeResponse(BaseModel):
    """Response from feature computation."""
    
    success: bool = Field(..., description="Whether computation was successful")
    feature_id: str = Field(..., description="Computed feature ID")
    computation_time_ms: float = Field(..., description="Computation time")
    records_processed: int = Field(default=0, description="Number of records processed")
    error_message: Optional[str] = Field(default=None, description="Error message if failed")


class FeatureSearchRequest(BaseModel):
    """Request for feature discovery/search."""
    
    query: str = Field(default="", description="Search query")
    namespace: Optional[str] = Field(default=None, description="Feature namespace filter")
    category: Optional[str] = Field(default=None, description="Feature category filter")
    tags: Optional[List[str]] = Field(default=None, description="Feature tags filter")
    limit: int = Field(default=50, description="Maximum results")
    include_metadata: bool = Field(default=True, description="Include full metadata")


class FeatureSearchResponse(BaseModel):
    """Response from feature search."""
    
    success: bool = Field(..., description="Whether search was successful")
    features: List[Dict[str, Any]] = Field(default_factory=list, description="Matching features")
    total_matches: int = Field(default=0, description="Total number of matches")
    error_message: Optional[str] = Field(default=None, description="Error message if failed")
    response_time_ms: float = Field(default=0.0, description="Response time")


class FeatureStatsRequest(BaseModel):
    """Request for feature statistics."""
    
    feature_id: str = Field(..., description="Feature ID")
    start_time: Optional[datetime] = Field(default=None, description="Start time for stats")
    end_time: Optional[datetime] = Field(default=None, description="End time for stats")
    include_distribution: bool = Field(default=False, description="Include value distribution")


class FeatureStatsResponse(BaseModel):
    """Response with feature statistics."""
    
    success: bool = Field(..., description="Whether request was successful")
    feature_id: str = Field(..., description="Feature ID")
    statistics: Dict[str, Any] = Field(default_factory=dict, description="Feature statistics")
    error_message: Optional[str] = Field(default=None, description="Error message if failed")


class ServingMetrics(BaseModel):
    """Feature serving performance metrics."""
    
    total_requests: int = Field(default=0, description="Total requests served")
    successful_requests: int = Field(default=0, description="Successful requests")
    failed_requests: int = Field(default=0, description="Failed requests")
    cache_hits: int = Field(default=0, description="Cache hits")
    cache_misses: int = Field(default=0, description="Cache misses")
    avg_response_time_ms: float = Field(default=0.0, description="Average response time")
    p95_response_time_ms: float = Field(default=0.0, description="P95 response time")
    p99_response_time_ms: float = Field(default=0.0, description="P99 response time")
    features_served: int = Field(default=0, description="Total features served")
    unique_features: int = Field(default=0, description="Unique features served")
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.total_requests == 0:
            return 0.0
        return self.successful_requests / self.total_requests
    
    @property
    def cache_hit_rate(self) -> float:
        """Calculate cache hit rate."""
        total_cache_ops = self.cache_hits + self.cache_misses
        if total_cache_ops == 0:
            return 0.0
        return self.cache_hits / total_cache_ops


class HealthCheckResponse(BaseModel):
    """Health check response."""
    
    status: str = Field(..., description="Overall health status")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Check timestamp")
    components: Dict[str, str] = Field(default_factory=dict, description="Component health status")
    metrics: ServingMetrics = Field(default_factory=ServingMetrics, description="Current metrics")
    version: str = Field(default="1.0.0", description="API version")


class CacheInvalidateRequest(BaseModel):
    """Cache invalidation request."""
    
    feature_id: Optional[str] = Field(default=None, description="Feature to invalidate")
    entity_ids: Optional[List[str]] = Field(default=None, description="Specific entities to invalidate")
    invalidate_all: bool = Field(default=False, description="Invalidate all cache entries")
    pattern: Optional[str] = Field(default=None, description="Cache key pattern to match")


class CacheInvalidateResponse(BaseModel):
    """Cache invalidation response."""
    
    success: bool = Field(..., description="Whether invalidation was successful")
    keys_invalidated: int = Field(default=0, description="Number of cache keys invalidated")
    error_message: Optional[str] = Field(default=None, description="Error message if failed")


class FeatureLineageRequest(BaseModel):
    """Feature lineage request."""
    
    feature_id: str = Field(..., description="Feature ID to get lineage for")
    depth: int = Field(default=5, description="Maximum depth for lineage traversal")
    include_usage: bool = Field(default=True, description="Include usage statistics")


class FeatureLineageResponse(BaseModel):
    """Feature lineage response."""
    
    success: bool = Field(..., description="Whether request was successful")
    feature_id: str = Field(..., description="Feature ID")
    lineage: Dict[str, Any] = Field(default_factory=dict, description="Lineage information")
    error_message: Optional[str] = Field(default=None, description="Error message if failed")


# Configuration models
class ServingConfig(BaseModel):
    """Feature serving configuration."""
    
    max_batch_size: int = Field(default=1000, description="Maximum batch size")
    cache_ttl_seconds: int = Field(default=3600, description="Cache TTL in seconds")
    response_timeout_ms: int = Field(default=5000, description="Response timeout")
    max_concurrent_requests: int = Field(default=1000, description="Max concurrent requests")
    enable_metrics: bool = Field(default=True, description="Enable metrics collection")
    enable_tracing: bool = Field(default=False, description="Enable request tracing")
    
    # Performance thresholds
    latency_threshold_ms: float = Field(default=50.0, description="Latency SLA threshold")
    cache_hit_rate_threshold: float = Field(default=0.95, description="Cache hit rate threshold")
    error_rate_threshold: float = Field(default=0.01, description="Error rate threshold")


class RateLimitConfig(BaseModel):
    """Rate limiting configuration."""
    
    requests_per_minute: int = Field(default=6000, description="Requests per minute limit")
    burst_size: int = Field(default=100, description="Burst request limit")
    enable_per_entity_limits: bool = Field(default=True, description="Enable per-entity rate limiting")
    per_entity_rpm: int = Field(default=600, description="Per-entity requests per minute")