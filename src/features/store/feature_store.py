"""
Main Feature Store Implementation

Central feature store orchestrating all components for comprehensive
feature management, serving, and lifecycle operations.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Union, Tuple, Callable
import time
import json
from pathlib import Path

import pandas as pd
import numpy as np
import redis.asyncio as redis
from pydantic import BaseModel

from .feature_metadata import (
    FeatureMetadata, 
    FeatureSchema,
    FeatureType,
    IndicatorCategory,
    FeatureVersion,
    FeatureSet
)
from .feature_registry import FeatureRegistry, FeatureSearchFilter
from .storage_backend import DeltaStorageBackend
from .lineage_tracker import LineageTracker
from ..utils.feature_validator import FeatureValidator
from ..utils.performance_monitor import PerformanceMonitor


logger = logging.getLogger(__name__)


class FeatureComputationResult(BaseModel):
    """Result of feature computation."""
    
    feature_id: str
    data: Any  # Will be converted to appropriate format
    computation_time_ms: float
    success: bool
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class FeatureStore:
    """
    Central feature store providing unified interface for:
    - Feature registration and lifecycle management
    - Real-time and batch feature computation
    - Feature serving with caching
    - Time-travel queries for backtesting
    - Feature lineage and versioning
    - Quality monitoring and validation
    """
    
    def __init__(
        self,
        storage_backend: DeltaStorageBackend,
        registry: FeatureRegistry,
        cache_client: redis.Redis,
        lineage_tracker: Optional[LineageTracker] = None,
        validator: Optional[FeatureValidator] = None,
        performance_monitor: Optional[PerformanceMonitor] = None,
        cache_ttl_seconds: int = 3600
    ):
        self.storage = storage_backend
        self.registry = registry
        self.cache = cache_client
        self.lineage_tracker = lineage_tracker
        self.validator = validator
        self.performance_monitor = performance_monitor
        self.cache_ttl = cache_ttl_seconds
        
        # Feature computation functions registry
        self.computation_functions: Dict[str, Callable] = {}
        
        # Performance metrics
        self.metrics = {
            'features_computed': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'errors': 0
        }
    
    async def register_feature(
        self,
        metadata: FeatureMetadata,
        computation_function: Optional[Callable] = None,
        overwrite: bool = False
    ) -> bool:
        """
        Register a new feature with the feature store.
        
        Args:
            metadata: Feature metadata
            computation_function: Function to compute the feature
            overwrite: Whether to overwrite existing feature
            
        Returns:
            Success status
        """
        try:
            # Validate metadata
            if self.validator:
                validation_result = await self.validator.validate_metadata(metadata)
                if not validation_result.is_valid:
                    logger.error(f"Metadata validation failed: {validation_result.errors}")
                    return False
            
            # Register with registry
            success = await self.registry.register_feature(metadata, overwrite)
            if not success:
                return False
            
            # Register computation function if provided
            if computation_function:
                self.computation_functions[metadata.feature_id] = computation_function
            
            # Track lineage
            if self.lineage_tracker:
                await self.lineage_tracker.track_feature_creation(
                    metadata.feature_id,
                    metadata.dependencies,
                    metadata.data_source
                )
            
            logger.info(f"Successfully registered feature: {metadata.feature_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register feature {metadata.feature_id}: {e}")
            return False
    
    async def compute_feature(
        self,
        feature_id: str,
        input_data: pd.DataFrame,
        entities: Optional[List[str]] = None,
        timestamp_column: str = "timestamp",
        entity_column: str = "entity_id",
        store_result: bool = True
    ) -> Optional[FeatureComputationResult]:
        """
        Compute feature values for given input data.
        
        Args:
            feature_id: Feature to compute
            input_data: Input market data
            entities: Specific entities to compute for
            timestamp_column: Timestamp column name
            entity_column: Entity column name  
            store_result: Whether to store computed results
            
        Returns:
            Computation result
        """
        start_time = time.time()
        
        try:
            # Get feature metadata
            feature_metadata = await self.registry.get_feature(feature_id)
            if not feature_metadata:
                return FeatureComputationResult(
                    feature_id=feature_id,
                    data=None,
                    computation_time_ms=0,
                    success=False,
                    error_message=f"Feature {feature_id} not found"
                )
            
            # Get computation function
            computation_func = self.computation_functions.get(feature_id)
            if not computation_func:
                return FeatureComputationResult(
                    feature_id=feature_id,
                    data=None,
                    computation_time_ms=0,
                    success=False,
                    error_message=f"No computation function for {feature_id}"
                )
            
            # Filter entities if specified
            if entities and entity_column in input_data.columns:
                input_data = input_data[input_data[entity_column].isin(entities)]
            
            # Compute feature
            result_data = computation_func(input_data, **feature_metadata.parameters)
            
            # Validate results if validator available
            if self.validator:
                validation_result = await self.validator.validate_data(
                    feature_metadata, result_data
                )
                if not validation_result.is_valid:
                    logger.warning(f"Data validation failed for {feature_id}: {validation_result.errors}")
            
            # Store results if requested
            if store_result:
                await self.storage.write_features(
                    feature_id=feature_id,
                    data=result_data,
                    timestamp_column=timestamp_column,
                    entity_columns=[entity_column] if entity_column in result_data.columns else None
                )
            
            computation_time_ms = (time.time() - start_time) * 1000
            
            # Update metrics
            await self.registry.update_feature_usage(feature_id, computation_time_ms)
            self.metrics['features_computed'] += 1
            
            # Track lineage
            if self.lineage_tracker:
                await self.lineage_tracker.track_feature_computation(
                    feature_id,
                    input_data.shape[0],  # Input record count
                    len(result_data) if isinstance(result_data, pd.DataFrame) else 1
                )
            
            return FeatureComputationResult(
                feature_id=feature_id,
                data=result_data,
                computation_time_ms=computation_time_ms,
                success=True,
                metadata={
                    'input_records': len(input_data),
                    'output_records': len(result_data) if isinstance(result_data, pd.DataFrame) else 1
                }
            )
            
        except Exception as e:
            computation_time_ms = (time.time() - start_time) * 1000
            self.metrics['errors'] += 1
            
            logger.error(f"Failed to compute feature {feature_id}: {e}")
            return FeatureComputationResult(
                feature_id=feature_id,
                data=None,
                computation_time_ms=computation_time_ms,
                success=False,
                error_message=str(e)
            )
    
    async def get_features(
        self,
        feature_ids: List[str],
        entities: List[str],
        timestamp: Optional[datetime] = None,
        entity_column: str = "entity_id",
        use_cache: bool = True
    ) -> Dict[str, pd.DataFrame]:
        """
        Retrieve feature values for specified entities.
        
        Args:
            feature_ids: Features to retrieve
            entities: Entity identifiers
            timestamp: Point-in-time timestamp (latest if None)
            entity_column: Entity column name
            use_cache: Whether to use cache
            
        Returns:
            Dictionary mapping feature_id to values DataFrame
        """
        try:
            results = {}
            
            for feature_id in feature_ids:
                # Check cache first
                if use_cache:
                    cache_key = self._generate_cache_key(feature_id, entities, timestamp)
                    cached_data = await self.cache.get(cache_key)
                    
                    if cached_data:
                        self.metrics['cache_hits'] += 1
                        try:
                            results[feature_id] = pd.read_json(cached_data, orient='records')
                            continue
                        except Exception as e:
                            logger.warning(f"Failed to deserialize cached data for {feature_id}: {e}")
                
                # Cache miss - retrieve from storage
                self.metrics['cache_misses'] += 1
                
                if timestamp:
                    # Point-in-time query
                    data = await self.storage.read_features(
                        feature_id=feature_id,
                        entities=entities,
                        entity_column=entity_column,
                        as_of_timestamp=timestamp
                    )
                else:
                    # Latest values
                    latest_data = await self.storage.get_latest_features(
                        feature_ids=[feature_id],
                        entities=entities,
                        entity_column=entity_column
                    )
                    data = latest_data.get(feature_id)
                
                if data is not None and not data.empty:
                    results[feature_id] = data
                    
                    # Cache the result
                    if use_cache:
                        cache_value = data.to_json(orient='records')
                        await self.cache.setex(
                            cache_key,
                            self.cache_ttl,
                            cache_value
                        )
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to get features: {e}")
            return {}
    
    async def get_feature_vector(
        self,
        feature_ids: List[str],
        entity_id: str,
        timestamp: Optional[datetime] = None,
        entity_column: str = "entity_id",
        use_cache: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Get feature vector for a single entity.
        
        Args:
            feature_ids: Features to include in vector
            entity_id: Single entity identifier
            timestamp: Point-in-time timestamp
            entity_column: Entity column name
            use_cache: Whether to use cache
            
        Returns:
            Feature vector as dictionary
        """
        try:
            features_data = await self.get_features(
                feature_ids=feature_ids,
                entities=[entity_id],
                timestamp=timestamp,
                entity_column=entity_column,
                use_cache=use_cache
            )
            
            if not features_data:
                return None
            
            # Build feature vector
            feature_vector = {}
            for feature_id, data in features_data.items():
                if not data.empty:
                    # Get latest value for the entity
                    entity_data = data[data[entity_column] == entity_id]
                    if not entity_data.empty:
                        # Get feature value columns (exclude metadata columns)
                        value_cols = [col for col in entity_data.columns 
                                    if not col.startswith('_') and col not in [entity_column, 'timestamp']]
                        
                        if len(value_cols) == 1:
                            feature_vector[feature_id] = entity_data.iloc[0][value_cols[0]]
                        else:
                            # Multiple value columns - return as dict
                            feature_vector[feature_id] = {
                                col: entity_data.iloc[0][col] for col in value_cols
                            }
            
            return feature_vector if feature_vector else None
            
        except Exception as e:
            logger.error(f"Failed to get feature vector for entity {entity_id}: {e}")
            return None
    
    async def batch_compute_features(
        self,
        feature_ids: List[str],
        input_data: pd.DataFrame,
        timestamp_column: str = "timestamp",
        entity_column: str = "entity_id",
        parallel: bool = True,
        batch_size: int = 1000
    ) -> Dict[str, FeatureComputationResult]:
        """
        Compute multiple features in batch.
        
        Args:
            feature_ids: Features to compute
            input_data: Input data
            timestamp_column: Timestamp column name
            entity_column: Entity column name
            parallel: Whether to compute in parallel
            batch_size: Batch size for processing
            
        Returns:
            Dictionary of computation results
        """
        try:
            if parallel:
                # Parallel computation
                tasks = [
                    self.compute_feature(
                        feature_id=feature_id,
                        input_data=input_data,
                        timestamp_column=timestamp_column,
                        entity_column=entity_column
                    )
                    for feature_id in feature_ids
                ]
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                return {
                    feature_id: result if not isinstance(result, Exception) else
                    FeatureComputationResult(
                        feature_id=feature_id,
                        data=None,
                        computation_time_ms=0,
                        success=False,
                        error_message=str(result)
                    )
                    for feature_id, result in zip(feature_ids, results)
                }
            
            else:
                # Sequential computation
                results = {}
                for feature_id in feature_ids:
                    result = await self.compute_feature(
                        feature_id=feature_id,
                        input_data=input_data,
                        timestamp_column=timestamp_column,
                        entity_column=entity_column
                    )
                    results[feature_id] = result
                
                return results
                
        except Exception as e:
            logger.error(f"Failed to batch compute features: {e}")
            return {}
    
    async def create_feature_set(
        self,
        set_id: str,
        name: str,
        feature_ids: List[str],
        description: str = "",
        tags: Optional[List[str]] = None
    ) -> bool:
        """Create a new feature set."""
        try:
            feature_set = FeatureSet(
                set_id=set_id,
                name=name,
                description=description,
                features=feature_ids,
                version="1.0.0",
                tags=tags or []
            )
            
            return await self.registry.register_feature_set(feature_set)
            
        except Exception as e:
            logger.error(f"Failed to create feature set {set_id}: {e}")
            return False
    
    async def get_feature_lineage(
        self,
        feature_id: str,
        depth: int = 5
    ) -> Optional[Dict[str, Any]]:
        """Get feature lineage information."""
        if not self.lineage_tracker:
            return None
        
        return await self.lineage_tracker.get_feature_lineage(feature_id, depth)
    
    async def search_features(
        self,
        query: str = "",
        filters: Optional[FeatureSearchFilter] = None,
        limit: int = 50
    ) -> List[FeatureMetadata]:
        """Search features using text query and filters."""
        if query:
            return await self.registry.search_features(query, filters, limit)
        else:
            return await self.registry.list_features(filters)
    
    async def get_feature_statistics(
        self,
        feature_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Optional[Dict[str, Any]]:
        """Get feature data statistics."""
        return await self.storage.get_feature_statistics(
            feature_id, start_time, end_time
        )
    
    async def invalidate_cache(
        self,
        feature_id: Optional[str] = None,
        entity_ids: Optional[List[str]] = None
    ) -> bool:
        """Invalidate cached feature data."""
        try:
            if feature_id and entity_ids:
                # Invalidate specific feature-entity combinations
                pattern = f"features:{feature_id}:*"
            elif feature_id:
                # Invalidate all cache entries for feature
                pattern = f"features:{feature_id}:*"
            else:
                # Invalidate all feature cache
                pattern = "features:*"
            
            # Get matching keys and delete
            keys = []
            async for key in self.cache.scan_iter(match=pattern):
                keys.append(key)
            
            if keys:
                await self.cache.delete(*keys)
                logger.info(f"Invalidated {len(keys)} cache entries")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to invalidate cache: {e}")
            return False
    
    def _generate_cache_key(
        self,
        feature_id: str,
        entities: List[str],
        timestamp: Optional[datetime]
    ) -> str:
        """Generate cache key for feature data."""
        entities_str = ",".join(sorted(entities))
        timestamp_str = timestamp.isoformat() if timestamp else "latest"
        return f"features:{feature_id}:{entities_str}:{timestamp_str}"
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get feature store metrics."""
        return {
            **self.metrics,
            'cache_hit_rate': (
                self.metrics['cache_hits'] / 
                (self.metrics['cache_hits'] + self.metrics['cache_misses'])
                if (self.metrics['cache_hits'] + self.metrics['cache_misses']) > 0 else 0
            )
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on feature store components."""
        health_status = {
            'overall': 'healthy',
            'components': {}
        }
        
        try:
            # Check registry (Redis)
            await self.cache.ping()
            health_status['components']['redis'] = 'healthy'
        except Exception as e:
            health_status['components']['redis'] = f'unhealthy: {e}'
            health_status['overall'] = 'degraded'
        
        try:
            # Check storage backend
            # Could add specific Delta Lake health checks here
            health_status['components']['storage'] = 'healthy'
        except Exception as e:
            health_status['components']['storage'] = f'unhealthy: {e}'
            health_status['overall'] = 'unhealthy'
        
        return health_status