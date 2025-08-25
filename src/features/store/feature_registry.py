"""
Feature Registry

Central registry for managing feature definitions, metadata, and discovery.
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Any, Tuple
import json
import logging
from pathlib import Path

import redis.asyncio as redis
from pydantic import BaseModel

from .feature_metadata import (
    FeatureMetadata, 
    FeatureVersion, 
    FeatureSet, 
    FeatureValidationRule,
    IndicatorCategory
)


logger = logging.getLogger(__name__)


class FeatureSearchFilter(BaseModel):
    """Feature search and filter criteria."""
    
    namespace: Optional[str] = None
    category: Optional[IndicatorCategory] = None
    tags: Optional[List[str]] = None
    feature_type: Optional[str] = None
    is_active: Optional[bool] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None
    min_quality_score: Optional[float] = None
    has_dependencies: Optional[bool] = None


class FeatureRegistry:
    """
    Central feature registry for metadata management and discovery.
    
    Provides capabilities for:
    - Feature registration and lifecycle management
    - Metadata storage and retrieval
    - Feature discovery and search
    - Version management
    - Dependency tracking
    """
    
    def __init__(
        self,
        redis_client: redis.Redis,
        registry_prefix: str = "feature_registry"
    ):
        self.redis = redis_client
        self.prefix = registry_prefix
        self._lock = asyncio.Lock()
        
        # Redis key patterns
        self.feature_key_pattern = f"{self.prefix}:features:{{feature_id}}"
        self.feature_versions_pattern = f"{self.prefix}:versions:{{feature_id}}"
        self.namespace_pattern = f"{self.prefix}:namespaces:{{namespace}}"
        self.category_pattern = f"{self.prefix}:categories:{{category}}"
        self.tag_pattern = f"{self.prefix}:tags:{{tag}}"
        self.feature_sets_pattern = f"{self.prefix}:sets:{{set_id}}"
        self.validation_rules_pattern = f"{self.prefix}:rules:{{feature_id}}"
        
        # Index keys
        self.all_features_key = f"{self.prefix}:all_features"
        self.active_features_key = f"{self.prefix}:active_features"
        self.feature_dependencies_key = f"{self.prefix}:dependencies"
    
    async def register_feature(
        self,
        metadata: FeatureMetadata,
        overwrite: bool = False
    ) -> bool:
        """
        Register a new feature or update existing one.
        
        Args:
            metadata: Feature metadata to register
            overwrite: Whether to overwrite existing feature
            
        Returns:
            True if feature was registered successfully
        """
        async with self._lock:
            try:
                feature_key = self.feature_key_pattern.format(feature_id=metadata.feature_id)
                
                # Check if feature already exists
                existing_data = await self.redis.get(feature_key)
                if existing_data and not overwrite:
                    existing_metadata = FeatureMetadata.parse_raw(existing_data)
                    
                    # Auto-increment version if needed
                    if existing_metadata.generate_signature() != metadata.generate_signature():
                        current_version = existing_metadata.get_version_object()
                        
                        if existing_metadata.should_increment_major(metadata):
                            new_version = current_version.increment_major()
                        elif existing_metadata.should_increment_minor(metadata):
                            new_version = current_version.increment_minor()
                        else:
                            new_version = current_version.increment_patch()
                        
                        metadata.set_version(new_version)
                        logger.info(f"Auto-incremented feature {metadata.feature_id} to version {new_version}")
                
                # Store feature metadata
                await self.redis.set(feature_key, metadata.json())
                
                # Update version history
                versions_key = self.feature_versions_pattern.format(feature_id=metadata.feature_id)
                await self.redis.zadd(
                    versions_key,
                    {metadata.version: datetime.now(timezone.utc).timestamp()}
                )
                
                # Update indexes
                await self._update_indexes_on_register(metadata)
                
                logger.info(f"Registered feature: {metadata.feature_id} v{metadata.version}")
                return True
                
            except Exception as e:
                logger.error(f"Failed to register feature {metadata.feature_id}: {e}")
                return False
    
    async def get_feature(
        self,
        feature_id: str,
        version: Optional[str] = None
    ) -> Optional[FeatureMetadata]:
        """
        Retrieve feature metadata.
        
        Args:
            feature_id: Feature identifier
            version: Specific version (latest if not specified)
            
        Returns:
            Feature metadata if found
        """
        try:
            if version:
                # Get specific version (implementation would need versioned storage)
                feature_key = f"{self.feature_key_pattern.format(feature_id=feature_id)}:v{version}"
            else:
                feature_key = self.feature_key_pattern.format(feature_id=feature_id)
            
            data = await self.redis.get(feature_key)
            if data:
                return FeatureMetadata.parse_raw(data)
                
            return None
            
        except Exception as e:
            logger.error(f"Failed to retrieve feature {feature_id}: {e}")
            return None
    
    async def list_features(
        self,
        filter_criteria: Optional[FeatureSearchFilter] = None
    ) -> List[FeatureMetadata]:
        """
        List features matching filter criteria.
        
        Args:
            filter_criteria: Optional search filters
            
        Returns:
            List of matching features
        """
        try:
            # Get all feature IDs
            if filter_criteria and filter_criteria.is_active is False:
                all_feature_ids = await self.redis.smembers(self.all_features_key)
            else:
                all_feature_ids = await self.redis.smembers(self.active_features_key)
            
            features = []
            for feature_id in all_feature_ids:
                feature = await self.get_feature(feature_id.decode())
                if feature and self._matches_filter(feature, filter_criteria):
                    features.append(feature)
            
            return features
            
        except Exception as e:
            logger.error(f"Failed to list features: {e}")
            return []
    
    async def search_features(
        self,
        query: str,
        filter_criteria: Optional[FeatureSearchFilter] = None,
        limit: int = 50
    ) -> List[FeatureMetadata]:
        """
        Search features by text query.
        
        Args:
            query: Search query
            filter_criteria: Optional filters
            limit: Maximum results
            
        Returns:
            List of matching features
        """
        try:
            # Get candidate features
            candidates = await self.list_features(filter_criteria)
            
            # Simple text matching (could be enhanced with full-text search)
            query_lower = query.lower()
            matches = []
            
            for feature in candidates:
                score = 0
                
                # Match in name (highest weight)
                if query_lower in feature.name.lower():
                    score += 10
                
                # Match in description
                if query_lower in feature.description.lower():
                    score += 5
                
                # Match in tags
                for tag in feature.tags:
                    if query_lower in tag.lower():
                        score += 3
                
                # Match in feature ID
                if query_lower in feature.feature_id.lower():
                    score += 2
                
                if score > 0:
                    matches.append((feature, score))
            
            # Sort by score and limit results
            matches.sort(key=lambda x: x[1], reverse=True)
            return [feature for feature, _ in matches[:limit]]
            
        except Exception as e:
            logger.error(f"Failed to search features: {e}")
            return []
    
    async def get_feature_versions(self, feature_id: str) -> List[str]:
        """Get all versions for a feature."""
        try:
            versions_key = self.feature_versions_pattern.format(feature_id=feature_id)
            versions = await self.redis.zrevrange(versions_key, 0, -1)
            return [v.decode() for v in versions]
            
        except Exception as e:
            logger.error(f"Failed to get versions for feature {feature_id}: {e}")
            return []
    
    async def get_dependencies(
        self,
        feature_id: str,
        recursive: bool = False
    ) -> List[str]:
        """Get feature dependencies."""
        try:
            feature = await self.get_feature(feature_id)
            if not feature:
                return []
            
            dependencies = feature.dependencies.copy()
            
            if recursive:
                # Get transitive dependencies
                all_deps = set(dependencies)
                queue = dependencies.copy()
                
                while queue:
                    current = queue.pop(0)
                    dep_feature = await self.get_feature(current)
                    if dep_feature:
                        for dep in dep_feature.dependencies:
                            if dep not in all_deps:
                                all_deps.add(dep)
                                queue.append(dep)
                
                dependencies = list(all_deps)
            
            return dependencies
            
        except Exception as e:
            logger.error(f"Failed to get dependencies for {feature_id}: {e}")
            return []
    
    async def get_dependents(self, feature_id: str) -> List[str]:
        """Get features that depend on this feature."""
        try:
            dependents = []
            all_features = await self.list_features()
            
            for feature in all_features:
                if feature_id in feature.dependencies:
                    dependents.append(feature.feature_id)
            
            return dependents
            
        except Exception as e:
            logger.error(f"Failed to get dependents for {feature_id}: {e}")
            return []
    
    async def deactivate_feature(self, feature_id: str) -> bool:
        """Deactivate a feature."""
        try:
            feature = await self.get_feature(feature_id)
            if not feature:
                return False
            
            feature.is_active = False
            feature.updated_at = datetime.now(timezone.utc)
            
            # Update storage
            feature_key = self.feature_key_pattern.format(feature_id=feature_id)
            await self.redis.set(feature_key, feature.json())
            
            # Update indexes
            await self.redis.srem(self.active_features_key, feature_id)
            
            logger.info(f"Deactivated feature: {feature_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to deactivate feature {feature_id}: {e}")
            return False
    
    async def update_feature_usage(
        self,
        feature_id: str,
        computation_time_ms: Optional[float] = None
    ) -> None:
        """Update feature usage statistics."""
        try:
            feature = await self.get_feature(feature_id)
            if feature:
                feature.update_usage_stats(computation_time_ms)
                
                feature_key = self.feature_key_pattern.format(feature_id=feature_id)
                await self.redis.set(feature_key, feature.json())
                
        except Exception as e:
            logger.error(f"Failed to update usage for feature {feature_id}: {e}")
    
    async def register_feature_set(self, feature_set: FeatureSet) -> bool:
        """Register a feature set."""
        try:
            set_key = self.feature_sets_pattern.format(set_id=feature_set.set_id)
            await self.redis.set(set_key, feature_set.json())
            
            logger.info(f"Registered feature set: {feature_set.set_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register feature set {feature_set.set_id}: {e}")
            return False
    
    async def get_feature_set(self, set_id: str) -> Optional[FeatureSet]:
        """Get feature set by ID."""
        try:
            set_key = self.feature_sets_pattern.format(set_id=set_id)
            data = await self.redis.get(set_key)
            if data:
                return FeatureSet.parse_raw(data)
            return None
            
        except Exception as e:
            logger.error(f"Failed to get feature set {set_id}: {e}")
            return None
    
    async def add_validation_rule(self, rule: FeatureValidationRule) -> bool:
        """Add validation rule for a feature."""
        try:
            rules_key = self.validation_rules_pattern.format(feature_id=rule.feature_id)
            await self.redis.hset(rules_key, rule.rule_id, rule.json())
            
            logger.info(f"Added validation rule {rule.rule_id} for feature {rule.feature_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add validation rule: {e}")
            return False
    
    async def get_validation_rules(self, feature_id: str) -> List[FeatureValidationRule]:
        """Get validation rules for a feature."""
        try:
            rules_key = self.validation_rules_pattern.format(feature_id=feature_id)
            rule_data = await self.redis.hgetall(rules_key)
            
            rules = []
            for rule_json in rule_data.values():
                rule = FeatureValidationRule.parse_raw(rule_json)
                if rule.is_active:
                    rules.append(rule)
            
            return rules
            
        except Exception as e:
            logger.error(f"Failed to get validation rules for {feature_id}: {e}")
            return []
    
    async def _update_indexes_on_register(self, metadata: FeatureMetadata) -> None:
        """Update search indexes when registering a feature."""
        # Add to main indexes
        await self.redis.sadd(self.all_features_key, metadata.feature_id)
        if metadata.is_active:
            await self.redis.sadd(self.active_features_key, metadata.feature_id)
        
        # Add to namespace index
        namespace_key = self.namespace_pattern.format(namespace=metadata.namespace)
        await self.redis.sadd(namespace_key, metadata.feature_id)
        
        # Add to category index
        category_key = self.category_pattern.format(category=metadata.category.value)
        await self.redis.sadd(category_key, metadata.feature_id)
        
        # Add to tag indexes
        for tag in metadata.tags:
            tag_key = self.tag_pattern.format(tag=tag)
            await self.redis.sadd(tag_key, metadata.feature_id)
    
    def _matches_filter(
        self,
        feature: FeatureMetadata,
        filter_criteria: Optional[FeatureSearchFilter]
    ) -> bool:
        """Check if feature matches filter criteria."""
        if not filter_criteria:
            return True
        
        # Namespace filter
        if filter_criteria.namespace and feature.namespace != filter_criteria.namespace:
            return False
        
        # Category filter
        if filter_criteria.category and feature.category != filter_criteria.category:
            return False
        
        # Tags filter (feature must have all specified tags)
        if filter_criteria.tags:
            if not all(tag in feature.tags for tag in filter_criteria.tags):
                return False
        
        # Feature type filter
        if (filter_criteria.feature_type and 
            feature.schema.feature_type.value != filter_criteria.feature_type):
            return False
        
        # Active status filter
        if (filter_criteria.is_active is not None and 
            feature.is_active != filter_criteria.is_active):
            return False
        
        # Date filters
        if (filter_criteria.created_after and 
            feature.created_at < filter_criteria.created_after):
            return False
        
        if (filter_criteria.created_before and 
            feature.created_at > filter_criteria.created_before):
            return False
        
        # Quality score filter
        if (filter_criteria.min_quality_score is not None and
            (feature.quality_score is None or 
             feature.quality_score < filter_criteria.min_quality_score)):
            return False
        
        # Dependencies filter
        if (filter_criteria.has_dependencies is not None):
            has_deps = len(feature.dependencies) > 0
            if has_deps != filter_criteria.has_dependencies:
                return False
        
        return True