"""
Feature Lineage Tracking

Tracks feature dependencies, computation history, and data lineage
for reproducibility and debugging.
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Any, Tuple
import json
import logging
from enum import Enum

import redis.asyncio as redis
from pydantic import BaseModel, Field


logger = logging.getLogger(__name__)


class LineageEventType(str, Enum):
    """Types of lineage events."""
    FEATURE_CREATED = "feature_created"
    FEATURE_COMPUTED = "feature_computed"
    FEATURE_ACCESSED = "feature_accessed"
    FEATURE_UPDATED = "feature_updated"
    FEATURE_DELETED = "feature_deleted"
    DATA_INGESTED = "data_ingested"
    MODEL_TRAINED = "model_trained"
    MODEL_INFERENCE = "model_inference"


class LineageEvent(BaseModel):
    """Individual lineage event."""
    
    event_id: str = Field(..., description="Unique event identifier")
    event_type: LineageEventType = Field(..., description="Type of lineage event")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    feature_id: str = Field(..., description="Primary feature involved")
    actor: str = Field(default="system", description="Who/what triggered the event")
    
    # Event-specific data
    inputs: List[str] = Field(default_factory=list, description="Input feature IDs or data sources")
    outputs: List[str] = Field(default_factory=list, description="Output feature IDs")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Event parameters")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    # Performance metrics
    duration_ms: Optional[float] = Field(default=None, description="Event duration in milliseconds")
    input_records: Optional[int] = Field(default=None, description="Number of input records")
    output_records: Optional[int] = Field(default=None, description="Number of output records")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class FeatureLineage(BaseModel):
    """Complete lineage information for a feature."""
    
    feature_id: str
    creation_event: Optional[LineageEvent] = None
    upstream_features: List[str] = Field(default_factory=list)
    downstream_features: List[str] = Field(default_factory=list)
    data_sources: List[str] = Field(default_factory=list)
    computation_history: List[LineageEvent] = Field(default_factory=list)
    access_history: List[LineageEvent] = Field(default_factory=list)
    
    # Statistics
    total_computations: int = 0
    total_accesses: int = 0
    last_computed: Optional[datetime] = None
    last_accessed: Optional[datetime] = None


class LineageTracker:
    """
    Feature lineage tracking system.
    
    Provides comprehensive tracking of:
    - Feature creation and dependencies
    - Data transformations and computations  
    - Feature access patterns
    - Model training and inference lineage
    """
    
    def __init__(
        self,
        redis_client: redis.Redis,
        lineage_prefix: str = "feature_lineage",
        max_history_size: int = 1000
    ):
        self.redis = redis_client
        self.prefix = lineage_prefix
        self.max_history_size = max_history_size
        
        # Redis key patterns
        self.events_key_pattern = f"{self.prefix}:events:{{feature_id}}"
        self.global_events_key = f"{self.prefix}:global_events"
        self.dependencies_key_pattern = f"{self.prefix}:deps:{{feature_id}}"
        self.dependents_key_pattern = f"{self.prefix}:dependents:{{feature_id}}"
        self.data_sources_key_pattern = f"{self.prefix}:sources:{{feature_id}}"
        
        # Indices for efficient querying
        self.events_by_type_pattern = f"{self.prefix}:by_type:{{event_type}}"
        self.events_by_actor_pattern = f"{self.prefix}:by_actor:{{actor}}"
        self.events_by_time_key = f"{self.prefix}:by_time"
    
    async def track_feature_creation(
        self,
        feature_id: str,
        dependencies: List[str],
        data_source: str,
        actor: str = "system",
        parameters: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Track feature creation event.
        
        Args:
            feature_id: Feature being created
            dependencies: List of dependent feature IDs
            data_source: Primary data source
            actor: Who created the feature
            parameters: Creation parameters
            
        Returns:
            Success status
        """
        try:
            event = LineageEvent(
                event_id=f"{feature_id}_created_{int(datetime.now(timezone.utc).timestamp())}",
                event_type=LineageEventType.FEATURE_CREATED,
                feature_id=feature_id,
                actor=actor,
                inputs=dependencies + [data_source] if data_source else dependencies,
                outputs=[feature_id],
                parameters=parameters or {}
            )
            
            # Store the event
            await self._store_event(event)
            
            # Update dependency mappings
            if dependencies:
                await self._update_dependencies(feature_id, dependencies)
            
            # Update data source mapping
            if data_source:
                await self._update_data_sources(feature_id, [data_source])
            
            logger.info(f"Tracked feature creation: {feature_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to track feature creation for {feature_id}: {e}")
            return False
    
    async def track_feature_computation(
        self,
        feature_id: str,
        input_records: int,
        output_records: int,
        duration_ms: Optional[float] = None,
        input_features: Optional[List[str]] = None,
        actor: str = "system",
        parameters: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Track feature computation event.
        
        Args:
            feature_id: Feature being computed
            input_records: Number of input records
            output_records: Number of output records
            duration_ms: Computation duration
            input_features: Input feature dependencies
            actor: Who triggered computation
            parameters: Computation parameters
            
        Returns:
            Success status
        """
        try:
            event = LineageEvent(
                event_id=f"{feature_id}_computed_{int(datetime.now(timezone.utc).timestamp())}",
                event_type=LineageEventType.FEATURE_COMPUTED,
                feature_id=feature_id,
                actor=actor,
                inputs=input_features or [],
                outputs=[feature_id],
                parameters=parameters or {},
                duration_ms=duration_ms,
                input_records=input_records,
                output_records=output_records
            )
            
            await self._store_event(event)
            
            logger.debug(f"Tracked feature computation: {feature_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to track feature computation for {feature_id}: {e}")
            return False
    
    async def track_feature_access(
        self,
        feature_id: str,
        entities: List[str],
        timestamp: Optional[datetime] = None,
        actor: str = "system",
        purpose: str = "serving"
    ) -> bool:
        """
        Track feature access event.
        
        Args:
            feature_id: Feature being accessed
            entities: Entity IDs accessed
            timestamp: Query timestamp
            actor: Who accessed the feature
            purpose: Purpose of access (serving, training, etc.)
            
        Returns:
            Success status
        """
        try:
            event = LineageEvent(
                event_id=f"{feature_id}_accessed_{int(datetime.now(timezone.utc).timestamp())}",
                event_type=LineageEventType.FEATURE_ACCESSED,
                feature_id=feature_id,
                actor=actor,
                outputs=[feature_id],
                parameters={
                    "purpose": purpose,
                    "query_timestamp": timestamp.isoformat() if timestamp else None
                },
                metadata={
                    "entity_count": len(entities),
                    "entities": entities[:10]  # Store sample of entities
                }
            )
            
            await self._store_event(event)
            
            logger.debug(f"Tracked feature access: {feature_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to track feature access for {feature_id}: {e}")
            return False
    
    async def track_model_training(
        self,
        model_id: str,
        feature_ids: List[str],
        training_records: int,
        actor: str = "system",
        parameters: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Track model training lineage.
        
        Args:
            model_id: Model identifier
            feature_ids: Features used in training
            training_records: Number of training records
            actor: Who triggered training
            parameters: Training parameters
            
        Returns:
            Success status
        """
        try:
            event = LineageEvent(
                event_id=f"{model_id}_trained_{int(datetime.now(timezone.utc).timestamp())}",
                event_type=LineageEventType.MODEL_TRAINED,
                feature_id=model_id,  # Using model_id as primary identifier
                actor=actor,
                inputs=feature_ids,
                outputs=[model_id],
                parameters=parameters or {},
                input_records=training_records
            )
            
            await self._store_event(event)
            
            # Track this event for all input features
            for feature_id in feature_ids:
                feature_event = LineageEvent(
                    event_id=f"{feature_id}_used_training_{int(datetime.now(timezone.utc).timestamp())}",
                    event_type=LineageEventType.MODEL_TRAINED,
                    feature_id=feature_id,
                    actor=actor,
                    inputs=[feature_id],
                    outputs=[model_id],
                    parameters=parameters or {},
                    metadata={"training_records": training_records}
                )
                await self._store_event(feature_event)
            
            logger.info(f"Tracked model training: {model_id} with {len(feature_ids)} features")
            return True
            
        except Exception as e:
            logger.error(f"Failed to track model training for {model_id}: {e}")
            return False
    
    async def track_model_inference(
        self,
        model_id: str,
        feature_ids: List[str],
        prediction_count: int,
        actor: str = "system"
    ) -> bool:
        """Track model inference lineage."""
        try:
            event = LineageEvent(
                event_id=f"{model_id}_inference_{int(datetime.now(timezone.utc).timestamp())}",
                event_type=LineageEventType.MODEL_INFERENCE,
                feature_id=model_id,
                actor=actor,
                inputs=feature_ids,
                outputs=[model_id],
                output_records=prediction_count
            )
            
            await self._store_event(event)
            
            # Track inference usage for each feature
            for feature_id in feature_ids:
                feature_event = LineageEvent(
                    event_id=f"{feature_id}_used_inference_{int(datetime.now(timezone.utc).timestamp())}",
                    event_type=LineageEventType.MODEL_INFERENCE,
                    feature_id=feature_id,
                    actor=actor,
                    inputs=[feature_id],
                    outputs=[model_id],
                    metadata={"prediction_count": prediction_count}
                )
                await self._store_event(feature_event)
            
            logger.debug(f"Tracked model inference: {model_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to track model inference for {model_id}: {e}")
            return False
    
    async def get_feature_lineage(
        self,
        feature_id: str,
        depth: int = 5
    ) -> Optional[FeatureLineage]:
        """
        Get comprehensive lineage for a feature.
        
        Args:
            feature_id: Feature to get lineage for
            depth: Maximum depth for dependency traversal
            
        Returns:
            Feature lineage information
        """
        try:
            # Get all events for the feature
            events = await self._get_feature_events(feature_id)
            if not events:
                return None
            
            # Build lineage object
            lineage = FeatureLineage(feature_id=feature_id)
            
            # Process events
            computation_events = []
            access_events = []
            creation_event = None
            
            for event in events:
                if event.event_type == LineageEventType.FEATURE_CREATED:
                    creation_event = event
                elif event.event_type == LineageEventType.FEATURE_COMPUTED:
                    computation_events.append(event)
                elif event.event_type == LineageEventType.FEATURE_ACCESSED:
                    access_events.append(event)
            
            lineage.creation_event = creation_event
            lineage.computation_history = computation_events
            lineage.access_history = access_events
            
            # Get dependencies
            upstream_features = await self._get_dependencies(feature_id, depth)
            lineage.upstream_features = upstream_features
            
            # Get dependents
            downstream_features = await self._get_dependents(feature_id, depth)
            lineage.downstream_features = downstream_features
            
            # Get data sources
            data_sources = await self._get_data_sources(feature_id)
            lineage.data_sources = data_sources
            
            # Calculate statistics
            lineage.total_computations = len(computation_events)
            lineage.total_accesses = len(access_events)
            
            if computation_events:
                lineage.last_computed = max(e.timestamp for e in computation_events)
            
            if access_events:
                lineage.last_accessed = max(e.timestamp for e in access_events)
            
            return lineage
            
        except Exception as e:
            logger.error(f"Failed to get feature lineage for {feature_id}: {e}")
            return None
    
    async def get_lineage_graph(
        self,
        feature_ids: List[str],
        depth: int = 3
    ) -> Dict[str, Any]:
        """
        Get lineage graph for multiple features.
        
        Args:
            feature_ids: Features to include in graph
            depth: Maximum depth for traversal
            
        Returns:
            Graph representation with nodes and edges
        """
        try:
            nodes = {}
            edges = []
            visited = set()
            
            # BFS to build graph
            queue = [(fid, 0) for fid in feature_ids]
            
            while queue:
                feature_id, current_depth = queue.pop(0)
                
                if feature_id in visited or current_depth > depth:
                    continue
                
                visited.add(feature_id)
                
                # Add node
                lineage = await self.get_feature_lineage(feature_id, 1)
                if lineage:
                    nodes[feature_id] = {
                        "id": feature_id,
                        "type": "feature",
                        "total_computations": lineage.total_computations,
                        "total_accesses": lineage.total_accesses,
                        "last_computed": lineage.last_computed.isoformat() if lineage.last_computed else None,
                        "last_accessed": lineage.last_accessed.isoformat() if lineage.last_accessed else None
                    }
                    
                    # Add edges for dependencies
                    for upstream in lineage.upstream_features:
                        edges.append({
                            "from": upstream,
                            "to": feature_id,
                            "type": "dependency"
                        })
                        
                        if current_depth < depth:
                            queue.append((upstream, current_depth + 1))
                    
                    # Add edges for data sources
                    for source in lineage.data_sources:
                        if source not in nodes:
                            nodes[source] = {
                                "id": source,
                                "type": "data_source"
                            }
                        
                        edges.append({
                            "from": source,
                            "to": feature_id,
                            "type": "data_source"
                        })
            
            return {
                "nodes": list(nodes.values()),
                "edges": edges
            }
            
        except Exception as e:
            logger.error(f"Failed to build lineage graph: {e}")
            return {"nodes": [], "edges": []}
    
    async def get_feature_impact_analysis(
        self,
        feature_id: str,
        depth: int = 5
    ) -> Dict[str, Any]:
        """
        Analyze the impact of a feature on downstream consumers.
        
        Args:
            feature_id: Feature to analyze
            depth: Maximum depth for analysis
            
        Returns:
            Impact analysis results
        """
        try:
            downstream_features = await self._get_dependents(feature_id, depth)
            
            # Get usage statistics for downstream features
            impact_analysis = {
                "feature_id": feature_id,
                "direct_dependents": [],
                "indirect_dependents": [],
                "total_downstream_features": len(downstream_features),
                "models_affected": [],
                "estimated_impact_score": 0
            }
            
            # Analyze each downstream feature
            for downstream_id in downstream_features:
                lineage = await self.get_feature_lineage(downstream_id, 1)
                if lineage:
                    dependent_info = {
                        "feature_id": downstream_id,
                        "total_computations": lineage.total_computations,
                        "total_accesses": lineage.total_accesses,
                        "last_accessed": lineage.last_accessed.isoformat() if lineage.last_accessed else None
                    }
                    
                    # Classify as direct or indirect dependent
                    if feature_id in lineage.upstream_features:
                        impact_analysis["direct_dependents"].append(dependent_info)
                    else:
                        impact_analysis["indirect_dependents"].append(dependent_info)
            
            # Calculate impact score based on downstream usage
            total_accesses = sum(
                info.get("total_accesses", 0) 
                for info in impact_analysis["direct_dependents"] + impact_analysis["indirect_dependents"]
            )
            impact_analysis["estimated_impact_score"] = min(total_accesses / 100, 10.0)  # Scale 0-10
            
            return impact_analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze feature impact for {feature_id}: {e}")
            return {}
    
    async def _store_event(self, event: LineageEvent) -> None:
        """Store lineage event in Redis."""
        # Store in feature-specific event list
        events_key = self.events_key_pattern.format(feature_id=event.feature_id)
        await self.redis.lpush(events_key, event.json())
        await self.redis.ltrim(events_key, 0, self.max_history_size - 1)
        
        # Store in global events (for time-based queries)
        await self.redis.zadd(
            self.global_events_key,
            {event.event_id: event.timestamp.timestamp()}
        )
        
        # Store in type-based index
        type_key = self.events_by_type_pattern.format(event_type=event.event_type.value)
        await self.redis.zadd(
            type_key,
            {event.event_id: event.timestamp.timestamp()}
        )
        
        # Store in actor-based index
        actor_key = self.events_by_actor_pattern.format(actor=event.actor)
        await self.redis.zadd(
            actor_key,
            {event.event_id: event.timestamp.timestamp()}
        )
        
        # Store full event data
        await self.redis.hset(f"{self.prefix}:event_data", event.event_id, event.json())
    
    async def _get_feature_events(self, feature_id: str) -> List[LineageEvent]:
        """Get all events for a feature."""
        events_key = self.events_key_pattern.format(feature_id=feature_id)
        event_data = await self.redis.lrange(events_key, 0, -1)
        
        events = []
        for data in event_data:
            try:
                event = LineageEvent.parse_raw(data)
                events.append(event)
            except Exception as e:
                logger.warning(f"Failed to parse event data: {e}")
        
        return events
    
    async def _update_dependencies(self, feature_id: str, dependencies: List[str]) -> None:
        """Update dependency mappings."""
        deps_key = self.dependencies_key_pattern.format(feature_id=feature_id)
        if dependencies:
            await self.redis.sadd(deps_key, *dependencies)
        
        # Update reverse mappings (dependents)
        for dep in dependencies:
            dependents_key = self.dependents_key_pattern.format(feature_id=dep)
            await self.redis.sadd(dependents_key, feature_id)
    
    async def _update_data_sources(self, feature_id: str, sources: List[str]) -> None:
        """Update data source mappings."""
        sources_key = self.data_sources_key_pattern.format(feature_id=feature_id)
        if sources:
            await self.redis.sadd(sources_key, *sources)
    
    async def _get_dependencies(self, feature_id: str, depth: int) -> List[str]:
        """Get feature dependencies up to specified depth."""
        all_deps = set()
        queue = [(feature_id, 0)]
        visited = set()
        
        while queue:
            current_id, current_depth = queue.pop(0)
            
            if current_id in visited or current_depth >= depth:
                continue
            
            visited.add(current_id)
            
            # Get direct dependencies
            deps_key = self.dependencies_key_pattern.format(feature_id=current_id)
            direct_deps = await self.redis.smembers(deps_key)
            
            for dep in direct_deps:
                dep_str = dep.decode()
                all_deps.add(dep_str)
                queue.append((dep_str, current_depth + 1))
        
        return list(all_deps)
    
    async def _get_dependents(self, feature_id: str, depth: int) -> List[str]:
        """Get features that depend on this feature up to specified depth."""
        all_dependents = set()
        queue = [(feature_id, 0)]
        visited = set()
        
        while queue:
            current_id, current_depth = queue.pop(0)
            
            if current_id in visited or current_depth >= depth:
                continue
            
            visited.add(current_id)
            
            # Get direct dependents
            dependents_key = self.dependents_key_pattern.format(feature_id=current_id)
            direct_dependents = await self.redis.smembers(dependents_key)
            
            for dependent in direct_dependents:
                dependent_str = dependent.decode()
                all_dependents.add(dependent_str)
                queue.append((dependent_str, current_depth + 1))
        
        return list(all_dependents)
    
    async def _get_data_sources(self, feature_id: str) -> List[str]:
        """Get data sources for a feature."""
        sources_key = self.data_sources_key_pattern.format(feature_id=feature_id)
        sources = await self.redis.smembers(sources_key)
        return [source.decode() for source in sources]