"""
Performance Monitoring for Feature Store

Provides comprehensive performance monitoring, alerting, and optimization
recommendations for feature store operations.
"""

import asyncio
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Callable
from collections import deque, defaultdict
import statistics
from dataclasses import dataclass, field
from enum import Enum
import json

import redis.asyncio as redis
from pydantic import BaseModel


logger = logging.getLogger(__name__)


class MetricType(str, Enum):
    """Types of performance metrics."""
    LATENCY = "latency"
    THROUGHPUT = "throughput"
    ERROR_RATE = "error_rate"
    CACHE_HIT_RATE = "cache_hit_rate"
    FEATURE_FRESHNESS = "feature_freshness"
    RESOURCE_USAGE = "resource_usage"


class AlertLevel(str, Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class PerformanceMetric:
    """Single performance metric."""
    
    name: str
    value: float
    timestamp: datetime
    metric_type: MetricType
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Alert:
    """Performance alert."""
    
    alert_id: str
    level: AlertLevel
    message: str
    metric_name: str
    metric_value: float
    threshold: float
    timestamp: datetime
    resolved: bool = False
    resolved_at: Optional[datetime] = None


class PerformanceThresholds(BaseModel):
    """Performance thresholds configuration."""
    
    # Latency thresholds (milliseconds)
    latency_p95_warning: float = 50.0
    latency_p95_error: float = 100.0
    latency_p99_critical: float = 500.0
    
    # Throughput thresholds (requests per second)
    throughput_min_warning: float = 100.0
    throughput_min_error: float = 50.0
    
    # Error rate thresholds (percentage)
    error_rate_warning: float = 1.0
    error_rate_error: float = 5.0
    error_rate_critical: float = 10.0
    
    # Cache hit rate thresholds (percentage)
    cache_hit_rate_warning: float = 90.0
    cache_hit_rate_error: float = 80.0
    
    # Feature freshness thresholds (minutes)
    freshness_warning: float = 60.0
    freshness_error: float = 120.0
    freshness_critical: float = 300.0
    
    # Resource usage thresholds (percentage)
    memory_usage_warning: float = 80.0
    memory_usage_error: float = 90.0
    cpu_usage_warning: float = 80.0
    cpu_usage_error: float = 90.0


class PerformanceMonitor:
    """
    Comprehensive performance monitoring system.
    
    Provides:
    - Real-time metric collection and aggregation
    - Threshold-based alerting
    - Performance trend analysis
    - Optimization recommendations
    """
    
    def __init__(
        self,
        redis_client: redis.Redis,
        thresholds: PerformanceThresholds,
        metric_retention_hours: int = 24,
        alert_callbacks: Optional[List[Callable]] = None
    ):
        self.redis = redis_client
        self.thresholds = thresholds
        self.metric_retention_hours = metric_retention_hours
        self.alert_callbacks = alert_callbacks or []
        
        # In-memory metric storage for fast access
        self.metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.alerts: List[Alert] = []
        
        # Redis keys for persistent storage
        self.metrics_key_pattern = "performance:metrics:{metric_name}"
        self.alerts_key = "performance:alerts"
        self.thresholds_key = "performance:thresholds"
        
        # Monitoring state
        self.monitoring_active = False
        self.last_alert_check = datetime.now(timezone.utc)
        
        # Performance statistics
        self.stats = {
            'total_metrics_collected': 0,
            'total_alerts_generated': 0,
            'active_alerts': 0,
            'last_metric_time': None
        }
    
    async def start_monitoring(self, interval_seconds: int = 10) -> None:
        """Start continuous performance monitoring."""
        self.monitoring_active = True
        logger.info("Performance monitoring started")
        
        # Background tasks
        asyncio.create_task(self._metric_collection_loop(interval_seconds))
        asyncio.create_task(self._alert_processing_loop())
        asyncio.create_task(self._cleanup_old_metrics_loop())
    
    async def stop_monitoring(self) -> None:
        """Stop performance monitoring."""
        self.monitoring_active = False
        logger.info("Performance monitoring stopped")
    
    async def record_metric(
        self,
        name: str,
        value: float,
        metric_type: MetricType,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Record a performance metric.
        
        Args:
            name: Metric name
            value: Metric value
            metric_type: Type of metric
            metadata: Additional metadata
        """
        try:
            timestamp = datetime.now(timezone.utc)
            metric = PerformanceMetric(
                name=name,
                value=value,
                timestamp=timestamp,
                metric_type=metric_type,
                metadata=metadata or {}
            )
            
            # Store in memory
            self.metrics[name].append(metric)
            
            # Store in Redis for persistence
            await self._store_metric_redis(metric)
            
            # Update statistics
            self.stats['total_metrics_collected'] += 1
            self.stats['last_metric_time'] = timestamp
            
            # Check for threshold violations
            await self._check_thresholds(metric)
            
        except Exception as e:
            logger.error(f"Error recording metric {name}: {e}")
    
    async def get_metrics(
        self,
        metric_name: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[PerformanceMetric]:
        """
        Retrieve performance metrics.
        
        Args:
            metric_name: Specific metric name (all metrics if None)
            start_time: Start time filter
            end_time: End time filter
            
        Returns:
            List of matching metrics
        """
        try:
            metrics = []
            
            if metric_name:
                metric_names = [metric_name]
            else:
                metric_names = list(self.metrics.keys())
            
            for name in metric_names:
                # Get metrics from memory first (most recent)
                for metric in self.metrics[name]:
                    if self._metric_matches_timerange(metric, start_time, end_time):
                        metrics.append(metric)
                
                # If we need more historical data, fetch from Redis
                if start_time and (
                    not metrics or 
                    min(m.timestamp for m in metrics) > start_time
                ):
                    redis_metrics = await self._get_metrics_from_redis(name, start_time, end_time)
                    metrics.extend(redis_metrics)
            
            # Sort by timestamp
            metrics.sort(key=lambda m: m.timestamp)
            return metrics
            
        except Exception as e:
            logger.error(f"Error retrieving metrics: {e}")
            return []
    
    async def get_metric_summary(
        self,
        metric_name: str,
        window_minutes: int = 60
    ) -> Dict[str, Any]:
        """
        Get statistical summary of a metric over a time window.
        
        Args:
            metric_name: Metric name
            window_minutes: Time window in minutes
            
        Returns:
            Statistical summary
        """
        try:
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(minutes=window_minutes)
            
            metrics = await self.get_metrics(metric_name, start_time, end_time)
            
            if not metrics:
                return {
                    "metric_name": metric_name,
                    "window_minutes": window_minutes,
                    "count": 0
                }
            
            values = [m.value for m in metrics]
            
            return {
                "metric_name": metric_name,
                "window_minutes": window_minutes,
                "count": len(values),
                "min": min(values),
                "max": max(values),
                "mean": statistics.mean(values),
                "median": statistics.median(values),
                "p95": self._percentile(values, 0.95),
                "p99": self._percentile(values, 0.99),
                "std_dev": statistics.stdev(values) if len(values) > 1 else 0,
                "latest": values[-1] if values else None,
                "timestamp_range": {
                    "start": metrics[0].timestamp.isoformat(),
                    "end": metrics[-1].timestamp.isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting metric summary: {e}")
            return {"error": str(e)}
    
    async def get_active_alerts(self) -> List[Alert]:
        """Get currently active alerts."""
        return [alert for alert in self.alerts if not alert.resolved]
    
    async def resolve_alert(self, alert_id: str) -> bool:
        """
        Resolve an active alert.
        
        Args:
            alert_id: Alert identifier
            
        Returns:
            Success status
        """
        try:
            for alert in self.alerts:
                if alert.alert_id == alert_id and not alert.resolved:
                    alert.resolved = True
                    alert.resolved_at = datetime.now(timezone.utc)
                    
                    # Update in Redis
                    await self._update_alert_redis(alert)
                    
                    logger.info(f"Resolved alert: {alert_id}")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error resolving alert {alert_id}: {e}")
            return False
    
    async def get_performance_report(self) -> Dict[str, Any]:
        """
        Generate comprehensive performance report.
        
        Returns:
            Performance report with metrics and recommendations
        """
        try:
            report = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "monitoring_stats": self.stats.copy(),
                "metric_summaries": {},
                "active_alerts": [
                    {
                        "alert_id": alert.alert_id,
                        "level": alert.level,
                        "message": alert.message,
                        "timestamp": alert.timestamp.isoformat()
                    }
                    for alert in await self.get_active_alerts()
                ],
                "recommendations": []
            }
            
            # Get summaries for key metrics
            key_metrics = [
                "feature_serving_latency",
                "feature_computation_time",
                "cache_hit_rate",
                "error_rate",
                "throughput"
            ]
            
            for metric_name in key_metrics:
                if metric_name in self.metrics and self.metrics[metric_name]:
                    summary = await self.get_metric_summary(metric_name, 60)
                    report["metric_summaries"][metric_name] = summary
            
            # Generate recommendations
            recommendations = await self._generate_recommendations(report["metric_summaries"])
            report["recommendations"] = recommendations
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating performance report: {e}")
            return {"error": str(e)}
    
    async def _metric_collection_loop(self, interval_seconds: int) -> None:
        """Background loop for metric collection."""
        while self.monitoring_active:
            try:
                await self._collect_system_metrics()
                await asyncio.sleep(interval_seconds)
                
            except Exception as e:
                logger.error(f"Error in metric collection loop: {e}")
                await asyncio.sleep(interval_seconds)
    
    async def _alert_processing_loop(self) -> None:
        """Background loop for processing alerts."""
        while self.monitoring_active:
            try:
                # Process pending alerts
                await self._process_pending_alerts()
                
                # Auto-resolve old alerts
                await self._auto_resolve_alerts()
                
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in alert processing loop: {e}")
                await asyncio.sleep(30)
    
    async def _cleanup_old_metrics_loop(self) -> None:
        """Background loop for cleaning up old metrics."""
        while self.monitoring_active:
            try:
                await self._cleanup_old_metrics()
                await asyncio.sleep(3600)  # Clean up every hour
                
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                await asyncio.sleep(3600)
    
    async def _collect_system_metrics(self) -> None:
        """Collect system-level metrics."""
        try:
            import psutil
            
            # Memory usage
            memory = psutil.virtual_memory()
            await self.record_metric(
                "system_memory_usage",
                memory.percent,
                MetricType.RESOURCE_USAGE,
                {"total": memory.total, "available": memory.available}
            )
            
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            await self.record_metric(
                "system_cpu_usage",
                cpu_percent,
                MetricType.RESOURCE_USAGE
            )
            
        except ImportError:
            # psutil not available, skip system metrics
            pass
        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}")
    
    async def _check_thresholds(self, metric: PerformanceMetric) -> None:
        """Check metric against thresholds and generate alerts."""
        try:
            thresholds_map = {
                "feature_serving_latency": [
                    (self.thresholds.latency_p95_warning, AlertLevel.WARNING),
                    (self.thresholds.latency_p95_error, AlertLevel.ERROR),
                    (self.thresholds.latency_p99_critical, AlertLevel.CRITICAL)
                ],
                "error_rate": [
                    (self.thresholds.error_rate_warning, AlertLevel.WARNING),
                    (self.thresholds.error_rate_error, AlertLevel.ERROR),
                    (self.thresholds.error_rate_critical, AlertLevel.CRITICAL)
                ],
                "cache_hit_rate": [
                    (self.thresholds.cache_hit_rate_warning, AlertLevel.WARNING, "below"),
                    (self.thresholds.cache_hit_rate_error, AlertLevel.ERROR, "below")
                ],
                "system_memory_usage": [
                    (self.thresholds.memory_usage_warning, AlertLevel.WARNING),
                    (self.thresholds.memory_usage_error, AlertLevel.ERROR)
                ],
                "system_cpu_usage": [
                    (self.thresholds.cpu_usage_warning, AlertLevel.WARNING),
                    (self.thresholds.cpu_usage_error, AlertLevel.ERROR)
                ]
            }
            
            if metric.name in thresholds_map:
                for threshold_config in thresholds_map[metric.name]:
                    threshold_value = threshold_config[0]
                    alert_level = threshold_config[1]
                    comparison = threshold_config[2] if len(threshold_config) > 2 else "above"
                    
                    violated = (
                        (comparison == "above" and metric.value > threshold_value) or
                        (comparison == "below" and metric.value < threshold_value)
                    )
                    
                    if violated:
                        await self._generate_alert(
                            alert_level,
                            f"{metric.name} {comparison} threshold: {metric.value:.2f} vs {threshold_value:.2f}",
                            metric.name,
                            metric.value,
                            threshold_value
                        )
                        break  # Only alert for the first violated threshold
            
        except Exception as e:
            logger.error(f"Error checking thresholds for {metric.name}: {e}")
    
    async def _generate_alert(
        self,
        level: AlertLevel,
        message: str,
        metric_name: str,
        metric_value: float,
        threshold: float
    ) -> None:
        """Generate a new alert."""
        try:
            alert_id = f"{metric_name}_{level}_{int(time.time())}"
            alert = Alert(
                alert_id=alert_id,
                level=level,
                message=message,
                metric_name=metric_name,
                metric_value=metric_value,
                threshold=threshold,
                timestamp=datetime.now(timezone.utc)
            )
            
            self.alerts.append(alert)
            self.stats['total_alerts_generated'] += 1
            self.stats['active_alerts'] += 1
            
            # Store in Redis
            await self._store_alert_redis(alert)
            
            # Execute alert callbacks
            for callback in self.alert_callbacks:
                try:
                    await callback(alert)
                except Exception as e:
                    logger.error(f"Error in alert callback: {e}")
            
            logger.warning(f"Generated {level} alert: {message}")
            
        except Exception as e:
            logger.error(f"Error generating alert: {e}")
    
    def _percentile(self, values: List[float], percentile: float) -> float:
        """Calculate percentile value."""
        if not values:
            return 0.0
        
        sorted_values = sorted(values)
        index = int(percentile * len(sorted_values))
        index = min(index, len(sorted_values) - 1)
        return sorted_values[index]
    
    def _metric_matches_timerange(
        self,
        metric: PerformanceMetric,
        start_time: Optional[datetime],
        end_time: Optional[datetime]
    ) -> bool:
        """Check if metric falls within time range."""
        if start_time and metric.timestamp < start_time:
            return False
        if end_time and metric.timestamp > end_time:
            return False
        return True
    
    async def _store_metric_redis(self, metric: PerformanceMetric) -> None:
        """Store metric in Redis."""
        try:
            key = self.metrics_key_pattern.format(metric_name=metric.name)
            value = {
                "value": metric.value,
                "timestamp": metric.timestamp.isoformat(),
                "metric_type": metric.metric_type,
                "metadata": metric.metadata
            }
            
            # Use sorted set with timestamp as score for time-series data
            await self.redis.zadd(key, {json.dumps(value): metric.timestamp.timestamp()})
            
        except Exception as e:
            logger.error(f"Error storing metric in Redis: {e}")
    
    async def _get_metrics_from_redis(
        self,
        metric_name: str,
        start_time: Optional[datetime],
        end_time: Optional[datetime]
    ) -> List[PerformanceMetric]:
        """Retrieve metrics from Redis."""
        try:
            key = self.metrics_key_pattern.format(metric_name=metric_name)
            
            min_score = start_time.timestamp() if start_time else 0
            max_score = end_time.timestamp() if end_time else "+inf"
            
            data = await self.redis.zrangebyscore(key, min_score, max_score)
            
            metrics = []
            for item in data:
                try:
                    value_data = json.loads(item.decode())
                    metric = PerformanceMetric(
                        name=metric_name,
                        value=value_data["value"],
                        timestamp=datetime.fromisoformat(value_data["timestamp"]),
                        metric_type=MetricType(value_data["metric_type"]),
                        metadata=value_data["metadata"]
                    )
                    metrics.append(metric)
                    
                except Exception as e:
                    logger.warning(f"Error parsing metric data: {e}")
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error retrieving metrics from Redis: {e}")
            return []
    
    async def _store_alert_redis(self, alert: Alert) -> None:
        """Store alert in Redis."""
        try:
            alert_data = {
                "alert_id": alert.alert_id,
                "level": alert.level,
                "message": alert.message,
                "metric_name": alert.metric_name,
                "metric_value": alert.metric_value,
                "threshold": alert.threshold,
                "timestamp": alert.timestamp.isoformat(),
                "resolved": alert.resolved,
                "resolved_at": alert.resolved_at.isoformat() if alert.resolved_at else None
            }
            
            await self.redis.hset(self.alerts_key, alert.alert_id, json.dumps(alert_data))
            
        except Exception as e:
            logger.error(f"Error storing alert in Redis: {e}")
    
    async def _update_alert_redis(self, alert: Alert) -> None:
        """Update alert in Redis."""
        await self._store_alert_redis(alert)
    
    async def _process_pending_alerts(self) -> None:
        """Process any pending alert actions."""
        # Placeholder for alert processing logic
        pass
    
    async def _auto_resolve_alerts(self) -> None:
        """Auto-resolve old alerts if conditions have improved."""
        try:
            current_time = datetime.now(timezone.utc)
            auto_resolve_threshold = timedelta(minutes=30)
            
            for alert in self.alerts:
                if (not alert.resolved and 
                    current_time - alert.timestamp > auto_resolve_threshold):
                    
                    # Check if metric has improved
                    recent_metrics = await self.get_metrics(
                        alert.metric_name,
                        current_time - timedelta(minutes=5),
                        current_time
                    )
                    
                    if recent_metrics:
                        recent_values = [m.value for m in recent_metrics]
                        avg_recent = statistics.mean(recent_values)
                        
                        # Simple auto-resolution logic
                        if ((alert.metric_name in ["error_rate"] and avg_recent < alert.threshold) or
                            (alert.metric_name in ["cache_hit_rate"] and avg_recent > alert.threshold) or
                            (alert.metric_name in ["feature_serving_latency"] and avg_recent < alert.threshold)):
                            
                            await self.resolve_alert(alert.alert_id)
                            self.stats['active_alerts'] -= 1
            
        except Exception as e:
            logger.error(f"Error in auto-resolve alerts: {e}")
    
    async def _cleanup_old_metrics(self) -> None:
        """Clean up old metrics from Redis."""
        try:
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=self.metric_retention_hours)
            cutoff_timestamp = cutoff_time.timestamp()
            
            for metric_name in self.metrics.keys():
                key = self.metrics_key_pattern.format(metric_name=metric_name)
                
                # Remove old entries
                removed_count = await self.redis.zremrangebyscore(key, 0, cutoff_timestamp)
                
                if removed_count > 0:
                    logger.debug(f"Cleaned up {removed_count} old metrics for {metric_name}")
            
        except Exception as e:
            logger.error(f"Error cleaning up old metrics: {e}")
    
    async def _generate_recommendations(self, metric_summaries: Dict[str, Any]) -> List[str]:
        """Generate performance optimization recommendations."""
        recommendations = []
        
        try:
            # Latency recommendations
            if "feature_serving_latency" in metric_summaries:
                latency = metric_summaries["feature_serving_latency"]
                if latency.get("p95", 0) > self.thresholds.latency_p95_warning:
                    recommendations.append(
                        "Consider increasing cache size or implementing feature pre-computation "
                        "to reduce serving latency"
                    )
            
            # Cache hit rate recommendations
            if "cache_hit_rate" in metric_summaries:
                cache_rate = metric_summaries["cache_hit_rate"]
                if cache_rate.get("mean", 100) < self.thresholds.cache_hit_rate_warning:
                    recommendations.append(
                        "Cache hit rate is low. Consider warming up cache for popular features "
                        "or increasing cache TTL"
                    )
            
            # Error rate recommendations
            if "error_rate" in metric_summaries:
                error_rate = metric_summaries["error_rate"]
                if error_rate.get("mean", 0) > self.thresholds.error_rate_warning:
                    recommendations.append(
                        "High error rate detected. Review logs for common failure patterns "
                        "and implement circuit breakers"
                    )
            
            # Resource usage recommendations
            if "system_memory_usage" in metric_summaries:
                memory = metric_summaries["system_memory_usage"]
                if memory.get("max", 0) > self.thresholds.memory_usage_warning:
                    recommendations.append(
                        "High memory usage detected. Consider implementing memory limits "
                        "or optimizing data structures"
                    )
            
        except Exception as e:
            logger.error(f"Error generating recommendations: {e}")
        
        return recommendations