"""
Metrics collection and monitoring utilities.

This module provides comprehensive metrics collection, aggregation, and
export capabilities for monitoring the ingestion pipeline performance.
"""

import time
import asyncio
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict, deque
import logging
import threading
import json
from contextlib import contextmanager
import statistics

try:
    from prometheus_client import Counter, Histogram, Gauge, Summary, CollectorRegistry, generate_latest
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False


class MetricType(Enum):
    """Types of metrics."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"
    TIMER = "timer"


@dataclass
class MetricValue:
    """Individual metric value with metadata."""
    value: Union[int, float]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    labels: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
            "labels": self.labels
        }


@dataclass
class MetricSummary:
    """Summary statistics for a metric."""
    name: str
    metric_type: MetricType
    count: int = 0
    sum_value: float = 0.0
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    avg_value: float = 0.0
    percentiles: Dict[str, float] = field(default_factory=dict)
    labels: Dict[str, str] = field(default_factory=dict)
    last_updated: datetime = field(default_factory=datetime.utcnow)
    
    def update(self, value: float):
        """Update summary with new value."""
        self.count += 1
        self.sum_value += value
        self.avg_value = self.sum_value / self.count
        
        if self.min_value is None or value < self.min_value:
            self.min_value = value
        if self.max_value is None or value > self.max_value:
            self.max_value = value
        
        self.last_updated = datetime.utcnow()


class BaseMetric(ABC):
    """Abstract base class for metrics."""
    
    def __init__(self, name: str, description: str = "", labels: Optional[Dict[str, str]] = None):
        self.name = name
        self.description = description
        self.labels = labels or {}
        self.created_at = datetime.utcnow()
        self.last_updated = datetime.utcnow()
    
    @abstractmethod
    def record(self, value: Union[int, float], labels: Optional[Dict[str, str]] = None):
        """Record a value for this metric."""
        pass
    
    @abstractmethod
    def get_value(self, labels: Optional[Dict[str, str]] = None) -> Any:
        """Get current metric value."""
        pass
    
    @abstractmethod
    def reset(self):
        """Reset metric to initial state."""
        pass
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metric to dictionary representation."""
        return {
            "name": self.name,
            "description": self.description,
            "labels": self.labels,
            "created_at": self.created_at.isoformat(),
            "last_updated": self.last_updated.isoformat()
        }


class CounterMetric(BaseMetric):
    """Counter metric that only increases."""
    
    def __init__(self, name: str, description: str = "", labels: Optional[Dict[str, str]] = None):
        super().__init__(name, description, labels)
        self._values: Dict[str, float] = defaultdict(float)
        self._lock = threading.Lock()
    
    def record(self, value: Union[int, float] = 1, labels: Optional[Dict[str, str]] = None):
        """Increment counter by value."""
        if value < 0:
            raise ValueError("Counter values must be non-negative")
        
        label_key = self._get_label_key(labels)
        with self._lock:
            self._values[label_key] += value
            self.last_updated = datetime.utcnow()
    
    def get_value(self, labels: Optional[Dict[str, str]] = None) -> float:
        """Get current counter value."""
        label_key = self._get_label_key(labels)
        return self._values[label_key]
    
    def reset(self):
        """Reset counter to zero."""
        with self._lock:
            self._values.clear()
    
    def _get_label_key(self, labels: Optional[Dict[str, str]] = None) -> str:
        """Generate key from labels for storage."""
        all_labels = {**self.labels}
        if labels:
            all_labels.update(labels)
        return json.dumps(sorted(all_labels.items()), sort_keys=True)


class GaugeMetric(BaseMetric):
    """Gauge metric that can increase or decrease."""
    
    def __init__(self, name: str, description: str = "", labels: Optional[Dict[str, str]] = None):
        super().__init__(name, description, labels)
        self._values: Dict[str, float] = defaultdict(float)
        self._lock = threading.Lock()
    
    def record(self, value: Union[int, float], labels: Optional[Dict[str, str]] = None):
        """Set gauge to specific value."""
        label_key = self._get_label_key(labels)
        with self._lock:
            self._values[label_key] = value
            self.last_updated = datetime.utcnow()
    
    def increment(self, amount: Union[int, float] = 1, labels: Optional[Dict[str, str]] = None):
        """Increment gauge by amount."""
        label_key = self._get_label_key(labels)
        with self._lock:
            self._values[label_key] += amount
            self.last_updated = datetime.utcnow()
    
    def decrement(self, amount: Union[int, float] = 1, labels: Optional[Dict[str, str]] = None):
        """Decrement gauge by amount."""
        self.increment(-amount, labels)
    
    def get_value(self, labels: Optional[Dict[str, str]] = None) -> float:
        """Get current gauge value."""
        label_key = self._get_label_key(labels)
        return self._values[label_key]
    
    def reset(self):
        """Reset gauge to zero."""
        with self._lock:
            self._values.clear()
    
    def _get_label_key(self, labels: Optional[Dict[str, str]] = None) -> str:
        """Generate key from labels for storage."""
        all_labels = {**self.labels}
        if labels:
            all_labels.update(labels)
        return json.dumps(sorted(all_labels.items()), sort_keys=True)


class HistogramMetric(BaseMetric):
    """Histogram metric for tracking distributions."""
    
    def __init__(self, name: str, description: str = "", 
                 buckets: Optional[List[float]] = None, labels: Optional[Dict[str, str]] = None):
        super().__init__(name, description, labels)
        self.buckets = buckets or [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0]
        self._bucket_counts: Dict[str, Dict[float, int]] = defaultdict(lambda: defaultdict(int))
        self._sums: Dict[str, float] = defaultdict(float)
        self._counts: Dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()
    
    def record(self, value: Union[int, float], labels: Optional[Dict[str, str]] = None):
        """Record a value in the histogram."""
        label_key = self._get_label_key(labels)
        with self._lock:
            self._sums[label_key] += value
            self._counts[label_key] += 1
            
            # Update buckets
            for bucket in self.buckets:
                if value <= bucket:
                    self._bucket_counts[label_key][bucket] += 1
            
            self.last_updated = datetime.utcnow()
    
    def get_value(self, labels: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Get histogram statistics."""
        label_key = self._get_label_key(labels)
        count = self._counts[label_key]
        sum_value = self._sums[label_key]
        
        return {
            "count": count,
            "sum": sum_value,
            "average": sum_value / count if count > 0 else 0,
            "buckets": dict(self._bucket_counts[label_key])
        }
    
    def reset(self):
        """Reset histogram."""
        with self._lock:
            self._bucket_counts.clear()
            self._sums.clear()
            self._counts.clear()
    
    def _get_label_key(self, labels: Optional[Dict[str, str]] = None) -> str:
        """Generate key from labels for storage."""
        all_labels = {**self.labels}
        if labels:
            all_labels.update(labels)
        return json.dumps(sorted(all_labels.items()), sort_keys=True)


class TimerMetric(BaseMetric):
    """Timer metric for measuring execution time."""
    
    def __init__(self, name: str, description: str = "", labels: Optional[Dict[str, str]] = None):
        super().__init__(name, description, labels)
        self._durations: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self._lock = threading.Lock()
    
    def record(self, value: Union[int, float], labels: Optional[Dict[str, str]] = None):
        """Record a duration."""
        label_key = self._get_label_key(labels)
        with self._lock:
            self._durations[label_key].append(value)
            self.last_updated = datetime.utcnow()
    
    @contextmanager
    def time(self, labels: Optional[Dict[str, str]] = None):
        """Context manager for timing operations."""
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            self.record(duration, labels)
    
    def get_value(self, labels: Optional[Dict[str, str]] = None) -> Dict[str, float]:
        """Get timer statistics."""
        label_key = self._get_label_key(labels)
        durations = list(self._durations[label_key])
        
        if not durations:
            return {"count": 0, "sum": 0, "avg": 0, "min": 0, "max": 0}
        
        return {
            "count": len(durations),
            "sum": sum(durations),
            "avg": statistics.mean(durations),
            "min": min(durations),
            "max": max(durations),
            "median": statistics.median(durations) if len(durations) > 1 else durations[0],
            "p95": statistics.quantiles(durations, n=20)[18] if len(durations) >= 20 else max(durations),
            "p99": statistics.quantiles(durations, n=100)[98] if len(durations) >= 100 else max(durations)
        }
    
    def reset(self):
        """Reset timer."""
        with self._lock:
            self._durations.clear()
    
    def _get_label_key(self, labels: Optional[Dict[str, str]] = None) -> str:
        """Generate key from labels for storage."""
        all_labels = {**self.labels}
        if labels:
            all_labels.update(labels)
        return json.dumps(sorted(all_labels.items()), sort_keys=True)


class MetricRegistry:
    """Registry for managing metrics."""
    
    def __init__(self, namespace: str = "quantstream"):
        self.namespace = namespace
        self._metrics: Dict[str, BaseMetric] = {}
        self._lock = threading.Lock()
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Built-in metrics
        self._register_builtin_metrics()
    
    def _register_builtin_metrics(self):
        """Register built-in metrics."""
        self.counter("messages_processed_total", "Total number of messages processed")
        self.counter("messages_failed_total", "Total number of failed messages")
        self.counter("api_requests_total", "Total number of API requests")
        self.gauge("active_connections", "Number of active connections")
        self.gauge("queue_size", "Current queue size")
        self.timer("message_processing_duration", "Message processing duration")
        self.histogram("request_duration", "Request duration histogram")
    
    def counter(self, name: str, description: str = "", labels: Optional[Dict[str, str]] = None) -> CounterMetric:
        """Create or get counter metric."""
        full_name = f"{self.namespace}_{name}"
        with self._lock:
            if full_name not in self._metrics:
                self._metrics[full_name] = CounterMetric(full_name, description, labels)
            return self._metrics[full_name]
    
    def gauge(self, name: str, description: str = "", labels: Optional[Dict[str, str]] = None) -> GaugeMetric:
        """Create or get gauge metric."""
        full_name = f"{self.namespace}_{name}"
        with self._lock:
            if full_name not in self._metrics:
                self._metrics[full_name] = GaugeMetric(full_name, description, labels)
            return self._metrics[full_name]
    
    def histogram(self, name: str, description: str = "", buckets: Optional[List[float]] = None, 
                  labels: Optional[Dict[str, str]] = None) -> HistogramMetric:
        """Create or get histogram metric."""
        full_name = f"{self.namespace}_{name}"
        with self._lock:
            if full_name not in self._metrics:
                self._metrics[full_name] = HistogramMetric(full_name, description, buckets, labels)
            return self._metrics[full_name]
    
    def timer(self, name: str, description: str = "", labels: Optional[Dict[str, str]] = None) -> TimerMetric:
        """Create or get timer metric."""
        full_name = f"{self.namespace}_{name}"
        with self._lock:
            if full_name not in self._metrics:
                self._metrics[full_name] = TimerMetric(full_name, description, labels)
            return self._metrics[full_name]
    
    def get_metric(self, name: str) -> Optional[BaseMetric]:
        """Get metric by name."""
        full_name = f"{self.namespace}_{name}"
        return self._metrics.get(full_name)
    
    def list_metrics(self) -> List[str]:
        """List all metric names."""
        return list(self._metrics.keys())
    
    def get_all_metrics(self) -> Dict[str, BaseMetric]:
        """Get all metrics."""
        return self._metrics.copy()
    
    def reset_all(self):
        """Reset all metrics."""
        with self._lock:
            for metric in self._metrics.values():
                metric.reset()
    
    def remove_metric(self, name: str):
        """Remove metric from registry."""
        full_name = f"{self.namespace}_{name}"
        with self._lock:
            self._metrics.pop(full_name, None)
    
    def export_json(self) -> str:
        """Export all metrics as JSON."""
        data = {}
        for name, metric in self._metrics.items():
            try:
                if isinstance(metric, (CounterMetric, GaugeMetric)):
                    data[name] = {"value": metric.get_value(), "type": type(metric).__name__}
                elif isinstance(metric, (HistogramMetric, TimerMetric)):
                    data[name] = {"value": metric.get_value(), "type": type(metric).__name__}
            except Exception as e:
                self.logger.error(f"Error exporting metric {name}: {e}")
                data[name] = {"error": str(e)}
        
        return json.dumps(data, indent=2, default=str)


class PrometheusExporter:
    """Prometheus metrics exporter."""
    
    def __init__(self, registry: MetricRegistry):
        if not PROMETHEUS_AVAILABLE:
            raise ImportError("prometheus_client is required for Prometheus export")
        
        self.registry = registry
        self.prom_registry = CollectorRegistry()
        self._prom_metrics: Dict[str, Any] = {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def register_prometheus_metrics(self):
        """Register metrics with Prometheus client."""
        for name, metric in self.registry.get_all_metrics().items():
            try:
                if isinstance(metric, CounterMetric):
                    prom_metric = Counter(name, metric.description, registry=self.prom_registry)
                elif isinstance(metric, GaugeMetric):
                    prom_metric = Gauge(name, metric.description, registry=self.prom_registry)
                elif isinstance(metric, HistogramMetric):
                    prom_metric = Histogram(name, metric.description, buckets=metric.buckets, registry=self.prom_registry)
                elif isinstance(metric, TimerMetric):
                    prom_metric = Summary(name, metric.description, registry=self.prom_registry)
                else:
                    continue
                
                self._prom_metrics[name] = prom_metric
                
            except Exception as e:
                self.logger.error(f"Error registering Prometheus metric {name}: {e}")
    
    def update_prometheus_metrics(self):
        """Update Prometheus metrics with current values."""
        for name, metric in self.registry.get_all_metrics().items():
            if name not in self._prom_metrics:
                continue
            
            try:
                prom_metric = self._prom_metrics[name]
                
                if isinstance(metric, CounterMetric):
                    # Prometheus counters are cumulative, so we set to current value
                    prom_metric._value.set(metric.get_value())
                elif isinstance(metric, GaugeMetric):
                    prom_metric.set(metric.get_value())
                elif isinstance(metric, TimerMetric):
                    stats = metric.get_value()
                    if stats["count"] > 0:
                        prom_metric.observe(stats["avg"])
                
            except Exception as e:
                self.logger.error(f"Error updating Prometheus metric {name}: {e}")
    
    def generate_metrics(self) -> bytes:
        """Generate Prometheus metrics format."""
        self.update_prometheus_metrics()
        return generate_latest(self.prom_registry)


class MetricsCollector:
    """Background metrics collector."""
    
    def __init__(self, registry: MetricRegistry, collection_interval: float = 30.0):
        self.registry = registry
        self.collection_interval = collection_interval
        self._stop_event = threading.Event()
        self._collector_thread: Optional[threading.Thread] = None
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Custom metric collectors
        self._collectors: List[Callable[[], None]] = []
    
    def add_collector(self, collector: Callable[[], None]):
        """Add custom metric collector function."""
        self._collectors.append(collector)
    
    def start(self):
        """Start metrics collection in background thread."""
        if self._collector_thread and self._collector_thread.is_alive():
            return
        
        self._stop_event.clear()
        self._collector_thread = threading.Thread(
            target=self._collection_loop,
            name="metrics-collector",
            daemon=True
        )
        self._collector_thread.start()
        self.logger.info("Metrics collector started")
    
    def stop(self):
        """Stop metrics collection."""
        if self._collector_thread and self._collector_thread.is_alive():
            self._stop_event.set()
            self._collector_thread.join(timeout=10)
        self.logger.info("Metrics collector stopped")
    
    def _collection_loop(self):
        """Main collection loop."""
        while not self._stop_event.wait(self.collection_interval):
            try:
                # Run custom collectors
                for collector in self._collectors:
                    try:
                        collector()
                    except Exception as e:
                        self.logger.error(f"Error in custom collector: {e}")
                
                # Update system metrics
                self._collect_system_metrics()
                
            except Exception as e:
                self.logger.error(f"Error in metrics collection: {e}")
    
    def _collect_system_metrics(self):
        """Collect system-level metrics."""
        try:
            import psutil
            
            # CPU usage
            cpu_percent = psutil.cpu_percent()
            self.registry.gauge("cpu_usage_percent").record(cpu_percent)
            
            # Memory usage
            memory = psutil.virtual_memory()
            self.registry.gauge("memory_usage_percent").record(memory.percent)
            self.registry.gauge("memory_used_bytes").record(memory.used)
            
            # Disk usage
            disk = psutil.disk_usage('/')
            self.registry.gauge("disk_usage_percent").record(disk.percent)
            
        except ImportError:
            pass  # psutil not available
        except Exception as e:
            self.logger.error(f"Error collecting system metrics: {e}")


# Global registry instance
_global_registry = MetricRegistry()


def get_registry() -> MetricRegistry:
    """Get the global metrics registry."""
    return _global_registry


def counter(name: str, description: str = "", labels: Optional[Dict[str, str]] = None) -> CounterMetric:
    """Create or get counter from global registry."""
    return _global_registry.counter(name, description, labels)


def gauge(name: str, description: str = "", labels: Optional[Dict[str, str]] = None) -> GaugeMetric:
    """Create or get gauge from global registry."""
    return _global_registry.gauge(name, description, labels)


def histogram(name: str, description: str = "", buckets: Optional[List[float]] = None,
              labels: Optional[Dict[str, str]] = None) -> HistogramMetric:
    """Create or get histogram from global registry."""
    return _global_registry.histogram(name, description, buckets, labels)


def timer(name: str, description: str = "", labels: Optional[Dict[str, str]] = None) -> TimerMetric:
    """Create or get timer from global registry."""
    return _global_registry.timer(name, description, labels)