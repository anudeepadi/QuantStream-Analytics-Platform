"""
Prometheus Metrics Collector

Custom metrics collection for QuantStream Dashboard monitoring.
"""

import time
import threading
import psutil
import logging
from typing import Dict, Any, Optional
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, generate_latest
from datetime import datetime
import asyncio
import aioredis
import asyncpg

logger = logging.getLogger(__name__)

class QuantStreamMetricsCollector:
    """Custom metrics collector for QuantStream Dashboard"""
    
    def __init__(self, registry: CollectorRegistry = None):
        self.registry = registry or CollectorRegistry()
        self.setup_metrics()
        self.collection_interval = 15  # seconds
        self.is_collecting = False
        self.collection_thread: Optional[threading.Thread] = None
        
        # Service connections
        self.redis_client = None
        self.db_pool = None
    
    def setup_metrics(self):
        """Setup Prometheus metrics"""
        
        # Application metrics
        self.http_requests_total = Counter(
            'quantstream_http_requests_total',
            'Total HTTP requests',
            ['method', 'endpoint', 'status_code'],
            registry=self.registry
        )
        
        self.http_request_duration = Histogram(
            'quantstream_http_request_duration_seconds',
            'HTTP request duration',
            ['method', 'endpoint'],
            registry=self.registry
        )
        
        self.websocket_connections = Gauge(
            'quantstream_websocket_connections_total',
            'Active WebSocket connections',
            registry=self.registry
        )
        
        self.market_data_updates = Counter(
            'quantstream_market_data_updates_total',
            'Total market data updates',
            ['symbol'],
            registry=self.registry
        )
        
        self.alert_notifications = Counter(
            'quantstream_alert_notifications_total',
            'Total alert notifications sent',
            ['alert_type', 'severity'],
            registry=self.registry
        )
        
        # System metrics
        self.system_cpu_usage = Gauge(
            'quantstream_system_cpu_usage_percent',
            'System CPU usage percentage',
            registry=self.registry
        )
        
        self.system_memory_usage = Gauge(
            'quantstream_system_memory_usage_percent',
            'System memory usage percentage',
            registry=self.registry
        )
        
        self.system_disk_usage = Gauge(
            'quantstream_system_disk_usage_percent',
            'System disk usage percentage',
            registry=self.registry
        )
        
        self.system_network_bytes_sent = Counter(
            'quantstream_system_network_bytes_sent_total',
            'Total network bytes sent',
            registry=self.registry
        )
        
        self.system_network_bytes_recv = Counter(
            'quantstream_system_network_bytes_recv_total',
            'Total network bytes received',
            registry=self.registry
        )
        
        # Database metrics
        self.database_connections_active = Gauge(
            'quantstream_database_connections_active',
            'Active database connections',
            registry=self.registry
        )
        
        self.database_query_duration = Histogram(
            'quantstream_database_query_duration_seconds',
            'Database query duration',
            ['query_type'],
            registry=self.registry
        )
        
        # Redis metrics
        self.redis_connections_active = Gauge(
            'quantstream_redis_connections_active',
            'Active Redis connections',
            registry=self.registry
        )
        
        self.redis_cache_hits = Counter(
            'quantstream_redis_cache_hits_total',
            'Total Redis cache hits',
            registry=self.registry
        )
        
        self.redis_cache_misses = Counter(
            'quantstream_redis_cache_misses_total',
            'Total Redis cache misses',
            registry=self.registry
        )
        
        # Business metrics
        self.active_users = Gauge(
            'quantstream_active_users_total',
            'Number of active users',
            registry=self.registry
        )
        
        self.portfolio_value_total = Gauge(
            'quantstream_portfolio_value_total_usd',
            'Total portfolio value in USD',
            registry=self.registry
        )
        
        self.data_quality_score = Gauge(
            'quantstream_data_quality_score',
            'Data quality score (0-1)',
            ['source'],
            registry=self.registry
        )
        
        # Error metrics
        self.errors_total = Counter(
            'quantstream_errors_total',
            'Total errors',
            ['component', 'error_type'],
            registry=self.registry
        )
    
    async def initialize_services(self):
        """Initialize Redis and database connections for metrics"""
        try:
            # Initialize Redis connection
            self.redis_client = await aioredis.create_redis_pool(
                'redis://localhost:6379',
                encoding='utf-8'
            )
            
            # Initialize database connection pool
            self.db_pool = await asyncpg.create_pool(
                'postgresql://postgres:postgres@localhost:5432/quantstream',
                min_size=1,
                max_size=5
            )
            
            logger.info("Metrics collector services initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize metrics collector services: {e}")
    
    def start_collection(self):
        """Start metrics collection in background thread"""
        if not self.is_collecting:
            self.is_collecting = True
            self.collection_thread = threading.Thread(
                target=self._collection_loop,
                daemon=True
            )
            self.collection_thread.start()
            logger.info("Metrics collection started")
    
    def stop_collection(self):
        """Stop metrics collection"""
        self.is_collecting = False
        if self.collection_thread:
            self.collection_thread.join(timeout=5)
        logger.info("Metrics collection stopped")
    
    def _collection_loop(self):
        """Main collection loop running in background thread"""
        while self.is_collecting:
            try:
                # Collect system metrics
                self._collect_system_metrics()
                
                # Collect application metrics
                asyncio.run(self._collect_application_metrics())
                
                # Wait for next collection
                time.sleep(self.collection_interval)
                
            except Exception as e:
                logger.error(f"Error in metrics collection loop: {e}")
                time.sleep(1)  # Brief pause before retrying
    
    def _collect_system_metrics(self):
        """Collect system-level metrics"""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            self.system_cpu_usage.set(cpu_percent)
            
            # Memory usage
            memory = psutil.virtual_memory()
            self.system_memory_usage.set(memory.percent)
            
            # Disk usage
            disk = psutil.disk_usage('/')
            disk_percent = (disk.used / disk.total) * 100
            self.system_disk_usage.set(disk_percent)
            
            # Network I/O
            network = psutil.net_io_counters()
            if hasattr(self, '_last_network_stats'):
                bytes_sent_delta = network.bytes_sent - self._last_network_stats.bytes_sent
                bytes_recv_delta = network.bytes_recv - self._last_network_stats.bytes_recv
                
                if bytes_sent_delta > 0:
                    self.system_network_bytes_sent.inc(bytes_sent_delta)
                if bytes_recv_delta > 0:
                    self.system_network_bytes_recv.inc(bytes_recv_delta)
            
            self._last_network_stats = network
            
        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}")
    
    async def _collect_application_metrics(self):
        """Collect application-specific metrics"""
        try:
            # Database metrics
            if self.db_pool:
                self.database_connections_active.set(len(self.db_pool._queue._queue))
            
            # Redis metrics
            if self.redis_client:
                redis_info = await self.redis_client.info()
                self.redis_connections_active.set(redis_info.get('connected_clients', 0))
            
            # Mock business metrics (in production, these would be real)
            self.active_users.set(len(self._get_mock_active_users()))
            self.portfolio_value_total.set(self._get_mock_portfolio_value())
            
            # Data quality scores
            self.data_quality_score.labels(source='market_data').set(0.98)
            self.data_quality_score.labels(source='portfolio').set(0.95)
            
        except Exception as e:
            logger.error(f"Error collecting application metrics: {e}")
    
    def _get_mock_active_users(self) -> list:
        """Get mock active users (in production, would query real data)"""
        # Mock data for demonstration
        import random
        return [f"user_{i}" for i in range(random.randint(10, 50))]
    
    def _get_mock_portfolio_value(self) -> float:
        """Get mock portfolio value (in production, would calculate real value)"""
        # Mock data for demonstration
        import random
        return random.uniform(1000000, 5000000)  # $1M to $5M
    
    # Public methods for recording metrics
    def record_http_request(self, method: str, endpoint: str, status_code: int, duration: float):
        """Record HTTP request metrics"""
        self.http_requests_total.labels(
            method=method,
            endpoint=endpoint,
            status_code=str(status_code)
        ).inc()
        
        self.http_request_duration.labels(
            method=method,
            endpoint=endpoint
        ).observe(duration)
    
    def record_websocket_connection_change(self, delta: int):
        """Record WebSocket connection change"""
        if delta > 0:
            self.websocket_connections.inc(delta)
        elif delta < 0:
            self.websocket_connections.dec(abs(delta))
    
    def record_market_data_update(self, symbol: str):
        """Record market data update"""
        self.market_data_updates.labels(symbol=symbol).inc()
    
    def record_alert_notification(self, alert_type: str, severity: str):
        """Record alert notification"""
        self.alert_notifications.labels(
            alert_type=alert_type,
            severity=severity
        ).inc()
    
    def record_database_query(self, query_type: str, duration: float):
        """Record database query metrics"""
        self.database_query_duration.labels(query_type=query_type).observe(duration)
    
    def record_cache_hit(self):
        """Record cache hit"""
        self.redis_cache_hits.inc()
    
    def record_cache_miss(self):
        """Record cache miss"""
        self.redis_cache_misses.inc()
    
    def record_error(self, component: str, error_type: str):
        """Record error"""
        self.errors_total.labels(
            component=component,
            error_type=error_type
        ).inc()
    
    def get_metrics(self) -> str:
        """Get metrics in Prometheus format"""
        return generate_latest(self.registry).decode('utf-8')
    
    async def cleanup(self):
        """Cleanup resources"""
        self.stop_collection()
        
        if self.redis_client:
            self.redis_client.close()
            await self.redis_client.wait_closed()
        
        if self.db_pool:
            await self.db_pool.close()

# Global metrics collector instance
metrics_collector = QuantStreamMetricsCollector()

# Convenience functions for easy access
def record_http_request(method: str, endpoint: str, status_code: int, duration: float):
    """Record HTTP request metrics"""
    metrics_collector.record_http_request(method, endpoint, status_code, duration)

def record_websocket_connection_change(delta: int):
    """Record WebSocket connection change"""
    metrics_collector.record_websocket_connection_change(delta)

def record_market_data_update(symbol: str):
    """Record market data update"""
    metrics_collector.record_market_data_update(symbol)

def record_alert_notification(alert_type: str, severity: str):
    """Record alert notification"""
    metrics_collector.record_alert_notification(alert_type, severity)

def record_database_query(query_type: str, duration: float):
    """Record database query metrics"""
    metrics_collector.record_database_query(query_type, duration)

def record_cache_hit():
    """Record cache hit"""
    metrics_collector.record_cache_hit()

def record_cache_miss():
    """Record cache miss"""
    metrics_collector.record_cache_miss()

def record_error(component: str, error_type: str):
    """Record error"""
    metrics_collector.record_error(component, error_type)

def get_metrics() -> str:
    """Get all metrics in Prometheus format"""
    return metrics_collector.get_metrics()