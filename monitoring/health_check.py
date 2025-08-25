#!/usr/bin/env python3
"""
Health check and monitoring script for QuantStream Analytics Platform.

This script provides comprehensive health checks for all pipeline components
and can be used for monitoring, alerting, and automated health verification.
"""

import asyncio
import json
import time
import sys
import aiohttp
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    import psycopg2
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False


@dataclass
class HealthStatus:
    """Health status for a component."""
    component: str
    status: str  # "healthy", "degraded", "unhealthy", "unknown"
    message: str
    response_time_ms: float
    timestamp: str
    details: Optional[Dict[str, Any]] = None


@dataclass
class SystemHealth:
    """Overall system health status."""
    overall_status: str
    timestamp: str
    components: List[HealthStatus]
    summary: Dict[str, int]


class HealthChecker:
    """Comprehensive health checker for QuantStream components."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.session = None
        
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    async def check_http_endpoint(self, name: str, url: str, expected_status: int = 200) -> HealthStatus:
        """Check HTTP endpoint health."""
        start_time = time.time()
        
        try:
            async with self.session.get(url) as response:
                response_time = (time.time() - start_time) * 1000
                
                if response.status == expected_status:
                    return HealthStatus(
                        component=name,
                        status="healthy",
                        message=f"HTTP {response.status}",
                        response_time_ms=response_time,
                        timestamp=datetime.now(timezone.utc).isoformat(),
                        details={"status_code": response.status, "url": url}
                    )
                else:
                    return HealthStatus(
                        component=name,
                        status="unhealthy",
                        message=f"HTTP {response.status}, expected {expected_status}",
                        response_time_ms=response_time,
                        timestamp=datetime.now(timezone.utc).isoformat(),
                        details={"status_code": response.status, "url": url}
                    )
                    
        except asyncio.TimeoutError:
            return HealthStatus(
                component=name,
                status="unhealthy",
                message="Timeout",
                response_time_ms=(time.time() - start_time) * 1000,
                timestamp=datetime.now(timezone.utc).isoformat(),
                details={"error": "timeout", "url": url}
            )
        except Exception as e:
            return HealthStatus(
                component=name,
                status="unhealthy",
                message=f"Error: {str(e)}",
                response_time_ms=(time.time() - start_time) * 1000,
                timestamp=datetime.now(timezone.utc).isoformat(),
                details={"error": str(e), "url": url}
            )
    
    async def check_quantstream_service(self) -> HealthStatus:
        """Check QuantStream ingestion service health."""
        service_config = self.config.get("quantstream", {})
        base_url = service_config.get("base_url", "http://localhost:8000")
        
        # Check main health endpoint
        health_url = f"{base_url}/health"
        return await self.check_http_endpoint("quantstream-service", health_url)
    
    async def check_prometheus(self) -> HealthStatus:
        """Check Prometheus health."""
        prometheus_config = self.config.get("prometheus", {})
        base_url = prometheus_config.get("base_url", "http://localhost:9090")
        
        # Check Prometheus ready endpoint
        ready_url = f"{base_url}/-/ready"
        return await self.check_http_endpoint("prometheus", ready_url)
    
    async def check_grafana(self) -> HealthStatus:
        """Check Grafana health."""
        grafana_config = self.config.get("grafana", {})
        base_url = grafana_config.get("base_url", "http://localhost:3000")
        
        # Check Grafana health endpoint
        health_url = f"{base_url}/api/health"
        return await self.check_http_endpoint("grafana", health_url)
    
    async def check_kafka(self) -> HealthStatus:
        """Check Kafka health via Kafka UI or direct connection."""
        kafka_config = self.config.get("kafka", {})
        
        # Try Kafka UI first if available
        ui_url = kafka_config.get("ui_url")
        if ui_url:
            return await self.check_http_endpoint("kafka-ui", ui_url)
        
        # For now, return unknown status as direct Kafka check requires kafka-python
        return HealthStatus(
            component="kafka",
            status="unknown",
            message="Kafka health check not implemented",
            response_time_ms=0,
            timestamp=datetime.now(timezone.utc).isoformat(),
            details={"note": "Direct Kafka health check requires additional dependencies"}
        )
    
    def check_redis(self) -> HealthStatus:
        """Check Redis health."""
        if not REDIS_AVAILABLE:
            return HealthStatus(
                component="redis",
                status="unknown",
                message="Redis client not available",
                response_time_ms=0,
                timestamp=datetime.now(timezone.utc).isoformat(),
                details={"note": "pip install redis to enable Redis health checks"}
            )
        
        redis_config = self.config.get("redis", {})
        host = redis_config.get("host", "localhost")
        port = redis_config.get("port", 6379)
        
        start_time = time.time()
        
        try:
            client = redis.Redis(host=host, port=port, socket_timeout=5, socket_connect_timeout=5)
            client.ping()
            response_time = (time.time() - start_time) * 1000
            
            return HealthStatus(
                component="redis",
                status="healthy",
                message="PONG",
                response_time_ms=response_time,
                timestamp=datetime.now(timezone.utc).isoformat(),
                details={"host": host, "port": port}
            )
            
        except Exception as e:
            return HealthStatus(
                component="redis",
                status="unhealthy",
                message=f"Error: {str(e)}",
                response_time_ms=(time.time() - start_time) * 1000,
                timestamp=datetime.now(timezone.utc).isoformat(),
                details={"error": str(e), "host": host, "port": port}
            )
    
    def check_postgres(self) -> HealthStatus:
        """Check PostgreSQL health."""
        if not POSTGRES_AVAILABLE:
            return HealthStatus(
                component="postgres",
                status="unknown",
                message="PostgreSQL client not available",
                response_time_ms=0,
                timestamp=datetime.now(timezone.utc).isoformat(),
                details={"note": "pip install psycopg2-binary to enable PostgreSQL health checks"}
            )
        
        postgres_config = self.config.get("postgres", {})
        host = postgres_config.get("host", "localhost")
        port = postgres_config.get("port", 5432)
        database = postgres_config.get("database", "quantstream")
        user = postgres_config.get("user", "quantstream_user")
        password = postgres_config.get("password", "quantstream_pass")
        
        start_time = time.time()
        
        try:
            connection = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                connect_timeout=5
            )
            
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            
            connection.close()
            response_time = (time.time() - start_time) * 1000
            
            return HealthStatus(
                component="postgres",
                status="healthy",
                message="Connection successful",
                response_time_ms=response_time,
                timestamp=datetime.now(timezone.utc).isoformat(),
                details={"host": host, "port": port, "database": database}
            )
            
        except Exception as e:
            return HealthStatus(
                component="postgres",
                status="unhealthy",
                message=f"Error: {str(e)}",
                response_time_ms=(time.time() - start_time) * 1000,
                timestamp=datetime.now(timezone.utc).isoformat(),
                details={"error": str(e), "host": host, "port": port}
            )
    
    async def check_all_components(self) -> SystemHealth:
        """Check health of all components."""
        components = []
        
        # Async checks
        async_tasks = [
            self.check_quantstream_service(),
            self.check_prometheus(),
            self.check_grafana(),
            self.check_kafka()
        ]
        
        async_results = await asyncio.gather(*async_tasks, return_exceptions=True)
        
        for result in async_results:
            if isinstance(result, HealthStatus):
                components.append(result)
            else:
                # Handle exceptions
                components.append(HealthStatus(
                    component="unknown",
                    status="unhealthy",
                    message=f"Check failed: {str(result)}",
                    response_time_ms=0,
                    timestamp=datetime.now(timezone.utc).isoformat()
                ))
        
        # Sync checks
        components.append(self.check_redis())
        components.append(self.check_postgres())
        
        # Calculate overall status
        status_counts = {"healthy": 0, "degraded": 0, "unhealthy": 0, "unknown": 0}
        for component in components:
            status_counts[component.status] += 1
        
        # Determine overall status
        if status_counts["unhealthy"] > 0:
            overall_status = "unhealthy"
        elif status_counts["degraded"] > 0:
            overall_status = "degraded"
        elif status_counts["unknown"] > 2:  # Allow some unknown components
            overall_status = "degraded"
        else:
            overall_status = "healthy"
        
        return SystemHealth(
            overall_status=overall_status,
            timestamp=datetime.now(timezone.utc).isoformat(),
            components=components,
            summary=status_counts
        )


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load health check configuration."""
    default_config = {
        "quantstream": {
            "base_url": "http://localhost:8000"
        },
        "prometheus": {
            "base_url": "http://localhost:9090"
        },
        "grafana": {
            "base_url": "http://localhost:3000"
        },
        "kafka": {
            "ui_url": "http://localhost:8080"
        },
        "redis": {
            "host": "localhost",
            "port": 6379
        },
        "postgres": {
            "host": "localhost",
            "port": 5432,
            "database": "quantstream",
            "user": "quantstream_user",
            "password": "quantstream_pass"
        }
    }
    
    if config_path and Path(config_path).exists():
        with open(config_path, 'r') as f:
            file_config = json.load(f)
        
        # Merge with defaults
        for key, value in file_config.items():
            if key in default_config and isinstance(value, dict):
                default_config[key].update(value)
            else:
                default_config[key] = value
    
    return default_config


async def main():
    """Main health check function."""
    parser = argparse.ArgumentParser(description="QuantStream Health Check")
    parser.add_argument("--config", "-c", help="Configuration file path")
    parser.add_argument("--output", "-o", choices=["json", "text"], default="text", help="Output format")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--component", help="Check specific component only")
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Perform health checks
    async with HealthChecker(config) as checker:
        if args.component:
            # Check specific component
            component_map = {
                "quantstream": checker.check_quantstream_service,
                "prometheus": checker.check_prometheus,
                "grafana": checker.check_grafana,
                "kafka": checker.check_kafka,
                "redis": lambda: checker.check_redis(),
                "postgres": lambda: checker.check_postgres()
            }
            
            if args.component in component_map:
                if asyncio.iscoroutinefunction(component_map[args.component]):
                    result = await component_map[args.component]()
                else:
                    result = component_map[args.component]()
                
                if args.output == "json":
                    print(json.dumps(asdict(result), indent=2))
                else:
                    print(f"Component: {result.component}")
                    print(f"Status: {result.status}")
                    print(f"Message: {result.message}")
                    print(f"Response Time: {result.response_time_ms:.2f}ms")
                    if args.verbose and result.details:
                        print(f"Details: {json.dumps(result.details, indent=2)}")
            else:
                print(f"Unknown component: {args.component}")
                print(f"Available components: {', '.join(component_map.keys())}")
                return 1
        else:
            # Check all components
            system_health = await checker.check_all_components()
            
            if args.output == "json":
                # Convert to dict for JSON serialization
                health_dict = asdict(system_health)
                print(json.dumps(health_dict, indent=2))
            else:
                # Text output
                print(f"QuantStream System Health: {system_health.overall_status.upper()}")
                print(f"Timestamp: {system_health.timestamp}")
                print(f"Summary: {system_health.summary}")
                print()
                
                for component in system_health.components:
                    status_icon = {
                        "healthy": "✅",
                        "degraded": "⚠️",
                        "unhealthy": "❌",
                        "unknown": "❓"
                    }.get(component.status, "❓")
                    
                    print(f"{status_icon} {component.component}: {component.status} - {component.message} ({component.response_time_ms:.1f}ms)")
                    
                    if args.verbose and component.details:
                        for key, value in component.details.items():
                            print(f"    {key}: {value}")
            
            # Return appropriate exit code
            return 0 if system_health.overall_status == "healthy" else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))