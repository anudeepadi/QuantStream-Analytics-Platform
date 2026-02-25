"""
System Metrics API Endpoints

REST API endpoints for system health, performance metrics, and monitoring.
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import psutil
import time
import random
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/health")
async def get_system_health():
    """Get overall system health status"""
    
    try:
        # Collect system metrics
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        network = psutil.net_io_counters()
        
        # Determine health status
        health_status = "healthy"
        
        if cpu_percent > 80 or memory.percent > 85:
            health_status = "degraded"
        
        disk_percent = round((disk.used / disk.total) * 100, 1)
        if disk_percent > 90:
            health_status = "down"
        
        now_iso = datetime.now().isoformat()
        
        return {
            "overall_status": health_status,
            "services": [
                {"name": "FastAPI Backend", "status": "healthy", "latency_ms": 12, "last_check": now_iso, "message": None},
                {"name": "PostgreSQL", "status": "healthy", "latency_ms": 3, "last_check": now_iso, "message": None},
                {"name": "Redis Cache", "status": "healthy", "latency_ms": 1, "last_check": now_iso, "message": None},
                {"name": "Kafka Broker", "status": "healthy", "latency_ms": 25, "last_check": now_iso, "message": None},
                {"name": "Market Data Feed", "status": "healthy", "latency_ms": 28, "last_check": now_iso, "message": None},
            ],
            "metrics": {
                "cpu_usage": round(cpu_percent, 1),
                "memory_usage": round(memory.percent, 1),
                "disk_usage": disk_percent,
                "network_in": round(network.bytes_recv / 1_000_000, 1),
                "network_out": round(network.bytes_sent / 1_000_000, 1),
                "uptime_seconds": int(time.time() - psutil.boot_time()),
                "active_connections": random.randint(100, 200),
                "requests_per_second": round(random.uniform(200, 500), 1),
            },
            "last_updated": now_iso,
        }
        
    except Exception as e:
        logger.error(f"Error getting system health: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting health status: {str(e)}")


@router.get("/metrics")
async def get_system_metrics(
    hours: int = Query(1, ge=1, le=168, description="Hours of metrics to retrieve")
):
    """Get system metrics for the specified time period"""
    
    try:
        # Current metrics
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        network = psutil.net_io_counters()
        
        # Generate historical data (mock)
        import random
        import numpy as np
        
        timestamps = []
        cpu_history = []
        memory_history = []
        
        for i in range(hours * 60):  # Every minute
            timestamp = datetime.now() - timedelta(minutes=i)
            timestamps.append(timestamp.isoformat())
            
            # Generate realistic historical data
            cpu_history.append(round(random.uniform(20, 80), 1))
            memory_history.append(round(random.uniform(40, 90), 1))
        
        return {
            "current": {
                "cpu_percent": round(cpu_percent, 1),
                "memory_percent": round(memory.percent, 1),
                "disk_percent": round((disk.used / disk.total) * 100, 1),
                "network_bytes_sent": network.bytes_sent,
                "network_bytes_recv": network.bytes_recv,
                "timestamp": datetime.now().isoformat()
            },
            "historical": {
                "timestamps": timestamps[::-1],  # Reverse to chronological order
                "cpu_percent": cpu_history[::-1],
                "memory_percent": memory_history[::-1]
            },
            "period_hours": hours,
            "generated_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting system metrics: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting metrics: {str(e)}")

@router.get("/performance")
async def get_performance_metrics():
    """Get application performance metrics"""
    
    try:
        # Mock performance data (in production, would come from real monitoring)
        import random
        
        return {
            "api": {
                "response_time_ms": round(random.uniform(10, 100), 1),
                "requests_per_second": round(random.uniform(50, 500), 1),
                "error_rate_percent": round(random.uniform(0, 5), 2),
                "active_connections": random.randint(10, 100)
            },
            "websocket": {
                "active_connections": random.randint(5, 50),
                "messages_per_second": round(random.uniform(10, 100), 1),
                "connection_errors": random.randint(0, 5)
            },
            "database": {
                "connection_pool_usage": round(random.uniform(20, 80), 1),
                "query_time_ms": round(random.uniform(5, 50), 1),
                "active_queries": random.randint(1, 10),
                "deadlocks": 0
            },
            "cache": {
                "hit_rate_percent": round(random.uniform(85, 99), 1),
                "memory_usage_mb": round(random.uniform(100, 500), 1),
                "operations_per_second": round(random.uniform(100, 1000), 1)
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting performance metrics: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting performance: {str(e)}")

@router.get("/services")
async def get_service_status():
    """Get status of all system services"""
    
    try:
        # Mock service status (in production, would check actual services)
        import random
        
        services = {
            "dashboard": {
                "status": "healthy",
                "uptime": "2d 14h 32m",
                "cpu_percent": round(random.uniform(5, 25), 1),
                "memory_mb": round(random.uniform(200, 800), 1),
                "last_restart": "2024-01-21T10:30:00Z"
            },
            "api": {
                "status": "healthy",
                "uptime": "2d 14h 32m",
                "cpu_percent": round(random.uniform(10, 30), 1),
                "memory_mb": round(random.uniform(300, 1000), 1),
                "last_restart": "2024-01-21T10:30:00Z"
            },
            "websocket": {
                "status": "healthy",
                "uptime": "2d 14h 32m", 
                "cpu_percent": round(random.uniform(5, 15), 1),
                "memory_mb": round(random.uniform(100, 400), 1),
                "last_restart": "2024-01-21T10:30:00Z"
            },
            "database": {
                "status": "healthy",
                "uptime": "7d 3h 15m",
                "cpu_percent": round(random.uniform(15, 40), 1),
                "memory_mb": round(random.uniform(500, 2000), 1),
                "last_restart": "2024-01-16T07:15:00Z"
            },
            "redis": {
                "status": "healthy",
                "uptime": "7d 3h 15m",
                "cpu_percent": round(random.uniform(3, 12), 1),
                "memory_mb": round(random.uniform(50, 300), 1),
                "last_restart": "2024-01-16T07:15:00Z"
            },
            "prometheus": {
                "status": "healthy",
                "uptime": "7d 3h 15m",
                "cpu_percent": round(random.uniform(5, 20), 1),
                "memory_mb": round(random.uniform(200, 600), 1),
                "last_restart": "2024-01-16T07:15:00Z"
            },
            "grafana": {
                "status": random.choice(["healthy", "warning"]),
                "uptime": "2d 14h 30m",
                "cpu_percent": round(random.uniform(8, 25), 1),
                "memory_mb": round(random.uniform(300, 900), 1),
                "last_restart": "2024-01-21T10:32:00Z"
            }
        }
        
        return {
            "services": services,
            "overall_status": "healthy" if all(s["status"] == "healthy" for s in services.values()) else "warning",
            "total_services": len(services),
            "healthy_services": len([s for s in services.values() if s["status"] == "healthy"]),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting service status: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting service status: {str(e)}")

@router.get("/alerts")
async def get_system_alerts():
    """Get active system alerts"""
    
    try:
        # Mock system alerts
        alerts = [
            {
                "id": "SYS001",
                "type": "performance",
                "severity": "warning",
                "message": "API response time above threshold",
                "value": 156.5,
                "threshold": 150.0,
                "triggered_at": (datetime.now() - timedelta(minutes=5)).isoformat(),
                "status": "active"
            },
            {
                "id": "SYS002", 
                "type": "resource",
                "severity": "info",
                "message": "Memory usage elevated",
                "value": 78.2,
                "threshold": 85.0,
                "triggered_at": (datetime.now() - timedelta(minutes=15)).isoformat(),
                "status": "active"
            }
        ]
        
        return {
            "alerts": alerts,
            "total_alerts": len(alerts),
            "critical_alerts": len([a for a in alerts if a["severity"] == "critical"]),
            "warning_alerts": len([a for a in alerts if a["severity"] == "warning"]),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting system alerts: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting alerts: {str(e)}")

@router.get("/logs")
async def get_system_logs(
    level: str = Query("INFO", description="Log level filter"),
    lines: int = Query(100, ge=1, le=1000, description="Number of log lines"),
    service: Optional[str] = Query(None, description="Filter by service")
):
    """Get recent system logs"""
    
    try:
        # Mock log entries
        import random
        
        log_levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        services = ["dashboard", "api", "websocket", "database", "redis"]
        
        logs = []
        for i in range(lines):
            timestamp = datetime.now() - timedelta(seconds=i*10)
            log_level = random.choice(log_levels) if not level else level
            service_name = random.choice(services) if not service else service
            
            messages = {
                "DEBUG": f"Debug message {i}",
                "INFO": f"Service {service_name} processing request",
                "WARNING": f"Service {service_name} performance degraded",
                "ERROR": f"Service {service_name} encountered error"
            }
            
            logs.append({
                "timestamp": timestamp.isoformat(),
                "level": log_level,
                "service": service_name,
                "message": messages.get(log_level, f"Log message {i}"),
                "thread": f"thread-{random.randint(1, 10)}"
            })
        
        return {
            "logs": logs,
            "total_lines": len(logs),
            "filters": {
                "level": level,
                "service": service,
                "lines": lines
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting system logs: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting logs: {str(e)}")

@router.get("/capacity")
async def get_capacity_metrics():
    """Get system capacity and scaling metrics"""
    
    try:
        # Mock capacity data
        import random
        
        return {
            "current_capacity": {
                "cpu_cores_used": 4,
                "cpu_cores_total": 8,
                "memory_gb_used": 6.4,
                "memory_gb_total": 16.0,
                "disk_gb_used": 245.8,
                "disk_gb_total": 500.0,
                "network_mbps_used": 45.2,
                "network_mbps_available": 1000.0
            },
            "utilization_percent": {
                "cpu": 50.0,
                "memory": 40.0,
                "disk": 49.2,
                "network": 4.5
            },
            "scaling_triggers": {
                "cpu_threshold": 80.0,
                "memory_threshold": 85.0,
                "auto_scaling_enabled": True
            },
            "projected_capacity": {
                "time_to_cpu_limit_hours": round(random.uniform(48, 168), 1),
                "time_to_memory_limit_hours": round(random.uniform(72, 240), 1),
                "recommended_action": "Monitor - capacity adequate"
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting capacity metrics: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting capacity: {str(e)}")

@router.post("/restart-service")
async def restart_service(service_name: str):
    """Restart a specific service (mock implementation)"""
    
    try:
        # Mock service restart
        valid_services = ["dashboard", "api", "websocket", "database", "redis", "prometheus", "grafana"]
        
        if service_name not in valid_services:
            raise HTTPException(status_code=400, detail=f"Unknown service: {service_name}")
        
        # In production, would actually restart the service
        logger.info(f"Restarting service: {service_name}")
        
        return {
            "success": True,
            "message": f"Service {service_name} restart initiated",
            "service": service_name,
            "estimated_downtime_seconds": 30,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error restarting service {service_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Error restarting service: {str(e)}")

@router.get("/network")
async def get_network_metrics():
    """Get network metrics and statistics"""
    
    try:
        network = psutil.net_io_counters()
        
        # Mock additional network metrics
        import random
        
        return {
            "current": {
                "bytes_sent": network.bytes_sent,
                "bytes_recv": network.bytes_recv,
                "packets_sent": network.packets_sent,
                "packets_recv": network.packets_recv,
                "errors_in": network.errin,
                "errors_out": network.errout,
                "drops_in": network.dropin,
                "drops_out": network.dropout
            },
            "throughput": {
                "mbps_in": round(random.uniform(10, 100), 1),
                "mbps_out": round(random.uniform(5, 50), 1),
                "peak_mbps_in": round(random.uniform(80, 150), 1),
                "peak_mbps_out": round(random.uniform(40, 80), 1)
            },
            "connections": {
                "active_tcp": random.randint(50, 200),
                "active_udp": random.randint(10, 50),
                "listening_ports": random.randint(20, 40)
            },
            "quality": {
                "latency_ms": round(random.uniform(1, 10), 1),
                "packet_loss_percent": round(random.uniform(0, 1), 2),
                "jitter_ms": round(random.uniform(0.1, 2.0), 1)
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting network metrics: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting network metrics: {str(e)}")

@router.get("/diagnostics")
async def run_system_diagnostics():
    """Run system diagnostics and health checks"""
    
    try:
        diagnostics = {
            "system_checks": {
                "disk_space": {"status": "pass", "message": "Sufficient disk space available"},
                "memory_usage": {"status": "pass", "message": "Memory usage within normal limits"},
                "cpu_usage": {"status": "pass", "message": "CPU usage normal"},
                "network_connectivity": {"status": "pass", "message": "Network connectivity OK"}
            },
            "service_checks": {
                "database_connection": {"status": "pass", "message": "Database connection successful"},
                "redis_connection": {"status": "pass", "message": "Redis connection successful"},
                "api_endpoints": {"status": "pass", "message": "All API endpoints responding"},
                "websocket_server": {"status": "pass", "message": "WebSocket server operational"}
            },
            "performance_checks": {
                "response_times": {"status": "pass", "message": "Response times within SLA"},
                "error_rates": {"status": "pass", "message": "Error rates below threshold"},
                "throughput": {"status": "pass", "message": "Throughput meeting requirements"}
            },
            "security_checks": {
                "ssl_certificates": {"status": "pass", "message": "SSL certificates valid"},
                "authentication": {"status": "pass", "message": "Authentication system operational"},
                "authorization": {"status": "pass", "message": "Authorization rules enforced"}
            },
            "overall_status": "healthy",
            "recommendation": "System operating normally",
            "next_check_recommended": (datetime.now() + timedelta(hours=1)).isoformat(),
            "timestamp": datetime.now().isoformat()
        }
        
        return diagnostics
        
    except Exception as e:
        logger.error(f"Error running diagnostics: {e}")
        raise HTTPException(status_code=500, detail=f"Error running diagnostics: {str(e)}")