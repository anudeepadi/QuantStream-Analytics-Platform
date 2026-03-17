"""
Redis Service

Handles Redis connections and caching operations for the dashboard backend.
"""

import asyncio
import json
import logging
from typing import Any, Optional, Dict, List, Union
import redis.asyncio as redis
import os
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class RedisService:
    """Redis service for caching and session management"""
    
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.connection_pool: Optional[redis.ConnectionPool] = None
        self._build_connection_config()
    
    def _build_connection_config(self):
        """Build Redis connection configuration"""
        
        self.redis_url = os.getenv("REDIS_URL")
        
        if self.redis_url:
            # Parse Redis URL
            self.config = {"url": self.redis_url}
        else:
            # Build from individual components
            self.config = {
                "host": os.getenv("REDIS_HOST", "localhost"),
                "port": int(os.getenv("REDIS_PORT", "6379")),
                "db": int(os.getenv("REDIS_DB", "0")),
                "password": os.getenv("REDIS_PASSWORD"),
                "decode_responses": True,
                "socket_timeout": 5,
                "socket_connect_timeout": 5,
                "retry_on_timeout": True
            }
    
    async def initialize(self):
        """Initialize Redis connection"""
        try:
            if self.redis_url:
                self.redis_client = redis.from_url(
                    self.redis_url, decode_responses=True,
                    socket_timeout=5, socket_connect_timeout=5,
                )
            else:
                self.connection_pool = redis.ConnectionPool(**self.config)
                self.redis_client = redis.Redis(connection_pool=self.connection_pool)

            # Test connection with timeout
            await asyncio.wait_for(self.redis_client.ping(), timeout=5)

            logger.info("Redis connection initialized")

        except Exception as e:
            logger.error("Failed to initialize Redis: %s", e)
            raise
    
    async def close(self):
        """Close Redis connection"""
        if self.redis_client:
            await self.redis_client.close()
            logger.info("Redis connection closed")
    
    async def health_check(self) -> bool:
        """Check Redis connectivity"""
        try:
            if not self.redis_client:
                return False
            
            pong = await self.redis_client.ping()
            return pong
            
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return False
    
    # Basic cache operations
    async def set(self, key: str, value: Any, expire: Optional[int] = None) -> bool:
        """Set a key-value pair with optional expiration"""
        try:
            # Serialize complex objects to JSON
            if isinstance(value, (dict, list)):
                value = json.dumps(value, default=str)
            
            result = await self.redis_client.set(key, value, ex=expire)
            return result
            
        except Exception as e:
            logger.error(f"Error setting key {key}: {e}")
            return False
    
    async def get(self, key: str, default: Any = None) -> Any:
        """Get value by key"""
        try:
            value = await self.redis_client.get(key)
            
            if value is None:
                return default
            
            # Try to deserialize JSON
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
                
        except Exception as e:
            logger.error(f"Error getting key {key}: {e}")
            return default
    
    async def delete(self, key: str) -> bool:
        """Delete a key"""
        try:
            result = await self.redis_client.delete(key)
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error deleting key {key}: {e}")
            return False
    
    async def exists(self, key: str) -> bool:
        """Check if key exists"""
        try:
            return bool(await self.redis_client.exists(key))
        except Exception as e:
            logger.error(f"Error checking key existence {key}: {e}")
            return False
    
    async def expire(self, key: str, seconds: int) -> bool:
        """Set expiration for a key"""
        try:
            return bool(await self.redis_client.expire(key, seconds))
        except Exception as e:
            logger.error(f"Error setting expiration for key {key}: {e}")
            return False
    
    async def ttl(self, key: str) -> int:
        """Get time to live for a key"""
        try:
            return await self.redis_client.ttl(key)
        except Exception as e:
            logger.error(f"Error getting TTL for key {key}: {e}")
            return -1
    
    # Hash operations
    async def hset(self, name: str, mapping: Dict[str, Any]) -> bool:
        """Set hash field values"""
        try:
            # Serialize complex values
            serialized_mapping = {}
            for k, v in mapping.items():
                if isinstance(v, (dict, list)):
                    serialized_mapping[k] = json.dumps(v, default=str)
                else:
                    serialized_mapping[k] = str(v)
            
            result = await self.redis_client.hset(name, mapping=serialized_mapping)
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error setting hash {name}: {e}")
            return False
    
    async def hget(self, name: str, key: str, default: Any = None) -> Any:
        """Get hash field value"""
        try:
            value = await self.redis_client.hget(name, key)
            
            if value is None:
                return default
            
            # Try to deserialize JSON
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
                
        except Exception as e:
            logger.error(f"Error getting hash field {name}:{key}: {e}")
            return default
    
    async def hgetall(self, name: str) -> Dict[str, Any]:
        """Get all hash field values"""
        try:
            hash_data = await self.redis_client.hgetall(name)
            
            # Try to deserialize JSON values
            result = {}
            for k, v in hash_data.items():
                try:
                    result[k] = json.loads(v)
                except (json.JSONDecodeError, TypeError):
                    result[k] = v
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting hash {name}: {e}")
            return {}
    
    # List operations
    async def lpush(self, name: str, *values) -> int:
        """Push values to the left of list"""
        try:
            # Serialize complex objects
            serialized_values = []
            for value in values:
                if isinstance(value, (dict, list)):
                    serialized_values.append(json.dumps(value, default=str))
                else:
                    serialized_values.append(str(value))
            
            return await self.redis_client.lpush(name, *serialized_values)
            
        except Exception as e:
            logger.error(f"Error pushing to list {name}: {e}")
            return 0
    
    async def lrange(self, name: str, start: int = 0, end: int = -1) -> List[Any]:
        """Get range of values from list"""
        try:
            values = await self.redis_client.lrange(name, start, end)
            
            # Try to deserialize JSON values
            result = []
            for value in values:
                try:
                    result.append(json.loads(value))
                except (json.JSONDecodeError, TypeError):
                    result.append(value)
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting list range {name}: {e}")
            return []
    
    async def ltrim(self, name: str, start: int, end: int) -> bool:
        """Trim list to specified range"""
        try:
            await self.redis_client.ltrim(name, start, end)
            return True
        except Exception as e:
            logger.error(f"Error trimming list {name}: {e}")
            return False
    
    # Set operations
    async def sadd(self, name: str, *values) -> int:
        """Add values to set"""
        try:
            serialized_values = []
            for value in values:
                if isinstance(value, (dict, list)):
                    serialized_values.append(json.dumps(value, default=str))
                else:
                    serialized_values.append(str(value))
            
            return await self.redis_client.sadd(name, *serialized_values)
            
        except Exception as e:
            logger.error(f"Error adding to set {name}: {e}")
            return 0
    
    async def smembers(self, name: str) -> set:
        """Get all set members"""
        try:
            values = await self.redis_client.smembers(name)
            
            result = set()
            for value in values:
                try:
                    result.add(json.loads(value))
                except (json.JSONDecodeError, TypeError):
                    result.add(value)
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting set members {name}: {e}")
            return set()
    
    # Cache-specific operations for dashboard
    async def cache_market_data(self, symbol: str, data: Dict[str, Any], expire: int = 60):
        """Cache market data for a symbol"""
        key = f"market_data:{symbol.upper()}"
        data["cached_at"] = datetime.now().isoformat()
        await self.set(key, data, expire)
    
    async def get_cached_market_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get cached market data for a symbol"""
        key = f"market_data:{symbol.upper()}"
        return await self.get(key)
    
    async def cache_user_session(self, session_token: str, user_data: Dict[str, Any], expire: int = 3600):
        """Cache user session data"""
        key = f"session:{session_token}"
        await self.set(key, user_data, expire)
    
    async def get_user_session(self, session_token: str) -> Optional[Dict[str, Any]]:
        """Get cached user session data"""
        key = f"session:{session_token}"
        return await self.get(key)
    
    async def invalidate_user_session(self, session_token: str):
        """Invalidate user session"""
        key = f"session:{session_token}"
        await self.delete(key)
    
    async def cache_portfolio_data(self, user_id: str, portfolio_data: Dict[str, Any], expire: int = 300):
        """Cache portfolio data for a user"""
        key = f"portfolio:{user_id}"
        await self.set(key, portfolio_data, expire)
    
    async def get_cached_portfolio_data(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get cached portfolio data"""
        key = f"portfolio:{user_id}"
        return await self.get(key)
    
    async def store_alert(self, alert_id: str, alert_data: Dict[str, Any]):
        """Store alert in Redis for fast access"""
        key = f"alert:{alert_id}"
        await self.set(key, alert_data, expire=86400)  # 24 hours
    
    async def get_user_alerts(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all alerts for a user from cache"""
        pattern = f"alert:*"
        
        try:
            # Get all alert keys (in production, would use scan for large datasets)
            keys = await self.redis_client.keys(pattern)
            
            alerts = []
            for key in keys:
                alert_data = await self.get(key)
                if alert_data and alert_data.get("user_id") == user_id:
                    alerts.append(alert_data)
            
            return sorted(alerts, key=lambda x: x.get("created_at", ""), reverse=True)
            
        except Exception as e:
            logger.error(f"Error getting user alerts: {e}")
            return []
    
    async def store_system_metric(self, metric_name: str, value: float, timestamp: str = None):
        """Store system metric with timestamp"""
        if not timestamp:
            timestamp = datetime.now().isoformat()
        
        # Store in a sorted set for time-series data
        key = f"metrics:{metric_name}"
        score = datetime.fromisoformat(timestamp.replace('Z', '+00:00')).timestamp()
        
        await self.redis_client.zadd(key, {json.dumps({"value": value, "timestamp": timestamp}): score})
        
        # Keep only last 24 hours of data
        cutoff = datetime.now().timestamp() - 86400
        await self.redis_client.zremrangebyscore(key, 0, cutoff)
    
    async def get_system_metrics(self, metric_name: str, hours: int = 1) -> List[Dict[str, Any]]:
        """Get system metrics for the last N hours"""
        key = f"metrics:{metric_name}"
        
        # Get data from last N hours
        cutoff = datetime.now().timestamp() - (hours * 3600)
        
        try:
            data = await self.redis_client.zrangebyscore(key, cutoff, "+inf")
            
            metrics = []
            for item in data:
                try:
                    metric_data = json.loads(item)
                    metrics.append(metric_data)
                except (json.JSONDecodeError, TypeError):
                    continue
            
            return sorted(metrics, key=lambda x: x["timestamp"])
            
        except Exception as e:
            logger.error(f"Error getting system metrics: {e}")
            return []
    
    # Bulk operations
    async def mget(self, keys: List[str]) -> List[Any]:
        """Get multiple keys at once"""
        try:
            values = await self.redis_client.mget(keys)
            
            result = []
            for value in values:
                if value is None:
                    result.append(None)
                else:
                    try:
                        result.append(json.loads(value))
                    except (json.JSONDecodeError, TypeError):
                        result.append(value)
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting multiple keys: {e}")
            return [None] * len(keys)
    
    async def mset(self, mapping: Dict[str, Any]) -> bool:
        """Set multiple keys at once"""
        try:
            serialized_mapping = {}
            for k, v in mapping.items():
                if isinstance(v, (dict, list)):
                    serialized_mapping[k] = json.dumps(v, default=str)
                else:
                    serialized_mapping[k] = str(v)
            
            return await self.redis_client.mset(serialized_mapping)
            
        except Exception as e:
            logger.error(f"Error setting multiple keys: {e}")
            return False
    
    async def flushdb(self) -> bool:
        """Flush current database (use with caution!)"""
        try:
            await self.redis_client.flushdb()
            return True
        except Exception as e:
            logger.error(f"Error flushing database: {e}")
            return False