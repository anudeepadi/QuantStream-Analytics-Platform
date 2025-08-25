"""
Rate limiting utilities for API connectors.

This module provides various rate limiting strategies to ensure compliance
with API rate limits and prevent overwhelming data sources.
"""

import asyncio
import time
from abc import ABC, abstractmethod
from typing import Dict, Optional, Any, List
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import logging
import threading
import redis
from collections import deque, defaultdict


class RateLimitStrategy(Enum):
    """Rate limiting strategies."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"
    ADAPTIVE = "adaptive"


@dataclass
class RateLimitConfig:
    """Rate limit configuration."""
    requests_per_second: float = 1.0
    requests_per_minute: int = 60
    requests_per_hour: int = 3600
    burst_size: int = 10
    strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET
    backoff_multiplier: float = 1.5
    max_backoff_seconds: float = 300.0
    redis_key_prefix: str = "rate_limit"
    distributed: bool = False


class RateLimitExceeded(Exception):
    """Exception raised when rate limit is exceeded."""
    
    def __init__(self, message: str, retry_after: float = None):
        super().__init__(message)
        self.retry_after = retry_after


class BaseRateLimiter(ABC):
    """Abstract base class for rate limiters."""
    
    def __init__(self, config: RateLimitConfig, identifier: str = "default"):
        self.config = config
        self.identifier = identifier
        self.logger = logging.getLogger(f"{self.__class__.__name__}.{identifier}")
    
    @abstractmethod
    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens from the rate limiter."""
        pass
    
    @abstractmethod
    async def wait_for_availability(self, tokens: int = 1) -> float:
        """Wait until tokens are available and return wait time."""
        pass
    
    @abstractmethod
    def get_available_tokens(self) -> int:
        """Get number of currently available tokens."""
        pass
    
    @abstractmethod
    def reset(self) -> None:
        """Reset the rate limiter state."""
        pass


class TokenBucketRateLimiter(BaseRateLimiter):
    """Token bucket rate limiter implementation."""
    
    def __init__(self, config: RateLimitConfig, identifier: str = "default"):
        super().__init__(config, identifier)
        self.bucket_size = config.burst_size
        self.refill_rate = config.requests_per_second
        self.tokens = float(self.bucket_size)
        self.last_refill = time.time()
        self._lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens from the bucket."""
        async with self._lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False
    
    async def wait_for_availability(self, tokens: int = 1) -> float:
        """Wait until tokens are available."""
        start_time = time.time()
        
        while True:
            if await self.acquire(tokens):
                return time.time() - start_time
            
            # Calculate wait time
            async with self._lock:
                self._refill()
                if self.tokens >= tokens:
                    continue
                
                wait_time = max(0.1, (tokens - self.tokens) / self.refill_rate)
                
            await asyncio.sleep(wait_time)
    
    def get_available_tokens(self) -> int:
        """Get number of available tokens."""
        self._refill()
        return int(self.tokens)
    
    def reset(self) -> None:
        """Reset the bucket to full capacity."""
        self.tokens = float(self.bucket_size)
        self.last_refill = time.time()
    
    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(self.bucket_size, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now


class SlidingWindowRateLimiter(BaseRateLimiter):
    """Sliding window rate limiter implementation."""
    
    def __init__(self, config: RateLimitConfig, identifier: str = "default"):
        super().__init__(config, identifier)
        self.window_size = 60.0  # 1 minute window
        self.max_requests = config.requests_per_minute
        self.request_times: deque = deque()
        self._lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens using sliding window."""
        async with self._lock:
            now = time.time()
            self._cleanup_old_requests(now)
            
            if len(self.request_times) + tokens <= self.max_requests:
                for _ in range(tokens):
                    self.request_times.append(now)
                return True
            return False
    
    async def wait_for_availability(self, tokens: int = 1) -> float:
        """Wait until tokens are available."""
        start_time = time.time()
        
        while True:
            if await self.acquire(tokens):
                return time.time() - start_time
            
            # Calculate wait time until oldest request expires
            async with self._lock:
                if self.request_times:
                    oldest_request = self.request_times[0]
                    wait_time = max(0.1, oldest_request + self.window_size - time.time())
                else:
                    wait_time = 0.1
            
            await asyncio.sleep(wait_time)
    
    def get_available_tokens(self) -> int:
        """Get number of available tokens."""
        self._cleanup_old_requests(time.time())
        return max(0, self.max_requests - len(self.request_times))
    
    def reset(self) -> None:
        """Reset the sliding window."""
        self.request_times.clear()
    
    def _cleanup_old_requests(self, now: float) -> None:
        """Remove requests older than the window."""
        cutoff_time = now - self.window_size
        while self.request_times and self.request_times[0] < cutoff_time:
            self.request_times.popleft()


class FixedWindowRateLimiter(BaseRateLimiter):
    """Fixed window rate limiter implementation."""
    
    def __init__(self, config: RateLimitConfig, identifier: str = "default"):
        super().__init__(config, identifier)
        self.window_size = 60.0  # 1 minute window
        self.max_requests = config.requests_per_minute
        self.current_window = 0
        self.request_count = 0
        self._lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens using fixed window."""
        async with self._lock:
            current_window = int(time.time() // self.window_size)
            
            if current_window != self.current_window:
                self.current_window = current_window
                self.request_count = 0
            
            if self.request_count + tokens <= self.max_requests:
                self.request_count += tokens
                return True
            return False
    
    async def wait_for_availability(self, tokens: int = 1) -> float:
        """Wait until tokens are available."""
        start_time = time.time()
        
        while True:
            if await self.acquire(tokens):
                return time.time() - start_time
            
            # Wait until next window
            current_time = time.time()
            next_window = (int(current_time // self.window_size) + 1) * self.window_size
            wait_time = max(0.1, next_window - current_time)
            
            await asyncio.sleep(wait_time)
    
    def get_available_tokens(self) -> int:
        """Get number of available tokens."""
        current_window = int(time.time() // self.window_size)
        if current_window != self.current_window:
            return self.max_requests
        return max(0, self.max_requests - self.request_count)
    
    def reset(self) -> None:
        """Reset the fixed window."""
        self.request_count = 0
        self.current_window = int(time.time() // self.window_size)


class AdaptiveRateLimiter(BaseRateLimiter):
    """Adaptive rate limiter that adjusts based on success/failure rates."""
    
    def __init__(self, config: RateLimitConfig, identifier: str = "default"):
        super().__init__(config, identifier)
        self.base_rate = config.requests_per_second
        self.current_rate = self.base_rate
        self.success_count = 0
        self.failure_count = 0
        self.last_adjustment = time.time()
        self.adjustment_interval = 60.0  # Adjust every minute
        self.underlying_limiter = TokenBucketRateLimiter(config, identifier)
        self._lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens with adaptive rate adjustment."""
        await self._maybe_adjust_rate()
        return await self.underlying_limiter.acquire(tokens)
    
    async def wait_for_availability(self, tokens: int = 1) -> float:
        """Wait for tokens with adaptive rate adjustment."""
        await self._maybe_adjust_rate()
        return await self.underlying_limiter.wait_for_availability(tokens)
    
    def get_available_tokens(self) -> int:
        """Get available tokens from underlying limiter."""
        return self.underlying_limiter.get_available_tokens()
    
    def reset(self) -> None:
        """Reset the adaptive limiter."""
        self.current_rate = self.base_rate
        self.success_count = 0
        self.failure_count = 0
        self.underlying_limiter.reset()
    
    def record_success(self) -> None:
        """Record a successful request."""
        self.success_count += 1
    
    def record_failure(self) -> None:
        """Record a failed request."""
        self.failure_count += 1
    
    async def _maybe_adjust_rate(self) -> None:
        """Adjust rate based on success/failure ratio."""
        async with self._lock:
            now = time.time()
            if now - self.last_adjustment < self.adjustment_interval:
                return
            
            total_requests = self.success_count + self.failure_count
            if total_requests == 0:
                return
            
            success_rate = self.success_count / total_requests
            
            if success_rate > 0.95:  # High success rate, can increase
                self.current_rate = min(self.base_rate * 2, self.current_rate * 1.1)
            elif success_rate < 0.8:  # Low success rate, decrease
                self.current_rate = max(self.base_rate * 0.5, self.current_rate * 0.9)
            
            # Update underlying limiter
            self.underlying_limiter.refill_rate = self.current_rate
            
            # Reset counters
            self.success_count = 0
            self.failure_count = 0
            self.last_adjustment = now
            
            self.logger.debug(f"Adjusted rate to {self.current_rate:.2f} req/s (success rate: {success_rate:.2%})")


class DistributedRateLimiter(BaseRateLimiter):
    """Distributed rate limiter using Redis."""
    
    def __init__(self, config: RateLimitConfig, identifier: str = "default", redis_client: Optional[redis.Redis] = None):
        super().__init__(config, identifier)
        self.redis_client = redis_client or redis.Redis(host='localhost', port=6379, db=0)
        self.key_prefix = f"{config.redis_key_prefix}:{identifier}"
        self.window_size = 60  # 1 minute window
        self.max_requests = config.requests_per_minute
    
    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens using distributed rate limiting."""
        current_window = int(time.time() // self.window_size)
        key = f"{self.key_prefix}:{current_window}"
        
        try:
            pipe = self.redis_client.pipeline()
            pipe.incr(key, tokens)
            pipe.expire(key, self.window_size * 2)  # Set expiry
            results = pipe.execute()
            
            current_count = results[0]
            return current_count <= self.max_requests
            
        except Exception as e:
            self.logger.error(f"Redis error in rate limiter: {e}")
            # Fall back to allowing the request if Redis is down
            return True
    
    async def wait_for_availability(self, tokens: int = 1) -> float:
        """Wait for tokens to be available."""
        start_time = time.time()
        
        while True:
            if await self.acquire(tokens):
                return time.time() - start_time
            
            # Wait until next window
            current_time = time.time()
            next_window = (int(current_time // self.window_size) + 1) * self.window_size
            wait_time = max(0.1, next_window - current_time)
            
            await asyncio.sleep(wait_time)
    
    def get_available_tokens(self) -> int:
        """Get available tokens in current window."""
        current_window = int(time.time() // self.window_size)
        key = f"{self.key_prefix}:{current_window}"
        
        try:
            current_count = self.redis_client.get(key)
            if current_count is None:
                return self.max_requests
            return max(0, self.max_requests - int(current_count))
        except Exception as e:
            self.logger.error(f"Redis error getting available tokens: {e}")
            return self.max_requests
    
    def reset(self) -> None:
        """Reset the distributed rate limiter."""
        try:
            pattern = f"{self.key_prefix}:*"
            keys = self.redis_client.keys(pattern)
            if keys:
                self.redis_client.delete(*keys)
        except Exception as e:
            self.logger.error(f"Redis error resetting rate limiter: {e}")


class RateLimiterFactory:
    """Factory for creating rate limiters."""
    
    _limiters: Dict[str, BaseRateLimiter] = {}
    
    @classmethod
    def create_rate_limiter(cls, config: RateLimitConfig, identifier: str = "default", 
                          redis_client: Optional[redis.Redis] = None) -> BaseRateLimiter:
        """Create a rate limiter based on configuration."""
        key = f"{config.strategy.value}:{identifier}"
        
        if key in cls._limiters:
            return cls._limiters[key]
        
        if config.distributed and redis_client:
            limiter = DistributedRateLimiter(config, identifier, redis_client)
        elif config.strategy == RateLimitStrategy.TOKEN_BUCKET:
            limiter = TokenBucketRateLimiter(config, identifier)
        elif config.strategy == RateLimitStrategy.SLIDING_WINDOW:
            limiter = SlidingWindowRateLimiter(config, identifier)
        elif config.strategy == RateLimitStrategy.FIXED_WINDOW:
            limiter = FixedWindowRateLimiter(config, identifier)
        elif config.strategy == RateLimitStrategy.ADAPTIVE:
            limiter = AdaptiveRateLimiter(config, identifier)
        else:
            raise ValueError(f"Unknown rate limit strategy: {config.strategy}")
        
        cls._limiters[key] = limiter
        return limiter
    
    @classmethod
    def get_rate_limiter(cls, identifier: str, strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET) -> Optional[BaseRateLimiter]:
        """Get existing rate limiter."""
        key = f"{strategy.value}:{identifier}"
        return cls._limiters.get(key)
    
    @classmethod
    def remove_rate_limiter(cls, identifier: str, strategy: RateLimitStrategy) -> None:
        """Remove rate limiter from cache."""
        key = f"{strategy.value}:{identifier}"
        cls._limiters.pop(key, None)


class RateLimitedClient:
    """Wrapper client with built-in rate limiting."""
    
    def __init__(self, rate_limiter: BaseRateLimiter):
        self.rate_limiter = rate_limiter
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def execute_with_rate_limit(self, func, *args, **kwargs) -> Any:
        """Execute function with rate limiting."""
        await self.rate_limiter.wait_for_availability()
        
        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            
            # Record success for adaptive rate limiters
            if isinstance(self.rate_limiter, AdaptiveRateLimiter):
                self.rate_limiter.record_success()
            
            return result
            
        except Exception as e:
            # Record failure for adaptive rate limiters
            if isinstance(self.rate_limiter, AdaptiveRateLimiter):
                self.rate_limiter.record_failure()
            raise
    
    def get_rate_limit_status(self) -> Dict[str, Any]:
        """Get current rate limit status."""
        return {
            "available_tokens": self.rate_limiter.get_available_tokens(),
            "strategy": self.rate_limiter.config.strategy.value,
            "identifier": self.rate_limiter.identifier
        }


# Utility functions
async def with_rate_limit(rate_limiter: BaseRateLimiter, func, *args, **kwargs):
    """Decorator-like function for rate limiting."""
    client = RateLimitedClient(rate_limiter)
    return await client.execute_with_rate_limit(func, *args, **kwargs)