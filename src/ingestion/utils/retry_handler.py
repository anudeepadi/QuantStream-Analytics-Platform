"""
Retry handler utilities for robust error handling and recovery.

This module provides comprehensive retry mechanisms with various backoff
strategies, circuit breaker patterns, and failure classification.
"""

import asyncio
import random
import time
from abc import ABC, abstractmethod
from typing import Callable, Any, Type, Tuple, Optional, Dict, List, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import logging
import functools
import inspect
from contextlib import asynccontextmanager


class BackoffStrategy(Enum):
    """Backoff strategy types."""
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    EXPONENTIAL_JITTER = "exponential_jitter"
    FIBONACCI = "fibonacci"
    CUSTOM = "custom"


class RetryCondition(Enum):
    """Retry condition types."""
    ALWAYS = "always"
    ON_EXCEPTION = "on_exception"
    ON_RESULT = "on_result"
    CUSTOM = "custom"


@dataclass
class RetryConfig:
    """Retry configuration."""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    jitter_max: float = 0.1
    exponential_base: float = 2.0
    retry_on_exceptions: Tuple[Type[Exception], ...] = (Exception,)
    stop_on_exceptions: Tuple[Type[Exception], ...] = ()
    retry_condition: Optional[Callable[[Any], bool]] = None
    on_retry: Optional[Callable[[int, Exception, float], None]] = None
    on_failure: Optional[Callable[[int, Exception], None]] = None
    on_success: Optional[Callable[[int, Any], None]] = None


@dataclass
class RetryState:
    """State tracking for retry attempts."""
    attempt: int = 0
    total_delay: float = 0.0
    start_time: datetime = field(default_factory=datetime.utcnow)
    last_exception: Optional[Exception] = None
    success: bool = False


class RetryError(Exception):
    """Exception raised when max retry attempts are exceeded."""
    
    def __init__(self, message: str, attempts: int, last_exception: Exception = None):
        super().__init__(message)
        self.attempts = attempts
        self.last_exception = last_exception


class BackoffCalculator(ABC):
    """Abstract base class for backoff calculators."""
    
    @abstractmethod
    def calculate_delay(self, attempt: int, base_delay: float, max_delay: float) -> float:
        """Calculate delay for given attempt."""
        pass


class FixedBackoff(BackoffCalculator):
    """Fixed delay backoff strategy."""
    
    def calculate_delay(self, attempt: int, base_delay: float, max_delay: float) -> float:
        return min(base_delay, max_delay)


class LinearBackoff(BackoffCalculator):
    """Linear backoff strategy."""
    
    def calculate_delay(self, attempt: int, base_delay: float, max_delay: float) -> float:
        return min(attempt * base_delay, max_delay)


class ExponentialBackoff(BackoffCalculator):
    """Exponential backoff strategy."""
    
    def __init__(self, base: float = 2.0):
        self.base = base
    
    def calculate_delay(self, attempt: int, base_delay: float, max_delay: float) -> float:
        delay = base_delay * (self.base ** (attempt - 1))
        return min(delay, max_delay)


class ExponentialJitterBackoff(BackoffCalculator):
    """Exponential backoff with jitter to prevent thundering herd."""
    
    def __init__(self, base: float = 2.0, jitter_max: float = 0.1):
        self.base = base
        self.jitter_max = jitter_max
    
    def calculate_delay(self, attempt: int, base_delay: float, max_delay: float) -> float:
        exponential_delay = base_delay * (self.base ** (attempt - 1))
        jitter = random.uniform(-self.jitter_max, self.jitter_max) * exponential_delay
        delay = exponential_delay + jitter
        return min(max(delay, 0), max_delay)


class FibonacciBackoff(BackoffCalculator):
    """Fibonacci sequence backoff strategy."""
    
    def __init__(self):
        self.fib_cache = {0: 0, 1: 1}
    
    def calculate_delay(self, attempt: int, base_delay: float, max_delay: float) -> float:
        fib_number = self._fibonacci(attempt)
        delay = base_delay * fib_number
        return min(delay, max_delay)
    
    def _fibonacci(self, n: int) -> int:
        """Calculate fibonacci number with caching."""
        if n in self.fib_cache:
            return self.fib_cache[n]
        
        self.fib_cache[n] = self._fibonacci(n - 1) + self._fibonacci(n - 2)
        return self.fib_cache[n]


class BackoffFactory:
    """Factory for creating backoff calculators."""
    
    _strategies = {
        BackoffStrategy.FIXED: FixedBackoff,
        BackoffStrategy.LINEAR: LinearBackoff,
        BackoffStrategy.EXPONENTIAL: ExponentialBackoff,
        BackoffStrategy.EXPONENTIAL_JITTER: ExponentialJitterBackoff,
        BackoffStrategy.FIBONACCI: FibonacciBackoff
    }
    
    @classmethod
    def create(cls, strategy: BackoffStrategy, **kwargs) -> BackoffCalculator:
        """Create backoff calculator for given strategy."""
        if strategy not in cls._strategies:
            raise ValueError(f"Unknown backoff strategy: {strategy}")
        
        calculator_class = cls._strategies[strategy]
        
        # Filter kwargs based on calculator constructor
        sig = inspect.signature(calculator_class.__init__)
        filtered_kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}
        
        return calculator_class(**filtered_kwargs)


class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    expected_exception: Type[Exception] = Exception
    success_threshold: int = 1  # For half-open -> closed transition


class CircuitBreaker:
    """Circuit breaker pattern implementation."""
    
    def __init__(self, config: CircuitBreakerConfig, name: str = "default"):
        self.config = config
        self.name = name
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.logger = logging.getLogger(f"{self.__class__.__name__}.{name}")
        self._lock = asyncio.Lock()
    
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.logger.info(f"Circuit breaker {self.name} transitioning to HALF_OPEN")
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
            
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            await self._on_success()
            return result
            
        except Exception as e:
            await self._on_failure(e)
            raise
    
    async def _on_success(self):
        """Handle successful call."""
        async with self._lock:
            self.failure_count = 0
            
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self.state = CircuitBreakerState.CLOSED
                    self.success_count = 0
                    self.logger.info(f"Circuit breaker {self.name} transitioning to CLOSED")
    
    async def _on_failure(self, exception: Exception):
        """Handle failed call."""
        if isinstance(exception, self.config.expected_exception):
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = datetime.utcnow()
                
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.OPEN
                    self.logger.warning(f"Circuit breaker {self.name} transitioning to OPEN from HALF_OPEN")
                elif self.failure_count >= self.config.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    self.logger.warning(f"Circuit breaker {self.name} transitioning to OPEN")
    
    def _should_attempt_reset(self) -> bool:
        """Check if circuit breaker should attempt reset."""
        if not self.last_failure_time:
            return True
        
        time_since_failure = datetime.utcnow() - self.last_failure_time
        return time_since_failure.total_seconds() >= self.config.recovery_timeout
    
    @property
    def is_open(self) -> bool:
        """Check if circuit breaker is open."""
        return self.state == CircuitBreakerState.OPEN
    
    def reset(self):
        """Manually reset circuit breaker."""
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.logger.info(f"Circuit breaker {self.name} manually reset")


class CircuitBreakerOpenError(Exception):
    """Exception raised when circuit breaker is open."""
    pass


class RetryHandler:
    """Main retry handler with multiple strategies."""
    
    def __init__(self, config: RetryConfig, circuit_breaker: Optional[CircuitBreaker] = None):
        self.config = config
        self.circuit_breaker = circuit_breaker
        self.backoff_calculator = self._create_backoff_calculator()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _create_backoff_calculator(self) -> BackoffCalculator:
        """Create backoff calculator based on configuration."""
        return BackoffFactory.create(
            self.config.backoff_strategy,
            base=self.config.exponential_base,
            jitter_max=self.config.jitter_max
        )
    
    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with retry logic."""
        state = RetryState()
        
        for attempt in range(1, self.config.max_attempts + 1):
            state.attempt = attempt
            
            try:
                # Use circuit breaker if configured
                if self.circuit_breaker:
                    result = await self.circuit_breaker.call(func, *args, **kwargs)
                else:
                    if asyncio.iscoroutinefunction(func):
                        result = await func(*args, **kwargs)
                    else:
                        result = func(*args, **kwargs)
                
                # Check if result should trigger retry
                if self._should_retry_on_result(result):
                    if attempt == self.config.max_attempts:
                        break
                    await self._handle_retry(state, None, attempt)
                    continue
                
                # Success
                state.success = True
                if self.config.on_success:
                    self.config.on_success(attempt, result)
                
                return result
                
            except Exception as e:
                state.last_exception = e
                
                # Check if exception should stop retries
                if self._should_stop_on_exception(e):
                    self.logger.error(f"Stopping retries due to exception: {e}")
                    break
                
                # Check if exception should trigger retry
                if not self._should_retry_on_exception(e):
                    self.logger.error(f"Not retrying due to exception: {e}")
                    break
                
                # Last attempt reached
                if attempt == self.config.max_attempts:
                    break
                
                await self._handle_retry(state, e, attempt)
        
        # All attempts failed
        if self.config.on_failure:
            self.config.on_failure(state.attempt, state.last_exception)
        
        error_msg = f"Max retry attempts ({self.config.max_attempts}) exceeded"
        if state.last_exception:
            error_msg += f". Last exception: {state.last_exception}"
        
        raise RetryError(error_msg, state.attempt, state.last_exception)
    
    async def _handle_retry(self, state: RetryState, exception: Optional[Exception], attempt: int):
        """Handle retry logic including delay calculation."""
        delay = self.backoff_calculator.calculate_delay(
            attempt, self.config.base_delay, self.config.max_delay
        )
        state.total_delay += delay
        
        if self.config.on_retry:
            self.config.on_retry(attempt, exception, delay)
        
        self.logger.debug(f"Retrying in {delay:.2f}s (attempt {attempt}/{self.config.max_attempts})")
        await asyncio.sleep(delay)
    
    def _should_retry_on_exception(self, exception: Exception) -> bool:
        """Check if exception should trigger retry."""
        return isinstance(exception, self.config.retry_on_exceptions)
    
    def _should_stop_on_exception(self, exception: Exception) -> bool:
        """Check if exception should stop retries."""
        return isinstance(exception, self.config.stop_on_exceptions)
    
    def _should_retry_on_result(self, result: Any) -> bool:
        """Check if result should trigger retry."""
        if self.config.retry_condition:
            return self.config.retry_condition(result)
        return False


class AsyncRetryDecorator:
    """Async retry decorator."""
    
    def __init__(self, config: RetryConfig, circuit_breaker: Optional[CircuitBreaker] = None):
        self.handler = RetryHandler(config, circuit_breaker)
    
    def __call__(self, func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            return await self.handler.execute(func, *args, **kwargs)
        return wrapper


def retry(max_attempts: int = 3,
          base_delay: float = 1.0,
          max_delay: float = 60.0,
          backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL,
          retry_on_exceptions: Tuple[Type[Exception], ...] = (Exception,),
          stop_on_exceptions: Tuple[Type[Exception], ...] = (),
          circuit_breaker: Optional[CircuitBreaker] = None) -> Callable:
    """Decorator for adding retry functionality to async functions."""
    config = RetryConfig(
        max_attempts=max_attempts,
        base_delay=base_delay,
        max_delay=max_delay,
        backoff_strategy=backoff_strategy,
        retry_on_exceptions=retry_on_exceptions,
        stop_on_exceptions=stop_on_exceptions
    )
    return AsyncRetryDecorator(config, circuit_breaker)


@asynccontextmanager
async def with_retries(config: RetryConfig, circuit_breaker: Optional[CircuitBreaker] = None):
    """Context manager for retry functionality."""
    handler = RetryHandler(config, circuit_breaker)
    yield handler


# Predefined configurations for common scenarios
HTTP_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=1.0,
    max_delay=30.0,
    backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER,
    retry_on_exceptions=(ConnectionError, TimeoutError)
)

DATABASE_RETRY_CONFIG = RetryConfig(
    max_attempts=5,
    base_delay=0.5,
    max_delay=60.0,
    backoff_strategy=BackoffStrategy.EXPONENTIAL,
    retry_on_exceptions=(ConnectionError,)
)

API_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=2.0,
    max_delay=120.0,
    backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER,
    jitter_max=0.2
)