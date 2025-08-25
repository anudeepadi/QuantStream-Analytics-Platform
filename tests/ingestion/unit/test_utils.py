"""
Unit tests for utility modules.
"""

import pytest
import asyncio
import time
import threading
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from decimal import Decimal
import tempfile
import os
import json

from src.ingestion.utils import (
    # Rate limiter
    RateLimitConfig, RateLimitStrategy, TokenBucketRateLimiter,
    SlidingWindowRateLimiter, FixedWindowRateLimiter, AdaptiveRateLimiter,
    RateLimiterFactory, RateLimitedClient,
    
    # Retry handler
    RetryConfig, BackoffStrategy, RetryHandler, CircuitBreaker, CircuitBreakerConfig,
    BackoffFactory, FixedBackoff, ExponentialBackoff, ExponentialJitterBackoff,
    
    # Metrics
    MetricRegistry, CounterMetric, GaugeMetric, HistogramMetric, TimerMetric,
    get_registry,
    
    # Configuration
    ConfigLoader, ConfigManager, QuantStreamConfig, ConnectorConfig,
    APIEndpointConfig, Environment,
    
    # Logging
    LogConfig, LogLevel, setup_logging, get_logger
)


class TestRateLimiting:
    """Test rate limiting functionality."""
    
    def test_rate_limit_config(self):
        """Test rate limit configuration."""
        config = RateLimitConfig(
            requests_per_second=10.0,
            requests_per_minute=600,
            burst_size=20,
            strategy=RateLimitStrategy.TOKEN_BUCKET
        )
        
        assert config.requests_per_second == 10.0
        assert config.requests_per_minute == 600
        assert config.burst_size == 20
        assert config.strategy == RateLimitStrategy.TOKEN_BUCKET
    
    @pytest.mark.asyncio
    async def test_token_bucket_rate_limiter(self):
        """Test token bucket rate limiter."""
        config = RateLimitConfig(
            requests_per_second=5.0,
            burst_size=10
        )
        limiter = TokenBucketRateLimiter(config, "test")
        
        # Should be able to acquire initial burst
        for _ in range(10):
            acquired = await limiter.acquire()
            assert acquired is True
        
        # Should fail to acquire more immediately
        acquired = await limiter.acquire()
        assert acquired is False
        
        # Should have some tokens available
        available = limiter.get_available_tokens()
        assert available >= 0
    
    @pytest.mark.asyncio
    async def test_token_bucket_refill(self):
        """Test token bucket refill over time."""
        config = RateLimitConfig(
            requests_per_second=10.0,  # High rate for fast test
            burst_size=5
        )
        limiter = TokenBucketRateLimiter(config, "test")
        
        # Exhaust tokens
        for _ in range(5):
            await limiter.acquire()
        
        # Wait for refill
        await asyncio.sleep(0.2)  # 200ms should add ~2 tokens at 10/sec
        
        # Should be able to acquire some tokens
        acquired = await limiter.acquire()
        assert acquired is True
    
    @pytest.mark.asyncio
    async def test_sliding_window_rate_limiter(self):
        """Test sliding window rate limiter."""
        config = RateLimitConfig(requests_per_minute=60)  # 1 per second
        limiter = SlidingWindowRateLimiter(config, "test")
        
        # Should be able to acquire initial requests
        for _ in range(60):
            acquired = await limiter.acquire()
            assert acquired is True
        
        # Should fail to acquire more
        acquired = await limiter.acquire()
        assert acquired is False
        
        available = limiter.get_available_tokens()
        assert available == 0
    
    @pytest.mark.asyncio
    async def test_fixed_window_rate_limiter(self):
        """Test fixed window rate limiter."""
        config = RateLimitConfig(requests_per_minute=60)
        limiter = FixedWindowRateLimiter(config, "test")
        
        # Should be able to acquire requests within window
        for _ in range(60):
            acquired = await limiter.acquire()
            assert acquired is True
        
        # Should fail to acquire more in same window
        acquired = await limiter.acquire()
        assert acquired is False
        
        available = limiter.get_available_tokens()
        assert available == 0
    
    @pytest.mark.asyncio
    async def test_adaptive_rate_limiter(self):
        """Test adaptive rate limiter."""
        config = RateLimitConfig(
            requests_per_second=5.0,
            burst_size=10
        )
        limiter = AdaptiveRateLimiter(config, "test")
        
        # Should behave like token bucket initially
        acquired = await limiter.acquire()
        assert acquired is True
        
        # Test success/failure recording
        limiter.record_success()
        limiter.record_failure()
        
        # Should still work
        acquired = await limiter.acquire()
        assert acquired is True
    
    def test_rate_limiter_factory(self):
        """Test rate limiter factory."""
        config = RateLimitConfig(strategy=RateLimitStrategy.TOKEN_BUCKET)
        limiter = RateLimiterFactory.create_rate_limiter(config, "test")
        assert isinstance(limiter, TokenBucketRateLimiter)
        
        config = RateLimitConfig(strategy=RateLimitStrategy.SLIDING_WINDOW)
        limiter = RateLimiterFactory.create_rate_limiter(config, "test")
        assert isinstance(limiter, SlidingWindowRateLimiter)
        
        config = RateLimitConfig(strategy=RateLimitStrategy.FIXED_WINDOW)
        limiter = RateLimiterFactory.create_rate_limiter(config, "test")
        assert isinstance(limiter, FixedWindowRateLimiter)
        
        config = RateLimitConfig(strategy=RateLimitStrategy.ADAPTIVE)
        limiter = RateLimiterFactory.create_rate_limiter(config, "test")
        assert isinstance(limiter, AdaptiveRateLimiter)
    
    @pytest.mark.asyncio
    async def test_rate_limited_client(self):
        """Test rate limited client."""
        config = RateLimitConfig(
            requests_per_second=10.0,
            burst_size=5
        )
        limiter = TokenBucketRateLimiter(config, "test")
        client = RateLimitedClient(limiter)
        
        # Mock function to call
        mock_func = Mock(return_value="success")
        
        result = await client.execute_with_rate_limit(mock_func, "arg1", keyword="arg2")
        
        assert result == "success"
        mock_func.assert_called_once_with("arg1", keyword="arg2")
        
        # Test status
        status = client.get_rate_limit_status()
        assert "available_tokens" in status
        assert "strategy" in status


class TestRetryHandling:
    """Test retry handling functionality."""
    
    def test_retry_config(self):
        """Test retry configuration."""
        config = RetryConfig(
            max_attempts=5,
            base_delay=2.0,
            max_delay=60.0,
            backoff_strategy=BackoffStrategy.EXPONENTIAL
        )
        
        assert config.max_attempts == 5
        assert config.base_delay == 2.0
        assert config.max_delay == 60.0
        assert config.backoff_strategy == BackoffStrategy.EXPONENTIAL
    
    def test_backoff_strategies(self):
        """Test different backoff strategies."""
        # Fixed backoff
        fixed = FixedBackoff()
        assert fixed.calculate_delay(1, 1.0, 10.0) == 1.0
        assert fixed.calculate_delay(5, 1.0, 10.0) == 1.0
        
        # Linear backoff
        linear = BackoffFactory.create(BackoffStrategy.LINEAR)
        assert linear.calculate_delay(1, 1.0, 10.0) == 1.0
        assert linear.calculate_delay(2, 1.0, 10.0) == 2.0
        assert linear.calculate_delay(3, 1.0, 10.0) == 3.0
        
        # Exponential backoff
        exponential = ExponentialBackoff()
        assert exponential.calculate_delay(1, 1.0, 10.0) == 1.0
        assert exponential.calculate_delay(2, 1.0, 10.0) == 2.0
        assert exponential.calculate_delay(3, 1.0, 10.0) == 4.0
        
        # With max delay constraint
        assert exponential.calculate_delay(10, 1.0, 5.0) == 5.0
    
    def test_exponential_jitter_backoff(self):
        """Test exponential backoff with jitter."""
        jitter_backoff = ExponentialJitterBackoff(base=2.0, jitter_max=0.1)
        
        # Calculate multiple times to test jitter
        delays = []
        for _ in range(10):
            delay = jitter_backoff.calculate_delay(3, 1.0, 10.0)
            delays.append(delay)
        
        # All delays should be around 4.0 but with variation
        base_delay = 4.0
        assert all(base_delay * 0.8 <= d <= base_delay * 1.2 for d in delays)
        
        # Should have some variation (not all exactly the same)
        assert len(set(delays)) > 1
    
    @pytest.mark.asyncio
    async def test_retry_handler_success(self):
        """Test retry handler with successful function."""
        config = RetryConfig(max_attempts=3, base_delay=0.01)
        handler = RetryHandler(config)
        
        mock_func = Mock(return_value="success")
        
        result = await handler.execute(mock_func)
        
        assert result == "success"
        assert mock_func.call_count == 1
    
    @pytest.mark.asyncio
    async def test_retry_handler_eventual_success(self):
        """Test retry handler with eventual success."""
        config = RetryConfig(max_attempts=3, base_delay=0.01)
        handler = RetryHandler(config)
        
        # Mock function that fails twice then succeeds
        mock_func = Mock(side_effect=[Exception("fail"), Exception("fail"), "success"])
        
        result = await handler.execute(mock_func)
        
        assert result == "success"
        assert mock_func.call_count == 3
    
    @pytest.mark.asyncio
    async def test_retry_handler_max_attempts(self):
        """Test retry handler exceeding max attempts."""
        config = RetryConfig(max_attempts=2, base_delay=0.01)
        handler = RetryHandler(config)
        
        mock_func = Mock(side_effect=Exception("always fail"))
        
        with pytest.raises(Exception):  # RetryError specifically
            await handler.execute(mock_func)
        
        assert mock_func.call_count == 2
    
    @pytest.mark.asyncio
    async def test_circuit_breaker(self):
        """Test circuit breaker functionality."""
        config = CircuitBreakerConfig(
            failure_threshold=2,
            recovery_timeout=0.1  # 100ms for fast test
        )
        breaker = CircuitBreaker(config, "test")
        
        # Should start in closed state
        assert not breaker.is_open
        
        # Successful call
        mock_func = Mock(return_value="success")
        result = await breaker.call(mock_func)
        assert result == "success"
        assert not breaker.is_open
        
        # Failed calls to trip breaker
        failing_func = Mock(side_effect=Exception("fail"))
        
        for _ in range(2):
            with pytest.raises(Exception):
                await breaker.call(failing_func)
        
        # Should now be open
        assert breaker.is_open
        
        # Calls should be rejected
        with pytest.raises(Exception):  # CircuitBreakerOpenError
            await breaker.call(mock_func)
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_recovery(self):
        """Test circuit breaker recovery."""
        config = CircuitBreakerConfig(
            failure_threshold=1,
            recovery_timeout=0.01,  # 10ms
            success_threshold=1
        )
        breaker = CircuitBreaker(config, "test")
        
        # Trip the breaker
        failing_func = Mock(side_effect=Exception("fail"))
        with pytest.raises(Exception):
            await breaker.call(failing_func)
        
        assert breaker.is_open
        
        # Wait for recovery timeout
        await asyncio.sleep(0.02)
        
        # Should allow one call (half-open state)
        success_func = Mock(return_value="success")
        result = await breaker.call(success_func)
        assert result == "success"
        
        # Should be closed again
        assert not breaker.is_open


class TestMetrics:
    """Test metrics functionality."""
    
    def test_counter_metric(self):
        """Test counter metric."""
        counter = CounterMetric("test_counter", "Test counter")
        
        assert counter.get_value() == 0
        
        counter.record(5)
        assert counter.get_value() == 5
        
        counter.record(3)
        assert counter.get_value() == 8
        
        # Test with labels
        counter.record(2, {"type": "test"})
        assert counter.get_value({"type": "test"}) == 2
        assert counter.get_value() == 8  # Default labels unchanged
    
    def test_gauge_metric(self):
        """Test gauge metric."""
        gauge = GaugeMetric("test_gauge", "Test gauge")
        
        assert gauge.get_value() == 0
        
        gauge.record(10)
        assert gauge.get_value() == 10
        
        gauge.increment(5)
        assert gauge.get_value() == 15
        
        gauge.decrement(3)
        assert gauge.get_value() == 12
        
        gauge.record(-5)
        assert gauge.get_value() == -5
    
    def test_histogram_metric(self):
        """Test histogram metric."""
        histogram = HistogramMetric("test_histogram", "Test histogram")
        
        # Record some values
        values = [0.1, 0.5, 1.0, 2.0, 5.0]
        for value in values:
            histogram.record(value)
        
        stats = histogram.get_value()
        assert stats["count"] == 5
        assert stats["sum"] == sum(values)
        assert stats["average"] == sum(values) / len(values)
        assert isinstance(stats["buckets"], dict)
    
    def test_timer_metric(self):
        """Test timer metric."""
        timer = TimerMetric("test_timer", "Test timer")
        
        # Record some durations
        durations = [0.1, 0.2, 0.3, 0.4, 0.5]
        for duration in durations:
            timer.record(duration)
        
        stats = timer.get_value()
        assert stats["count"] == 5
        assert stats["sum"] == sum(durations)
        assert stats["avg"] == sum(durations) / len(durations)
        assert stats["min"] == min(durations)
        assert stats["max"] == max(durations)
        assert "median" in stats
        assert "p95" in stats
        assert "p99" in stats
    
    def test_timer_context_manager(self):
        """Test timer context manager."""
        timer = TimerMetric("test_timer", "Test timer")
        
        with timer.time():
            time.sleep(0.01)  # Sleep for 10ms
        
        stats = timer.get_value()
        assert stats["count"] == 1
        assert stats["sum"] >= 0.01  # Should be at least 10ms
        assert stats["sum"] < 0.1   # But not too much more
    
    def test_metric_registry(self):
        """Test metric registry."""
        registry = MetricRegistry("test")
        
        # Create metrics
        counter = registry.counter("test_counter")
        gauge = registry.gauge("test_gauge")
        histogram = registry.histogram("test_histogram")
        timer = registry.timer("test_timer")
        
        # Check they exist
        assert registry.get_metric("test_counter") == counter
        assert registry.get_metric("test_gauge") == gauge
        assert registry.get_metric("test_histogram") == histogram
        assert registry.get_metric("test_timer") == timer
        
        # List metrics
        metric_names = registry.list_metrics()
        assert "test_test_counter" in metric_names
        assert "test_test_gauge" in metric_names
        assert "test_test_histogram" in metric_names
        assert "test_test_timer" in metric_names
    
    def test_global_registry(self):
        """Test global registry functions."""
        from src.ingestion.utils.metrics import counter, gauge, histogram, timer
        
        # Create metrics through global functions
        test_counter = counter("global_counter")
        test_gauge = gauge("global_gauge")
        test_histogram = histogram("global_histogram")
        test_timer = timer("global_timer")
        
        # Should be accessible through global registry
        global_registry = get_registry()
        assert global_registry.get_metric("global_counter") == test_counter


class TestConfiguration:
    """Test configuration management."""
    
    def test_quantstream_config_defaults(self):
        """Test QuantStream config with defaults."""
        config = QuantStreamConfig()
        
        assert config.environment == Environment.DEVELOPMENT
        assert config.app_name == "quantstream-ingestion"
        assert config.version == "1.0.0"
        assert config.debug is False
        
        # Check nested configs exist
        assert config.database is not None
        assert config.kafka is not None
        assert config.redis is not None
        assert config.monitoring is not None
    
    def test_connector_config(self):
        """Test connector configuration."""
        config = ConnectorConfig(
            name="test-connector",
            type="rest_api",
            data_source="alpha_vantage",
            symbols=["AAPL", "GOOGL"],
            batch_size=500
        )
        
        assert config.name == "test-connector"
        assert config.type == "rest_api"
        assert config.data_source == "alpha_vantage"
        assert config.symbols == ["AAPL", "GOOGL"]
        assert config.batch_size == 500
    
    def test_api_endpoint_config(self):
        """Test API endpoint configuration."""
        config = APIEndpointConfig(
            name="test-api",
            base_url="https://api.example.com",
            api_key="test-key",
            rate_limit=60
        )
        
        assert config.name == "test-api"
        assert config.base_url == "https://api.example.com"
        assert config.api_key == "test-key"
        assert config.rate_limit == 60
    
    def test_config_loader_with_temp_file(self):
        """Test config loader with temporary file."""
        # Create temporary config file
        config_data = {
            "app_name": "test-app",
            "environment": "production",
            "debug": True,
            "database": {
                "host": "test-host",
                "port": 5432
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            temp_path = f.name
        
        try:
            loader = ConfigLoader(temp_path)
            config = loader.load_config()
            
            assert config.app_name == "test-app"
            assert config.environment == Environment.PRODUCTION
            assert config.debug is True
            assert config.database.host == "test-host"
            assert config.database.port == 5432
            
        finally:
            os.unlink(temp_path)
    
    def test_config_manager_singleton(self):
        """Test config manager singleton behavior."""
        manager1 = ConfigManager()
        manager2 = ConfigManager()
        
        # Should be the same instance
        assert manager1 is manager2


class TestLogging:
    """Test logging functionality."""
    
    def test_log_config(self):
        """Test log configuration."""
        config = LogConfig(
            level=LogLevel.DEBUG,
            console_output=True,
            file_output=False,
            json_format=True
        )
        
        assert config.level == LogLevel.DEBUG
        assert config.console_output is True
        assert config.file_output is False
        assert config.json_format is True
    
    def test_logging_setup(self):
        """Test logging setup."""
        config = LogConfig(
            level=LogLevel.INFO,
            console_output=True,
            file_output=False
        )
        
        manager = setup_logging(config)
        assert manager is not None
        
        # Get logger and test
        logger = get_logger("test_logger")
        assert logger is not None
        
        # Test logging (just ensure no exceptions)
        logger.info("Test message")
        logger.debug("Debug message")
        logger.error("Error message")
    
    def test_logger_with_extra_fields(self):
        """Test logger with extra fields."""
        config = LogConfig(console_output=False, file_output=False)  # Minimal setup
        setup_logging(config)
        
        logger = get_logger("test_logger", {"component": "test"})
        
        # Should not raise exceptions
        logger.info("Test message with extra fields", extra={"request_id": "123"})
        logger.error("Error with correlation", correlation_id="test-correlation")
    
    def test_logger_context_managers(self):
        """Test logger context managers."""
        config = LogConfig(console_output=False, file_output=False)
        setup_logging(config)
        
        logger = get_logger("test_logger")
        
        # Performance context
        with logger.performance_context("test_operation") as operation:
            time.sleep(0.01)
            # Should complete without exceptions
        
        # Correlation context
        with logger.correlation_context() as correlation_id:
            assert isinstance(correlation_id, str)
            assert len(correlation_id) > 0
            logger.info("Message with correlation")


if __name__ == "__main__":
    pytest.main([__file__])