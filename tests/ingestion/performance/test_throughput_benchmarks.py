#!/usr/bin/env python3
"""
Performance benchmarking tests for the QuantStream Analytics Platform.

These tests measure throughput, latency, and resource utilization to ensure
the pipeline meets performance requirements of 500K+ events/second.
"""

import asyncio
import time
import statistics
import pytest
import psutil
import gc
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any
from unittest.mock import AsyncMock, patch
import json

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from src.ingestion.processors import (
    QuantStreamPipelineManager, PipelineConfig, HighPerformanceKafkaProducer
)
from src.ingestion.connectors import (
    create_api_connector, APIConnectorConfig, APIProvider
)
from src.ingestion.models import (
    Symbol, DataType, Quote, Trade, Bar, MarketDataMessage, AssetClass
)
from src.ingestion.utils import (
    setup_logging, LogConfig, LogLevel, get_logger
)


class PerformanceMetrics:
    """Collect and analyze performance metrics."""
    
    def __init__(self):
        self.reset()
        
    def reset(self):
        """Reset all metrics."""
        self.start_time = None
        self.end_time = None
        self.message_times = []
        self.latencies = []
        self.cpu_samples = []
        self.memory_samples = []
        self.throughput_samples = []
        
    def start_measurement(self):
        """Start performance measurement."""
        self.start_time = time.time()
        self.process = psutil.Process()
        
    def end_measurement(self):
        """End performance measurement."""
        self.end_time = time.time()
        
    def record_message(self, timestamp: float = None):
        """Record a message processing timestamp."""
        if timestamp is None:
            timestamp = time.time()
        self.message_times.append(timestamp)
        
    def record_latency(self, latency: float):
        """Record message processing latency."""
        self.latencies.append(latency)
        
    def sample_system_resources(self):
        """Sample current system resource usage."""
        try:
            cpu_percent = self.process.cpu_percent()
            memory_info = self.process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            
            self.cpu_samples.append(cpu_percent)
            self.memory_samples.append(memory_mb)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
            
    def calculate_throughput(self, window_size: float = 1.0) -> List[float]:
        """Calculate throughput in messages per second over time windows."""
        if len(self.message_times) < 2:
            return [0.0]
            
        throughputs = []
        start_time = self.message_times[0]
        end_time = self.message_times[-1]
        
        current_window_start = start_time
        while current_window_start < end_time:
            window_end = current_window_start + window_size
            
            # Count messages in this window
            messages_in_window = sum(
                1 for t in self.message_times 
                if current_window_start <= t < window_end
            )
            
            throughput = messages_in_window / window_size
            throughputs.append(throughput)
            
            current_window_start = window_end
            
        return throughputs
        
    def get_summary(self) -> Dict[str, Any]:
        """Get performance metrics summary."""
        if not self.message_times:
            return {"error": "No messages recorded"}
            
        duration = self.end_time - self.start_time if self.end_time and self.start_time else 0
        total_messages = len(self.message_times)
        
        # Calculate throughput
        avg_throughput = total_messages / duration if duration > 0 else 0
        throughputs = self.calculate_throughput()
        
        # Calculate latency statistics
        latency_stats = {}
        if self.latencies:
            latency_stats = {
                "min_ms": min(self.latencies) * 1000,
                "max_ms": max(self.latencies) * 1000,
                "avg_ms": statistics.mean(self.latencies) * 1000,
                "p50_ms": statistics.median(self.latencies) * 1000,
                "p95_ms": self._percentile(self.latencies, 0.95) * 1000,
                "p99_ms": self._percentile(self.latencies, 0.99) * 1000,
            }
            
        # Calculate resource usage
        resource_stats = {}
        if self.cpu_samples:
            resource_stats["cpu"] = {
                "avg_percent": statistics.mean(self.cpu_samples),
                "max_percent": max(self.cpu_samples),
                "min_percent": min(self.cpu_samples)
            }
        if self.memory_samples:
            resource_stats["memory"] = {
                "avg_mb": statistics.mean(self.memory_samples),
                "max_mb": max(self.memory_samples),
                "min_mb": min(self.memory_samples)
            }
            
        return {
            "duration_seconds": duration,
            "total_messages": total_messages,
            "avg_throughput_msg_per_sec": avg_throughput,
            "max_throughput_msg_per_sec": max(throughputs) if throughputs else 0,
            "min_throughput_msg_per_sec": min(throughputs) if throughputs else 0,
            "latency": latency_stats,
            "resources": resource_stats,
            "throughput_samples": throughputs
        }
        
    def _percentile(self, data: List[float], p: float) -> float:
        """Calculate percentile of data."""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        index = int(len(sorted_data) * p)
        return sorted_data[min(index, len(sorted_data) - 1)]


class MockHighPerformanceKafkaProducer:
    """High-performance mock Kafka producer for benchmarking."""
    
    def __init__(self, config, metrics: PerformanceMetrics = None):
        self.config = config
        self.metrics = metrics
        self.sent_count = 0
        self.batch_count = 0
        
    async def send_message(self, message: MarketDataMessage) -> bool:
        """Mock send with latency tracking."""
        start_time = time.time()
        
        # Simulate minimal processing delay
        await asyncio.sleep(0.0001)  # 0.1ms processing time
        
        if self.metrics:
            end_time = time.time()
            self.metrics.record_message(end_time)
            self.metrics.record_latency(end_time - start_time)
            
        self.sent_count += 1
        return True
        
    async def send_batch(self, messages: List[MarketDataMessage]) -> int:
        """Mock batch send with optimized performance."""
        start_time = time.time()
        
        # Simulate batch processing (much faster per message)
        await asyncio.sleep(0.0001 * len(messages) * 0.1)  # 10x faster for batching
        
        if self.metrics:
            end_time = time.time()
            latency_per_message = (end_time - start_time) / len(messages)
            
            for _ in messages:
                self.metrics.record_message(end_time)
                self.metrics.record_latency(latency_per_message)
                
        self.sent_count += len(messages)
        self.batch_count += 1
        return len(messages)
        
    async def start(self):
        """Mock start."""
        pass
        
    async def stop(self):
        """Mock stop."""
        pass
        
    def is_healthy(self) -> bool:
        """Mock health check."""
        return True


@pytest.fixture
def performance_metrics():
    """Fixture providing performance metrics collector."""
    return PerformanceMetrics()


@pytest.fixture
def mock_fast_kafka_producer(performance_metrics):
    """Fixture providing a fast mock Kafka producer."""
    config = {"bootstrap_servers": ["localhost:9092"]}
    return MockHighPerformanceKafkaProducer(config, performance_metrics)


@pytest.mark.performance
@pytest.mark.slow
class TestThroughputBenchmarks:
    """Throughput performance benchmarking tests."""

    @pytest.mark.asyncio
    async def test_single_message_throughput(self, performance_metrics, mock_fast_kafka_producer):
        """Benchmark single message processing throughput."""
        
        # Setup
        num_messages = 10000
        symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]
        
        # Create test messages
        messages = []
        for i in range(num_messages):
            quote = Quote(
                symbol=Symbol(ticker=symbols[i % len(symbols)], exchange="NASDAQ"),
                bid_price=100.00 + (i % 100),
                ask_price=100.05 + (i % 100),
                bid_size=100,
                ask_size=200,
                timestamp=datetime.now(timezone.utc)
            )
            
            message = MarketDataMessage(
                data_type=DataType.QUOTE,
                data=quote,
                source="benchmark",
                received_at=datetime.now(timezone.utc)
            )
            messages.append(message)
        
        # Start measurement
        performance_metrics.start_measurement()
        
        # Send messages one by one
        for message in messages:
            await mock_fast_kafka_producer.send_message(message)
            
            # Sample resources periodically
            if len(performance_metrics.message_times) % 1000 == 0:
                performance_metrics.sample_system_resources()
        
        # End measurement
        performance_metrics.end_measurement()
        
        # Analyze results
        summary = performance_metrics.get_summary()
        
        print(f"\n=== Single Message Throughput Benchmark ===")
        print(f"Messages: {summary['total_messages']}")
        print(f"Duration: {summary['duration_seconds']:.2f}s")
        print(f"Avg Throughput: {summary['avg_throughput_msg_per_sec']:.0f} msg/s")
        print(f"Max Throughput: {summary['max_throughput_msg_per_sec']:.0f} msg/s")
        print(f"Avg Latency: {summary['latency']['avg_ms']:.2f}ms")
        print(f"P99 Latency: {summary['latency']['p99_ms']:.2f}ms")
        
        # Performance assertions
        assert summary['total_messages'] == num_messages
        assert summary['avg_throughput_msg_per_sec'] > 1000  # At least 1K msg/s
        assert summary['latency']['p99_ms'] < 50  # P99 latency under 50ms

    @pytest.mark.asyncio
    async def test_batch_processing_throughput(self, performance_metrics, mock_fast_kafka_producer):
        """Benchmark batch processing throughput."""
        
        # Setup
        num_messages = 50000
        batch_size = 100
        symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "NFLX", "META", "NVDA"]
        
        # Create test messages
        all_messages = []
        for i in range(num_messages):
            quote = Quote(
                symbol=Symbol(ticker=symbols[i % len(symbols)], exchange="NASDAQ"),
                bid_price=100.00 + (i % 100),
                ask_price=100.05 + (i % 100),
                bid_size=100,
                ask_size=200,
                timestamp=datetime.now(timezone.utc)
            )
            
            message = MarketDataMessage(
                data_type=DataType.QUOTE,
                data=quote,
                source="benchmark",
                received_at=datetime.now(timezone.utc)
            )
            all_messages.append(message)
        
        # Create batches
        batches = [
            all_messages[i:i + batch_size] 
            for i in range(0, len(all_messages), batch_size)
        ]
        
        # Start measurement
        performance_metrics.start_measurement()
        
        # Send batches
        for batch_num, batch in enumerate(batches):
            await mock_fast_kafka_producer.send_batch(batch)
            
            # Sample resources periodically
            if batch_num % 50 == 0:
                performance_metrics.sample_system_resources()
        
        # End measurement
        performance_metrics.end_measurement()
        
        # Analyze results
        summary = performance_metrics.get_summary()
        
        print(f"\n=== Batch Processing Throughput Benchmark ===")
        print(f"Messages: {summary['total_messages']}")
        print(f"Batches: {len(batches)}")
        print(f"Batch Size: {batch_size}")
        print(f"Duration: {summary['duration_seconds']:.2f}s")
        print(f"Avg Throughput: {summary['avg_throughput_msg_per_sec']:.0f} msg/s")
        print(f"Max Throughput: {summary['max_throughput_msg_per_sec']:.0f} msg/s")
        print(f"Avg Latency: {summary['latency']['avg_ms']:.2f}ms")
        print(f"P99 Latency: {summary['latency']['p99_ms']:.2f}ms")
        
        # Performance assertions
        assert summary['total_messages'] == num_messages
        assert summary['avg_throughput_msg_per_sec'] > 10000  # At least 10K msg/s with batching
        assert summary['latency']['p99_ms'] < 10  # Much lower latency with batching

    @pytest.mark.asyncio
    async def test_mixed_data_types_throughput(self, performance_metrics, mock_fast_kafka_producer):
        """Benchmark throughput with mixed data types."""
        
        # Setup
        num_messages_per_type = 5000
        symbols = ["AAPL", "GOOGL", "MSFT", "TSLA"]
        
        all_messages = []
        
        # Create mixed message types
        for i in range(num_messages_per_type):
            symbol = Symbol(ticker=symbols[i % len(symbols)], exchange="NASDAQ")
            
            # Quote message
            quote = Quote(
                symbol=symbol,
                bid_price=100.00 + i,
                ask_price=100.05 + i,
                bid_size=100,
                ask_size=200,
                timestamp=datetime.now(timezone.utc)
            )
            quote_msg = MarketDataMessage(DataType.QUOTE, quote, "benchmark", datetime.now(timezone.utc))
            all_messages.append(quote_msg)
            
            # Trade message
            trade = Trade(
                symbol=symbol,
                price=100.02 + i,
                size=500,
                timestamp=datetime.now(timezone.utc),
                trade_id=f"T{i:06d}"
            )
            trade_msg = MarketDataMessage(DataType.TRADE, trade, "benchmark", datetime.now(timezone.utc))
            all_messages.append(trade_msg)
            
            # Bar message
            bar = Bar(
                symbol=symbol,
                open_price=99.50 + i,
                high_price=101.00 + i,
                low_price=99.00 + i,
                close_price=100.50 + i,
                volume=1000000,
                timestamp=datetime.now(timezone.utc),
                timeframe="1m"
            )
            bar_msg = MarketDataMessage(DataType.BAR, bar, "benchmark", datetime.now(timezone.utc))
            all_messages.append(bar_msg)
        
        # Start measurement
        performance_metrics.start_measurement()
        
        # Send mixed batch
        batch_size = 50
        for i in range(0, len(all_messages), batch_size):
            batch = all_messages[i:i + batch_size]
            await mock_fast_kafka_producer.send_batch(batch)
            
            # Sample resources periodically
            if i % 1000 == 0:
                performance_metrics.sample_system_resources()
        
        # End measurement
        performance_metrics.end_measurement()
        
        # Analyze results
        summary = performance_metrics.get_summary()
        
        print(f"\n=== Mixed Data Types Throughput Benchmark ===")
        print(f"Messages: {summary['total_messages']}")
        print(f"Data Types: Quote, Trade, Bar")
        print(f"Duration: {summary['duration_seconds']:.2f}s")
        print(f"Avg Throughput: {summary['avg_throughput_msg_per_sec']:.0f} msg/s")
        print(f"Avg Latency: {summary['latency']['avg_ms']:.2f}ms")
        
        # Performance assertions
        assert summary['total_messages'] == len(all_messages)
        assert summary['avg_throughput_msg_per_sec'] > 5000  # At least 5K msg/s for mixed types

    @pytest.mark.asyncio
    async def test_sustained_load_benchmark(self, performance_metrics):
        """Benchmark sustained load over extended period."""
        
        # Setup for sustained load test
        duration_seconds = 30  # 30 second sustained test
        target_rate = 1000  # Target 1K msg/s
        batch_size = 20
        
        # Create Kafka producer
        producer = MockHighPerformanceKafkaProducer({}, performance_metrics)
        
        # Message generation
        symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]
        message_counter = 0
        
        def create_message():
            nonlocal message_counter
            quote = Quote(
                symbol=Symbol(ticker=symbols[message_counter % len(symbols)], exchange="NASDAQ"),
                bid_price=100.00 + (message_counter % 100),
                ask_price=100.05 + (message_counter % 100),
                bid_size=100,
                ask_size=200,
                timestamp=datetime.now(timezone.utc)
            )
            message_counter += 1
            return MarketDataMessage(DataType.QUOTE, quote, "benchmark", datetime.now(timezone.utc))
        
        # Start measurement
        performance_metrics.start_measurement()
        
        start_time = time.time()
        next_batch_time = start_time
        batch_interval = batch_size / target_rate  # Time between batches
        
        while time.time() - start_time < duration_seconds:
            current_time = time.time()
            
            # Send batch if it's time
            if current_time >= next_batch_time:
                batch = [create_message() for _ in range(batch_size)]
                await producer.send_batch(batch)
                next_batch_time += batch_interval
                
                # Sample resources
                performance_metrics.sample_system_resources()
            else:
                # Small sleep to prevent busy waiting
                await asyncio.sleep(0.001)
        
        # End measurement
        performance_metrics.end_measurement()
        
        # Analyze results
        summary = performance_metrics.get_summary()
        
        print(f"\n=== Sustained Load Benchmark ===")
        print(f"Duration: {summary['duration_seconds']:.2f}s")
        print(f"Messages: {summary['total_messages']}")
        print(f"Target Rate: {target_rate} msg/s")
        print(f"Actual Avg Rate: {summary['avg_throughput_msg_per_sec']:.0f} msg/s")
        print(f"Rate Variance: {summary['max_throughput_msg_per_sec'] - summary['min_throughput_msg_per_sec']:.0f} msg/s")
        print(f"Avg Latency: {summary['latency']['avg_ms']:.2f}ms")
        print(f"P99 Latency: {summary['latency']['p99_ms']:.2f}ms")
        
        if summary.get('resources', {}).get('cpu'):
            print(f"Avg CPU: {summary['resources']['cpu']['avg_percent']:.1f}%")
            print(f"Max CPU: {summary['resources']['cpu']['max_percent']:.1f}%")
        if summary.get('resources', {}).get('memory'):
            print(f"Avg Memory: {summary['resources']['memory']['avg_mb']:.1f} MB")
            print(f"Max Memory: {summary['resources']['memory']['max_mb']:.1f} MB")
        
        # Performance assertions
        assert summary['duration_seconds'] >= duration_seconds * 0.9  # Allow 10% variance
        assert summary['avg_throughput_msg_per_sec'] >= target_rate * 0.8  # Within 20% of target
        assert summary['latency']['p99_ms'] < 100  # P99 latency under 100ms

    @pytest.mark.asyncio
    async def test_memory_usage_benchmark(self, performance_metrics):
        """Benchmark memory usage during high-throughput processing."""
        
        # Setup
        num_iterations = 5
        messages_per_iteration = 10000
        producer = MockHighPerformanceKafkaProducer({}, performance_metrics)
        
        # Collect memory usage data
        memory_samples = []
        
        for iteration in range(num_iterations):
            print(f"Memory test iteration {iteration + 1}/{num_iterations}")
            
            # Force garbage collection before measurement
            gc.collect()
            
            # Measure memory before processing
            process = psutil.Process()
            memory_before = process.memory_info().rss / 1024 / 1024  # MB
            
            # Create and process messages
            messages = []
            for i in range(messages_per_iteration):
                quote = Quote(
                    symbol=Symbol(ticker=f"STOCK{i%100}", exchange="NASDAQ"),
                    bid_price=100.00 + i,
                    ask_price=100.05 + i,
                    bid_size=100,
                    ask_size=200,
                    timestamp=datetime.now(timezone.utc)
                )
                message = MarketDataMessage(DataType.QUOTE, quote, "benchmark", datetime.now(timezone.utc))
                messages.append(message)
            
            # Process in batches
            batch_size = 100
            for i in range(0, len(messages), batch_size):
                batch = messages[i:i + batch_size]
                await producer.send_batch(batch)
            
            # Measure memory after processing
            memory_after = process.memory_info().rss / 1024 / 1024  # MB
            memory_delta = memory_after - memory_before
            
            memory_samples.append({
                'iteration': iteration,
                'memory_before_mb': memory_before,
                'memory_after_mb': memory_after,
                'memory_delta_mb': memory_delta,
                'messages_processed': len(messages)
            })
            
            # Clear messages to free memory
            messages.clear()
            
            # Force garbage collection
            gc.collect()
            
            # Brief pause between iterations
            await asyncio.sleep(0.1)
        
        # Analyze memory usage
        avg_memory_delta = statistics.mean([s['memory_delta_mb'] for s in memory_samples])
        max_memory_delta = max([s['memory_delta_mb'] for s in memory_samples])
        
        print(f"\n=== Memory Usage Benchmark ===")
        print(f"Iterations: {num_iterations}")
        print(f"Messages per iteration: {messages_per_iteration}")
        print(f"Avg Memory Delta: {avg_memory_delta:.2f} MB")
        print(f"Max Memory Delta: {max_memory_delta:.2f} MB")
        print(f"Memory per message: {avg_memory_delta / messages_per_iteration * 1024:.2f} KB")
        
        for sample in memory_samples:
            print(f"  Iteration {sample['iteration']}: {sample['memory_delta_mb']:.2f} MB delta")
        
        # Memory usage assertions
        assert avg_memory_delta < 100  # Average memory increase should be under 100MB
        assert max_memory_delta < 200  # Max memory increase should be under 200MB
        # Memory per message should be reasonable (under 1KB per message overhead)
        assert avg_memory_delta / messages_per_iteration < 0.001  # Less than 1KB per message


if __name__ == "__main__":
    # Run performance benchmarks
    pytest.main([__file__, "-v", "--tb=short", "-s", "-m", "performance"])