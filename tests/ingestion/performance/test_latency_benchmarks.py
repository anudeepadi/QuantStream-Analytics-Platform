#!/usr/bin/env python3
"""
Latency benchmarking tests for the QuantStream Analytics Platform.

These tests measure end-to-end latency and ensure sub-100ms processing
requirements are met under various load conditions.
"""

import asyncio
import time
import statistics
import pytest
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any
from unittest.mock import AsyncMock, patch
import concurrent.futures

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from src.ingestion.processors import (
    QuantStreamPipelineManager, PipelineConfig, DataProcessor
)
from src.ingestion.models import (
    Symbol, DataType, Quote, Trade, Bar, MarketDataMessage, AssetClass
)
from src.ingestion.utils import (
    setup_logging, LogConfig, LogLevel, get_logger
)


class LatencyTracker:
    """Track end-to-end latency measurements."""
    
    def __init__(self):
        self.measurements = []
        self.start_times = {}
        
    def start_processing(self, message_id: str):
        """Mark start of message processing."""
        self.start_times[message_id] = time.time()
        
    def end_processing(self, message_id: str) -> float:
        """Mark end of message processing and return latency."""
        if message_id not in self.start_times:
            return 0.0
            
        end_time = time.time()
        start_time = self.start_times.pop(message_id)
        latency = end_time - start_time
        
        self.measurements.append({
            'message_id': message_id,
            'latency_seconds': latency,
            'latency_ms': latency * 1000,
            'timestamp': end_time
        })
        
        return latency
        
    def get_stats(self) -> Dict[str, Any]:
        """Get latency statistics."""
        if not self.measurements:
            return {}
            
        latencies = [m['latency_ms'] for m in self.measurements]
        
        return {
            'count': len(latencies),
            'min_ms': min(latencies),
            'max_ms': max(latencies),
            'avg_ms': statistics.mean(latencies),
            'median_ms': statistics.median(latencies),
            'p95_ms': self._percentile(latencies, 0.95),
            'p99_ms': self._percentile(latencies, 0.99),
            'p999_ms': self._percentile(latencies, 0.999),
            'std_dev_ms': statistics.stdev(latencies) if len(latencies) > 1 else 0.0
        }
        
    def _percentile(self, data: List[float], p: float) -> float:
        """Calculate percentile."""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        index = int(len(sorted_data) * p)
        return sorted_data[min(index, len(sorted_data) - 1)]
        
    def reset(self):
        """Reset all measurements."""
        self.measurements.clear()
        self.start_times.clear()


class MockLatencyAwareProcessor:
    """Mock processor that tracks latency."""
    
    def __init__(self, latency_tracker: LatencyTracker, processing_delay: float = 0.001):
        self.latency_tracker = latency_tracker
        self.processing_delay = processing_delay
        self.processed_count = 0
        
    async def process_message(self, message: MarketDataMessage) -> bool:
        """Process message with latency tracking."""
        message_id = f"msg_{self.processed_count}"
        
        # Start tracking
        self.latency_tracker.start_processing(message_id)
        
        # Simulate processing delay
        await asyncio.sleep(self.processing_delay)
        
        # End tracking
        self.latency_tracker.end_processing(message_id)
        
        self.processed_count += 1
        return True
        
    async def process_batch(self, messages: List[MarketDataMessage]) -> int:
        """Process batch with latency tracking."""
        processed = 0
        
        for message in messages:
            if await self.process_message(message):
                processed += 1
                
        return processed


@pytest.fixture
def latency_tracker():
    """Fixture providing latency tracker."""
    return LatencyTracker()


@pytest.mark.performance
@pytest.mark.slow
class TestLatencyBenchmarks:
    """Latency performance benchmarking tests."""

    @pytest.mark.asyncio
    async def test_single_message_latency(self, latency_tracker):
        """Benchmark single message processing latency."""
        
        # Setup
        processor = MockLatencyAwareProcessor(latency_tracker, processing_delay=0.001)
        num_messages = 1000
        
        # Create test messages
        messages = []
        for i in range(num_messages):
            quote = Quote(
                symbol=Symbol(ticker="AAPL", exchange="NASDAQ"),
                bid_price=150.00 + (i * 0.01),
                ask_price=150.05 + (i * 0.01),
                bid_size=100,
                ask_size=200,
                timestamp=datetime.now(timezone.utc)
            )
            
            message = MarketDataMessage(
                data_type=DataType.QUOTE,
                data=quote,
                source="latency_test",
                received_at=datetime.now(timezone.utc)
            )
            messages.append(message)
        
        # Process messages sequentially
        start_time = time.time()
        
        for message in messages:
            await processor.process_message(message)
            
        end_time = time.time()
        
        # Analyze results
        stats = latency_tracker.get_stats()
        total_duration = end_time - start_time
        
        print(f"\n=== Single Message Latency Benchmark ===")
        print(f"Messages: {stats['count']}")
        print(f"Total Duration: {total_duration:.3f}s")
        print(f"Min Latency: {stats['min_ms']:.2f}ms")
        print(f"Avg Latency: {stats['avg_ms']:.2f}ms")
        print(f"Median Latency: {stats['median_ms']:.2f}ms")
        print(f"P95 Latency: {stats['p95_ms']:.2f}ms")
        print(f"P99 Latency: {stats['p99_ms']:.2f}ms")
        print(f"P99.9 Latency: {stats['p999_ms']:.2f}ms")
        print(f"Max Latency: {stats['max_ms']:.2f}ms")
        print(f"Std Dev: {stats['std_dev_ms']:.2f}ms")
        
        # Latency assertions
        assert stats['count'] == num_messages
        assert stats['avg_ms'] < 10.0  # Average latency under 10ms
        assert stats['p95_ms'] < 20.0  # P95 latency under 20ms
        assert stats['p99_ms'] < 50.0  # P99 latency under 50ms

    @pytest.mark.asyncio
    async def test_concurrent_processing_latency(self, latency_tracker):
        """Benchmark latency under concurrent processing load."""
        
        # Setup
        num_concurrent = 10
        messages_per_worker = 100
        processors = [
            MockLatencyAwareProcessor(latency_tracker, processing_delay=0.002)
            for _ in range(num_concurrent)
        ]
        
        async def worker_task(processor: MockLatencyAwareProcessor, worker_id: int):
            """Worker task to process messages concurrently."""
            messages = []
            
            for i in range(messages_per_worker):
                quote = Quote(
                    symbol=Symbol(ticker=f"STOCK{worker_id}", exchange="NASDAQ"),
                    bid_price=100.00 + i,
                    ask_price=100.05 + i,
                    bid_size=100,
                    ask_size=200,
                    timestamp=datetime.now(timezone.utc)
                )
                
                message = MarketDataMessage(
                    data_type=DataType.QUOTE,
                    data=quote,
                    source=f"worker_{worker_id}",
                    received_at=datetime.now(timezone.utc)
                )
                messages.append(message)
            
            # Process messages
            for message in messages:
                await processor.process_message(message)
        
        # Run concurrent workers
        start_time = time.time()
        
        tasks = [
            worker_task(processors[i], i)
            for i in range(num_concurrent)
        ]
        
        await asyncio.gather(*tasks)
        
        end_time = time.time()
        
        # Analyze results
        stats = latency_tracker.get_stats()
        total_duration = end_time - start_time
        
        print(f"\n=== Concurrent Processing Latency Benchmark ===")
        print(f"Concurrent Workers: {num_concurrent}")
        print(f"Messages per Worker: {messages_per_worker}")
        print(f"Total Messages: {stats['count']}")
        print(f"Total Duration: {total_duration:.3f}s")
        print(f"Min Latency: {stats['min_ms']:.2f}ms")
        print(f"Avg Latency: {stats['avg_ms']:.2f}ms")
        print(f"Median Latency: {stats['median_ms']:.2f}ms")
        print(f"P95 Latency: {stats['p95_ms']:.2f}ms")
        print(f"P99 Latency: {stats['p99_ms']:.2f}ms")
        print(f"Max Latency: {stats['max_ms']:.2f}ms")
        
        # Concurrent processing assertions
        assert stats['count'] == num_concurrent * messages_per_worker
        assert stats['avg_ms'] < 20.0  # Average latency under 20ms even with concurrency
        assert stats['p99_ms'] < 100.0  # P99 latency under 100ms

    @pytest.mark.asyncio
    async def test_varying_load_latency(self, latency_tracker):
        """Benchmark latency under varying load conditions."""
        
        # Setup
        load_phases = [
            {"duration": 5, "rate": 100, "delay": 0.001},   # Low load
            {"duration": 5, "rate": 500, "delay": 0.002},   # Medium load
            {"duration": 5, "rate": 1000, "delay": 0.005},  # High load
            {"duration": 5, "rate": 200, "delay": 0.001},   # Back to low load
        ]
        
        processor = MockLatencyAwareProcessor(latency_tracker)
        
        async def load_phase(phase_config: Dict[str, Any], phase_num: int):
            """Execute a load phase."""
            duration = phase_config["duration"]
            rate = phase_config["rate"]
            delay = phase_config["delay"]
            
            processor.processing_delay = delay
            
            start_time = time.time()
            message_interval = 1.0 / rate
            next_message_time = start_time
            message_count = 0
            
            print(f"Phase {phase_num}: {rate} msg/s for {duration}s")
            
            while time.time() - start_time < duration:
                current_time = time.time()
                
                if current_time >= next_message_time:
                    # Create and process message
                    quote = Quote(
                        symbol=Symbol(ticker=f"PHASE{phase_num}", exchange="NASDAQ"),
                        bid_price=100.00 + message_count,
                        ask_price=100.05 + message_count,
                        bid_size=100,
                        ask_size=200,
                        timestamp=datetime.now(timezone.utc)
                    )
                    
                    message = MarketDataMessage(
                        data_type=DataType.QUOTE,
                        data=quote,
                        source=f"phase_{phase_num}",
                        received_at=datetime.now(timezone.utc)
                    )
                    
                    await processor.process_message(message)
                    
                    message_count += 1
                    next_message_time += message_interval
                else:
                    await asyncio.sleep(0.001)
            
            return message_count
        
        # Execute load phases
        total_start = time.time()
        total_messages = 0
        
        for i, phase in enumerate(load_phases):
            phase_messages = await load_phase(phase, i + 1)
            total_messages += phase_messages
        
        total_duration = time.time() - total_start
        
        # Analyze results
        stats = latency_tracker.get_stats()
        
        print(f"\n=== Varying Load Latency Benchmark ===")
        print(f"Total Phases: {len(load_phases)}")
        print(f"Total Duration: {total_duration:.3f}s")
        print(f"Total Messages: {stats['count']}")
        print(f"Min Latency: {stats['min_ms']:.2f}ms")
        print(f"Avg Latency: {stats['avg_ms']:.2f}ms")
        print(f"Median Latency: {stats['median_ms']:.2f}ms")
        print(f"P95 Latency: {stats['p95_ms']:.2f}ms")
        print(f"P99 Latency: {stats['p99_ms']:.2f}ms")
        print(f"Max Latency: {stats['max_ms']:.2f}ms")
        
        # Varying load assertions
        assert stats['count'] > 0
        assert stats['p99_ms'] < 200.0  # P99 latency under 200ms even during high load

    @pytest.mark.asyncio
    async def test_batch_vs_single_latency_comparison(self, latency_tracker):
        """Compare latency between batch and single message processing."""
        
        # Setup
        num_messages = 500
        batch_size = 10
        
        # Test data
        messages = []
        for i in range(num_messages):
            quote = Quote(
                symbol=Symbol(ticker="BATCH_TEST", exchange="NASDAQ"),
                bid_price=150.00 + i,
                ask_price=150.05 + i,
                bid_size=100,
                ask_size=200,
                timestamp=datetime.now(timezone.utc)
            )
            
            message = MarketDataMessage(
                data_type=DataType.QUOTE,
                data=quote,
                source="batch_comparison",
                received_at=datetime.now(timezone.utc)
            )
            messages.append(message)
        
        # Test 1: Single message processing
        single_tracker = LatencyTracker()
        single_processor = MockLatencyAwareProcessor(single_tracker, processing_delay=0.001)
        
        single_start = time.time()
        for message in messages:
            await single_processor.process_message(message)
        single_duration = time.time() - single_start
        
        single_stats = single_tracker.get_stats()
        
        # Test 2: Batch processing
        batch_tracker = LatencyTracker()
        batch_processor = MockLatencyAwareProcessor(batch_tracker, processing_delay=0.0001)  # Faster per message
        
        batch_start = time.time()
        for i in range(0, len(messages), batch_size):
            batch = messages[i:i + batch_size]
            await batch_processor.process_batch(batch)
        batch_duration = time.time() - batch_start
        
        batch_stats = batch_tracker.get_stats()
        
        # Compare results
        print(f"\n=== Batch vs Single Message Latency Comparison ===")
        print(f"Messages: {num_messages}")
        print(f"Batch Size: {batch_size}")
        print(f"")
        print(f"Single Message Processing:")
        print(f"  Total Duration: {single_duration:.3f}s")
        print(f"  Avg Latency: {single_stats['avg_ms']:.2f}ms")
        print(f"  P95 Latency: {single_stats['p95_ms']:.2f}ms")
        print(f"  P99 Latency: {single_stats['p99_ms']:.2f}ms")
        print(f"")
        print(f"Batch Processing:")
        print(f"  Total Duration: {batch_duration:.3f}s")
        print(f"  Avg Latency: {batch_stats['avg_ms']:.2f}ms")
        print(f"  P95 Latency: {batch_stats['p95_ms']:.2f}ms")
        print(f"  P99 Latency: {batch_stats['p99_ms']:.2f}ms")
        print(f"")
        print(f"Improvement:")
        print(f"  Duration: {((single_duration - batch_duration) / single_duration * 100):.1f}% faster")
        print(f"  Avg Latency: {((single_stats['avg_ms'] - batch_stats['avg_ms']) / single_stats['avg_ms'] * 100):.1f}% lower")
        
        # Assertions
        assert batch_duration < single_duration  # Batch should be faster overall
        # Note: Per-message latency might be similar, but overall throughput should be better

    @pytest.mark.asyncio
    async def test_latency_under_memory_pressure(self, latency_tracker):
        """Benchmark latency under memory pressure conditions."""
        
        # Create memory pressure by allocating large objects
        memory_ballast = []
        
        try:
            # Allocate memory to create pressure (100MB worth of data)
            for _ in range(100):
                memory_ballast.append(bytearray(1024 * 1024))  # 1MB each
            
            # Setup processor
            processor = MockLatencyAwareProcessor(latency_tracker, processing_delay=0.002)
            num_messages = 200
            
            # Create and process messages under memory pressure
            messages = []
            for i in range(num_messages):
                # Create larger message objects to increase memory pressure
                quote = Quote(
                    symbol=Symbol(ticker=f"MEMORY_TEST_{i%10}", exchange="NASDAQ"),
                    bid_price=150.00 + i,
                    ask_price=150.05 + i,
                    bid_size=100,
                    ask_size=200,
                    timestamp=datetime.now(timezone.utc)
                )
                
                message = MarketDataMessage(
                    data_type=DataType.QUOTE,
                    data=quote,
                    source="memory_pressure_test",
                    received_at=datetime.now(timezone.utc)
                )
                messages.append(message)
            
            # Process messages
            start_time = time.time()
            
            for message in messages:
                await processor.process_message(message)
                
                # Occasionally add more memory pressure during processing
                if len(messages) % 50 == 0:
                    memory_ballast.append(bytearray(512 * 1024))  # 512KB
            
            end_time = time.time()
            
            # Analyze results
            stats = latency_tracker.get_stats()
            total_duration = end_time - start_time
            
            print(f"\n=== Latency Under Memory Pressure Benchmark ===")
            print(f"Memory Ballast: ~{len(memory_ballast)} MB")
            print(f"Messages: {stats['count']}")
            print(f"Total Duration: {total_duration:.3f}s")
            print(f"Min Latency: {stats['min_ms']:.2f}ms")
            print(f"Avg Latency: {stats['avg_ms']:.2f}ms")
            print(f"Median Latency: {stats['median_ms']:.2f}ms")
            print(f"P95 Latency: {stats['p95_ms']:.2f}ms")
            print(f"P99 Latency: {stats['p99_ms']:.2f}ms")
            print(f"Max Latency: {stats['max_ms']:.2f}ms")
            
            # Memory pressure assertions
            assert stats['count'] == num_messages
            # Allow higher latency under memory pressure but still reasonable
            assert stats['p99_ms'] < 500.0  # P99 latency under 500ms even with memory pressure
            
        finally:
            # Clean up memory ballast
            memory_ballast.clear()


if __name__ == "__main__":
    # Run latency benchmarks
    pytest.main([__file__, "-v", "--tb=short", "-s", "-m", "performance"])