#!/usr/bin/env python3
"""
Pytest configuration for performance tests.

This module provides shared configuration and fixtures for performance testing
of the QuantStream Analytics Platform.
"""

import pytest
import asyncio
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from src.ingestion.utils import setup_logging, LogConfig, LogLevel


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True, scope="session")
async def setup_performance_logging():
    """Setup logging for performance tests."""
    log_config = LogConfig(
        level=LogLevel.WARNING,  # Minimal logging for performance tests
        console_output=False,    # Disable console output during performance tests
        file_output=False,       # Disable file output during performance tests
        json_format=False
    )
    setup_logging(log_config)


# Performance test configuration
@pytest.fixture(scope="session")
def performance_config():
    """Configuration for performance tests."""
    return {
        "targets": {
            "throughput_msg_per_sec": 500000,  # 500K messages per second
            "latency_p99_ms": 100,             # Sub-100ms P99 latency
            "latency_avg_ms": 50,              # Average latency target
            "memory_limit_mb": 2048,           # Memory usage limit
            "cpu_limit_percent": 80            # CPU usage limit
        },
        "test_parameters": {
            "warm_up_messages": 1000,          # Messages for warm-up
            "benchmark_duration": 30,          # Benchmark duration in seconds
            "sample_interval": 1.0,            # Resource sampling interval
            "batch_sizes": [1, 10, 50, 100, 500],  # Batch sizes to test
            "concurrency_levels": [1, 5, 10, 20]   # Concurrency levels to test
        }
    }


# Pytest markers for performance tests
def pytest_configure(config):
    """Configure pytest markers for performance tests."""
    config.addinivalue_line(
        "markers", "performance: mark test as performance benchmark"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running performance test"
    )
    config.addinivalue_line(
        "markers", "throughput: mark test as throughput benchmark"
    )
    config.addinivalue_line(
        "markers", "latency: mark test as latency benchmark"
    )
    config.addinivalue_line(
        "markers", "memory: mark test as memory usage benchmark"
    )
    config.addinivalue_line(
        "markers", "cpu: mark test as CPU usage benchmark"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add performance markers."""
    for item in items:
        # Add performance marker to all tests in performance directory
        if "performance" in str(item.fspath):
            item.add_marker(pytest.mark.performance)
            
        # Add specific markers based on test name
        if "throughput" in item.name.lower():
            item.add_marker(pytest.mark.throughput)
        if "latency" in item.name.lower():
            item.add_marker(pytest.mark.latency)
        if "memory" in item.name.lower():
            item.add_marker(pytest.mark.memory)
        if "cpu" in item.name.lower():
            item.add_marker(pytest.mark.cpu)


def pytest_runtest_setup(item):
    """Setup for individual performance tests."""
    # Skip performance tests if SKIP_PERFORMANCE_TESTS is set
    import os
    if item.get_closest_marker("performance") and os.environ.get("SKIP_PERFORMANCE_TESTS"):
        pytest.skip("Performance tests disabled")


def pytest_runtest_teardown(item, nextitem):
    """Cleanup after individual performance tests."""
    # Force garbage collection after each performance test
    import gc
    gc.collect()


# Custom pytest hook for performance test reporting
def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Add performance test summary to terminal output."""
    if hasattr(terminalreporter.config.option, 'verbose') and terminalreporter.config.option.verbose:
        # Only show detailed summary in verbose mode
        performance_tests = [
            item for item in terminalreporter.stats.get('passed', [])
            if 'performance' in str(item.nodeid)
        ]
        
        if performance_tests:
            terminalreporter.write_sep("=", "Performance Test Summary")
            terminalreporter.write_line(f"Performance tests passed: {len(performance_tests)}")
            terminalreporter.write_line("")
            terminalreporter.write_line("Note: Check performance_reports/ directory for detailed benchmark results")
            terminalreporter.write_line("")