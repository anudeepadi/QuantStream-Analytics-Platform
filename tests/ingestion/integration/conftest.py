#!/usr/bin/env python3
"""
Pytest configuration and fixtures for integration tests.

This module provides shared fixtures and configuration for integration testing
of the QuantStream Analytics Platform ingestion pipeline.
"""

import asyncio
import pytest
import tempfile
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from src.ingestion.utils import setup_logging, LogConfig, LogLevel


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
async def setup_test_logging():
    """Automatically setup logging for all integration tests."""
    log_config = LogConfig(
        level=LogLevel.DEBUG,
        console_output=False,  # Disable console output during tests
        file_output=False,     # Disable file output during tests
        json_format=False
    )
    setup_logging(log_config)


@pytest.fixture
async def temp_directory():
    """Provide a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
async def sample_market_data():
    """Provide sample market data for testing."""
    from src.ingestion.models import Symbol, Quote, Trade, Bar, AssetClass, DataType
    from datetime import datetime, timezone
    
    symbol = Symbol(ticker="AAPL", exchange="NASDAQ", asset_class=AssetClass.STOCK)
    
    quote = Quote(
        symbol=symbol,
        bid_price=150.00,
        ask_price=150.05,
        bid_size=100,
        ask_size=200,
        timestamp=datetime.now(timezone.utc)
    )
    
    trade = Trade(
        symbol=symbol,
        price=150.02,
        size=500,
        timestamp=datetime.now(timezone.utc),
        trade_id="123456"
    )
    
    bar = Bar(
        symbol=symbol,
        open_price=149.50,
        high_price=151.00,
        low_price=149.00,
        close_price=150.50,
        volume=1000000,
        timestamp=datetime.now(timezone.utc),
        timeframe="1m"
    )
    
    return {
        'symbol': symbol,
        'quote': quote,
        'trade': trade,
        'bar': bar
    }


@pytest.fixture
async def mock_external_api():
    """Provide a mock external API for testing connectors."""
    
    class MockAPI:
        def __init__(self):
            self.call_count = 0
            self.responses = {}
            self.failures = []
            
        async def get_quote(self, symbol: str):
            """Mock quote API response."""
            self.call_count += 1
            
            if symbol in self.failures:
                raise Exception(f"API error for {symbol}")
                
            return {
                "symbol": symbol,
                "bid_price": 150.00 + (self.call_count * 0.01),
                "ask_price": 150.05 + (self.call_count * 0.01),
                "bid_size": 100,
                "ask_size": 200,
                "timestamp": "2024-01-15T10:30:00Z"
            }
            
        async def get_bars(self, symbol: str):
            """Mock bars API response."""
            self.call_count += 1
            
            if symbol in self.failures:
                raise Exception(f"API error for {symbol}")
                
            return {
                "symbol": symbol,
                "open_price": 149.50 + (self.call_count * 0.01),
                "high_price": 151.00 + (self.call_count * 0.01),
                "low_price": 149.00 + (self.call_count * 0.01),
                "close_price": 150.50 + (self.call_count * 0.01),
                "volume": 1000000,
                "timestamp": "2024-01-15T10:30:00Z"
            }
            
        def add_failure(self, symbol: str):
            """Add a symbol that should fail API calls."""
            self.failures.append(symbol)
            
        def reset(self):
            """Reset the mock API state."""
            self.call_count = 0
            self.responses = {}
            self.failures = []
    
    return MockAPI()


@pytest.fixture
async def integration_test_config():
    """Provide configuration for integration tests."""
    return {
        "kafka": {
            "bootstrap_servers": ["localhost:9092"],
            "test_mode": True,
            "topic_prefix": "integration_test_"
        },
        "redis": {
            "host": "localhost",
            "port": 6379,
            "database": 1,  # Use different database for tests
            "test_mode": True
        },
        "connectors": {
            "timeout": 5.0,  # Shorter timeout for tests
            "max_retries": 2,  # Fewer retries for tests
            "batch_size": 10   # Smaller batches for tests
        },
        "pipeline": {
            "buffer_size": 100,  # Smaller buffer for tests
            "max_throughput": 1000,  # Lower throughput for tests
            "enable_backpressure": True
        }
    }


@pytest.fixture
async def csv_test_data(temp_directory):
    """Create sample CSV files for testing."""
    
    # Create quotes CSV
    quotes_csv = temp_directory / "quotes.csv"
    quotes_content = """timestamp,symbol,bid_price,ask_price,bid_size,ask_size
2024-01-15T10:30:00Z,AAPL,150.00,150.05,100,200
2024-01-15T10:30:30Z,AAPL,150.01,150.06,150,180
2024-01-15T10:31:00Z,GOOGL,2800.00,2800.50,50,75
2024-01-15T10:31:30Z,MSFT,300.00,300.10,200,250
"""
    quotes_csv.write_text(quotes_content)
    
    # Create trades CSV
    trades_csv = temp_directory / "trades.csv"
    trades_content = """timestamp,symbol,price,size,trade_id
2024-01-15T10:30:15Z,AAPL,150.02,500,T001
2024-01-15T10:30:45Z,AAPL,150.03,300,T002
2024-01-15T10:31:15Z,GOOGL,2800.25,100,T003
2024-01-15T10:31:45Z,MSFT,300.05,750,T004
"""
    trades_csv.write_text(trades_content)
    
    # Create bars CSV
    bars_csv = temp_directory / "bars.csv"
    bars_content = """timestamp,symbol,open,high,low,close,volume
2024-01-15T10:30:00Z,AAPL,149.50,151.00,149.00,150.50,1000000
2024-01-15T10:31:00Z,AAPL,150.50,152.00,150.00,151.25,950000
2024-01-15T10:32:00Z,GOOGL,2800.00,2850.00,2795.00,2825.50,500000
2024-01-15T10:33:00Z,MSFT,299.50,301.00,299.00,300.75,800000
"""
    bars_csv.write_text(bars_content)
    
    return {
        'quotes': quotes_csv,
        'trades': trades_csv,
        'bars': bars_csv,
        'directory': temp_directory
    }


@pytest.fixture
async def mock_websocket_server():
    """Provide a mock WebSocket server for testing."""
    
    class MockWebSocketServer:
        def __init__(self):
            self.messages = []
            self.is_running = False
            self.connection_count = 0
            
        async def send_message(self, message: dict):
            """Send a message to connected clients."""
            self.messages.append(message)
            
        async def start(self, host="localhost", port=8765):
            """Start the mock WebSocket server."""
            self.is_running = True
            
        async def stop(self):
            """Stop the mock WebSocket server."""
            self.is_running = False
            
        def get_sent_messages(self):
            """Get all messages sent by the server."""
            return self.messages.copy()
            
        def reset(self):
            """Reset server state."""
            self.messages = []
            self.connection_count = 0
    
    return MockWebSocketServer()


# Pytest markers for different test categories
def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "kafka: mark test as requiring Kafka"
    )
    config.addinivalue_line(
        "markers", "redis: mark test as requiring Redis"
    )
    config.addinivalue_line(
        "markers", "websocket: mark test as requiring WebSocket"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "external: mark test as requiring external services"
    )


# Custom test collection for integration tests
def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on test location."""
    for item in items:
        # Add integration marker to all tests in integration directory
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
            
        # Add specific markers based on test name
        if "kafka" in item.name.lower():
            item.add_marker(pytest.mark.kafka)
        if "redis" in item.name.lower():
            item.add_marker(pytest.mark.redis)
        if "websocket" in item.name.lower():
            item.add_marker(pytest.mark.websocket)
        if "performance" in item.name.lower():
            item.add_marker(pytest.mark.slow)


# Skip tests based on environment
def pytest_runtest_setup(item):
    """Skip tests based on markers and environment."""
    # Skip Kafka tests if SKIP_KAFKA_TESTS environment variable is set
    if item.get_closest_marker("kafka") and os.environ.get("SKIP_KAFKA_TESTS"):
        pytest.skip("Kafka tests disabled")
        
    # Skip Redis tests if SKIP_REDIS_TESTS environment variable is set
    if item.get_closest_marker("redis") and os.environ.get("SKIP_REDIS_TESTS"):
        pytest.skip("Redis tests disabled")
        
    # Skip external tests if SKIP_EXTERNAL_TESTS environment variable is set
    if item.get_closest_marker("external") and os.environ.get("SKIP_EXTERNAL_TESTS"):
        pytest.skip("External service tests disabled")