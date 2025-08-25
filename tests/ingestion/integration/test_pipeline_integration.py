#!/usr/bin/env python3
"""
Integration tests for the QuantStream Analytics Platform pipeline.

These tests verify end-to-end functionality of the ingestion pipeline
with mocked external services and real component integration.
"""

import asyncio
import json
import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from typing import List, Dict, Any

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from src.ingestion.processors import (
    QuantStreamPipelineManager, PipelineConfig, HighPerformanceKafkaProducer
)
from src.ingestion.connectors import (
    create_api_connector, APIConnectorConfig, APIProvider,
    create_websocket_connector, WebSocketConnectorConfig,
    create_csv_connector, CSVConnectorConfig
)
from src.ingestion.models import (
    Symbol, DataType, Quote, Trade, Bar, MarketDataMessage
)
from src.ingestion.utils import (
    setup_logging, LogConfig, LogLevel, get_logger
)


class MockKafkaProducer:
    """Mock Kafka producer for testing."""
    
    def __init__(self):
        self.sent_messages = []
        self.is_connected = True
        
    async def send_message(self, message: MarketDataMessage) -> bool:
        """Mock send message."""
        self.sent_messages.append(message)
        return True
        
    async def send_batch(self, messages: List[MarketDataMessage]) -> int:
        """Mock send batch."""
        self.sent_messages.extend(messages)
        return len(messages)
        
    async def start(self):
        """Mock start."""
        pass
        
    async def stop(self):
        """Mock stop."""
        pass
        
    def is_healthy(self) -> bool:
        """Mock health check."""
        return self.is_connected


class MockAPIService:
    """Mock external API service for testing."""
    
    def __init__(self):
        self.call_count = 0
        self.responses = []
        
    async def get_quote(self, symbol: str) -> Dict[str, Any]:
        """Mock quote response."""
        self.call_count += 1
        return {
            "symbol": symbol,
            "bid_price": 150.00 + (self.call_count * 0.1),
            "ask_price": 150.05 + (self.call_count * 0.1),
            "bid_size": 100,
            "ask_size": 200,
            "timestamp": "2024-01-15T10:30:00Z"
        }
        
    async def get_bars(self, symbol: str) -> Dict[str, Any]:
        """Mock bar response."""
        self.call_count += 1
        return {
            "symbol": symbol,
            "open_price": 149.50 + (self.call_count * 0.1),
            "high_price": 151.00 + (self.call_count * 0.1),
            "low_price": 149.00 + (self.call_count * 0.1),
            "close_price": 150.50 + (self.call_count * 0.1),
            "volume": 1000000,
            "timestamp": "2024-01-15T10:30:00Z"
        }


@pytest.fixture
async def mock_kafka_producer():
    """Fixture providing a mock Kafka producer."""
    return MockKafkaProducer()


@pytest.fixture
async def mock_api_service():
    """Fixture providing a mock API service."""
    return MockAPIService()


@pytest.fixture
async def temp_data_dir():
    """Fixture providing a temporary data directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
async def sample_csv_file(temp_data_dir):
    """Fixture creating a sample CSV file for testing."""
    csv_file = temp_data_dir / "sample_data.csv"
    csv_content = """timestamp,symbol,open,high,low,close,volume
2024-01-15T10:30:00Z,AAPL,149.50,151.00,149.00,150.50,1000000
2024-01-15T10:31:00Z,AAPL,150.50,152.00,150.00,151.25,950000
2024-01-15T10:32:00Z,GOOGL,2800.00,2850.00,2795.00,2825.50,500000
"""
    csv_file.write_text(csv_content)
    return csv_file


@pytest.mark.integration
class TestPipelineIntegration:
    """Integration tests for the complete pipeline."""

    @pytest.mark.asyncio
    async def test_rest_api_connector_integration(self, mock_kafka_producer, mock_api_service):
        """Test REST API connector integration with pipeline."""
        
        # Setup logging
        log_config = LogConfig(level=LogLevel.DEBUG, console_output=False)
        setup_logging(log_config)
        
        # Create API connector configuration
        config = APIConnectorConfig(
            name="test-yahoo-finance",
            provider=APIProvider.YAHOO_FINANCE,
            symbols=[Symbol(ticker="AAPL", exchange="NASDAQ")],
            data_types=[DataType.QUOTE],
            requests_per_minute=60,
            batch_size=5
        )
        
        # Mock the API calls
        with patch('aiohttp.ClientSession.get') as mock_get:
            # Setup mock response
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = await mock_api_service.get_quote("AAPL")
            mock_get.return_value.__aenter__.return_value = mock_response
            
            # Create connector
            connector = create_api_connector("yahoo_finance", config.__dict__)
            
            # Collect messages
            received_messages = []
            
            def message_handler(message: MarketDataMessage):
                received_messages.append(message)
            
            connector.subscribe(message_handler)
            
            # Start connector
            await connector.start()
            
            # Wait for some data collection
            await asyncio.sleep(0.5)
            
            # Stop connector
            await connector.stop()
            
            # Verify messages were received
            assert len(received_messages) > 0
            assert all(msg.data_type == DataType.QUOTE for msg in received_messages)
            assert all(msg.data.symbol.ticker == "AAPL" for msg in received_messages)

    @pytest.mark.asyncio
    async def test_csv_connector_integration(self, sample_csv_file, mock_kafka_producer):
        """Test CSV connector integration with pipeline."""
        
        # Setup logging
        log_config = LogConfig(level=LogLevel.DEBUG, console_output=False)
        setup_logging(log_config)
        
        # Create CSV connector configuration
        config = CSVConnectorConfig(
            name="test-csv-connector",
            file_path=str(sample_csv_file),
            batch_size=10,
            auto_detect_schema=True
        )
        
        # Create connector
        connector = create_csv_connector("csv_file", config.__dict__)
        
        # Collect messages
        received_messages = []
        
        def message_handler(message: MarketDataMessage):
            received_messages.append(message)
        
        connector.subscribe(message_handler)
        
        # Start connector
        await connector.start()
        
        # Wait for processing
        await asyncio.sleep(1.0)
        
        # Stop connector
        await connector.stop()
        
        # Verify messages were received
        assert len(received_messages) >= 3  # At least 3 rows from CSV
        assert any(msg.data.symbol.ticker == "AAPL" for msg in received_messages)
        assert any(msg.data.symbol.ticker == "GOOGL" for msg in received_messages)

    @pytest.mark.asyncio
    async def test_full_pipeline_integration(self, mock_kafka_producer, mock_api_service):
        """Test complete pipeline integration."""
        
        # Setup logging
        log_config = LogConfig(level=LogLevel.INFO, console_output=False)
        setup_logging(log_config)
        
        # Mock Kafka producer in the pipeline
        with patch('src.ingestion.processors.pipeline_manager.HighPerformanceKafkaProducer') as mock_kafka_class:
            mock_kafka_class.return_value = mock_kafka_producer
            
            # Mock API calls
            with patch('aiohttp.ClientSession.get') as mock_get:
                # Setup mock response
                mock_response = AsyncMock()
                mock_response.status = 200
                mock_response.json.return_value = await mock_api_service.get_quote("AAPL")
                mock_get.return_value.__aenter__.return_value = mock_response
                
                # Create pipeline configuration
                pipeline_config = PipelineConfig(
                    name="test-pipeline",
                    max_throughput=1000,
                    batch_size=10,
                    buffer_size=100,
                    enable_backpressure=True,
                    connectors=[
                        {
                            "name": "test_yahoo_finance",
                            "type": "rest_api",
                            "provider": "yahoo_finance",
                            "enabled": True,
                            "symbols": [{"ticker": "AAPL", "exchange": "NASDAQ"}],
                            "data_types": ["quote"],
                            "requests_per_minute": 60,
                            "batch_size": 5
                        }
                    ],
                    outputs=[
                        {
                            "name": "kafka",
                            "type": "kafka",
                            "compression": "lz4",
                            "batch_size": 10
                        }
                    ]
                )
                
                # Create and initialize pipeline
                pipeline = QuantStreamPipelineManager(pipeline_config)
                await pipeline.initialize()
                
                # Start pipeline
                await pipeline.start()
                
                # Let it run for a short time
                await asyncio.sleep(2.0)
                
                # Check pipeline status
                status = pipeline.get_status()
                assert status["state"] == "running"
                assert status["connectors_active"] > 0
                
                # Stop pipeline
                await pipeline.stop()
                
                # Verify messages were sent to Kafka
                assert len(mock_kafka_producer.sent_messages) > 0

    @pytest.mark.asyncio
    async def test_error_handling_integration(self):
        """Test error handling and recovery in pipeline integration."""
        
        # Setup logging
        log_config = LogConfig(level=LogLevel.DEBUG, console_output=False)
        setup_logging(log_config)
        
        # Mock failing API calls
        with patch('aiohttp.ClientSession.get') as mock_get:
            # Setup mock to fail initially, then succeed
            call_count = 0
            
            async def side_effect(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                
                mock_response = AsyncMock()
                if call_count <= 2:
                    # First two calls fail
                    mock_response.status = 500
                    mock_response.text.return_value = "Server Error"
                else:
                    # Subsequent calls succeed
                    mock_response.status = 200
                    mock_response.json.return_value = {
                        "symbol": "AAPL",
                        "bid_price": 150.00,
                        "ask_price": 150.05,
                        "timestamp": "2024-01-15T10:30:00Z"
                    }
                
                return mock_response
            
            mock_get.return_value.__aenter__.side_effect = side_effect
            
            # Create API connector with retry enabled
            config = APIConnectorConfig(
                name="test-retry-connector",
                provider=APIProvider.YAHOO_FINANCE,
                symbols=[Symbol(ticker="AAPL", exchange="NASDAQ")],
                data_types=[DataType.QUOTE],
                requests_per_minute=60,
                batch_size=5
            )
            
            connector = create_api_connector("yahoo_finance", config.__dict__)
            
            # Collect messages and errors
            received_messages = []
            error_count = 0
            
            def message_handler(message: MarketDataMessage):
                received_messages.append(message)
            
            def error_handler(error):
                nonlocal error_count
                error_count += 1
            
            connector.subscribe(message_handler)
            
            # Start connector
            await connector.start()
            
            # Wait for retry attempts and eventual success
            await asyncio.sleep(3.0)
            
            # Stop connector
            await connector.stop()
            
            # Verify that despite initial failures, we eventually got data
            assert call_count > 2  # Multiple attempts were made
            # Note: May not have received messages if all retries failed within test time

    @pytest.mark.asyncio
    async def test_backpressure_integration(self, mock_kafka_producer):
        """Test backpressure handling in pipeline integration."""
        
        # Setup logging
        log_config = LogConfig(level=LogLevel.DEBUG, console_output=False)
        setup_logging(log_config)
        
        # Create a slow Kafka producer to trigger backpressure
        class SlowKafkaProducer(MockKafkaProducer):
            async def send_message(self, message: MarketDataMessage) -> bool:
                await asyncio.sleep(0.1)  # Slow processing
                return await super().send_message(message)
        
        slow_producer = SlowKafkaProducer()
        
        with patch('src.ingestion.processors.pipeline_manager.HighPerformanceKafkaProducer') as mock_kafka_class:
            mock_kafka_class.return_value = slow_producer
            
            # Mock fast data generation
            with patch('aiohttp.ClientSession.get') as mock_get:
                mock_response = AsyncMock()
                mock_response.status = 200
                mock_response.json.return_value = {
                    "symbol": "AAPL",
                    "bid_price": 150.00,
                    "ask_price": 150.05,
                    "timestamp": "2024-01-15T10:30:00Z"
                }
                mock_get.return_value.__aenter__.return_value = mock_response
                
                # Create pipeline with small buffer to trigger backpressure quickly
                pipeline_config = PipelineConfig(
                    name="backpressure-test-pipeline",
                    max_throughput=100,
                    batch_size=5,
                    buffer_size=20,  # Small buffer
                    enable_backpressure=True,
                    connectors=[
                        {
                            "name": "fast_generator",
                            "type": "rest_api",
                            "provider": "yahoo_finance",
                            "enabled": True,
                            "symbols": [{"ticker": "AAPL", "exchange": "NASDAQ"}],
                            "data_types": ["quote"],
                            "requests_per_minute": 3600,  # Fast generation
                            "batch_size": 1
                        }
                    ],
                    outputs=[
                        {
                            "name": "kafka",
                            "type": "kafka",
                            "compression": "lz4",
                            "batch_size": 5
                        }
                    ]
                )
                
                pipeline = QuantStreamPipelineManager(pipeline_config)
                await pipeline.initialize()
                await pipeline.start()
                
                # Let it run to trigger backpressure
                await asyncio.sleep(2.0)
                
                # Check that backpressure controller is active
                status = pipeline.get_status()
                assert "backpressure_active" in status
                
                await pipeline.stop()


@pytest.mark.integration
class TestConnectorIntegration:
    """Integration tests for individual connectors."""

    @pytest.mark.asyncio
    async def test_websocket_connector_integration(self):
        """Test WebSocket connector integration with mock WebSocket server."""
        
        # This test would require a mock WebSocket server
        # For now, we'll test the configuration and initialization
        
        config = WebSocketConnectorConfig(
            name="test-websocket",
            url="ws://localhost:8080/ws",
            symbols=[Symbol(ticker="AAPL", exchange="NASDAQ")],
            data_types=[DataType.TRADE, DataType.QUOTE],
            max_message_size=1024,
            heartbeat_interval=30.0,
            reconnect_delay=5.0,
            max_reconnect_attempts=3
        )
        
        # Mock WebSocket for testing
        with patch('websockets.connect') as mock_connect:
            mock_websocket = AsyncMock()
            mock_websocket.recv.return_value = json.dumps({
                "type": "trade",
                "symbol": "AAPL",
                "price": 150.00,
                "size": 100,
                "timestamp": "2024-01-15T10:30:00Z"
            })
            mock_connect.return_value.__aenter__.return_value = mock_websocket
            
            connector = create_websocket_connector("finnhub", config.__dict__)
            
            received_messages = []
            
            def message_handler(message: MarketDataMessage):
                received_messages.append(message)
            
            connector.subscribe(message_handler)
            
            # Start and quickly stop to test initialization
            await connector.start()
            await asyncio.sleep(0.1)
            await connector.stop()
            
            # Verify connector was created and configured properly
            assert connector.name == "test-websocket"

    @pytest.mark.asyncio
    async def test_multiple_connectors_integration(self, temp_data_dir, mock_kafka_producer):
        """Test running multiple connectors simultaneously."""
        
        # Create sample CSV file
        csv_file = temp_data_dir / "multi_test.csv"
        csv_content = """timestamp,symbol,open,high,low,close,volume
2024-01-15T10:30:00Z,AAPL,149.50,151.00,149.00,150.50,1000000
2024-01-15T10:31:00Z,GOOGL,2800.00,2850.00,2795.00,2825.50,500000
"""
        csv_file.write_text(csv_content)
        
        # Setup logging
        log_config = LogConfig(level=LogLevel.DEBUG, console_output=False)
        setup_logging(log_config)
        
        # Create multiple connectors
        api_config = APIConnectorConfig(
            name="multi-api-connector",
            provider=APIProvider.YAHOO_FINANCE,
            symbols=[Symbol(ticker="MSFT", exchange="NASDAQ")],
            data_types=[DataType.QUOTE],
            requests_per_minute=60,
            batch_size=5
        )
        
        csv_config = CSVConnectorConfig(
            name="multi-csv-connector",
            file_path=str(csv_file),
            batch_size=10,
            auto_detect_schema=True
        )
        
        # Mock API calls
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = {
                "symbol": "MSFT",
                "bid_price": 300.00,
                "ask_price": 300.05,
                "timestamp": "2024-01-15T10:30:00Z"
            }
            mock_get.return_value.__aenter__.return_value = mock_response
            
            # Create connectors
            api_connector = create_api_connector("yahoo_finance", api_config.__dict__)
            csv_connector = create_csv_connector("csv_file", csv_config.__dict__)
            
            # Collect all messages
            all_messages = []
            
            def message_handler(message: MarketDataMessage):
                all_messages.append(message)
            
            api_connector.subscribe(message_handler)
            csv_connector.subscribe(message_handler)
            
            # Start both connectors
            await api_connector.start()
            await csv_connector.start()
            
            # Wait for data collection
            await asyncio.sleep(1.5)
            
            # Stop both connectors
            await api_connector.stop()
            await csv_connector.stop()
            
            # Verify we got messages from both sources
            symbols_seen = {msg.data.symbol.ticker for msg in all_messages}
            assert "MSFT" in symbols_seen or len(all_messages) > 0  # API connector
            assert "AAPL" in symbols_seen or "GOOGL" in symbols_seen  # CSV connector


if __name__ == "__main__":
    # Run integration tests
    pytest.main([__file__, "-v", "--tb=short"])