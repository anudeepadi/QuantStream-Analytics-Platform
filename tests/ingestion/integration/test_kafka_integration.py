#!/usr/bin/env python3
"""
Kafka integration tests for the QuantStream Analytics Platform.

These tests verify Kafka producer functionality and message serialization
with a real or mocked Kafka cluster.
"""

import asyncio
import json
import pytest
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from typing import List, Dict, Any

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from src.ingestion.processors.kafka_producer import (
    HighPerformanceKafkaProducer, KafkaProducerConfig, PartitionStrategy
)
from src.ingestion.models import (
    Symbol, DataType, Quote, Trade, Bar, MarketDataMessage, AssetClass
)
from src.ingestion.utils import setup_logging, LogConfig, LogLevel


class MockKafkaProducerClient:
    """Mock Kafka producer client for testing."""
    
    def __init__(self):
        self.sent_messages = []
        self.partitions = {}
        self.is_running = False
        
    async def send_and_wait(self, topic: str, value: bytes, key: bytes = None, partition: int = None):
        """Mock send and wait."""
        message = {
            'topic': topic,
            'value': value,
            'key': key,
            'partition': partition,
            'timestamp': time.time()
        }
        self.sent_messages.append(message)
        return message
        
    async def send(self, topic: str, value: bytes, key: bytes = None, partition: int = None):
        """Mock send."""
        return await self.send_and_wait(topic, value, key, partition)
        
    def flush(self, timeout: float = None):
        """Mock flush."""
        pass
        
    def close(self):
        """Mock close."""
        self.is_running = False


@pytest.fixture
async def mock_kafka_client():
    """Fixture providing a mock Kafka client."""
    return MockKafkaProducerClient()


@pytest.fixture
async def kafka_config():
    """Fixture providing Kafka producer configuration."""
    return KafkaProducerConfig(
        bootstrap_servers=["localhost:9092"],
        topic_prefix="test_",
        compression_type="lz4",
        batch_size=100,
        linger_ms=10,
        acks="all",
        retries=3,
        partition_strategy=PartitionStrategy.SYMBOL_BASED,
        enable_dead_letter_queue=True,
        dead_letter_topic="test_dead_letter"
    )


@pytest.mark.integration
@pytest.mark.kafka
class TestKafkaProducerIntegration:
    """Integration tests for Kafka producer."""

    @pytest.mark.asyncio
    async def test_kafka_producer_initialization(self, kafka_config, mock_kafka_client):
        """Test Kafka producer initialization and configuration."""
        
        with patch('src.ingestion.processors.kafka_producer.AIOKafkaProducer') as mock_producer_class:
            mock_producer_class.return_value = mock_kafka_client
            
            producer = HighPerformanceKafkaProducer(kafka_config)
            
            # Initialize producer
            await producer.start()
            
            assert producer.is_healthy()
            assert producer._client == mock_kafka_client
            
            # Cleanup
            await producer.stop()

    @pytest.mark.asyncio
    async def test_single_message_sending(self, kafka_config, mock_kafka_client):
        """Test sending a single message to Kafka."""
        
        with patch('src.ingestion.processors.kafka_producer.AIOKafkaProducer') as mock_producer_class:
            mock_producer_class.return_value = mock_kafka_client
            
            producer = HighPerformanceKafkaProducer(kafka_config)
            await producer.start()
            
            # Create test message
            quote = Quote(
                symbol=Symbol(ticker="AAPL", exchange="NASDAQ", asset_class=AssetClass.STOCK),
                bid_price=150.00,
                ask_price=150.05,
                bid_size=100,
                ask_size=200,
                timestamp=datetime.now(timezone.utc)
            )
            
            message = MarketDataMessage(
                data_type=DataType.QUOTE,
                data=quote,
                source="test_source",
                received_at=datetime.now(timezone.utc)
            )
            
            # Send message
            success = await producer.send_message(message)
            
            assert success
            assert len(mock_kafka_client.sent_messages) == 1
            
            sent_msg = mock_kafka_client.sent_messages[0]
            assert sent_msg['topic'] == 'test_market_data_quotes'
            assert sent_msg['key'] is not None  # Symbol-based key
            
            # Verify message content
            sent_data = json.loads(sent_msg['value'].decode('utf-8'))
            assert sent_data['data_type'] == 'quote'
            assert sent_data['data']['symbol']['ticker'] == 'AAPL'
            assert sent_data['data']['bid_price'] == 150.00
            
            await producer.stop()

    @pytest.mark.asyncio
    async def test_batch_message_sending(self, kafka_config, mock_kafka_client):
        """Test sending multiple messages in batch to Kafka."""
        
        with patch('src.ingestion.processors.kafka_producer.AIOKafkaProducer') as mock_producer_class:
            mock_producer_class.return_value = mock_kafka_client
            
            producer = HighPerformanceKafkaProducer(kafka_config)
            await producer.start()
            
            # Create test messages
            messages = []
            symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]
            
            for i, ticker in enumerate(symbols):
                quote = Quote(
                    symbol=Symbol(ticker=ticker, exchange="NASDAQ", asset_class=AssetClass.STOCK),
                    bid_price=100.00 + i,
                    ask_price=100.05 + i,
                    bid_size=100,
                    ask_size=200,
                    timestamp=datetime.now(timezone.utc)
                )
                
                message = MarketDataMessage(
                    data_type=DataType.QUOTE,
                    data=quote,
                    source="test_source",
                    received_at=datetime.now(timezone.utc)
                )
                messages.append(message)
            
            # Send batch
            sent_count = await producer.send_batch(messages)
            
            assert sent_count == len(symbols)
            assert len(mock_kafka_client.sent_messages) == len(symbols)
            
            # Verify all messages were sent to correct topic
            for sent_msg in mock_kafka_client.sent_messages:
                assert sent_msg['topic'] == 'test_market_data_quotes'
                sent_data = json.loads(sent_msg['value'].decode('utf-8'))
                assert sent_data['data_type'] == 'quote'
                assert sent_data['data']['symbol']['ticker'] in symbols
            
            await producer.stop()

    @pytest.mark.asyncio
    async def test_different_data_types_routing(self, kafka_config, mock_kafka_client):
        """Test that different data types are routed to correct topics."""
        
        with patch('src.ingestion.processors.kafka_producer.AIOKafkaProducer') as mock_producer_class:
            mock_producer_class.return_value = mock_kafka_client
            
            producer = HighPerformanceKafkaProducer(kafka_config)
            await producer.start()
            
            symbol = Symbol(ticker="AAPL", exchange="NASDAQ", asset_class=AssetClass.STOCK)
            
            # Create different message types
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
            
            # Create messages
            quote_msg = MarketDataMessage(DataType.QUOTE, quote, "test", datetime.now(timezone.utc))
            trade_msg = MarketDataMessage(DataType.TRADE, trade, "test", datetime.now(timezone.utc))
            bar_msg = MarketDataMessage(DataType.BAR, bar, "test", datetime.now(timezone.utc))
            
            # Send messages
            await producer.send_message(quote_msg)
            await producer.send_message(trade_msg)
            await producer.send_message(bar_msg)
            
            # Verify routing to different topics
            sent_topics = {msg['topic'] for msg in mock_kafka_client.sent_messages}
            expected_topics = {
                'test_market_data_quotes',
                'test_market_data_trades', 
                'test_market_data_bars'
            }
            
            assert sent_topics == expected_topics
            
            await producer.stop()

    @pytest.mark.asyncio
    async def test_partition_strategy(self, kafka_config, mock_kafka_client):
        """Test symbol-based partitioning strategy."""
        
        with patch('src.ingestion.processors.kafka_producer.AIOKafkaProducer') as mock_producer_class:
            mock_producer_class.return_value = mock_kafka_client
            
            producer = HighPerformanceKafkaProducer(kafka_config)
            await producer.start()
            
            # Create messages for different symbols
            symbols = ["AAPL", "GOOGL", "AAPL", "MSFT", "AAPL"]  # AAPL appears 3 times
            messages = []
            
            for ticker in symbols:
                quote = Quote(
                    symbol=Symbol(ticker=ticker, exchange="NASDAQ"),
                    bid_price=150.00,
                    ask_price=150.05,
                    bid_size=100,
                    ask_size=200,
                    timestamp=datetime.now(timezone.utc)
                )
                
                message = MarketDataMessage(
                    data_type=DataType.QUOTE,
                    data=quote,
                    source="test_source",
                    received_at=datetime.now(timezone.utc)
                )
                messages.append(message)
            
            # Send messages
            for message in messages:
                await producer.send_message(message)
            
            # Verify that messages with same symbol use same key
            aapl_keys = [
                msg['key'] for msg in mock_kafka_client.sent_messages 
                if 'AAPL' in msg['value'].decode('utf-8')
            ]
            
            # All AAPL messages should have the same key (for partitioning)
            assert len(set(aapl_keys)) == 1  # All keys should be identical
            
            await producer.stop()

    @pytest.mark.asyncio
    async def test_error_handling_and_dead_letter_queue(self, kafka_config, mock_kafka_client):
        """Test error handling and dead letter queue functionality."""
        
        # Configure mock to fail on certain messages
        original_send = mock_kafka_client.send_and_wait
        
        async def failing_send(topic, value, key=None, partition=None):
            # Fail if message contains "FAIL"
            if b"FAIL" in value:
                raise Exception("Simulated Kafka send failure")
            return await original_send(topic, value, key, partition)
        
        mock_kafka_client.send_and_wait = failing_send
        
        with patch('src.ingestion.processors.kafka_producer.AIOKafkaProducer') as mock_producer_class:
            mock_producer_class.return_value = mock_kafka_client
            
            producer = HighPerformanceKafkaProducer(kafka_config)
            await producer.start()
            
            # Create normal message
            good_quote = Quote(
                symbol=Symbol(ticker="AAPL", exchange="NASDAQ"),
                bid_price=150.00,
                ask_price=150.05,
                bid_size=100,
                ask_size=200,
                timestamp=datetime.now(timezone.utc)
            )
            
            # Create message that will fail
            bad_quote = Quote(
                symbol=Symbol(ticker="FAIL", exchange="NASDAQ"),
                bid_price=150.00,
                ask_price=150.05,
                bid_size=100,
                ask_size=200,
                timestamp=datetime.now(timezone.utc)
            )
            
            good_msg = MarketDataMessage(DataType.QUOTE, good_quote, "test", datetime.now(timezone.utc))
            bad_msg = MarketDataMessage(DataType.QUOTE, bad_quote, "test", datetime.now(timezone.utc))
            
            # Send messages
            good_result = await producer.send_message(good_msg)
            bad_result = await producer.send_message(bad_msg)
            
            assert good_result is True  # Good message should succeed
            assert bad_result is False  # Bad message should fail
            
            # Check that good message was sent normally and bad message went to DLQ
            normal_messages = [msg for msg in mock_kafka_client.sent_messages if msg['topic'] == 'test_market_data_quotes']
            dlq_messages = [msg for msg in mock_kafka_client.sent_messages if msg['topic'] == 'test_dead_letter']
            
            assert len(normal_messages) == 1  # Good message
            assert len(dlq_messages) == 1     # Bad message in DLQ
            
            await producer.stop()

    @pytest.mark.asyncio
    async def test_high_throughput_performance(self, kafka_config, mock_kafka_client):
        """Test high throughput message sending performance."""
        
        with patch('src.ingestion.processors.kafka_producer.AIOKafkaProducer') as mock_producer_class:
            mock_producer_class.return_value = mock_kafka_client
            
            producer = HighPerformanceKafkaProducer(kafka_config)
            await producer.start()
            
            # Create a large batch of messages
            num_messages = 1000
            messages = []
            
            for i in range(num_messages):
                quote = Quote(
                    symbol=Symbol(ticker=f"STOCK{i%10}", exchange="NASDAQ"),
                    bid_price=100.00 + (i % 100),
                    ask_price=100.05 + (i % 100),
                    bid_size=100,
                    ask_size=200,
                    timestamp=datetime.now(timezone.utc)
                )
                
                message = MarketDataMessage(
                    data_type=DataType.QUOTE,
                    data=quote,
                    source="test_source",
                    received_at=datetime.now(timezone.utc)
                )
                messages.append(message)
            
            # Measure sending time
            start_time = time.time()
            sent_count = await producer.send_batch(messages)
            end_time = time.time()
            
            duration = end_time - start_time
            throughput = sent_count / duration if duration > 0 else float('inf')
            
            assert sent_count == num_messages
            assert len(mock_kafka_client.sent_messages) == num_messages
            
            # Should achieve reasonable throughput (this is with mock, so should be very fast)
            assert throughput > 1000  # Should handle at least 1000 messages/second
            
            await producer.stop()

    @pytest.mark.asyncio
    async def test_connection_resilience(self, kafka_config):
        """Test Kafka producer connection resilience and recovery."""
        
        # Create a mock that simulates connection issues
        class UnreliableKafkaClient:
            def __init__(self):
                self.connection_attempts = 0
                self.is_connected = False
                self.sent_messages = []
                
            async def send_and_wait(self, topic, value, key=None, partition=None):
                if not self.is_connected:
                    raise Exception("Not connected to Kafka")
                    
                self.sent_messages.append({
                    'topic': topic, 'value': value, 'key': key, 'partition': partition
                })
                return {'topic': topic, 'value': value}
                
            async def start(self):
                self.connection_attempts += 1
                # Fail first two connection attempts
                if self.connection_attempts <= 2:
                    raise Exception("Connection failed")
                self.is_connected = True
                
            async def stop(self):
                self.is_connected = False
                
            def close(self):
                self.is_connected = False
                
            def flush(self, timeout=None):
                pass
        
        unreliable_client = UnreliableKafkaClient()
        
        with patch('src.ingestion.processors.kafka_producer.AIOKafkaProducer') as mock_producer_class:
            mock_producer_class.return_value = unreliable_client
            
            producer = HighPerformanceKafkaProducer(kafka_config)
            
            # This should eventually succeed after retries
            await producer.start()
            
            assert unreliable_client.is_connected
            assert unreliable_client.connection_attempts >= 3
            
            await producer.stop()


if __name__ == "__main__":
    # Run Kafka integration tests
    pytest.main([__file__, "-v", "--tb=short", "-m", "kafka"])