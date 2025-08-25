"""
High-performance Kafka producer for market data streaming.

This module provides a robust Kafka producer with features like batching,
compression, partitioning strategies, exactly-once semantics, and comprehensive
error handling for high-throughput market data streaming.
"""

import asyncio
import json
import time
from typing import Dict, Any, List, Optional, Callable, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging
from concurrent.futures import ThreadPoolExecutor
from collections import deque, defaultdict
import pickle
import hashlib

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError, KafkaTimeoutError
    from kafka.partitioner import DefaultPartitioner, RoundRobinPartitioner, Murmur2Partitioner
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

from ..models import (
    MarketData, MarketDataMessage, Quote, Trade, Bar, Symbol,
    DataSource, DataType, MarketDataMetadata
)
from ..models.schemas import MarketDataSerializer
from ..utils import get_logger, MetricRegistry, RetryHandler, RetryConfig


class CompressionType(Enum):
    """Kafka compression types."""
    NONE = "none"
    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"


class AcknowledgmentMode(Enum):
    """Kafka acknowledgment modes."""
    NONE = "0"  # No acknowledgment
    LEADER = "1"  # Leader acknowledgment
    ALL = "-1"  # All replicas acknowledgment


class PartitioningStrategy(Enum):
    """Partitioning strategies."""
    ROUND_ROBIN = "round_robin"
    SYMBOL_HASH = "symbol_hash"
    DATA_TYPE = "data_type"
    CUSTOM = "custom"
    DEFAULT = "default"


@dataclass
class KafkaProducerConfig:
    """Configuration for Kafka producer."""
    bootstrap_servers: List[str] = field(default_factory=lambda: ["localhost:9092"])
    client_id: str = "quantstream-producer"
    compression_type: CompressionType = CompressionType.LZ4
    acks: AcknowledgmentMode = AcknowledgmentMode.ALL
    retries: int = 5
    batch_size: int = 32768  # 32KB
    linger_ms: int = 10
    buffer_memory: int = 33554432  # 32MB
    max_request_size: int = 1048576  # 1MB
    request_timeout_ms: int = 30000
    delivery_timeout_ms: int = 120000
    partitioning_strategy: PartitioningStrategy = PartitioningStrategy.SYMBOL_HASH
    enable_idempotence: bool = True
    max_in_flight_requests_per_connection: int = 5
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_plain_username: Optional[str] = None
    sasl_plain_password: Optional[str] = None
    ssl_cafile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    custom_partitioner: Optional[Callable] = None
    topic_configs: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    def to_kafka_config(self) -> Dict[str, Any]:
        """Convert to Kafka producer configuration."""
        config = {
            "bootstrap_servers": self.bootstrap_servers,
            "client_id": self.client_id,
            "compression_type": self.compression_type.value,
            "acks": self.acks.value,
            "retries": self.retries,
            "batch_size": self.batch_size,
            "linger_ms": self.linger_ms,
            "buffer_memory": self.buffer_memory,
            "max_request_size": self.max_request_size,
            "request_timeout_ms": self.request_timeout_ms,
            "delivery_timeout_ms": self.delivery_timeout_ms,
            "enable_idempotence": self.enable_idempotence,
            "max_in_flight_requests_per_connection": self.max_in_flight_requests_per_connection,
            "security_protocol": self.security_protocol,
        }
        
        # Add SASL configuration if provided
        if self.sasl_mechanism:
            config["sasl_mechanism"] = self.sasl_mechanism
            if self.sasl_plain_username:
                config["sasl_plain_username"] = self.sasl_plain_username
            if self.sasl_plain_password:
                config["sasl_plain_password"] = self.sasl_plain_password
        
        # Add SSL configuration if provided
        if self.ssl_cafile:
            config["ssl_cafile"] = self.ssl_cafile
        if self.ssl_certfile:
            config["ssl_certfile"] = self.ssl_certfile
        if self.ssl_keyfile:
            config["ssl_keyfile"] = self.ssl_keyfile
        
        # Add partitioner
        if self.partitioning_strategy == PartitioningStrategy.ROUND_ROBIN:
            config["partitioner"] = RoundRobinPartitioner()
        elif self.partitioning_strategy == PartitioningStrategy.CUSTOM and self.custom_partitioner:
            config["partitioner"] = self.custom_partitioner
        # DEFAULT strategy uses Kafka's default partitioner
        
        return config


class MessageBatch:
    """Batch of messages for efficient processing."""
    
    def __init__(self, max_size: int = 1000, max_age_seconds: float = 1.0):
        self.max_size = max_size
        self.max_age_seconds = max_age_seconds
        self.messages: List[Tuple[str, bytes, Optional[bytes]]] = []  # topic, key, value
        self.created_at = time.time()
    
    def add_message(self, topic: str, key: Optional[bytes], value: bytes) -> bool:
        """Add message to batch. Returns False if batch is full."""
        if len(self.messages) >= self.max_size:
            return False
        
        self.messages.append((topic, key, value))
        return True
    
    def is_ready(self) -> bool:
        """Check if batch is ready for sending."""
        return (len(self.messages) >= self.max_size or 
                (time.time() - self.created_at) >= self.max_age_seconds)
    
    def size(self) -> int:
        """Get batch size."""
        return len(self.messages)
    
    def age(self) -> float:
        """Get batch age in seconds."""
        return time.time() - self.created_at


class SymbolPartitioner:
    """Custom partitioner based on symbol hash."""
    
    def __init__(self):
        self.partition_cache: Dict[str, int] = {}
    
    def partition(self, topic: str, partition_key: bytes, all_partitions: List[int]) -> int:
        """Partition based on symbol hash."""
        if not partition_key:
            # Fall back to round-robin for messages without keys
            return hash(time.time()) % len(all_partitions)
        
        key_str = partition_key.decode('utf-8') if isinstance(partition_key, bytes) else str(partition_key)
        
        # Use cached partition if available
        if key_str in self.partition_cache:
            return self.partition_cache[key_str]
        
        # Calculate partition using consistent hashing
        partition = hash(key_str) % len(all_partitions)
        self.partition_cache[key_str] = partition
        return partition


class DeadLetterQueue:
    """Dead letter queue for failed messages."""
    
    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self.messages: deque = deque(maxlen=max_size)
        self.metrics = defaultdict(int)
    
    def add_message(self, message: MarketDataMessage, error: str, timestamp: float = None):
        """Add failed message to dead letter queue."""
        if timestamp is None:
            timestamp = time.time()
        
        dlq_entry = {
            "message": message,
            "error": error,
            "timestamp": timestamp,
            "retry_count": getattr(message, '_retry_count', 0)
        }
        
        self.messages.append(dlq_entry)
        self.metrics['total_messages'] += 1
    
    def get_messages(self, max_count: int = 100) -> List[Dict[str, Any]]:
        """Get messages from dead letter queue."""
        messages = []
        for _ in range(min(max_count, len(self.messages))):
            if self.messages:
                messages.append(self.messages.popleft())
        return messages
    
    def size(self) -> int:
        """Get queue size."""
        return len(self.messages)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get dead letter queue metrics."""
        return {
            "current_size": len(self.messages),
            "total_messages": self.metrics['total_messages'],
            "max_size": self.max_size
        }


class HighPerformanceKafkaProducer:
    """High-performance Kafka producer for market data."""
    
    def __init__(self, config: KafkaProducerConfig):
        if not KAFKA_AVAILABLE:
            raise ImportError("kafka-python is required for Kafka producer")
        
        self.config = config
        self.logger = get_logger(self.__class__.__name__)
        self.serializer = MarketDataSerializer()
        
        # Initialize Kafka producer
        kafka_config = config.to_kafka_config()
        self.producer = KafkaProducer(
            value_serializer=lambda v: v if isinstance(v, bytes) else v.encode('utf-8'),
            key_serializer=lambda k: k if isinstance(k, bytes) else str(k).encode('utf-8') if k else None,
            **kafka_config
        )
        
        # Metrics
        self.metrics_registry = MetricRegistry("kafka_producer")
        self._setup_metrics()
        
        # Message batching
        self.batches: Dict[str, MessageBatch] = {}
        self.batch_lock = asyncio.Lock()
        
        # Dead letter queue
        self.dead_letter_queue = DeadLetterQueue()
        
        # Background tasks
        self._batch_sender_task: Optional[asyncio.Task] = None
        self._metrics_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        
        # Thread pool for blocking operations
        self.executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="kafka-producer")
        
        # Retry handler
        retry_config = RetryConfig(max_attempts=3, base_delay=0.1, max_delay=5.0)
        self.retry_handler = RetryHandler(retry_config)
        
        # Custom partitioner setup
        if config.partitioning_strategy == PartitioningStrategy.SYMBOL_HASH:
            self.partitioner = SymbolPartitioner()
        else:
            self.partitioner = None
        
        self.logger.info("Kafka producer initialized")
    
    def _setup_metrics(self):
        """Setup producer metrics."""
        self.sent_counter = self.metrics_registry.counter(
            "messages_sent_total", "Total messages sent to Kafka"
        )
        self.failed_counter = self.metrics_registry.counter(
            "messages_failed_total", "Total messages failed to send"
        )
        self.batch_counter = self.metrics_registry.counter(
            "batches_sent_total", "Total batches sent"
        )
        self.send_timer = self.metrics_registry.timer(
            "send_duration", "Time taken to send messages"
        )
        self.batch_size_gauge = self.metrics_registry.gauge(
            "current_batch_size", "Current batch size"
        )
        self.dlq_size_gauge = self.metrics_registry.gauge(
            "dlq_size", "Dead letter queue size"
        )
    
    async def start(self):
        """Start the producer and background tasks."""
        self._stop_event.clear()
        
        # Start background tasks
        self._batch_sender_task = asyncio.create_task(self._batch_sender_loop())
        self._metrics_task = asyncio.create_task(self._metrics_loop())
        
        self.logger.info("Kafka producer started")
    
    async def stop(self):
        """Stop the producer and cleanup resources."""
        self._stop_event.set()
        
        # Wait for background tasks to complete
        if self._batch_sender_task:
            await self._batch_sender_task
        if self._metrics_task:
            await self._metrics_task
        
        # Send any remaining batches
        await self._flush_all_batches()
        
        # Close Kafka producer
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self.executor, self.producer.close)
        
        # Shutdown executor
        self.executor.shutdown(wait=True)
        
        self.logger.info("Kafka producer stopped")
    
    async def send_message(self, message: MarketDataMessage) -> bool:
        """Send a single message to Kafka."""
        try:
            with self.send_timer.time():
                # Serialize message
                serialized_data = self.serializer.serialize_to_json(message.data)
                
                # Determine topic and partition key
                topic = message.topic or f"market_data_{message.data_type.value}"
                partition_key = self._get_partition_key(message)
                
                # Add to batch or send immediately
                if self.config.batch_size > 1:
                    await self._add_to_batch(topic, partition_key, serialized_data)
                else:
                    await self._send_single_message(topic, partition_key, serialized_data)
                
                self.sent_counter.record()
                return True
                
        except Exception as e:
            self.logger.error(f"Error sending message: {e}")
            self.failed_counter.record()
            self.dead_letter_queue.add_message(message, str(e))
            return False
    
    async def send_batch(self, messages: List[MarketDataMessage]) -> int:
        """Send a batch of messages to Kafka."""
        successful_sends = 0
        
        for message in messages:
            if await self.send_message(message):
                successful_sends += 1
        
        return successful_sends
    
    def _get_partition_key(self, message: MarketDataMessage) -> Optional[str]:
        """Get partition key for message."""
        if message.partition_key:
            return message.partition_key
        
        # Use symbol as partition key for consistent partitioning
        if hasattr(message.data, 'symbol') and message.data.symbol:
            return str(message.data.symbol)
        
        return None
    
    async def _add_to_batch(self, topic: str, key: Optional[str], value: str):
        """Add message to batch for efficient sending."""
        async with self.batch_lock:
            batch_key = topic
            
            if batch_key not in self.batches:
                self.batches[batch_key] = MessageBatch(
                    max_size=self.config.batch_size,
                    max_age_seconds=self.config.linger_ms / 1000.0
                )
            
            batch = self.batches[batch_key]
            key_bytes = key.encode('utf-8') if key else None
            value_bytes = value.encode('utf-8')
            
            if not batch.add_message(topic, key_bytes, value_bytes):
                # Batch is full, send it and create new batch
                await self._send_batch(batch)
                del self.batches[batch_key]
                
                # Create new batch and add message
                new_batch = MessageBatch(
                    max_size=self.config.batch_size,
                    max_age_seconds=self.config.linger_ms / 1000.0
                )
                new_batch.add_message(topic, key_bytes, value_bytes)
                self.batches[batch_key] = new_batch
    
    async def _send_single_message(self, topic: str, key: Optional[str], value: str):
        """Send single message immediately."""
        def _send():
            future = self.producer.send(
                topic=topic,
                key=key,
                value=value,
                partition=self._get_partition(topic, key) if self.partitioner else None
            )
            return future.get(timeout=30)  # 30 second timeout
        
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self.executor, _send)
    
    async def _send_batch(self, batch: MessageBatch):
        """Send a batch of messages."""
        def _send_batch_sync():
            futures = []
            for topic, key, value in batch.messages:
                future = self.producer.send(
                    topic=topic,
                    key=key,
                    value=value,
                    partition=self._get_partition(topic, key) if self.partitioner else None
                )
                futures.append(future)
            
            # Wait for all messages in batch to be sent
            for future in futures:
                future.get(timeout=30)
        
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(self.executor, _send_batch_sync)
            self.batch_counter.record()
            self.logger.debug(f"Sent batch of {batch.size()} messages")
        except Exception as e:
            self.logger.error(f"Error sending batch: {e}")
            raise
    
    def _get_partition(self, topic: str, key: Optional[str]) -> Optional[int]:
        """Get partition for message using custom partitioner."""
        if not self.partitioner or not key:
            return None
        
        # Get topic metadata to determine available partitions
        try:
            metadata = self.producer.partitions_for(topic)
            if metadata:
                partitions = list(metadata)
                return self.partitioner.partition(topic, key.encode('utf-8'), partitions)
        except Exception as e:
            self.logger.error(f"Error getting partition: {e}")
        
        return None
    
    async def _batch_sender_loop(self):
        """Background loop to send batches when they're ready."""
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(0.01)  # Check every 10ms
                
                async with self.batch_lock:
                    ready_batches = []
                    for batch_key, batch in list(self.batches.items()):
                        if batch.is_ready():
                            ready_batches.append((batch_key, batch))
                    
                    # Send ready batches
                    for batch_key, batch in ready_batches:
                        try:
                            await self._send_batch(batch)
                            del self.batches[batch_key]
                        except Exception as e:
                            self.logger.error(f"Error sending ready batch: {e}")
                
            except Exception as e:
                self.logger.error(f"Error in batch sender loop: {e}")
    
    async def _metrics_loop(self):
        """Background loop to update metrics."""
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(10)  # Update every 10 seconds
                
                # Update batch size metrics
                async with self.batch_lock:
                    total_batch_size = sum(batch.size() for batch in self.batches.values())
                    self.batch_size_gauge.record(total_batch_size)
                
                # Update dead letter queue size
                self.dlq_size_gauge.record(self.dead_letter_queue.size())
                
            except Exception as e:
                self.logger.error(f"Error in metrics loop: {e}")
    
    async def _flush_all_batches(self):
        """Flush all pending batches."""
        async with self.batch_lock:
            for batch_key, batch in list(self.batches.items()):
                try:
                    if batch.size() > 0:
                        await self._send_batch(batch)
                except Exception as e:
                    self.logger.error(f"Error flushing batch {batch_key}: {e}")
            
            self.batches.clear()
    
    async def flush(self, timeout: float = 30.0):
        """Flush all pending messages."""
        await self._flush_all_batches()
        
        # Flush Kafka producer
        def _flush():
            self.producer.flush(timeout=timeout)
        
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self.executor, _flush)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get producer metrics."""
        return {
            "messages_sent": self.sent_counter.get_value(),
            "messages_failed": self.failed_counter.get_value(),
            "batches_sent": self.batch_counter.get_value(),
            "send_stats": self.send_timer.get_value(),
            "current_batch_size": self.batch_size_gauge.get_value(),
            "dead_letter_queue": self.dead_letter_queue.get_metrics()
        }
    
    def get_dead_letter_messages(self, max_count: int = 100) -> List[Dict[str, Any]]:
        """Get messages from dead letter queue."""
        return self.dead_letter_queue.get_messages(max_count)


# Topic configuration helpers
def create_topic_configs() -> Dict[str, Dict[str, Any]]:
    """Create recommended topic configurations for market data."""
    return {
        "market_data_quotes": {
            "num_partitions": 12,
            "replication_factor": 3,
            "config": {
                "compression.type": "lz4",
                "cleanup.policy": "delete",
                "retention.ms": 86400000,  # 1 day
                "segment.ms": 3600000,     # 1 hour
            }
        },
        "market_data_trades": {
            "num_partitions": 12,
            "replication_factor": 3,
            "config": {
                "compression.type": "lz4",
                "cleanup.policy": "delete",
                "retention.ms": 604800000,  # 7 days
                "segment.ms": 3600000,      # 1 hour
            }
        },
        "market_data_bars": {
            "num_partitions": 6,
            "replication_factor": 3,
            "config": {
                "compression.type": "lz4",
                "cleanup.policy": "compact,delete",
                "retention.ms": 2592000000,  # 30 days
                "segment.ms": 86400000,      # 1 day
            }
        }
    }


# Example configuration
DEFAULT_KAFKA_CONFIG = KafkaProducerConfig(
    bootstrap_servers=["localhost:9092"],
    compression_type=CompressionType.LZ4,
    batch_size=32768,
    linger_ms=10,
    acks=AcknowledgmentMode.ALL,
    enable_idempotence=True,
    partitioning_strategy=PartitioningStrategy.SYMBOL_HASH
)