"""
Pipeline manager for orchestrating the complete market data ingestion pipeline.

This module provides the main orchestration layer that connects data connectors,
processors, and output destinations in a configurable and scalable pipeline.
"""

import asyncio
import signal
from typing import Dict, Any, List, Optional, Callable, Set
from dataclasses import dataclass, field
from enum import Enum
import logging
from datetime import datetime
import json
import threading

from ..connectors import (
    BaseConnector, ConnectorManager, create_api_connector, create_websocket_connector
)
from ..models import MarketData, MarketDataMessage, DataType
from .data_processor import (
    ProcessorPipeline, ValidationProcessor, TransformationProcessor,
    EnrichmentProcessor, AggregationProcessor, ProcessorConfig
)
from .kafka_producer import HighPerformanceKafkaProducer, KafkaProducerConfig
from ..utils import (
    get_logger, MetricRegistry, QuantStreamConfig, ConfigManager,
    counter, gauge, timer
)


class PipelineState(Enum):
    """Pipeline execution states."""
    INITIALIZING = "initializing"
    STARTING = "starting"
    RUNNING = "running"
    PAUSING = "pausing"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class PipelineConfig:
    """Configuration for the ingestion pipeline."""
    name: str = "quantstream-pipeline"
    max_throughput: int = 100000  # messages per second
    batch_size: int = 100
    buffer_size: int = 10000
    processing_timeout: float = 30.0
    enable_backpressure: bool = True
    backpressure_threshold: float = 0.8
    enable_dead_letter_queue: bool = True
    enable_metrics: bool = True
    health_check_interval: float = 30.0
    graceful_shutdown_timeout: float = 60.0
    connectors: List[Dict[str, Any]] = field(default_factory=list)
    processors: List[Dict[str, Any]] = field(default_factory=list)
    outputs: List[Dict[str, Any]] = field(default_factory=list)


class BackpressureController:
    """Controls backpressure in the pipeline."""
    
    def __init__(self, threshold: float = 0.8, max_buffer_size: int = 10000):
        self.threshold = threshold
        self.max_buffer_size = max_buffer_size
        self.current_load = 0.0
        self.is_active = False
        self._lock = threading.Lock()
    
    def update_load(self, current_buffer_size: int):
        """Update current load metrics."""
        with self._lock:
            self.current_load = current_buffer_size / self.max_buffer_size
            self.is_active = self.current_load >= self.threshold
    
    def should_apply_backpressure(self) -> bool:
        """Check if backpressure should be applied."""
        with self._lock:
            return self.is_active
    
    def get_delay(self) -> float:
        """Get backpressure delay in seconds."""
        if not self.is_active:
            return 0.0
        
        # Exponential backoff based on load
        excess_load = max(0, self.current_load - self.threshold)
        return min(1.0, excess_load * 2.0)  # Max 1 second delay


class PipelineMetrics:
    """Comprehensive pipeline metrics collection."""
    
    def __init__(self, pipeline_name: str):
        self.registry = MetricRegistry(f"pipeline_{pipeline_name}")
        self._setup_metrics()
    
    def _setup_metrics(self):
        """Setup pipeline metrics."""
        # Throughput metrics
        self.messages_ingested = self.registry.counter(
            "messages_ingested_total", "Total messages ingested"
        )
        self.messages_processed = self.registry.counter(
            "messages_processed_total", "Total messages processed"
        )
        self.messages_output = self.registry.counter(
            "messages_output_total", "Total messages sent to output"
        )
        self.messages_dropped = self.registry.counter(
            "messages_dropped_total", "Total messages dropped"
        )
        
        # Performance metrics
        self.processing_latency = self.registry.histogram(
            "processing_latency_seconds", "End-to-end processing latency"
        )
        self.throughput = self.registry.gauge(
            "current_throughput", "Current throughput (messages/second)"
        )
        
        # Resource metrics
        self.buffer_size = self.registry.gauge(
            "buffer_size", "Current buffer size"
        )
        self.active_connectors = self.registry.gauge(
            "active_connectors", "Number of active connectors"
        )
        self.backpressure_active = self.registry.gauge(
            "backpressure_active", "Backpressure activation flag"
        )
        
        # Error metrics
        self.connector_errors = self.registry.counter(
            "connector_errors_total", "Total connector errors"
        )
        self.processor_errors = self.registry.counter(
            "processor_errors_total", "Total processor errors"
        )
        self.output_errors = self.registry.counter(
            "output_errors_total", "Total output errors"
        )
    
    def record_message_ingested(self, connector_name: str):
        """Record message ingestion."""
        self.messages_ingested.record(labels={"connector": connector_name})
    
    def record_message_processed(self, processor_name: str):
        """Record message processing."""
        self.messages_processed.record(labels={"processor": processor_name})
    
    def record_message_output(self, output_name: str):
        """Record message output."""
        self.messages_output.record(labels={"output": output_name})
    
    def record_message_dropped(self, reason: str):
        """Record message drop."""
        self.messages_dropped.record(labels={"reason": reason})
    
    def record_processing_latency(self, latency_seconds: float):
        """Record processing latency."""
        self.processing_latency.record(latency_seconds)
    
    def update_throughput(self, throughput: float):
        """Update current throughput."""
        self.throughput.record(throughput)
    
    def update_buffer_size(self, size: int):
        """Update buffer size."""
        self.buffer_size.record(size)
    
    def update_active_connectors(self, count: int):
        """Update active connector count."""
        self.active_connectors.record(count)
    
    def update_backpressure_status(self, active: bool):
        """Update backpressure status."""
        self.backpressure_active.record(1 if active else 0)
    
    def record_connector_error(self, connector_name: str):
        """Record connector error."""
        self.connector_errors.record(labels={"connector": connector_name})
    
    def record_processor_error(self, processor_name: str):
        """Record processor error."""
        self.processor_errors.record(labels={"processor": processor_name})
    
    def record_output_error(self, output_name: str):
        """Record output error."""
        self.output_errors.record(labels={"output": output_name})
    
    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary."""
        return {
            "messages_ingested": self.messages_ingested.get_value(),
            "messages_processed": self.messages_processed.get_value(),
            "messages_output": self.messages_output.get_value(),
            "messages_dropped": self.messages_dropped.get_value(),
            "processing_latency": self.processing_latency.get_value(),
            "current_throughput": self.throughput.get_value(),
            "buffer_size": self.buffer_size.get_value(),
            "active_connectors": self.active_connectors.get_value(),
            "backpressure_active": self.backpressure_active.get_value(),
            "total_errors": {
                "connector": self.connector_errors.get_value(),
                "processor": self.processor_errors.get_value(),
                "output": self.output_errors.get_value()
            }
        }


class QuantStreamPipelineManager:
    """Main pipeline manager for QuantStream market data ingestion."""
    
    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or PipelineConfig()
        self.state = PipelineState.INITIALIZING
        self.logger = get_logger(self.__class__.__name__)
        
        # Core components
        self.connector_manager = ConnectorManager()
        self.processor_pipeline: Optional[ProcessorPipeline] = None
        self.kafka_producer: Optional[HighPerformanceKafkaProducer] = None
        
        # Pipeline control
        self.message_buffer = asyncio.Queue(maxsize=self.config.buffer_size)
        self.backpressure_controller = BackpressureController(
            threshold=self.config.backpressure_threshold,
            max_buffer_size=self.config.buffer_size
        )
        
        # Metrics and monitoring
        self.metrics = PipelineMetrics(self.config.name)
        self.last_throughput_check = datetime.utcnow()
        self.message_count_since_last_check = 0
        
        # Background tasks
        self._tasks: List[asyncio.Task] = []
        self._stop_event = asyncio.Event()
        
        # Signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
        self.logger.info(f"Pipeline manager initialized: {self.config.name}")
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, initiating shutdown")
            asyncio.create_task(self.stop())
        
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
    
    async def initialize(self):
        """Initialize all pipeline components."""
        self.state = PipelineState.INITIALIZING
        
        try:
            # Load configuration
            config_manager = ConfigManager()
            app_config = config_manager.get_config()
            
            # Initialize connectors
            await self._initialize_connectors(app_config)
            
            # Initialize processors
            await self._initialize_processors()
            
            # Initialize output destinations
            await self._initialize_outputs(app_config)
            
            self.logger.info("Pipeline initialization complete")
            
        except Exception as e:
            self.state = PipelineState.ERROR
            self.logger.error(f"Pipeline initialization failed: {e}")
            raise
    
    async def _initialize_connectors(self, app_config: QuantStreamConfig):
        """Initialize data source connectors."""
        for connector_config in self.config.connectors:
            try:
                connector_type = connector_config.get("type")
                connector_name = connector_config.get("name")
                
                if connector_type == "rest_api":
                    provider = connector_config.get("provider")
                    connector = create_api_connector(provider, connector_config)
                elif connector_type == "websocket":
                    provider = connector_config.get("provider")
                    connector = create_websocket_connector(provider, connector_config)
                else:
                    self.logger.error(f"Unknown connector type: {connector_type}")
                    continue
                
                # Subscribe to connector messages
                connector.subscribe(self._handle_connector_message)
                
                # Register with manager
                self.connector_manager.register_connector(connector)
                
                self.logger.info(f"Initialized connector: {connector_name}")
                
            except Exception as e:
                self.logger.error(f"Failed to initialize connector {connector_config}: {e}")
                self.metrics.record_connector_error(connector_config.get("name", "unknown"))
    
    async def _initialize_processors(self):
        """Initialize data processors."""
        processors = []
        
        # Create default processors if none configured
        if not self.config.processors:
            processors = [
                ValidationProcessor(ProcessorConfig(priority=100)),
                TransformationProcessor(ProcessorConfig(priority=200)),
                EnrichmentProcessor(ProcessorConfig(priority=300)),
                AggregationProcessor(ProcessorConfig(priority=400))
            ]
        else:
            # Create processors from configuration
            for proc_config in self.config.processors:
                # TODO: Implement processor factory
                pass
        
        self.processor_pipeline = ProcessorPipeline(processors)
        self.logger.info(f"Initialized {len(processors)} processors")
    
    async def _initialize_outputs(self, app_config: QuantStreamConfig):
        """Initialize output destinations."""
        # Initialize Kafka producer
        kafka_config = KafkaProducerConfig(
            bootstrap_servers=app_config.kafka.bootstrap_servers,
            compression_type=self.config.outputs[0].get("compression", "lz4") if self.config.outputs else "lz4"
        )
        
        self.kafka_producer = HighPerformanceKafkaProducer(kafka_config)
        await self.kafka_producer.start()
        
        self.logger.info("Initialized output destinations")
    
    async def start(self):
        """Start the pipeline."""
        if self.state != PipelineState.INITIALIZING:
            await self.initialize()
        
        self.state = PipelineState.STARTING
        
        try:
            # Start connectors
            await self.connector_manager.start_all()
            
            # Start background tasks
            self._tasks = [
                asyncio.create_task(self._message_processing_loop()),
                asyncio.create_task(self._metrics_collection_loop()),
                asyncio.create_task(self._health_check_loop()),
                asyncio.create_task(self._backpressure_monitoring_loop())
            ]
            
            self.state = PipelineState.RUNNING
            self.logger.info("Pipeline started successfully")
            
        except Exception as e:
            self.state = PipelineState.ERROR
            self.logger.error(f"Failed to start pipeline: {e}")
            raise
    
    async def stop(self):
        """Stop the pipeline gracefully."""
        if self.state in (PipelineState.STOPPED, PipelineState.STOPPING):
            return
        
        self.state = PipelineState.STOPPING
        self.logger.info("Stopping pipeline...")
        
        try:
            # Signal stop to all tasks
            self._stop_event.set()
            
            # Stop connectors
            await self.connector_manager.stop_all()
            
            # Wait for tasks to complete
            if self._tasks:
                await asyncio.gather(*self._tasks, return_exceptions=True)
            
            # Stop output destinations
            if self.kafka_producer:
                await self.kafka_producer.stop()
            
            # Process remaining messages in buffer
            await self._process_remaining_messages()
            
            self.state = PipelineState.STOPPED
            self.logger.info("Pipeline stopped successfully")
            
        except Exception as e:
            self.state = PipelineState.ERROR
            self.logger.error(f"Error during pipeline shutdown: {e}")
            raise
    
    async def _handle_connector_message(self, message: MarketDataMessage):
        """Handle message from connector."""
        try:
            # Apply backpressure if needed
            if self.config.enable_backpressure and self.backpressure_controller.should_apply_backpressure():
                delay = self.backpressure_controller.get_delay()
                if delay > 0:
                    await asyncio.sleep(delay)
            
            # Add message to buffer
            try:
                await asyncio.wait_for(
                    self.message_buffer.put(message),
                    timeout=1.0
                )
                self.metrics.record_message_ingested(message.data.metadata.source.value)
            except asyncio.TimeoutError:
                self.logger.warning("Message buffer full, dropping message")
                self.metrics.record_message_dropped("buffer_full")
                
        except Exception as e:
            self.logger.error(f"Error handling connector message: {e}")
            self.metrics.record_connector_error("message_handler")
    
    async def _message_processing_loop(self):
        """Main message processing loop."""
        batch = []
        batch_timeout = 0.1  # 100ms batch timeout
        
        while not self._stop_event.is_set():
            try:
                # Collect messages for batch processing
                try:
                    message = await asyncio.wait_for(
                        self.message_buffer.get(),
                        timeout=batch_timeout
                    )
                    batch.append(message)
                    
                    # Update backpressure controller
                    self.backpressure_controller.update_load(self.message_buffer.qsize())
                    self.metrics.update_buffer_size(self.message_buffer.qsize())
                    
                except asyncio.TimeoutError:
                    # Timeout reached, process current batch if any
                    pass
                
                # Process batch when full or timeout reached
                if len(batch) >= self.config.batch_size or (batch and len(batch) > 0):
                    await self._process_message_batch(batch)
                    batch = []
                    
            except Exception as e:
                self.logger.error(f"Error in message processing loop: {e}")
                await asyncio.sleep(1)  # Brief pause on error
    
    async def _process_message_batch(self, messages: List[MarketDataMessage]):
        """Process a batch of messages through the pipeline."""
        start_time = datetime.utcnow()
        
        try:
            # Extract market data from messages
            market_data_batch = [msg.data for msg in messages]
            
            # Process through pipeline
            if self.processor_pipeline:
                processed_data = await self.processor_pipeline.process_batch(market_data_batch)
                
                for processed_item in processed_data:
                    if processed_item:
                        self.metrics.record_message_processed("pipeline")
                        
                        # Send to output destinations
                        await self._send_to_outputs(processed_item, messages[0])  # Use first message for routing info
            else:
                # No processing pipeline, send directly
                for message in messages:
                    await self._send_to_outputs(message.data, message)
            
            # Record processing latency
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            self.metrics.record_processing_latency(processing_time)
            
            # Update throughput counter
            self.message_count_since_last_check += len(messages)
            
        except Exception as e:
            self.logger.error(f"Error processing message batch: {e}")
            self.metrics.record_processor_error("batch_processor")
    
    async def _send_to_outputs(self, data: MarketData, original_message: MarketDataMessage):
        """Send processed data to output destinations."""
        try:
            # Send to Kafka
            if self.kafka_producer:
                message = MarketDataMessage(
                    data_type=self._determine_data_type(data),
                    data=data,
                    topic=original_message.topic,
                    partition_key=original_message.partition_key,
                    headers=original_message.headers
                )
                
                success = await self.kafka_producer.send_message(message)
                if success:
                    self.metrics.record_message_output("kafka")
                else:
                    self.metrics.record_output_error("kafka")
            
        except Exception as e:
            self.logger.error(f"Error sending to outputs: {e}")
            self.metrics.record_output_error("general")
    
    def _determine_data_type(self, data: MarketData) -> DataType:
        """Determine data type from market data object."""
        type_mapping = {
            'Quote': DataType.QUOTE,
            'Trade': DataType.TRADE,
            'Bar': DataType.BAR,
            'OrderBook': DataType.ORDER_BOOK,
            'NewsItem': DataType.NEWS,
            'FundamentalData': DataType.FUNDAMENTAL
        }
        return type_mapping.get(type(data).__name__, DataType.QUOTE)
    
    async def _metrics_collection_loop(self):
        """Background metrics collection loop."""
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(10)  # Update every 10 seconds
                
                # Calculate throughput
                now = datetime.utcnow()
                time_diff = (now - self.last_throughput_check).total_seconds()
                
                if time_diff >= 10:  # Calculate every 10 seconds
                    throughput = self.message_count_since_last_check / time_diff
                    self.metrics.update_throughput(throughput)
                    
                    self.message_count_since_last_check = 0
                    self.last_throughput_check = now
                
                # Update connector metrics
                connector_count = len([
                    c for c in self.connector_manager.connectors.values()
                    if c.is_connected
                ])
                self.metrics.update_active_connectors(connector_count)
                
                # Update backpressure status
                self.metrics.update_backpressure_status(
                    self.backpressure_controller.should_apply_backpressure()
                )
                
            except Exception as e:
                self.logger.error(f"Error in metrics collection: {e}")
    
    async def _health_check_loop(self):
        """Background health check loop."""
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(self.config.health_check_interval)
                
                # Check connector health
                health_reports = await self.connector_manager.health_check_all()
                
                unhealthy_connectors = [
                    name for name, report in health_reports.items()
                    if report.get("state") == "error" or not report.get("is_connected", False)
                ]
                
                if unhealthy_connectors:
                    self.logger.warning(f"Unhealthy connectors: {unhealthy_connectors}")
                    for connector_name in unhealthy_connectors:
                        self.metrics.record_connector_error(connector_name)
                
                # Log health summary
                if self.config.enable_metrics:
                    metrics_summary = self.metrics.get_summary()
                    self.logger.info(f"Health check - Throughput: {metrics_summary['current_throughput']:.1f} msg/s, "
                                   f"Buffer: {metrics_summary['buffer_size']}, "
                                   f"Active connectors: {metrics_summary['active_connectors']}")
                
            except Exception as e:
                self.logger.error(f"Error in health check: {e}")
    
    async def _backpressure_monitoring_loop(self):
        """Monitor and adjust backpressure settings."""
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(1)  # Check every second
                
                # Update backpressure controller with current buffer size
                current_buffer_size = self.message_buffer.qsize()
                self.backpressure_controller.update_load(current_buffer_size)
                
            except Exception as e:
                self.logger.error(f"Error in backpressure monitoring: {e}")
    
    async def _process_remaining_messages(self):
        """Process any remaining messages in the buffer during shutdown."""
        remaining_messages = []
        
        try:
            while not self.message_buffer.empty():
                message = self.message_buffer.get_nowait()
                remaining_messages.append(message)
        except asyncio.QueueEmpty:
            pass
        
        if remaining_messages:
            self.logger.info(f"Processing {len(remaining_messages)} remaining messages")
            await self._process_message_batch(remaining_messages)
    
    def get_status(self) -> Dict[str, Any]:
        """Get pipeline status information."""
        return {
            "state": self.state.value,
            "config": {
                "name": self.config.name,
                "max_throughput": self.config.max_throughput,
                "batch_size": self.config.batch_size,
                "buffer_size": self.config.buffer_size
            },
            "metrics": self.metrics.get_summary() if self.config.enable_metrics else {},
            "connectors": len(self.connector_manager.connectors),
            "buffer_utilization": self.message_buffer.qsize() / self.config.buffer_size,
            "backpressure_active": self.backpressure_controller.should_apply_backpressure()
        }
    
    async def pause(self):
        """Pause the pipeline."""
        if self.state == PipelineState.RUNNING:
            self.state = PipelineState.PAUSING
            # TODO: Implement pause logic
            self.state = PipelineState.PAUSED
            self.logger.info("Pipeline paused")
    
    async def resume(self):
        """Resume the paused pipeline."""
        if self.state == PipelineState.PAUSED:
            self.state = PipelineState.RUNNING
            self.logger.info("Pipeline resumed")


# Factory function for creating pipeline from configuration
def create_pipeline_from_config(config_path: str) -> QuantStreamPipelineManager:
    """Create pipeline from configuration file."""
    config_manager = ConfigManager(config_path)
    app_config = config_manager.load_config()
    
    # Create pipeline configuration from app config
    pipeline_config = PipelineConfig(
        name=app_config.app_name,
        connectors=[
            {
                "name": name,
                "type": conn.type,
                "provider": conn.data_source,
                **conn.custom_config
            }
            for name, conn in app_config.connectors.items()
        ]
    )
    
    return QuantStreamPipelineManager(pipeline_config)