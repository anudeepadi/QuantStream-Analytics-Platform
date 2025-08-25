#!/usr/bin/env python3
"""
Basic example of running the QuantStream Analytics Platform ingestion pipeline.

This example shows how to:
1. Set up configuration
2. Create and configure connectors
3. Initialize the pipeline
4. Run data ingestion
5. Monitor performance
"""

import asyncio
import signal
import sys
import os
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.ingestion.processors import (
    QuantStreamPipelineManager, PipelineConfig
)
from src.ingestion.connectors import (
    create_api_connector, APIConnectorConfig, APIProvider
)
from src.ingestion.models import Symbol, DataType
from src.ingestion.utils import (
    setup_logging, LogConfig, LogLevel, get_logger,
    ConfigManager
)


async def create_sample_pipeline():
    """Create a sample pipeline with mock connectors."""
    
    # Setup logging
    log_config = LogConfig(
        level=LogLevel.INFO,
        console_output=True,
        file_output=True,
        file_path="./logs/pipeline.log",
        json_format=True
    )
    setup_logging(log_config)
    
    logger = get_logger("pipeline_example")
    logger.info("Starting QuantStream Pipeline Example")
    
    # Create pipeline configuration
    pipeline_config = PipelineConfig(
        name="example-pipeline",
        max_throughput=10000,
        batch_size=100,
        buffer_size=5000,
        enable_backpressure=True,
        connectors=[
            {
                "name": "yahoo_finance",
                "type": "rest_api",
                "provider": "yahoo_finance",
                "enabled": True,
                "symbols": [
                    {"ticker": "AAPL", "exchange": "NASDAQ"},
                    {"ticker": "GOOGL", "exchange": "NASDAQ"},
                    {"ticker": "MSFT", "exchange": "NASDAQ"}
                ],
                "data_types": ["quote", "bar"],
                "requests_per_minute": 60,
                "batch_size": 10
            }
        ],
        outputs=[
            {
                "name": "kafka",
                "type": "kafka",
                "compression": "lz4",
                "batch_size": 100
            }
        ]
    )
    
    return QuantStreamPipelineManager(pipeline_config)


async def run_pipeline_example():
    """Run the pipeline example."""
    
    # Create pipeline
    pipeline = await create_sample_pipeline()
    logger = get_logger("pipeline_example")
    
    try:
        # Initialize pipeline
        logger.info("Initializing pipeline...")
        await pipeline.initialize()
        
        # Start pipeline
        logger.info("Starting pipeline...")
        await pipeline.start()
        
        logger.info("Pipeline is running. Press Ctrl+C to stop.")
        
        # Monitor pipeline
        while pipeline.state.value == "running":
            await asyncio.sleep(10)  # Check every 10 seconds
            
            # Get status
            status = pipeline.get_status()
            logger.info(f"Pipeline Status: {status}")
            
            # Print metrics if available
            if "metrics" in status and status["metrics"]:
                metrics = status["metrics"]
                logger.info(
                    f"Throughput: {metrics.get('current_throughput', 0):.1f} msg/s, "
                    f"Processed: {metrics.get('messages_processed', 0)}, "
                    f"Buffer: {status.get('buffer_utilization', 0):.1%}"
                )
    
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    
    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
    
    finally:
        # Stop pipeline gracefully
        logger.info("Stopping pipeline...")
        await pipeline.stop()
        logger.info("Pipeline stopped successfully")


async def simple_connector_example():
    """Simple example using just a single connector."""
    
    logger = get_logger("connector_example")
    logger.info("Starting Simple Connector Example")
    
    # Configure a Yahoo Finance connector
    config = APIConnectorConfig(
        name="yahoo-finance-example",
        provider=APIProvider.YAHOO_FINANCE,
        symbols=[
            Symbol(ticker="AAPL", exchange="NASDAQ"),
            Symbol(ticker="GOOGL", exchange="NASDAQ")
        ],
        data_types=[DataType.QUOTE, DataType.BAR],
        requests_per_minute=60,
        batch_size=5
    )
    
    # Create connector
    connector = create_api_connector("yahoo_finance", config.__dict__)
    
    # Message handler
    def handle_message(message):
        logger.info(f"Received: {message.data_type.value} for {message.data.symbol}")
        if hasattr(message.data, 'bid_price') and message.data.bid_price:
            logger.info(f"  Bid: ${message.data.bid_price}")
        if hasattr(message.data, 'ask_price') and message.data.ask_price:
            logger.info(f"  Ask: ${message.data.ask_price}")
    
    # Subscribe to messages
    connector.subscribe(handle_message)
    
    try:
        # Start connector
        await connector.start()
        logger.info("Connector started, fetching data...")
        
        # Run for 30 seconds
        await asyncio.sleep(30)
        
    except Exception as e:
        logger.error(f"Connector error: {e}", exc_info=True)
    
    finally:
        # Stop connector
        await connector.stop()
        logger.info("Connector stopped")


async def performance_monitoring_example():
    """Example showing performance monitoring and metrics."""
    
    from src.ingestion.utils.metrics import get_registry
    
    logger = get_logger("performance_example")
    logger.info("Starting Performance Monitoring Example")
    
    # Get global metrics registry
    registry = get_registry()
    
    # Create some sample metrics
    request_counter = registry.counter("api_requests_total", "Total API requests")
    response_timer = registry.timer("response_time", "API response time")
    queue_gauge = registry.gauge("queue_size", "Current queue size")
    
    # Simulate some activity
    for i in range(100):
        # Simulate API request
        with response_timer.time():
            await asyncio.sleep(0.01)  # Simulate 10ms response time
        
        request_counter.record()
        queue_gauge.record(i % 50)  # Simulate varying queue size
        
        if i % 10 == 0:
            # Print metrics every 10 iterations
            metrics_json = registry.export_json()
            logger.info(f"Metrics update: {metrics_json}")
    
    logger.info("Performance monitoring example completed")


def main():
    """Main function to run examples."""
    
    # Create necessary directories
    os.makedirs("logs", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    
    print("QuantStream Analytics Platform - Examples")
    print("=" * 50)
    print("1. Basic Pipeline Example")
    print("2. Simple Connector Example") 
    print("3. Performance Monitoring Example")
    print("4. Exit")
    
    choice = input("Select example (1-4): ").strip()
    
    if choice == "1":
        print("Running Basic Pipeline Example...")
        asyncio.run(run_pipeline_example())
    elif choice == "2":
        print("Running Simple Connector Example...")
        asyncio.run(simple_connector_example())
    elif choice == "3":
        print("Running Performance Monitoring Example...")
        asyncio.run(performance_monitoring_example())
    elif choice == "4":
        print("Exiting...")
        sys.exit(0)
    else:
        print("Invalid choice. Please select 1-4.")
        main()


if __name__ == "__main__":
    main()