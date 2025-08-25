#!/usr/bin/env python3
"""
Feature Store Example

Demonstrates how to initialize and use the QuantStream Feature Store
for technical indicators and real-time feature serving.
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
import pandas as pd
import numpy as np
import redis.asyncio as redis
from concurrent.futures import ThreadPoolExecutor

# Import feature store components
from src.features.store import (
    FeatureStore, 
    FeatureRegistry, 
    DeltaStorageBackend,
    LineageTracker
)
from src.features.store.feature_metadata import (
    FeatureMetadata,
    FeatureSchema, 
    FeatureType,
    IndicatorCategory
)
from src.features.serving import (
    FeatureServer,
    create_feature_serving_app
)
from src.features.serving.api_models import (
    ServingConfig,
    PerformanceThresholds
)
from src.features.indicators import register_all_technical_indicators
from src.features.utils import FeatureValidator, PerformanceMonitor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def create_sample_data() -> pd.DataFrame:
    """Create sample OHLCV data for demonstration."""
    np.random.seed(42)
    
    # Generate 1000 days of sample data
    dates = pd.date_range(
        start=datetime.now(timezone.utc) - timedelta(days=1000),
        end=datetime.now(timezone.utc),
        freq='D'
    )
    
    # Generate realistic price data
    initial_price = 100.0
    prices = [initial_price]
    
    for i in range(1, len(dates)):
        # Random walk with slight upward bias
        change = np.random.normal(0.001, 0.02)
        new_price = prices[-1] * (1 + change)
        prices.append(max(new_price, 0.01))  # Ensure positive prices
    
    # Generate OHLC from close prices
    data = []
    for i, (date, close) in enumerate(zip(dates, prices)):
        volatility = np.random.uniform(0.005, 0.03)
        
        high = close * (1 + volatility)
        low = close * (1 - volatility)
        open_price = prices[i-1] if i > 0 else close
        volume = int(np.random.uniform(100000, 1000000))
        
        data.append({
            'timestamp': date,
            'entity_id': 'AAPL',
            'symbol': 'AAPL',
            'open': open_price,
            'high': high,
            'low': low,
            'close': close,
            'volume': volume
        })
    
    return pd.DataFrame(data)


async def initialize_feature_store() -> FeatureStore:
    """Initialize the complete feature store system."""
    logger.info("Initializing Feature Store...")
    
    # 1. Initialize Redis connection
    redis_client = redis.Redis(
        host='localhost',
        port=6379,
        db=0,
        decode_responses=False
    )
    
    try:
        await redis_client.ping()
        logger.info("Connected to Redis successfully")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise
    
    # 2. Initialize Delta Lake storage backend
    storage_config = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
    }
    
    executor = ThreadPoolExecutor(max_workers=4)
    storage_backend = DeltaStorageBackend(
        storage_path="/tmp/quantstream_features",
        spark_config=storage_config,
        executor=executor
    )
    
    # 3. Initialize feature registry
    feature_registry = FeatureRegistry(
        redis_client=redis_client,
        registry_prefix="quantstream_features"
    )
    
    # 4. Initialize lineage tracker
    lineage_tracker = LineageTracker(
        redis_client=redis_client,
        lineage_prefix="quantstream_lineage"
    )
    
    # 5. Initialize validator and performance monitor
    validator = FeatureValidator()
    
    thresholds = PerformanceThresholds()
    performance_monitor = PerformanceMonitor(
        redis_client=redis_client,
        thresholds=thresholds
    )
    
    # 6. Create main feature store
    feature_store = FeatureStore(
        storage_backend=storage_backend,
        registry=feature_registry,
        cache_client=redis_client,
        lineage_tracker=lineage_tracker,
        validator=validator,
        performance_monitor=performance_monitor,
        cache_ttl_seconds=3600
    )
    
    logger.info("Feature Store initialized successfully")
    return feature_store


async def register_indicators(feature_store: FeatureStore) -> None:
    """Register all technical indicators with the feature store."""
    logger.info("Registering technical indicators...")
    
    try:
        # Register all built-in technical indicators
        results = await register_all_technical_indicators(feature_store)
        
        success_count = sum(1 for success in results.values() if success)
        total_count = len(results)
        
        logger.info(f"Registered {success_count}/{total_count} technical indicators")
        
        # Log any failures
        for feature_id, success in results.items():
            if not success:
                logger.warning(f"Failed to register indicator: {feature_id}")
    
    except Exception as e:
        logger.error(f"Error registering indicators: {e}")
        raise


async def compute_sample_features(
    feature_store: FeatureStore, 
    data: pd.DataFrame
) -> None:
    """Compute features using sample data."""
    logger.info("Computing sample features...")
    
    try:
        # List of features to compute
        feature_ids = [
            "sma_20", "ema_50", "rsi_14",
            "macd_line_12_26", "bb_upper_20_20",
            "stoch_k_14", "obv", "atr_14"
        ]
        
        # Compute features in batch
        results = await feature_store.batch_compute_features(
            feature_ids=feature_ids,
            input_data=data,
            parallel=True
        )
        
        success_count = sum(1 for result in results.values() if result.success)
        logger.info(f"Computed {success_count}/{len(results)} features successfully")
        
        # Log computation times
        for feature_id, result in results.items():
            if result.success:
                logger.info(
                    f"Feature {feature_id}: {result.computation_time_ms:.2f}ms, "
                    f"{result.metadata.get('output_records', 0)} records"
                )
            else:
                logger.error(f"Failed to compute {feature_id}: {result.error_message}")
    
    except Exception as e:
        logger.error(f"Error computing features: {e}")
        raise


async def demonstrate_feature_serving(feature_store: FeatureStore) -> None:
    """Demonstrate feature serving capabilities."""
    logger.info("Demonstrating feature serving...")
    
    try:
        # 1. Get feature vector for single entity
        feature_vector = await feature_store.get_feature_vector(
            feature_ids=["sma_20", "rsi_14", "macd_line_12_26"],
            entity_id="AAPL",
            use_cache=True
        )
        
        if feature_vector:
            logger.info(f"Retrieved feature vector: {list(feature_vector.keys())}")
            for feature_id, value in feature_vector.items():
                logger.info(f"  {feature_id}: {value}")
        else:
            logger.warning("No feature vector retrieved")
        
        # 2. Get features for multiple entities (if we had multiple entities)
        features_data = await feature_store.get_features(
            feature_ids=["sma_20", "ema_50"],
            entities=["AAPL"],
            use_cache=True
        )
        
        logger.info(f"Retrieved features for entities: {list(features_data.keys())}")
        for feature_id, data in features_data.items():
            if not data.empty:
                logger.info(f"  {feature_id}: {len(data)} records")
        
        # 3. Get feature statistics
        stats = await feature_store.get_feature_statistics("sma_20")
        if stats:
            logger.info(f"SMA-20 statistics: {stats.get('total_records', 0)} records")
        
        # 4. Search features
        search_results = await feature_store.search_features(
            query="moving average",
            limit=5
        )
        
        logger.info(f"Found {len(search_results)} features matching 'moving average'")
        for feature in search_results[:3]:
            logger.info(f"  {feature.feature_id}: {feature.name}")
    
    except Exception as e:
        logger.error(f"Error in feature serving demonstration: {e}")
        raise


async def start_feature_server(feature_store: FeatureStore) -> None:
    """Start the feature serving API server."""
    logger.info("Starting feature serving API...")
    
    try:
        # Configure serving
        serving_config = ServingConfig(
            max_batch_size=1000,
            cache_ttl_seconds=3600,
            response_timeout_ms=5000,
            latency_threshold_ms=50.0
        )
        
        # Create feature server
        feature_server = FeatureServer(
            feature_store=feature_store,
            config=serving_config
        )
        
        # Precompute some popular feature sets
        feature_sets = {
            "momentum": ["rsi_14", "macd_line_12_26", "stoch_k_14"],
            "trend": ["sma_20", "ema_50", "bb_middle_20"],
            "volatility": ["atr_14", "bb_upper_20_20", "bb_lower_20_20"]
        }
        
        await feature_server.precompute_feature_sets(feature_sets)
        
        # Create FastAPI app
        app = create_feature_serving_app(
            feature_server=feature_server,
            feature_store=feature_store,
            config=serving_config
        )
        
        logger.info("Feature serving API is ready")
        logger.info("Available endpoints:")
        logger.info("  POST /features/serve - Serve features for single entity")
        logger.info("  POST /features/serve/batch - Batch feature serving")
        logger.info("  POST /features/search - Search and discover features")
        logger.info("  GET  /health - Health check")
        logger.info("  GET  /metrics - Performance metrics")
        
        # In a real application, you would run the server:
        # import uvicorn
        # uvicorn.run(app, host="0.0.0.0", port=8000)
        
    except Exception as e:
        logger.error(f"Error starting feature server: {e}")
        raise


async def demonstrate_monitoring(feature_store: FeatureStore) -> None:
    """Demonstrate monitoring and alerting capabilities."""
    logger.info("Demonstrating monitoring capabilities...")
    
    try:
        # Get performance monitor
        monitor = feature_store.performance_monitor
        if not monitor:
            logger.warning("Performance monitor not available")
            return
        
        # Start monitoring
        await monitor.start_monitoring(interval_seconds=5)
        
        # Record some sample metrics
        await monitor.record_metric(
            "feature_serving_latency", 25.5, 
            monitor.MetricType.LATENCY,
            {"feature_id": "sma_20", "entity_id": "AAPL"}
        )
        
        await monitor.record_metric(
            "cache_hit_rate", 95.2,
            monitor.MetricType.CACHE_HIT_RATE
        )
        
        # Wait a bit for metrics to be processed
        await asyncio.sleep(2)
        
        # Get metrics summary
        summary = await monitor.get_metric_summary("feature_serving_latency", 30)
        logger.info(f"Latency summary: {summary}")
        
        # Get performance report
        report = await monitor.get_performance_report()
        logger.info(f"Performance report generated with {len(report.get('recommendations', []))} recommendations")
        
        # Get active alerts
        alerts = await monitor.get_active_alerts()
        logger.info(f"Active alerts: {len(alerts)}")
        
    except Exception as e:
        logger.error(f"Error in monitoring demonstration: {e}")


async def main():
    """Main demonstration function."""
    logger.info("Starting QuantStream Feature Store Demo")
    
    try:
        # 1. Initialize feature store
        feature_store = await initialize_feature_store()
        
        # 2. Register technical indicators
        await register_indicators(feature_store)
        
        # 3. Create and use sample data
        sample_data = await create_sample_data()
        logger.info(f"Created sample data: {len(sample_data)} records")
        
        # 4. Compute features
        await compute_sample_features(feature_store, sample_data)
        
        # 5. Demonstrate feature serving
        await demonstrate_feature_serving(feature_store)
        
        # 6. Demonstrate monitoring
        await demonstrate_monitoring(feature_store)
        
        # 7. Set up feature server (but don't run it in this demo)
        await start_feature_server(feature_store)
        
        # 8. Get final metrics
        metrics = await feature_store.get_metrics()
        logger.info(f"Final metrics: {metrics}")
        
        logger.info("Demo completed successfully!")
        logger.info("\nTo run the feature serving API in production:")
        logger.info("  python -m uvicorn examples.feature_store_example:app --host 0.0.0.0 --port 8000")
        
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        raise
    
    finally:
        # Cleanup (in a real application, you'd want proper shutdown)
        logger.info("Demo finished")


if __name__ == "__main__":
    asyncio.run(main())