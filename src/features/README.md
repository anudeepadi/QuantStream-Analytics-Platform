# QuantStream Feature Store

A comprehensive feature store implementation for technical indicators with real-time serving, time-travel capabilities, and advanced monitoring.

## 🚀 Features

### Core Capabilities
- **Real-time Feature Serving** - Sub-50ms latency with Redis caching
- **Technical Indicators** - 25+ built-in indicators (RSI, MACD, Bollinger Bands, etc.)
- **Time-Travel Queries** - Historical point-in-time feature retrieval for backtesting
- **Feature Lineage** - Complete tracking of feature dependencies and computation history
- **Schema Evolution** - Automatic versioning with backward compatibility
- **Delta Lake Storage** - Optimized columnar storage with ACID transactions

### Advanced Features
- **Drift Detection** - Statistical monitoring of feature distribution changes
- **Data Quality** - Comprehensive validation and quality metrics
- **ML Integration** - Seamless model training and inference workflows
- **Performance Monitoring** - Real-time metrics and alerting
- **Feature Discovery** - Search and catalog capabilities

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Serving API   │    │  Feature Store  │    │ Storage Backend │
│   (FastAPI)     │◄──►│      Core       │◄──►│  (Delta Lake)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Redis Cache    │    │Feature Registry │    │  Spark Engine   │
│  (Sub-50ms)     │    │   (Metadata)    │    │ (Optimization)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Monitoring    │    │ Lineage Tracker│    │   Validators    │
│   & Alerting    │    │  & Versioning   │    │ & Quality Check │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🛠️ Components

### 1. Feature Store Core (`src/features/store/`)
- **FeatureStore** - Main orchestration class
- **FeatureRegistry** - Metadata management and discovery
- **DeltaStorageBackend** - Optimized storage layer
- **LineageTracker** - Dependency and computation tracking
- **FeatureMetadata** - Schema and metadata definitions

### 2. Feature Serving (`src/features/serving/`)
- **FeatureServer** - High-performance serving engine
- **FastAPI Endpoints** - REST API for feature access
- **Caching Layer** - Redis-based ultra-low latency caching
- **Rate Limiting** - Request throttling and resource management

### 3. Technical Indicators (`src/features/indicators/`)
- **FeaturizedIndicators** - Wrapper for existing technical indicators
- **Built-in Indicators** - 25+ pre-implemented indicators
- **Custom Functions** - Framework for custom indicator development
- **Batch Registration** - Automated indicator registration

### 4. Monitoring (`src/features/monitoring/`)
- **DriftDetector** - Statistical drift detection (KS, PSI, Chi-square)
- **DataQualityMonitor** - Completeness, accuracy, freshness monitoring
- **PerformanceMonitor** - Latency, throughput, error rate tracking
- **Alerting System** - Configurable alerts and notifications

### 5. ML Integration (`src/features/ml_integration/`)
- **TrainingDataBuilder** - Time-series aware training data preparation
- **InferenceEngine** - Real-time and batch model serving
- **ModelIntegration** - Complete ML lifecycle integration

### 6. Utilities (`src/features/utils/`)
- **FeatureValidator** - Schema and data validation
- **PerformanceMonitor** - System performance tracking
- **Configuration** - YAML-based configuration management

## 📊 Technical Indicators

### Trend Indicators
- Simple Moving Average (SMA) - Multiple periods
- Exponential Moving Average (EMA) - Multiple periods  
- Weighted Moving Average (WMA)

### Momentum Indicators
- Relative Strength Index (RSI)
- MACD (Line, Signal, Histogram)
- Stochastic Oscillator (%K, %D)
- Williams %R

### Volatility Indicators  
- Bollinger Bands (Upper, Middle, Lower)
- Average True Range (ATR)
- Keltner Channels

### Volume Indicators
- On-Balance Volume (OBV)
- Volume Weighted Average Price (VWAP)
- Accumulation/Distribution Line
- Chaikin Money Flow

### Advanced Indicators
- Ichimoku Cloud (all components)
- Parabolic SAR
- Average Directional Index (ADX)
- Vortex Indicator

## 🚀 Quick Start

### 1. Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Install Delta Lake for Spark
pip install delta-spark pyspark

# Setup Redis (Docker)
docker run -d -p 6379:6379 redis:alpine
```

### 2. Configuration

```yaml
# config/features/feature_store_config.yaml
redis:
  host: "localhost"
  port: 6379
  
storage:
  path: "/data/features/delta"
  
serving:
  latency_threshold_ms: 50.0
  max_batch_size: 1000
```

### 3. Basic Usage

```python
import asyncio
from src.features.store import FeatureStore, FeatureRegistry, DeltaStorageBackend
from src.features.indicators import register_all_technical_indicators

async def main():
    # Initialize components
    storage = DeltaStorageBackend("/tmp/features")
    registry = FeatureRegistry(redis_client)
    feature_store = FeatureStore(storage, registry, redis_client)
    
    # Register technical indicators
    await register_all_technical_indicators(feature_store)
    
    # Compute features
    result = await feature_store.compute_feature(
        feature_id="sma_20",
        input_data=market_data_df
    )
    
    # Get feature vector for entity
    features = await feature_store.get_feature_vector(
        feature_ids=["sma_20", "rsi_14", "macd_line_12_26"],
        entity_id="AAPL"
    )

asyncio.run(main())
```

### 4. API Server

```python
from src.features.serving import create_feature_serving_app
import uvicorn

# Create FastAPI app
app = create_feature_serving_app(feature_server, feature_store, config)

# Run server
uvicorn.run(app, host="0.0.0.0", port=8000)
```

### 5. API Endpoints

```bash
# Serve features for single entity
curl -X POST "http://localhost:8000/features/serve" \
  -H "Content-Type: application/json" \
  -d '{"feature_ids": ["sma_20", "rsi_14"], "entity_id": "AAPL"}'

# Batch feature serving
curl -X POST "http://localhost:8000/features/serve/batch" \
  -H "Content-Type: application/json" \
  -d '{"feature_ids": ["sma_20"], "entity_ids": ["AAPL", "GOOGL"]}'

# Search features
curl -X POST "http://localhost:8000/features/search" \
  -H "Content-Type: application/json" \
  -d '{"query": "moving average", "limit": 10}'

# Health check
curl http://localhost:8000/health
```

## 📈 Performance

### Latency Targets
- **Feature Serving**: < 50ms P95
- **Batch Processing**: 10,000+ entities/second
- **Cache Hit Rate**: > 99%
- **Availability**: 99.9% uptime

### Optimizations
- Redis caching with intelligent TTL
- Delta Lake Z-ordering and compaction
- Vectorized indicator computations
- Async processing with connection pooling
- Smart partitioning strategies

## 🔧 Configuration

### Feature Store Config
```yaml
# config/features/feature_store_config.yaml
serving:
  max_batch_size: 1000
  cache_ttl_seconds: 3600
  latency_threshold_ms: 50.0

monitoring:
  thresholds:
    latency_p95_warning: 50.0
    cache_hit_rate_warning: 90.0
    error_rate_critical: 10.0
```

### Indicators Catalog
```yaml  
# config/features/indicators_catalog.yaml
indicators:
  sma:
    parameters:
      period: {default: 20, min: 2, max: 500}
    output_type: "float"
    calculation: "sum(close_prices[t-period+1:t]) / period"
```

## 🧪 Testing

```bash
# Run all tests
pytest tests/features/ -v

# Run specific test categories
pytest tests/features/test_feature_store.py -v
pytest tests/features/test_indicators.py -v

# Run with coverage
pytest tests/features/ --cov=src/features --cov-report=html
```

## 📊 Monitoring

### Drift Detection
```python
from src.features.monitoring import DriftDetector

detector = DriftDetector(feature_store)
result = await detector.detect_drift("sma_20", method="ks_test")

if result.has_drift:
    print(f"Drift detected: {result.severity} (score: {result.drift_score})")
```

### Performance Monitoring
```python
from src.features.utils import PerformanceMonitor

monitor = PerformanceMonitor(redis_client, thresholds)
await monitor.start_monitoring()

# Get metrics
metrics = await monitor.get_performance_report()
```

## 🔗 ML Integration

### Training Data Preparation
```python
from src.features.ml_integration import TrainingDataBuilder, TrainingConfig

config = TrainingConfig(
    feature_ids=["sma_20", "rsi_14", "macd_line_12_26"],
    target_column="future_return_5d",
    train_start=datetime(2023, 1, 1),
    train_end=datetime(2023, 6, 30)
)

builder = TrainingDataBuilder(feature_store)
datasets = await builder.build_training_data(config)
```

### Model Inference
```python
from src.features.ml_integration import InferenceEngine, InferenceConfig

config = InferenceConfig(
    feature_ids=["sma_20", "rsi_14"],
    model_id="price_prediction_v1"
)

engine = InferenceEngine(feature_store)
engine.register_model("price_prediction_v1", model, config)

prediction = await engine.predict_single("price_prediction_v1", "AAPL")
```

## 📚 Documentation

- **API Documentation**: Available at `/docs` when server is running
- **Configuration Reference**: `config/features/`
- **Examples**: `examples/feature_store_example.py`
- **Tests**: `tests/features/`

## 🎯 Use Cases

### 1. Algorithmic Trading
- Real-time feature serving for trading signals
- Backtesting with time-travel queries
- Model serving for prediction-based strategies

### 2. Risk Management
- Drift detection for model degradation
- Feature quality monitoring
- Performance tracking and alerting

### 3. Research and Development
- Feature discovery and experimentation
- Historical analysis with point-in-time correctness
- A/B testing framework for feature variations

### 4. Production ML Pipelines
- Training data preparation with proper time splits
- Model serving with feature consistency
- Lineage tracking for regulatory compliance

## 🔮 Future Enhancements

- **Real-time Streaming**: Kafka integration for live feature updates
- **Advanced Caching**: Multi-tier caching with intelligent warming
- **Security**: Authentication, authorization, and encryption
- **Multi-tenancy**: Namespace isolation and resource quotas
- **Feature Sharing**: Cross-team feature discovery and reuse
- **AutoML Integration**: Automated feature selection and engineering

## 📄 License

This feature store implementation is part of the QuantStream Analytics Platform.

## 🤝 Contributing

1. Follow the existing code structure and patterns
2. Add comprehensive tests for new features
3. Update documentation and configuration examples
4. Ensure performance benchmarks are maintained
5. Add appropriate logging and monitoring

---

**QuantStream Feature Store** - Empowering financial ML with reliable, fast, and scalable feature management.