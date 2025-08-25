# QuantStream ETL Pipeline Overview

## Architecture

The QuantStream ETL pipeline implements a Delta Live Tables pattern with Bronze-Silver-Gold layers for real-time market data processing.

### Layer Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Kafka Topics  │    │  Bronze Layer   │    │  Silver Layer   │
│                 │───▶│                 │───▶│                 │
│ • Quotes        │    │ • Raw ingestion │    │ • Data cleaning │
│ • Trades        │    │ • Schema valid. │    │ • Validation    │
│ • Bars          │    │ • Lineage       │    │ • Enrichment    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Indicators    │    │   Gold Layer    │    │   Anomalies     │
│                 │◀───│                 │───▶│                 │
│ • Moving Avg    │    │ • OHLCV bars    │    │ • Price spikes  │
│ • RSI, MACD     │    │ • Aggregations  │    │ • Volume anom.  │
│ • Bollinger     │    │ • Metrics       │    │ • Patterns      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Components

### 1. Bronze Layer (`bronze_layer.py`)
**Purpose**: Raw data ingestion with minimal processing

**Features**:
- Kafka streaming source
- Exactly-once processing guarantees
- Schema validation
- Data lineage tracking
- Error handling with dead letter queues

**Key Transformations**:
- JSON parsing by topic
- Metadata enrichment (timestamps, hashes)
- Basic quality flags
- Partitioning by topic/date

### 2. Silver Layer (`silver_layer.py`)
**Purpose**: Clean, validated, and enriched data

**Features**:
- Data type conversions
- Quality validation
- Outlier detection and handling
- Deduplication
- Data enrichment

**Key Transformations**:
- Price/volume validation
- Statistical outlier detection (IQR/Z-score)
- Missing value interpolation
- Symbol standardization
- Quality scoring

### 3. Gold Layer (`gold_layer.py`)
**Purpose**: Business-ready aggregated metrics

**Features**:
- Multi-timeframe aggregations
- OHLCV bar generation
- Market statistics
- Performance metrics

**Key Transformations**:
- Real-time OHLCV bars (1min, 5min, 15min, 1hr, 1day)
- Volume-weighted metrics (VWAP)
- Quote metrics (spreads, counts)
- Trade metrics (intensity, size distribution)

### 4. Technical Indicators (`technical_indicators.py`)
**Purpose**: Real-time trading indicators

**Features**:
- Moving averages (SMA, EMA)
- Momentum indicators (RSI, Stochastic)
- Trend indicators (MACD, Bollinger Bands)
- Volume indicators (OBV)
- Price action indicators (ATR, Pivot Points)

### 5. Anomaly Detection (`anomaly_detection.py`)
**Purpose**: Real-time anomaly identification

**Features**:
- Statistical outlier detection
- Price spike detection
- Volume anomaly detection
- Pattern-based anomalies
- Composite scoring

## Configuration

### Main Configuration (`config/etl/streaming_config.yaml`)

```yaml
# Spark Configuration
spark:
  config:
    spark.sql.adaptive.enabled: "true"
    spark.databricks.delta.optimizeWrite.enabled: "true"

# Kafka Source
kafka:
  bootstrap_servers: ["localhost:9092"]
  topics:
    - name: "market_data_quotes"
      partitions: 12
    - name: "market_data_trades"
      partitions: 12

# Layer Configurations
bronze_layer:
  output_path: "/tmp/delta/bronze"
  trigger_interval: "30 seconds"
  optimization:
    z_order_columns: ["symbol", "kafka_timestamp", "topic"]

silver_layer:
  output_path: "/tmp/delta/silver"
  trigger_interval: "1 minute"
  quality_thresholds:
    min_price: 0.01
    max_price: 100000.0

gold_layer:
  output_path: "/tmp/delta/gold"
  trigger_interval: "2 minutes"
  time_windows: ["1 minute", "5 minutes", "15 minutes", "1 hour"]
```

## Data Flow

### 1. Ingestion Flow
```
Kafka → Bronze Layer → Delta Table (Partitioned by topic/date)
```

### 2. Processing Flow
```
Bronze → Silver (Cleaning/Validation) → Gold (Aggregation) → Indicators/Anomalies
```

### 3. Quality Flow
```
Raw Data → Validation → Quality Scoring → High-Quality Data → Business Logic
```

## Quality Assurance

### Data Quality Dimensions
1. **Completeness**: Missing value detection and handling
2. **Accuracy**: Business rule validation (price ranges, OHLC relationships)
3. **Consistency**: Cross-field validation, temporal consistency
4. **Timeliness**: Processing latency monitoring
5. **Uniqueness**: Duplicate detection and removal

### Quality Metrics
- Overall quality score (0-1)
- Quality grade (A-F)
- Dimension-specific scores
- Real-time quality monitoring

## Performance Optimizations

### Spark Optimizations
- **Adaptive Query Execution (AQE)**: Enabled for automatic optimization
- **Z-Ordering**: Optimized data layout for common query patterns
- **Auto-Compaction**: Automatic small file compaction
- **Photon Engine**: Available for premium performance

### Delta Lake Features
- **Optimize Write**: Improved write performance
- **Auto Optimize**: Background optimization
- **Schema Evolution**: Automatic schema handling
- **Time Travel**: Historical data access

### Partitioning Strategy
```
Bronze: topic/year/month/day
Silver: symbol/year/month/day
Gold: symbol/timeframe/year/month/day
```

## Monitoring and Alerting

### Health Checks
- Streaming query status
- Processing latency
- Error rates
- Data quality scores
- Resource utilization

### Alerts
- High processing latency (>5 minutes)
- Data quality degradation (<85%)
- Streaming query failures
- Anomaly detection (critical/warning levels)

## Usage

### Running the Pipeline

```bash
# Full pipeline
python -m src.etl.main run --config config/etl/streaming_config.yaml

# Bronze layer only
python -m src.etl.main run --config config/etl/streaming_config.yaml --mode bronze-only

# Validate configuration
python -m src.etl.main validate --config config/etl/streaming_config.yaml
```

### Pipeline Management

```python
from src.etl.pipeline_orchestrator import ETLPipelineOrchestrator

# Initialize and start
orchestrator = ETLPipelineOrchestrator("config/etl/streaming_config.yaml")
orchestrator.start_pipeline()

# Check status
status = orchestrator.get_pipeline_status()
print(f"Pipeline status: {status['overall_status']}")

# Stop gracefully
orchestrator.stop_pipeline()
```

## Testing

### Test Categories
1. **Unit Tests**: Individual component testing
2. **Integration Tests**: Layer-to-layer data flow
3. **Performance Tests**: Throughput and latency
4. **End-to-End Tests**: Complete pipeline validation

### Running Tests
```bash
# All tests
pytest tests/etl/

# Specific test category
pytest tests/etl/ -m "unit"
pytest tests/etl/ -m "integration"
pytest tests/etl/ -m "performance"
```

## Error Handling

### Failure Recovery
- **Automatic Restart**: Failed queries restart automatically (configurable)
- **Checkpoint Recovery**: Resume from last processed offset
- **Dead Letter Queue**: Invalid records stored for analysis
- **Circuit Breaker**: Stop processing on persistent failures

### Error Types
1. **Transient Errors**: Network issues, temporary resource constraints
2. **Data Errors**: Invalid schemas, corrupt records
3. **System Errors**: Out of memory, disk space issues
4. **Configuration Errors**: Invalid parameters, missing resources

## Deployment Considerations

### Resource Requirements
- **Driver**: 4-8GB memory, 2-4 cores
- **Executors**: 4-8GB memory, 2-4 cores each
- **Storage**: Delta Lake compatible storage (S3, ADLS, HDFS)

### Scaling
- **Horizontal**: Increase executor count for higher throughput
- **Vertical**: Increase memory/cores per executor for complex processing
- **Partitioning**: Optimize Kafka partitions and Delta table partitioning

### Security
- **Kafka Authentication**: SASL/SSL support
- **Data Encryption**: At-rest and in-transit encryption
- **Access Control**: Role-based access to data layers
- **Audit Logging**: Complete data lineage tracking

## Future Enhancements

### Planned Features
1. **ML Model Integration**: Real-time model scoring
2. **Advanced Anomaly Detection**: Machine learning algorithms
3. **Cross-Asset Correlation**: Multi-symbol analysis
4. **Real-time Alerting**: Push notifications for critical events
5. **Data Catalog Integration**: Automated metadata management

### Performance Improvements
1. **Incremental Processing**: Process only changed data
2. **Materialized Views**: Pre-computed aggregations
3. **Caching Layer**: Redis/Memcached for hot data
4. **GPU Acceleration**: RAPIDS for compute-intensive operations