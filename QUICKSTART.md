# 🚀 QuantStream Analytics Platform - Quick Start

## TL;DR - Get Running in 5 Minutes

### 1. Prerequisites
- Docker & Docker Compose installed
- Python 3.10+ installed
- At least 16GB RAM available

### 2. Clone & Setup
```bash
git clone <repo-url>
cd samp

# Run the automated quick start
./scripts/quick_start.sh
```

### 3. Access the Platform
- **Dashboard**: http://localhost:8501 📊
- **API**: http://localhost:8000/docs 🔧
- **Monitoring**: http://localhost:3000 📈

That's it! The platform is now running with sample data.

---

## 📋 Step-by-Step Setup

### Step 1: Environment Setup

```bash
# Copy environment file
cp .env.example .env

# Edit with your API keys (optional for demo)
nano .env
```

**Required for production**:
```bash
# Add at least one financial data provider
ALPHA_VANTAGE_API_KEY="JJACWWBLJBVC0BOK"
FINNHUB_API_KEY="d2lo4vpr01qr27gk6mbgd2lo4vpr01qr27gk6mc0"

# Database passwords
POSTGRES_PASSWORD="Factory2$"
REDIS_PASSWORD="Factory2$"
```

### Step 2: Start Services

```bash
# Start all services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f
```

### Step 3: Verify Installation

```bash
# Health check
curl http://localhost:8000/health

# Expected response:
# {"status": "healthy", "timestamp": "2024-01-01T00:00:00Z"}
```

## 🎯 What's Included

### Core Services
- **📊 Streamlit Dashboard** - Real-time market data visualization
- **🔧 FastAPI Backend** - RESTful API for all operations  
- **📈 Grafana Monitoring** - System metrics and alerts
- **⚡ Prometheus** - Metrics collection
- **🗃️ PostgreSQL** - Metadata and configuration storage
- **🚀 Redis** - High-speed caching layer
- **📨 Kafka + Zookeeper** - Real-time message streaming

### Data Pipeline
- **Ingestion**: Multi-source market data collection
- **ETL**: Real-time processing with Delta Lake
- **ML**: Anomaly detection with MLflow
- **Features**: Technical indicators calculation
- **Monitoring**: Complete observability stack

## 🔍 First Steps After Setup

### 1. Explore the Dashboard
Visit http://localhost:8501 and explore:
- **Market Data** tab - Live price charts
- **Technical Analysis** - RSI, MACD, Bollinger Bands
- **Portfolio** - Position tracking and P&L
- **Alerts** - Price and volume notifications
- **System** - Health and performance metrics

### 2. Configure Data Sources
```bash
# Via API
curl -X POST http://localhost:8000/ingestion/sources \
  -H "Content-Type: application/json" \
  -d '{
    "source": "alpha_vantage",
    "symbols": ["AAPL", "GOOGL", "MSFT", "TSLA"],
    "enabled": true
  }'

# Via Dashboard
# Go to Settings -> Data Sources -> Add Source
```

### 3. Start Data Ingestion
```bash
# Start ingestion for specific symbols
curl -X POST http://localhost:8000/ingestion/start \
  -H "Content-Type: application/json" \
  -d '{"sources": ["alpha_vantage"], "symbols": ["AAPL"]}'
```

### 4. Monitor the Pipeline
- **Grafana**: http://localhost:3000 (admin/admin)
  - Pre-built dashboards for all components
  - Real-time metrics and alerts
- **Prometheus**: http://localhost:9090
  - Raw metrics and query interface

## 📊 Sample Workflows

### Real-time Trading Analysis
1. Open dashboard at http://localhost:8501
2. Select symbols in sidebar (AAPL, GOOGL, MSFT, TSLA)
3. View real-time charts with technical indicators
4. Set up price alerts for key levels
5. Monitor portfolio performance

### ML Model Training
```bash
# Train anomaly detection models
curl -X POST http://localhost:8000/ml/train \
  -H "Content-Type: application/json" \
  -d '{
    "model_type": "ensemble",
    "symbols": ["AAPL"],
    "lookback_days": 30
  }'

# Check training progress
curl http://localhost:8000/ml/models/status
```

### Feature Store Usage
```bash
# Get technical indicators
curl "http://localhost:8000/features/indicators?symbol=AAPL&indicators=RSI,MACD,BB"

# Batch feature retrieval
curl -X POST http://localhost:8000/features/batch \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["AAPL", "GOOGL"],
    "features": ["sma_20", "rsi_14", "macd"],
    "start_date": "2024-01-01",
    "end_date": "2024-01-31"
  }'
```

## 🔧 Configuration

### Key Configuration Files
- **Data Sources**: `config/ingestion/sources_config.yaml`
- **ETL Pipeline**: `config/etl/streaming_config.yaml`
- **ML Models**: `config/ml/model_configs.yaml`
- **Dashboard**: `config/dashboard/dashboard_config.yaml`

### Environment Variables
```bash
# Development vs Production
QUANTSTREAM_ENV=development

# Logging
LOG_LEVEL=INFO
DEBUG=true

# Performance tuning
SPARK_EXECUTOR_MEMORY=2g
KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
```

## ❗ Troubleshooting

### Service Won't Start
```bash
# Check logs
docker-compose logs servicename

# Common issues:
# 1. Port conflicts - change ports in docker-compose.yml
# 2. Memory issues - increase Docker memory limits
# 3. Permission issues - check file permissions
```

### API Not Responding
```bash
# Check if service is running
docker-compose ps quantstream-api

# Check health endpoint
curl http://localhost:8000/health

# View API logs
docker-compose logs -f quantstream-api
```

### Dashboard Shows No Data
```bash
# Check if ingestion is running
curl http://localhost:8000/ingestion/status

# Start ingestion manually
curl -X POST http://localhost:8000/ingestion/start

# Check data in database
docker-compose exec postgres psql -U quantstream -c "SELECT COUNT(*) FROM quotes;"
```

### High Memory Usage
```bash
# Check resource usage
docker stats

# Reduce Spark memory in .env:
SPARK_EXECUTOR_MEMORY=1g
SPARK_DRIVER_MEMORY=1g

# Restart services
docker-compose restart
```

## 🔄 Updates & Maintenance

### Update Platform
```bash
# Pull latest changes
git pull origin main

# Rebuild and restart
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

### Backup Data
```bash
# Database backup
docker-compose exec postgres pg_dump -U quantstream quantstream > backup.sql

# Configuration backup
tar -czf config_backup.tar.gz config/ .env
```

### Clean Reset
```bash
# Stop and remove all data
docker-compose down -v

# Remove images (optional)
docker-compose down --rmi all

# Start fresh
./scripts/quick_start.sh
```

## 🆘 Getting Help

1. **Check the logs**: `docker-compose logs -f`
2. **Health status**: `curl http://localhost:8000/health`
3. **Documentation**: [SETUP.md](SETUP.md) for detailed setup
4. **API Docs**: http://localhost:8000/docs
5. **Grafana Metrics**: http://localhost:3000

## 🎉 What's Next?

Once you have the platform running:

1. **Configure API Keys** - Add real financial data providers
2. **Customize Dashboards** - Modify charts and indicators  
3. **Train ML Models** - Use your own data for anomaly detection
4. **Set Up Alerts** - Configure notifications for important events
5. **Scale to Production** - Use Kubernetes deployment

Happy trading! 📈