# QuantStream Analytics Platform - Setup & Deployment Guide

## 🚀 Quick Start

### Prerequisites

1. **System Requirements**:
   - Python 3.10+
   - Docker & Docker Compose
   - Node.js 18+ (for some tools)
   - Minimum 16GB RAM, 100GB disk space

2. **Required Accounts & API Keys**:
   ```bash
   # Financial data providers (choose at least one)
   ALPHA_VANTAGE_API_KEY=your_key_here
   FINNHUB_API_KEY=your_key_here
   POLYGON_API_KEY=your_key_here
   
   # ML & AI services
   MLFLOW_TRACKING_URI=your_mlflow_server
   ANTHROPIC_API_KEY=your_key_here  # For Task Master
   
   # Cloud providers (optional, for production)
   AWS_ACCESS_KEY_ID=your_key
   AWS_SECRET_ACCESS_KEY=your_secret
   AZURE_CLIENT_ID=your_client_id
   GOOGLE_APPLICATION_CREDENTIALS=path/to/service_account.json
   ```

### 1. Environment Setup

```bash
# Clone and setup
git clone <repository_url>
cd samp

# Copy environment template
cp .env.example .env

# Edit .env with your API keys
nano .env

# Make setup script executable
chmod +x scripts/setup.sh

# Run automated setup
./scripts/setup.sh
```

### 2. Local Development (Docker Compose)

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f quantstream-api
```

**Service URLs after startup:**
- **Dashboard**: http://localhost:8501
- **API**: http://localhost:8000
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Jupyter**: http://localhost:8888 (token in logs)
- **MLflow**: http://localhost:5000

## 🏗️ Production Deployment

### Option 1: Cloud Deployment with Terraform

```bash
# Install Terraform
curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add -
sudo apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com $(lsb_release -cs) main"
sudo apt-get update && sudo apt-get install terraform

# Configure cloud credentials
aws configure  # For AWS
az login       # For Azure
gcloud auth login  # For GCP

# Deploy infrastructure
cd infrastructure/
./scripts/deployment/deploy.sh -e prod -c aws

# Deploy application
kubectl apply -f k8s/
```

### Option 2: Kubernetes Deployment

```bash
# Create namespace
kubectl create namespace quantstream

# Deploy with Helm
helm install quantstream ./charts/quantstream -n quantstream

# Check deployment
kubectl get pods -n quantstream
```

### Option 3: Docker Swarm (Simple Production)

```bash
# Initialize swarm
docker swarm init

# Deploy stack
docker stack deploy -c docker-compose.prod.yml quantstream

# Check services
docker service ls
```

## 📊 Running the Platform

### Start Data Ingestion

```bash
# Method 1: Using Python directly
cd src/ingestion/
python -m main --config ../../config/ingestion/sources_config.yaml

# Method 2: Using Docker
docker-compose exec quantstream-ingestion python -m src.ingestion.main

# Method 3: Using API
curl -X POST http://localhost:8000/ingestion/start \
  -H "Content-Type: application/json" \
  -d '{"sources": ["alpha_vantage", "finnhub"]}'
```

### Start ETL Pipeline

```bash
# Start streaming ETL
cd src/etl/
python -m main run --config ../../config/etl/streaming_config.yaml

# Or via Docker
docker-compose exec quantstream-etl python -m src.etl.main run
```

### Launch Dashboard

```bash
# Start Streamlit dashboard
cd src/dashboard/
streamlit run app.py --server.port 8501

# Or access via Docker
# Already running at http://localhost:8501
```

### Start ML Training

```bash
# Train anomaly detection models
cd src/ml/
python -m training.train_models --config ../../config/ml/training_configs.yaml

# Or via API
curl -X POST http://localhost:8000/ml/train \
  -H "Content-Type: application/json" \
  -d '{"model_type": "ensemble", "retrain": true}'
```

## 🔧 Configuration

### Key Configuration Files

1. **Data Sources** (`config/ingestion/sources_config.yaml`):
```yaml
sources:
  alpha_vantage:
    api_key: ${ALPHA_VANTAGE_API_KEY}
    rate_limit: 5  # requests per minute
    symbols: ["AAPL", "GOOGL", "MSFT", "TSLA"]
```

2. **ETL Pipeline** (`config/etl/streaming_config.yaml`):
```yaml
spark:
  app_name: "QuantStream ETL"
  master: "local[*]"
  
streaming:
  trigger_interval: "10 seconds"
  watermark_delay: "1 minute"
```

3. **ML Models** (`config/ml/model_configs.yaml`):
```yaml
models:
  isolation_forest:
    contamination: 0.1
    max_samples: 1000
  
  lstm_autoencoder:
    sequence_length: 60
    encoding_dim: 32
```

### Environment Variables

```bash
# Core application
QUANTSTREAM_ENV=development  # development, staging, production
LOG_LEVEL=INFO
DEBUG=true

# Database connections
POSTGRES_URL=postgresql://user:pass@localhost:5432/quantstream
REDIS_URL=redis://localhost:6379/0

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SCHEMA_REGISTRY=http://localhost:8081

# Storage
DELTA_LAKE_PATH=./data/delta
S3_BUCKET=quantstream-data
```

## 🔍 Monitoring & Troubleshooting

### Health Checks

```bash
# Check all services
curl http://localhost:8000/health

# Specific component health
curl http://localhost:8000/health/ingestion
curl http://localhost:8000/health/etl
curl http://localhost:8000/health/ml
```

### View Logs

```bash
# Application logs
docker-compose logs -f quantstream-api
docker-compose logs -f quantstream-etl
docker-compose logs -f quantstream-ml

# System logs
tail -f logs/quantstream.log
tail -f logs/ingestion.log
tail -f logs/etl.log
```

### Common Issues & Solutions

1. **API Keys Not Working**:
```bash
# Verify environment variables
echo $ALPHA_VANTAGE_API_KEY
echo $FINNHUB_API_KEY

# Test API connectivity
curl "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=AAPL&apikey=$ALPHA_VANTAGE_API_KEY"
```

2. **Out of Memory**:
```bash
# Increase Docker memory limits
# Edit docker-compose.yml
services:
  quantstream-etl:
    mem_limit: 4g
    
# Or adjust Spark configuration
export SPARK_EXECUTOR_MEMORY=2g
export SPARK_DRIVER_MEMORY=2g
```

3. **Slow Performance**:
```bash
# Check system resources
docker stats

# Monitor Kafka lag
docker-compose exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group quantstream-etl
```

## 📈 Performance Tuning

### Spark Optimization

```yaml
# config/etl/streaming_config.yaml
spark:
  sql.adaptive.enabled: true
  sql.adaptive.coalescePartitions.enabled: true
  serializer: "org.apache.spark.serializer.KryoSerializer"
  sql.adaptive.skewJoin.enabled: true
```

### Kafka Tuning

```bash
# Increase partition count for better parallelism
docker-compose exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --alter --topic market-data \
  --partitions 12
```

### Database Optimization

```sql
-- Create indexes for better query performance
CREATE INDEX idx_quotes_symbol_timestamp ON quotes(symbol, timestamp);
CREATE INDEX idx_trades_timestamp ON trades(timestamp);

-- Enable connection pooling
ALTER SYSTEM SET max_connections = 200;
ALTER SYSTEM SET shared_buffers = '256MB';
```

## 🧪 Testing

### Run Tests

```bash
# Unit tests
pytest tests/ -v

# Integration tests
pytest tests/integration/ -v --integration

# Performance tests
pytest tests/performance/ -v --performance

# Chaos engineering tests
./scripts/chaos_tests.sh
```

### Load Testing

```bash
# Install k6
sudo apt install k6

# Run load tests
k6 run tests/load/api_load_test.js
k6 run tests/load/dashboard_load_test.js
```

## 🔄 Updates & Maintenance

### Update Platform

```bash
# Pull latest changes
git pull origin main

# Rebuild containers
docker-compose build --no-cache

# Restart services
docker-compose down && docker-compose up -d
```

### Database Migrations

```bash
# Run migrations
python -m alembic upgrade head

# Or via Docker
docker-compose exec quantstream-api alembic upgrade head
```

### Backup & Recovery

```bash
# Backup PostgreSQL
docker-compose exec postgres pg_dump -U quantstream quantstream > backup.sql

# Backup Delta Lake data
aws s3 sync ./data/delta s3://quantstream-backup/delta/$(date +%Y%m%d)/
```

## 📚 Additional Resources

- **Architecture Documentation**: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- **API Documentation**: http://localhost:8000/docs (Swagger UI)
- **Performance Benchmarks**: [docs/BENCHMARKS.md](docs/BENCHMARKS.md)
- **Troubleshooting Guide**: [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)

## 🆘 Support

For issues and questions:
1. Check the troubleshooting section above
2. Review logs for error details
3. Check system resources (CPU, memory, disk)
4. Verify API keys and network connectivity
5. Consult the documentation in the `docs/` directory

Happy trading! 📈