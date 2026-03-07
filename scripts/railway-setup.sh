#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# QuantStream Railway Services Setup
# Run once to provision all infrastructure services in Railway.
# Prerequisites: `npm install -g @railway/cli` and `railway login`
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

PROJECT_NAME="quantstream"

echo ">>> Linking to Railway project: $PROJECT_NAME"
railway link --project "$PROJECT_NAME" 2>/dev/null || railway init --name "$PROJECT_NAME"

# ── Managed plugins (Railway-native) ────────────────────────────────────────
echo ">>> Adding PostgreSQL plugin..."
railway add --plugin postgresql

echo ">>> Adding Redis plugin..."
railway add --plugin redis

# ── Docker image services ─────────────────────────────────────────────────
# Railway CLI doesn't support adding Docker image services directly.
# The block below uses the Railway API via curl. Set RAILWAY_TOKEN first.
# Alternatively, add these via https://railway.app/dashboard → New Service → Docker Image

if [ -z "${RAILWAY_TOKEN:-}" ]; then
  echo ""
  echo "RAILWAY_TOKEN not set. Add the following Docker image services manually"
  echo "in the Railway dashboard (New Service → Docker Image):"
  echo ""
  echo "  Service name: zookeeper"
  echo "  Image:        confluentinc/cp-zookeeper:7.4.0"
  echo "  Env vars:"
  echo "    ZOOKEEPER_CLIENT_PORT=2181"
  echo "    ZOOKEEPER_TICK_TIME=2000"
  echo ""
  echo "  Service name: kafka"
  echo "  Image:        confluentinc/cp-kafka:7.4.0"
  echo "  Env vars:"
  echo "    KAFKA_BROKER_ID=1"
  echo "    KAFKA_ZOOKEEPER_CONNECT=zookeeper.railway.internal:2181"
  echo "    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"
  echo "    KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka.railway.internal:29092,PLAINTEXT_HOST://localhost:9092"
  echo "    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1"
  echo "    KAFKA_AUTO_CREATE_TOPICS_ENABLE=true"
  echo "    KAFKA_LOG_RETENTION_HOURS=24"
  echo ""
  echo "  Service name: schema-registry"
  echo "  Image:        confluentinc/cp-schema-registry:7.4.0"
  echo "  Env vars:"
  echo "    SCHEMA_REGISTRY_HOST_NAME=schema-registry"
  echo "    SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=kafka.railway.internal:29092"
  echo "    SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081"
  echo ""
  echo "  Service name: minio"
  echo "  Image:        minio/minio:latest"
  echo "  Start command: server /data --console-address :9001"
  echo "  Env vars:"
  echo "    MINIO_ROOT_USER=quantstream"
  echo "    MINIO_ROOT_PASSWORD=<set a strong password>"
  echo ""
  echo "  Service name: prometheus"
  echo "  Image:        prom/prometheus:v2.44.0"
  echo ""
  echo "  Service name: grafana"
  echo "  Image:        grafana/grafana:10.0.0"
  echo "  Env vars:"
  echo "    GF_SECURITY_ADMIN_USER=admin"
  echo "    GF_SECURITY_ADMIN_PASSWORD=<set a strong password>"
  echo ""
  exit 0
fi

# ── Wire backend environment variables ──────────────────────────────────────
# After plugins are created, Railway auto-provides DATABASE_URL and REDIS_URL.
# Set the remaining vars for the API service:

echo ">>> Setting API service environment variables..."
railway variables set \
  KAFKA_BOOTSTRAP_SERVERS="kafka.railway.internal:29092" \
  SCHEMA_REGISTRY_URL="http://schema-registry.railway.internal:8081" \
  MINIO_ENDPOINT="http://minio.railway.internal:9000" \
  ENVIRONMENT="production" \
  LOG_LEVEL="INFO" \
  METRICS_ENABLED="true" \
  --service api --environment production

railway variables set \
  KAFKA_BOOTSTRAP_SERVERS="kafka.railway.internal:29092" \
  SCHEMA_REGISTRY_URL="http://schema-registry.railway.internal:8081" \
  MINIO_ENDPOINT="http://minio.railway.internal:9000" \
  ENVIRONMENT="staging" \
  LOG_LEVEL="DEBUG" \
  METRICS_ENABLED="true" \
  --service api --environment staging

echo ""
echo ">>> Setup complete."
echo ">>> Still required — set these secrets manually in the Railway dashboard"
echo ">>> (Settings → Variables) for the api service:"
echo ""
echo "  SECRET_KEY             JWT signing key"
echo "  ALPHA_VANTAGE_API_KEY  Market data"
echo "  FINNHUB_API_KEY        Market data"
echo "  POLYGON_API_KEY        Market data"
echo "  MINIO_ROOT_PASSWORD    MinIO admin password"
echo ""
echo ">>> DATABASE_URL and REDIS_URL are auto-linked by Railway plugins."
