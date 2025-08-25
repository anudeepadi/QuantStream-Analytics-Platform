#!/bin/bash

# QuantStream Analytics Platform - Setup Script
# This script sets up the local development environment

set -e

echo "🚀 Setting up QuantStream Analytics Platform..."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📋 Creating .env file from template..."
    cp .env.example .env
    echo "✅ Please edit .env file with your configuration before continuing"
    read -p "Press enter to continue once you've configured .env..."
fi

# Create necessary directories
echo "📁 Creating necessary directories..."
mkdir -p data/raw data/processed data/temp logs notebooks

# Build and start services
echo "🏗️  Building and starting services..."
docker-compose up --build -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 30

# Check if services are running
echo "🔍 Checking service health..."
services=("postgres" "redis" "kafka" "spark-master" "api")
for service in "${services[@]}"; do
    if docker-compose ps | grep -q "${service}.*Up"; then
        echo "✅ $service is running"
    else
        echo "❌ $service is not running"
    fi
done

echo ""
echo "🎉 Setup complete! You can now access:"
echo "📊 API Documentation: http://localhost:8000/docs"
echo "📈 Streamlit Dashboard: http://localhost:8501"
echo "⚡ Spark UI: http://localhost:8080"
echo "🧪 MLflow UI: http://localhost:5000"
echo "📊 Grafana: http://localhost:3000 (admin/admin)"
echo "🔍 Prometheus: http://localhost:9090"
echo "💾 MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
echo "📓 Jupyter Lab: http://localhost:8888"
echo "🌊 Airflow: http://localhost:8082 (admin/admin)"
echo ""
echo "To stop all services: docker-compose down"
echo "To view logs: docker-compose logs -f [service-name]"