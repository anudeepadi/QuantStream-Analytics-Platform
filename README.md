# QuantStream Analytics Platform

A comprehensive real-time analytics platform for quantitative data processing, machine learning, and financial market analysis.

## Overview

QuantStream Analytics Platform is a scalable, production-ready system designed for real-time data ingestion, processing, and analysis. Built with modern data engineering practices, it provides a complete solution for quantitative analytics with integrated machine learning capabilities, feature stores, and interactive dashboards.

## Architecture

The platform follows a microservices architecture with the following key components:

- **Data Ingestion**: Real-time streaming data ingestion using Apache Kafka
- **ETL Pipeline**: Scalable data processing with Apache Spark and Delta Lake
- **Machine Learning**: MLflow-integrated model training and deployment
- **Feature Store**: Redis-based feature serving with PostgreSQL offline storage
- **Dashboard**: Interactive Streamlit-based analytics dashboard
- **Monitoring**: Comprehensive observability with Prometheus and Grafana
- **API Layer**: FastAPI-based REST services

## Features

- **Real-time Data Processing**: Stream processing with Apache Spark and Kafka
- **Delta Lake Integration**: ACID transactions and time travel for data lakes
- **ML Pipeline**: End-to-end machine learning workflow with MLflow
- **Feature Engineering**: Scalable feature computation and serving
- **Interactive Dashboards**: Real-time analytics visualization
- **Monitoring & Alerting**: Production-grade observability
- **Containerized Deployment**: Docker and Docker Compose support
- **Infrastructure as Code**: Terraform configurations for cloud deployment

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.9+
- Git

### Local Development Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd quantstream-analytics
   ```

2. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Start the development environment**
   ```bash
   docker-compose up -d
   ```

4. **Install Python dependencies** (optional, for local development)
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\\Scripts\\activate
   pip install -r requirements.txt
   pip install -e .
   ```

### Accessing Services

Once the containers are running, you can access:

- **API Documentation**: http://localhost:8000/docs
- **Streamlit Dashboard**: http://localhost:8501
- **Spark UI**: http://localhost:8080
- **MLflow UI**: http://localhost:5000
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **Jupyter Lab**: http://localhost:8888
- **Airflow**: http://localhost:8082 (admin/admin)

## Project Structure

```
quantstream-analytics/
├── src/                          # Main source code
│   ├── ingestion/               # Data ingestion components
│   ├── etl/                     # ETL pipeline code
│   ├── ml/                      # Machine learning models
│   ├── features/                # Feature store
│   ├── dashboard/               # Dashboard and UI
│   └── monitoring/              # Observability
├── infrastructure/              # Terraform IaC
├── tests/                       # Testing suite
│   ├── unit/                   # Unit tests
│   ├── integration/            # Integration tests
│   └── e2e/                    # End-to-end tests
├── config/                     # Configuration files
├── scripts/                    # Utility scripts
├── docs/                       # Documentation
├── data/                       # Sample data and schemas
│   ├── raw/                    # Raw data samples
│   ├── processed/              # Processed data samples
│   └── schemas/                # Data schemas
├── requirements.txt            # Python dependencies
├── pyproject.toml             # Project configuration
├── Dockerfile                 # Multi-stage Docker build
├── docker-compose.yml         # Local development environment
├── .env.example              # Environment variables template
└── README.md                 # This file
```

## Core Technologies

### Data Processing
- **Apache Spark**: Distributed data processing
- **Delta Lake**: Data lake storage with ACID transactions
- **Apache Kafka**: Real-time streaming platform
- **Redis**: In-memory data structure store for caching and features

### Machine Learning
- **MLflow**: ML lifecycle management
- **scikit-learn**: Machine learning algorithms
- **XGBoost/LightGBM**: Gradient boosting frameworks
- **Pandas/NumPy**: Data manipulation and numerical computing

### Web Framework & APIs
- **FastAPI**: Modern Python web framework
- **Streamlit**: Interactive dashboard framework
- **Uvicorn**: ASGI server
- **Pydantic**: Data validation and settings management

### Storage & Databases
- **PostgreSQL**: Primary relational database
- **MinIO**: S3-compatible object storage
- **Redis**: Feature store and caching
- **Delta Lake**: Data lake storage format

### Monitoring & Observability
- **Prometheus**: Metrics collection
- **Grafana**: Metrics visualization
- **OpenTelemetry**: Distributed tracing
- **Structlog**: Structured logging

### DevOps & Infrastructure
- **Docker**: Containerization
- **Terraform**: Infrastructure as Code
- **Apache Airflow**: Workflow orchestration
- **pytest**: Testing framework

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test categories
pytest -m unit
pytest -m integration
pytest -m e2e
```

### Code Quality

The project uses several tools for code quality:

```bash
# Format code
black src/ tests/

# Sort imports
isort src/ tests/

# Type checking
mypy src/

# Linting
flake8 src/ tests/

# Run all quality checks
pre-commit run --all-files
```

### Development Workflow

1. Create a feature branch
2. Make your changes
3. Run tests and quality checks
4. Submit a pull request

## Configuration

### Environment Variables

Key environment variables (see `.env.example` for full list):

- `DATABASE_URL`: PostgreSQL connection string
- `REDIS_URL`: Redis connection string
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses
- `MLFLOW_TRACKING_URI`: MLflow server URI
- `S3_ENDPOINT_URL`: Object storage endpoint

### Service Configuration

Each service can be configured through environment variables or configuration files in the `config/` directory.

## Deployment

### Docker Deployment

```bash
# Build and start all services
docker-compose up --build

# Scale specific services
docker-compose up --scale spark-worker=3

# Production deployment
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

### Cloud Deployment

Use the Terraform configurations in the `infrastructure/` directory:

```bash
cd infrastructure
terraform init
terraform plan
terraform apply
```

## Monitoring

The platform includes comprehensive monitoring:

- **Application Metrics**: Custom business metrics via Prometheus
- **Infrastructure Metrics**: System and container metrics
- **Distributed Tracing**: Request tracing with OpenTelemetry
- **Logging**: Structured logging with correlation IDs
- **Alerting**: Grafana alerts and notifications

## Security

Security features include:

- **JWT Authentication**: Token-based authentication
- **CORS Configuration**: Cross-origin resource sharing setup
- **Rate Limiting**: API rate limiting and throttling
- **Environment Isolation**: Separate configurations for different environments
- **Secrets Management**: Secure handling of sensitive configuration

## API Documentation

The platform provides comprehensive API documentation:

- **OpenAPI/Swagger**: Interactive API docs at `/docs`
- **ReDoc**: Alternative API documentation at `/redoc`
- **API Versioning**: Versioned API endpoints
- **Authentication**: JWT-based API authentication

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For support and questions:

- Create an issue in the repository
- Check the documentation in the `docs/` directory
- Review the API documentation at `/docs` when running

## Roadmap

- [ ] Kubernetes deployment support
- [ ] Advanced ML model serving
- [ ] Real-time anomaly detection
- [ ] Enhanced security features
- [ ] Multi-tenant support
- [ ] Advanced visualization features

## Acknowledgments

- Apache Spark community
- MLflow contributors
- FastAPI developers
- Streamlit team
- All open-source contributors