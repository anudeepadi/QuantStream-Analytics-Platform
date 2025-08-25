# QuantStream Analytics Dashboard

A comprehensive real-time dashboard and monitoring system for financial analytics and trading operations.

## 🌟 Features

### Real-time Dashboard
- **Live Market Data**: Real-time candlestick charts with sub-second updates
- **Technical Indicators**: Bollinger Bands, RSI, MACD, Moving Averages
- **Portfolio Tracking**: Real-time P&L, position management, performance analytics
- **Interactive Charts**: Zoom, pan, drill-down capabilities with Plotly
- **Anomaly Detection**: AI-powered anomaly alerts and notifications
- **Mobile Responsive**: Cross-device compatibility

### Monitoring System
- **System Health**: CPU, memory, disk, network monitoring
- **Performance Metrics**: API response times, throughput, error rates
- **Service Monitoring**: Database, Redis, WebSocket, API health checks
- **Alert Management**: Configurable alerts with multiple notification channels
- **Data Quality**: Real-time data quality metrics and validation

## 🏗️ Architecture

### Frontend (Streamlit)
```
src/dashboard/frontend/
├── components/          # Reusable UI components
│   ├── sidebar.py      # Navigation and settings
│   ├── market_data.py  # Live market charts
│   ├── technical_indicators.py
│   ├── portfolio.py    # Portfolio management
│   ├── alerts.py       # Alert management
│   └── system_metrics.py
├── pages/              # Dashboard pages
├── utils/              # Frontend utilities
│   ├── config.py       # Configuration management
│   ├── auth.py         # Authentication
│   └── metrics.py      # User analytics
└── app.py              # Main application
```

### Backend (FastAPI + WebSocket)
```
src/dashboard/backend/
├── api/                # REST API endpoints
│   ├── main.py         # FastAPI application
│   └── endpoints/      # API routes
│       ├── market_data.py
│       ├── portfolio.py
│       ├── alerts.py
│       └── system_metrics.py
├── websocket/          # Real-time streaming
│   └── websocket_manager.py
├── services/           # Business logic
│   ├── database_service.py
│   ├── redis_service.py
│   └── auth_service.py
└── models/             # Data models
```

### Monitoring Infrastructure
```
src/monitoring/
├── prometheus/         # Metrics collection
│   └── metrics_collector.py
├── grafana/           # Dashboard configs
└── alerts/            # Alert management
```

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Docker & Docker Compose
- PostgreSQL
- Redis

### Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd quantstream-dashboard
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Start infrastructure services**
```bash
docker-compose up -d postgres redis prometheus grafana
```

4. **Configure environment**
```bash
cp .env.example .env
# Edit .env with your configuration
```

5. **Start the dashboard**
```bash
# Frontend
streamlit run src/dashboard/app.py

# Backend API
cd src/dashboard/backend && python -m api.main

# WebSocket server (automatically started with API)
```

### Docker Deployment

**Start all services:**
```bash
docker-compose up -d
```

**Services will be available at:**
- Dashboard: http://localhost:8501
- API: http://localhost:8000
- Grafana: http://localhost:3000
- Prometheus: http://localhost:9090

## 📊 Key Components

### 1. Market Data Component
- **Real-time streaming**: Sub-second price updates via WebSocket
- **Multiple timeframes**: 1m, 5m, 15m, 1h, 1d, 1w, 1m
- **Technical analysis**: 20+ technical indicators
- **Data sources**: YFinance (demo), extensible to real feeds

### 2. Portfolio Management
- **Position tracking**: Real-time P&L calculations
- **Performance analytics**: Sharpe ratio, max drawdown, alpha/beta
- **Risk metrics**: VaR, concentration risk, sector exposure
- **Transaction history**: Complete audit trail
- **Tax reporting**: Tax lot tracking, capital gains

### 3. Alert System
- **Price alerts**: Target prices, percentage moves
- **Volume alerts**: Unusual volume detection
- **Technical alerts**: Indicator-based signals
- **Anomaly detection**: AI-powered pattern recognition
- **Multi-channel notifications**: Email, Slack, webhooks

### 4. System Monitoring
- **Infrastructure metrics**: CPU, memory, disk, network
- **Application metrics**: Response times, error rates, throughput
- **Service health**: Database, Redis, API, WebSocket status
- **Performance optimization**: Caching, query optimization

## 🔧 Configuration

### Dashboard Configuration (`config/dashboard/config.yaml`)
```yaml
app:
  name: "QuantStream Analytics Dashboard"
  environment: "development"

server:
  host: "0.0.0.0"
  port: 8501

database:
  url: "postgresql://user:pass@localhost:5432/db"

redis:
  host: "localhost"
  port: 6379

dashboard:
  refresh_rate: 1000  # milliseconds
  auto_refresh: true

charts:
  default_theme: "plotly_white"
```

### Monitoring Configuration (`config/monitoring/prometheus.yml`)
- Custom metrics collection
- Service discovery
- Alert rules configuration
- Data retention settings

## 📈 Performance

### Metrics
- **Latency**: <300ms API response times
- **Throughput**: 1000+ concurrent users
- **Real-time updates**: <1 second chart refresh
- **Uptime**: 99.9% availability target
- **Cache hit rate**: >95% for market data

### Optimization Features
- **Redis caching**: Market data and user sessions
- **Database indexing**: Optimized queries
- **WebSocket streaming**: Efficient real-time updates
- **Load balancing**: Horizontal scaling support
- **Connection pooling**: Database and Redis connections

## 🔒 Security

### Authentication & Authorization
- **JWT tokens**: Secure API access
- **Role-based access**: Admin, Analyst, Trader, Viewer roles
- **Session management**: Redis-based sessions
- **Password security**: PBKDF2 hashing with salt

### API Security
- **Rate limiting**: Configurable request limits
- **CORS protection**: Cross-origin request filtering
- **Input validation**: Pydantic model validation
- **Error handling**: Secure error responses

## 📊 Monitoring & Observability

### Metrics Collection
- **System metrics**: CPU, memory, disk, network
- **Application metrics**: Request rates, response times, errors
- **Business metrics**: Portfolio values, trading volumes
- **Custom metrics**: Domain-specific KPIs

### Dashboards
- **Grafana dashboards**: Pre-configured monitoring views
- **Real-time alerting**: Threshold-based notifications
- **Historical analysis**: Time-series data visualization
- **Performance trending**: Long-term performance analysis

## 🧪 Testing

### Test Structure
```
tests/
├── dashboard/          # Dashboard tests
├── unit/              # Unit tests
├── integration/       # Integration tests
└── performance/       # Performance tests
```

### Running Tests
```bash
# Unit tests
pytest tests/unit/

# Integration tests
pytest tests/integration/

# Performance tests
pytest tests/performance/

# All tests with coverage
pytest --cov=src tests/
```

## 🐳 Deployment

### Docker Services
- **Application containers**: Dashboard, API, WebSocket
- **Infrastructure**: PostgreSQL, Redis, Prometheus, Grafana
- **Monitoring**: Node Exporter, Redis Exporter, Postgres Exporter

### Environment Variables
```bash
ENVIRONMENT=production
DATABASE_URL=postgresql://...
REDIS_URL=redis://...
SECRET_KEY=your-secret-key
PROMETHEUS_URL=http://prometheus:9090
```

### Scaling
- **Horizontal scaling**: Multiple dashboard instances
- **Load balancing**: Nginx reverse proxy
- **Database scaling**: Read replicas, connection pooling
- **Cache scaling**: Redis cluster support

## 📝 API Documentation

### REST API
- **Interactive docs**: http://localhost:8000/docs
- **OpenAPI spec**: Comprehensive API documentation
- **Authentication**: JWT bearer tokens
- **Rate limiting**: Configurable per endpoint

### WebSocket API
- **Market data streaming**: Real-time price feeds
- **System metrics**: Live monitoring data
- **Event notifications**: Alert and system events

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

### Code Standards
- **Python**: PEP 8, type hints, docstrings
- **Testing**: >90% coverage requirement
- **Documentation**: Comprehensive API docs
- **Security**: Security review for all changes

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

- **Documentation**: [Link to documentation]
- **Issues**: [GitHub Issues]
- **Discussions**: [GitHub Discussions]
- **Email**: support@quantstream.ai

---

**QuantStream Analytics Dashboard** - Real-time financial data visualization and monitoring platform.