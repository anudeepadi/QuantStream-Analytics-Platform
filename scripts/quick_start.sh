#!/bin/bash

# QuantStream Analytics Platform - Quick Start Script
# This script helps you get the platform running quickly

set -e

echo "🚀 QuantStream Analytics Platform - Quick Start"
echo "================================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    local missing_deps=()
    
    if ! command_exists python3; then
        missing_deps+=("python3")
    fi
    
    if ! command_exists docker; then
        missing_deps+=("docker")
    fi
    
    if ! command_exists docker-compose; then
        missing_deps+=("docker-compose")
    fi
    
    if [ ${#missing_deps[@]} -ne 0 ]; then
        log_error "Missing dependencies: ${missing_deps[*]}"
        log_info "Please install the missing dependencies and run this script again."
        exit 1
    fi
    
    log_success "All prerequisites satisfied"
}

# Check if .env file exists and has required keys
check_env_file() {
    log_info "Checking environment configuration..."
    
    if [ ! -f .env ]; then
        log_warn ".env file not found, creating from template..."
        if [ -f .env.example ]; then
            cp .env.example .env
            log_success "Created .env from template"
        else
            log_error ".env.example not found. Please create .env file manually."
            exit 1
        fi
    fi
    
    # Check for critical environment variables
    local required_vars=("POSTGRES_PASSWORD" "REDIS_PASSWORD")
    local missing_vars=()
    
    for var in "${required_vars[@]}"; do
        if ! grep -q "^$var=" .env; then
            missing_vars+=("$var")
        fi
    done
    
    if [ ${#missing_vars[@]} -ne 0 ]; then
        log_warn "Missing environment variables: ${missing_vars[*]}"
        log_info "Please edit .env file and add the missing variables."
    fi
    
    log_success "Environment configuration checked"
}

# Start services
start_services() {
    log_info "Starting QuantStream services..."
    
    # Pull latest images
    log_info "Pulling Docker images..."
    docker-compose pull
    
    # Start services in background
    log_info "Starting services..."
    docker-compose up -d
    
    # Wait for services to be ready
    log_info "Waiting for services to be ready..."
    sleep 30
    
    # Check service health
    check_service_health
}

# Check service health
check_service_health() {
    log_info "Checking service health..."
    
    local services=("postgres" "redis" "kafka" "zookeeper")
    
    for service in "${services[@]}"; do
        if docker-compose ps "$service" | grep -q "Up"; then
            log_success "$service is running"
        else
            log_error "$service is not running"
        fi
    done
    
    # Check if API is responding
    log_info "Checking API health..."
    local retries=0
    local max_retries=30
    
    while [ $retries -lt $max_retries ]; do
        if curl -s http://localhost:8000/health >/dev/null 2>&1; then
            log_success "API is responding"
            break
        else
            retries=$((retries + 1))
            if [ $retries -eq $max_retries ]; then
                log_warn "API is not responding yet. It may still be starting up."
            else
                sleep 2
            fi
        fi
    done
}

# Initialize database
init_database() {
    log_info "Initializing database..."
    
    # Wait for PostgreSQL to be ready
    local retries=0
    local max_retries=30
    
    while [ $retries -lt $max_retries ]; do
        if docker-compose exec -T postgres pg_isready -U quantstream >/dev/null 2>&1; then
            break
        else
            retries=$((retries + 1))
            if [ $retries -eq $max_retries ]; then
                log_error "PostgreSQL is not ready"
                return 1
            fi
            sleep 2
        fi
    done
    
    # Run database migrations (if they exist)
    if [ -d "migrations" ]; then
        log_info "Running database migrations..."
        # Add your migration command here
        # docker-compose exec quantstream-api alembic upgrade head
    fi
    
    log_success "Database initialized"
}

# Create sample data
create_sample_data() {
    log_info "Creating sample data..."
    
    # Create sample symbols and data
    cat > /tmp/sample_data.sql << 'EOF'
-- Sample symbols
INSERT INTO symbols (symbol, name, sector, market_cap) VALUES
('AAPL', 'Apple Inc.', 'Technology', 3000000000000),
('GOOGL', 'Alphabet Inc.', 'Technology', 1800000000000),
('MSFT', 'Microsoft Corporation', 'Technology', 2800000000000),
('TSLA', 'Tesla, Inc.', 'Automotive', 800000000000),
('AMZN', 'Amazon.com, Inc.', 'E-commerce', 1500000000000)
ON CONFLICT (symbol) DO NOTHING;

-- Sample portfolio
INSERT INTO portfolio_positions (symbol, quantity, avg_cost, current_price) VALUES
('AAPL', 100, 150.00, 175.00),
('GOOGL', 50, 2800.00, 2900.00),
('TSLA', 25, 800.00, 900.00)
ON CONFLICT (symbol) DO UPDATE SET
quantity = EXCLUDED.quantity,
avg_cost = EXCLUDED.avg_cost,
current_price = EXCLUDED.current_price;
EOF
    
    if docker-compose exec -T postgres psql -U quantstream -d quantstream -f - < /tmp/sample_data.sql >/dev/null 2>&1; then
        log_success "Sample data created"
    else
        log_warn "Could not create sample data (tables may not exist yet)"
    fi
    
    rm -f /tmp/sample_data.sql
}

# Start data ingestion
start_data_ingestion() {
    log_info "Starting data ingestion..."
    
    # Check if API keys are configured
    if grep -q "your_key_here" .env || grep -q "YOUR_" .env; then
        log_warn "API keys not configured. Data ingestion may not work properly."
        log_info "Please edit .env file and add your API keys."
        return
    fi
    
    # Start ingestion via API
    if curl -s -X POST http://localhost:8000/ingestion/start \
        -H "Content-Type: application/json" \
        -d '{"sources": ["alpha_vantage"], "symbols": ["AAPL", "GOOGL", "MSFT"]}' >/dev/null; then
        log_success "Data ingestion started"
    else
        log_warn "Could not start data ingestion automatically"
        log_info "You can start it manually from the dashboard"
    fi
}

# Show access URLs
show_access_urls() {
    echo ""
    echo "🎉 QuantStream Analytics Platform is ready!"
    echo "=========================================="
    echo ""
    echo "📊 Dashboard:     http://localhost:8501"
    echo "🔧 API:          http://localhost:8000"
    echo "📈 Grafana:      http://localhost:3000 (admin/admin)"
    echo "⚡ Prometheus:   http://localhost:9090"
    echo "📓 Jupyter:      http://localhost:8888 (check logs for token)"
    echo "🤖 MLflow:       http://localhost:5000"
    echo ""
    echo "🔍 Health Check: curl http://localhost:8000/health"
    echo "📋 API Docs:     http://localhost:8000/docs"
    echo ""
}

# Show next steps
show_next_steps() {
    echo "🚀 Next Steps:"
    echo "=============="
    echo "1. Edit .env file with your API keys"
    echo "2. Visit the dashboard at http://localhost:8501"
    echo "3. Configure data sources in the dashboard"
    echo "4. Start monitoring your data pipeline!"
    echo ""
    echo "📚 Documentation: ./SETUP.md"
    echo "🔧 Configuration: ./config/"
    echo "📊 Logs: docker-compose logs -f"
    echo ""
}

# Cleanup function
cleanup() {
    if [ $? -ne 0 ]; then
        log_error "Setup failed!"
        log_info "To check what went wrong:"
        echo "  docker-compose logs"
        echo "  docker-compose ps"
        echo ""
        log_info "To clean up and try again:"
        echo "  docker-compose down -v"
        echo "  ./scripts/quick_start.sh"
    fi
}

# Main function
main() {
    trap cleanup EXIT
    
    echo "Starting quick setup..."
    echo ""
    
    check_prerequisites
    check_env_file
    start_services
    init_database
    create_sample_data
    start_data_ingestion
    
    show_access_urls
    show_next_steps
    
    log_success "Quick start completed successfully!"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-data)
            SKIP_DATA=true
            shift
            ;;
        --dev)
            DEV_MODE=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --skip-data    Skip creating sample data"
            echo "  --dev          Development mode (additional logging)"
            echo "  -h, --help     Show this help message"
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Run main function
main