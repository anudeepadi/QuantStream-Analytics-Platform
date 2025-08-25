#!/bin/bash
# QuantStream Analytics Platform - Monitoring Setup Script
#
# This script sets up comprehensive monitoring for the QuantStream platform
# including Prometheus, Grafana, and alerting components.

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
MONITORING_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$MONITORING_DIR")"
GRAFANA_ADMIN_PASSWORD="${GRAFANA_ADMIN_PASSWORD:-quantstream}"

# Functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_dependencies() {
    log_info "Checking dependencies..."
    
    local deps=("docker" "docker-compose")
    local missing_deps=()
    
    for dep in "${deps[@]}"; do
        if ! command -v "$dep" &> /dev/null; then
            missing_deps+=("$dep")
        fi
    done
    
    if [ ${#missing_deps[@]} -gt 0 ]; then
        log_error "Missing dependencies: ${missing_deps[*]}"
        log_error "Please install the missing dependencies and try again."
        exit 1
    fi
    
    log_success "All dependencies are available"
}

setup_directories() {
    log_info "Setting up monitoring directories..."
    
    local dirs=(
        "$PROJECT_ROOT/data/prometheus"
        "$PROJECT_ROOT/data/grafana"
        "$PROJECT_ROOT/data/alertmanager"
        "$PROJECT_ROOT/logs/monitoring"
    )
    
    for dir in "${dirs[@]}"; do
        mkdir -p "$dir"
        log_info "Created directory: $dir"
    done
    
    # Set permissions for Grafana
    chmod 777 "$PROJECT_ROOT/data/grafana"
    
    log_success "Monitoring directories created"
}

validate_config_files() {
    log_info "Validating monitoring configuration files..."
    
    local config_files=(
        "$MONITORING_DIR/prometheus.yml"
        "$MONITORING_DIR/alert_rules.yml"
        "$MONITORING_DIR/recording_rules.yml"
        "$MONITORING_DIR/grafana/datasources/prometheus.yml"
        "$MONITORING_DIR/grafana/dashboards/dashboard.yml"
    )
    
    local missing_files=()
    
    for file in "${config_files[@]}"; do
        if [ ! -f "$file" ]; then
            missing_files+=("$file")
        fi
    done
    
    if [ ${#missing_files[@]} -gt 0 ]; then
        log_error "Missing configuration files:"
        for file in "${missing_files[@]}"; do
            log_error "  - $file"
        done
        exit 1
    fi
    
    log_success "All configuration files are present"
}

check_prometheus_config() {
    log_info "Validating Prometheus configuration..."
    
    # Use promtool if available
    if command -v promtool &> /dev/null; then
        if promtool check config "$MONITORING_DIR/prometheus.yml"; then
            log_success "Prometheus configuration is valid"
        else
            log_error "Prometheus configuration is invalid"
            exit 1
        fi
        
        if promtool check rules "$MONITORING_DIR/alert_rules.yml"; then
            log_success "Alert rules are valid"
        else
            log_error "Alert rules are invalid"
            exit 1
        fi
        
        if promtool check rules "$MONITORING_DIR/recording_rules.yml"; then
            log_success "Recording rules are valid"
        else
            log_error "Recording rules are invalid"
            exit 1
        fi
    else
        log_warning "promtool not available, skipping configuration validation"
    fi
}

start_monitoring_stack() {
    log_info "Starting monitoring stack..."
    
    cd "$PROJECT_ROOT"
    
    # Check if docker-compose.yml exists
    if [ ! -f "examples/docker-compose.yml" ]; then
        log_error "docker-compose.yml not found in examples directory"
        exit 1
    fi
    
    # Start the monitoring components
    docker-compose -f examples/docker-compose.yml up -d prometheus grafana
    
    # Wait for services to be ready
    log_info "Waiting for services to start..."
    sleep 10
    
    # Check if services are running
    if docker-compose -f examples/docker-compose.yml ps prometheus | grep -q "Up"; then
        log_success "Prometheus is running"
    else
        log_error "Prometheus failed to start"
        exit 1
    fi
    
    if docker-compose -f examples/docker-compose.yml ps grafana | grep -q "Up"; then
        log_success "Grafana is running"
    else
        log_error "Grafana failed to start"
        exit 1
    fi
}

test_monitoring_endpoints() {
    log_info "Testing monitoring endpoints..."
    
    # Test Prometheus
    local prometheus_url="http://localhost:9090"
    if curl -sf "$prometheus_url/-/ready" > /dev/null; then
        log_success "Prometheus is accessible at $prometheus_url"
    else
        log_error "Prometheus is not accessible at $prometheus_url"
        exit 1
    fi
    
    # Test Grafana
    local grafana_url="http://localhost:3000"
    if curl -sf "$grafana_url/api/health" > /dev/null; then
        log_success "Grafana is accessible at $grafana_url"
    else
        log_error "Grafana is not accessible at $grafana_url"
        exit 1
    fi
}

run_health_check() {
    log_info "Running comprehensive health check..."
    
    cd "$MONITORING_DIR"
    
    if [ -f "health_check.py" ]; then
        if python3 health_check.py --output text; then
            log_success "Health check passed"
        else
            log_warning "Health check found some issues (this is normal if not all services are running)"
        fi
    else
        log_warning "Health check script not found, skipping"
    fi
}

display_access_info() {
    log_success "Monitoring setup completed successfully!"
    echo
    echo "Access Information:"
    echo "=================="
    echo "Prometheus: http://localhost:9090"
    echo "Grafana:    http://localhost:3000"
    echo "  Username: admin"
    echo "  Password: $GRAFANA_ADMIN_PASSWORD"
    echo
    echo "Grafana Dashboards:"
    echo "- QuantStream Overview: http://localhost:3000/d/quantstream-overview"
    echo
    echo "Health Check:"
    echo "- Run: python3 monitoring/health_check.py"
    echo
    echo "Logs:"
    echo "- View logs: docker-compose -f examples/docker-compose.yml logs -f prometheus grafana"
    echo
    echo "To stop monitoring:"
    echo "- docker-compose -f examples/docker-compose.yml down"
}

main() {
    log_info "Setting up QuantStream Analytics Platform monitoring..."
    echo
    
    check_dependencies
    setup_directories
    validate_config_files
    check_prometheus_config
    start_monitoring_stack
    test_monitoring_endpoints
    run_health_check
    
    echo
    display_access_info
}

# Handle script arguments
case "${1:-}" in
    "start")
        start_monitoring_stack
        test_monitoring_endpoints
        display_access_info
        ;;
    "stop")
        log_info "Stopping monitoring stack..."
        cd "$PROJECT_ROOT"
        docker-compose -f examples/docker-compose.yml down
        log_success "Monitoring stack stopped"
        ;;
    "restart")
        log_info "Restarting monitoring stack..."
        cd "$PROJECT_ROOT"
        docker-compose -f examples/docker-compose.yml restart prometheus grafana
        sleep 5
        test_monitoring_endpoints
        log_success "Monitoring stack restarted"
        ;;
    "health")
        run_health_check
        ;;
    "logs")
        cd "$PROJECT_ROOT"
        docker-compose -f examples/docker-compose.yml logs -f prometheus grafana
        ;;
    *)
        main
        ;;
esac