#!/bin/bash
# QuantStream Analytics Platform - Infrastructure Health Check Script
# This script performs comprehensive health checks across all infrastructure components

set -euo pipefail

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

# Default values
ENVIRONMENT=""
CLOUD_PROVIDER="all"
OUTPUT_FORMAT="table"
SAVE_REPORT="false"
CHECK_CONNECTIVITY="true"
CHECK_PERFORMANCE="true"
ALERT_THRESHOLD="80"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Health check results
HEALTH_RESULTS=()
FAILED_CHECKS=0
TOTAL_CHECKS=0

# Logging functions
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

# Usage function
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Perform comprehensive health checks on QuantStream Analytics Platform infrastructure

OPTIONS:
    -e, --environment ENV       Environment to check (dev, staging, prod) [REQUIRED]
    -c, --cloud PROVIDER        Cloud provider (aws, azure, gcp, all) [default: all]
    -f, --format FORMAT         Output format (table, json, yaml) [default: table]
    -s, --save-report          Save health check report to file
    --no-connectivity          Skip connectivity checks
    --no-performance           Skip performance checks
    -t, --threshold PERCENT     Alert threshold percentage [default: 80]
    -h, --help                  Show this help message

EXAMPLES:
    # Check all infrastructure in production
    $0 -e prod

    # Check only AWS infrastructure with JSON output
    $0 -e staging -c aws -f json

    # Perform basic checks only
    $0 -e dev --no-performance

    # Save detailed report
    $0 -e prod --save-report

EOF
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -e|--environment)
                ENVIRONMENT="$2"
                shift 2
                ;;
            -c|--cloud)
                CLOUD_PROVIDER="$2"
                shift 2
                ;;
            -f|--format)
                OUTPUT_FORMAT="$2"
                shift 2
                ;;
            -s|--save-report)
                SAVE_REPORT="true"
                shift
                ;;
            --no-connectivity)
                CHECK_CONNECTIVITY="false"
                shift
                ;;
            --no-performance)
                CHECK_PERFORMANCE="false"
                shift
                ;;
            -t|--threshold)
                ALERT_THRESHOLD="$2"
                shift 2
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done

    # Validate required arguments
    if [[ -z "$ENVIRONMENT" ]]; then
        log_error "Environment is required (-e/--environment)"
        usage
        exit 1
    fi

    # Validate environment
    if [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
        log_error "Invalid environment: $ENVIRONMENT. Must be one of: dev, staging, prod"
        exit 1
    fi
}

# Add health check result
add_health_result() {
    local service="$1"
    local check="$2"
    local status="$3"
    local details="$4"
    local cloud="${5:-unknown}"
    
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    if [[ "$status" != "PASS" ]]; then
        FAILED_CHECKS=$((FAILED_CHECKS + 1))
    fi
    
    HEALTH_RESULTS+=("{\"service\":\"$service\",\"check\":\"$check\",\"status\":\"$status\",\"details\":\"$details\",\"cloud\":\"$cloud\",\"timestamp\":\"$TIMESTAMP\"}")
}

# Get Terraform outputs
get_terraform_outputs() {
    local terraform_dir="${PROJECT_ROOT}/infrastructure/environments/${ENVIRONMENT}"
    
    if [[ ! -d "$terraform_dir" ]]; then
        log_error "Terraform directory not found: $terraform_dir"
        return 1
    fi
    
    cd "$terraform_dir"
    
    if [[ ! -f ".terraform/terraform.tfstate" ]] && [[ ! -f "terraform.tfstate" ]]; then
        log_error "No Terraform state found. Infrastructure may not be deployed."
        return 1
    fi
    
    terraform output -json 2>/dev/null || echo '{}'
}

# Check AWS infrastructure
check_aws_infrastructure() {
    if [[ "$CLOUD_PROVIDER" != "aws" && "$CLOUD_PROVIDER" != "all" ]]; then
        return 0
    fi
    
    log_info "Checking AWS infrastructure..."
    
    # Check AWS CLI availability
    if ! command -v aws &> /dev/null; then
        add_health_result "AWS" "CLI" "FAIL" "AWS CLI not available" "aws"
        return 0
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        add_health_result "AWS" "Credentials" "FAIL" "AWS credentials not configured" "aws"
        return 0
    fi
    
    add_health_result "AWS" "Credentials" "PASS" "AWS credentials configured" "aws"
    
    # Get Terraform outputs for AWS endpoints
    local outputs=$(get_terraform_outputs)
    
    # Check EKS cluster
    local eks_endpoint=$(echo "$outputs" | jq -r '.cluster_endpoints.value.aws // empty' 2>/dev/null)
    if [[ -n "$eks_endpoint" && "$eks_endpoint" != "null" ]]; then
        if [[ "$CHECK_CONNECTIVITY" == "true" ]]; then
            if timeout 10 curl -k -s "$eks_endpoint/healthz" &> /dev/null; then
                add_health_result "EKS" "Connectivity" "PASS" "EKS API server reachable" "aws"
            else
                add_health_result "EKS" "Connectivity" "FAIL" "EKS API server unreachable" "aws"
            fi
        fi
    else
        add_health_result "EKS" "Deployment" "FAIL" "EKS cluster not found" "aws"
    fi
    
    # Check RDS
    local rds_endpoint=$(echo "$outputs" | jq -r '.data_infrastructure_info.value.database_endpoints.aws.primary_endpoint // empty' 2>/dev/null)
    if [[ -n "$rds_endpoint" && "$rds_endpoint" != "null" ]]; then
        add_health_result "RDS" "Deployment" "PASS" "RDS instance deployed" "aws"
        
        if [[ "$CHECK_CONNECTIVITY" == "true" ]]; then
            if timeout 5 nc -z "${rds_endpoint}" 5432 &> /dev/null; then
                add_health_result "RDS" "Connectivity" "PASS" "RDS port 5432 reachable" "aws"
            else
                add_health_result "RDS" "Connectivity" "FAIL" "RDS port 5432 unreachable" "aws"
            fi
        fi
    else
        add_health_result "RDS" "Deployment" "FAIL" "RDS instance not found" "aws"
    fi
    
    # Check Redis
    local redis_endpoint=$(echo "$outputs" | jq -r '.data_infrastructure_info.value.redis_endpoints.aws.primary_endpoint // empty' 2>/dev/null)
    if [[ -n "$redis_endpoint" && "$redis_endpoint" != "null" ]]; then
        add_health_result "Redis" "Deployment" "PASS" "Redis cluster deployed" "aws"
        
        if [[ "$CHECK_CONNECTIVITY" == "true" ]]; then
            if timeout 5 nc -z "${redis_endpoint}" 6379 &> /dev/null; then
                add_health_result "Redis" "Connectivity" "PASS" "Redis port 6379 reachable" "aws"
            else
                add_health_result "Redis" "Connectivity" "FAIL" "Redis port 6379 unreachable" "aws"
            fi
        fi
    else
        add_health_result "Redis" "Deployment" "FAIL" "Redis cluster not found" "aws"
    fi
    
    # Check MSK (Kafka)
    local kafka_arn=$(echo "$outputs" | jq -r '.data_infrastructure_info.value.kafka_endpoints.aws.cluster_arn // empty' 2>/dev/null)
    if [[ -n "$kafka_arn" && "$kafka_arn" != "null" ]]; then
        add_health_result "MSK" "Deployment" "PASS" "MSK cluster deployed" "aws"
    else
        add_health_result "MSK" "Deployment" "FAIL" "MSK cluster not found" "aws"
    fi
    
    # Check S3 bucket
    local s3_bucket=$(echo "$outputs" | jq -r '.data_infrastructure_info.value.storage_buckets.aws.bucket_name // empty' 2>/dev/null)
    if [[ -n "$s3_bucket" && "$s3_bucket" != "null" ]]; then
        if aws s3 ls "s3://${s3_bucket}" &> /dev/null; then
            add_health_result "S3" "Access" "PASS" "S3 bucket accessible" "aws"
        else
            add_health_result "S3" "Access" "FAIL" "S3 bucket not accessible" "aws"
        fi
    else
        add_health_result "S3" "Deployment" "FAIL" "S3 bucket not found" "aws"
    fi
}

# Check Azure infrastructure
check_azure_infrastructure() {
    if [[ "$CLOUD_PROVIDER" != "azure" && "$CLOUD_PROVIDER" != "all" ]]; then
        return 0
    fi
    
    log_info "Checking Azure infrastructure..."
    
    # Check Azure CLI availability
    if ! command -v az &> /dev/null; then
        add_health_result "Azure" "CLI" "FAIL" "Azure CLI not available" "azure"
        return 0
    fi
    
    # Check Azure authentication
    if ! az account show &> /dev/null; then
        add_health_result "Azure" "Authentication" "FAIL" "Azure authentication required" "azure"
        return 0
    fi
    
    add_health_result "Azure" "Authentication" "PASS" "Azure authenticated" "azure"
    
    # Get Terraform outputs for Azure endpoints
    local outputs=$(get_terraform_outputs)
    
    # Check AKS cluster
    local aks_endpoint=$(echo "$outputs" | jq -r '.cluster_endpoints.value.azure // empty' 2>/dev/null)
    if [[ -n "$aks_endpoint" && "$aks_endpoint" != "null" ]]; then
        add_health_result "AKS" "Deployment" "PASS" "AKS cluster deployed" "azure"
    else
        add_health_result "AKS" "Deployment" "FAIL" "AKS cluster not found" "azure"
    fi
    
    # Check PostgreSQL
    local pg_fqdn=$(echo "$outputs" | jq -r '.data_infrastructure_info.value.database_endpoints.azure.fqdn // empty' 2>/dev/null)
    if [[ -n "$pg_fqdn" && "$pg_fqdn" != "null" ]]; then
        add_health_result "PostgreSQL" "Deployment" "PASS" "PostgreSQL server deployed" "azure"
        
        if [[ "$CHECK_CONNECTIVITY" == "true" ]]; then
            if timeout 5 nc -z "${pg_fqdn}" 5432 &> /dev/null; then
                add_health_result "PostgreSQL" "Connectivity" "PASS" "PostgreSQL port 5432 reachable" "azure"
            else
                add_health_result "PostgreSQL" "Connectivity" "FAIL" "PostgreSQL port 5432 unreachable" "azure"
            fi
        fi
    else
        add_health_result "PostgreSQL" "Deployment" "FAIL" "PostgreSQL server not found" "azure"
    fi
    
    # Check Redis
    local redis_hostname=$(echo "$outputs" | jq -r '.data_infrastructure_info.value.redis_endpoints.azure.hostname // empty' 2>/dev/null)
    if [[ -n "$redis_hostname" && "$redis_hostname" != "null" ]]; then
        add_health_result "Redis" "Deployment" "PASS" "Redis cache deployed" "azure"
    else
        add_health_result "Redis" "Deployment" "FAIL" "Redis cache not found" "azure"
    fi
    
    # Check Event Hubs
    local eventhub_namespace=$(echo "$outputs" | jq -r '.data_infrastructure_info.value.kafka_endpoints.azure.namespace_name // empty' 2>/dev/null)
    if [[ -n "$eventhub_namespace" && "$eventhub_namespace" != "null" ]]; then
        add_health_result "EventHubs" "Deployment" "PASS" "Event Hubs namespace deployed" "azure"
    else
        add_health_result "EventHubs" "Deployment" "FAIL" "Event Hubs namespace not found" "azure"
    fi
}

# Check GCP infrastructure
check_gcp_infrastructure() {
    if [[ "$CLOUD_PROVIDER" != "gcp" && "$CLOUD_PROVIDER" != "all" ]]; then
        return 0
    fi
    
    log_info "Checking GCP infrastructure..."
    
    # Check gcloud CLI availability
    if ! command -v gcloud &> /dev/null; then
        add_health_result "GCP" "CLI" "FAIL" "gcloud CLI not available" "gcp"
        return 0
    fi
    
    # Check GCP authentication
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -1 &> /dev/null; then
        add_health_result "GCP" "Authentication" "FAIL" "GCP authentication required" "gcp"
        return 0
    fi
    
    add_health_result "GCP" "Authentication" "PASS" "GCP authenticated" "gcp"
    
    # Get Terraform outputs for GCP endpoints
    local outputs=$(get_terraform_outputs)
    
    # Check GKE cluster
    local gke_endpoint=$(echo "$outputs" | jq -r '.cluster_endpoints.value.gcp // empty' 2>/dev/null)
    if [[ -n "$gke_endpoint" && "$gke_endpoint" != "null" ]]; then
        add_health_result "GKE" "Deployment" "PASS" "GKE cluster deployed" "gcp"
    else
        add_health_result "GKE" "Deployment" "FAIL" "GKE cluster not found" "gcp"
    fi
    
    # Check Cloud SQL
    local sql_connection=$(echo "$outputs" | jq -r '.data_infrastructure_info.value.database_endpoints.gcp.connection_name // empty' 2>/dev/null)
    if [[ -n "$sql_connection" && "$sql_connection" != "null" ]]; then
        add_health_result "CloudSQL" "Deployment" "PASS" "Cloud SQL instance deployed" "gcp"
    else
        add_health_result "CloudSQL" "Deployment" "FAIL" "Cloud SQL instance not found" "gcp"
    fi
    
    # Check Memorystore Redis
    local redis_host=$(echo "$outputs" | jq -r '.data_infrastructure_info.value.redis_endpoints.gcp.host // empty' 2>/dev/null)
    if [[ -n "$redis_host" && "$redis_host" != "null" ]]; then
        add_health_result "Memorystore" "Deployment" "PASS" "Memorystore Redis deployed" "gcp"
    else
        add_health_result "Memorystore" "Deployment" "FAIL" "Memorystore Redis not found" "gcp"
    fi
    
    # Check Pub/Sub
    local pubsub_topic=$(echo "$outputs" | jq -r '.data_infrastructure_info.value.kafka_endpoints.gcp.topic_name // empty' 2>/dev/null)
    if [[ -n "$pubsub_topic" && "$pubsub_topic" != "null" ]]; then
        add_health_result "PubSub" "Deployment" "PASS" "Pub/Sub topic deployed" "gcp"
    else
        add_health_result "PubSub" "Deployment" "FAIL" "Pub/Sub topic not found" "gcp"
    fi
    
    # Check Cloud Storage
    local storage_bucket=$(echo "$outputs" | jq -r '.data_infrastructure_info.value.storage_buckets.gcp.bucket_name // empty' 2>/dev/null)
    if [[ -n "$storage_bucket" && "$storage_bucket" != "null" ]]; then
        if gsutil ls "gs://${storage_bucket}" &> /dev/null; then
            add_health_result "CloudStorage" "Access" "PASS" "Cloud Storage bucket accessible" "gcp"
        else
            add_health_result "CloudStorage" "Access" "FAIL" "Cloud Storage bucket not accessible" "gcp"
        fi
    else
        add_health_result "CloudStorage" "Deployment" "FAIL" "Cloud Storage bucket not found" "gcp"
    fi
}

# Output results in table format
output_table() {
    printf "%-15s %-20s %-10s %-10s %-50s\n" "CLOUD" "SERVICE" "CHECK" "STATUS" "DETAILS"
    printf "%s\n" "$(printf '%.100s' "$(yes '=' | head -100)")"
    
    for result in "${HEALTH_RESULTS[@]}"; do
        local cloud=$(echo "$result" | jq -r '.cloud')
        local service=$(echo "$result" | jq -r '.service')
        local check=$(echo "$result" | jq -r '.check')
        local status=$(echo "$result" | jq -r '.status')
        local details=$(echo "$result" | jq -r '.details')
        
        # Color code status
        case "$status" in
            "PASS")
                status_colored="${GREEN}PASS${NC}"
                ;;
            "FAIL")
                status_colored="${RED}FAIL${NC}"
                ;;
            "WARN")
                status_colored="${YELLOW}WARN${NC}"
                ;;
            *)
                status_colored="$status"
                ;;
        esac
        
        printf "%-15s %-20s %-10s %-18s %-50s\n" "$cloud" "$service" "$check" "$status_colored" "$details"
    done
}

# Output results in JSON format
output_json() {
    local json_output="["
    local first=true
    
    for result in "${HEALTH_RESULTS[@]}"; do
        if [[ "$first" == "true" ]]; then
            first=false
        else
            json_output+=","
        fi
        json_output+="$result"
    done
    
    json_output+="]"
    
    echo "$json_output" | jq '.'
}

# Save report to file
save_report() {
    if [[ "$SAVE_REPORT" != "true" ]]; then
        return 0
    fi
    
    local report_file="health-check-${ENVIRONMENT}-${TIMESTAMP}.${OUTPUT_FORMAT}"
    local report_dir="${PROJECT_ROOT}/reports"
    
    mkdir -p "$report_dir"
    
    case "$OUTPUT_FORMAT" in
        "json")
            output_json > "${report_dir}/${report_file}"
            ;;
        "table")
            output_table > "${report_dir}/${report_file}"
            ;;
    esac
    
    log_success "Health check report saved: ${report_dir}/${report_file}"
}

# Display summary
display_summary() {
    local success_rate=0
    if [[ $TOTAL_CHECKS -gt 0 ]]; then
        success_rate=$(( (TOTAL_CHECKS - FAILED_CHECKS) * 100 / TOTAL_CHECKS ))
    fi
    
    echo
    log_info "Health Check Summary"
    echo "===================="
    echo "Environment: $ENVIRONMENT"
    echo "Cloud Provider: $CLOUD_PROVIDER"
    echo "Total Checks: $TOTAL_CHECKS"
    echo "Passed: $((TOTAL_CHECKS - FAILED_CHECKS))"
    echo "Failed: $FAILED_CHECKS"
    echo "Success Rate: ${success_rate}%"
    
    if [[ $FAILED_CHECKS -gt 0 ]]; then
        if [[ $success_rate -lt $ALERT_THRESHOLD ]]; then
            log_error "Health check failed - Success rate ${success_rate}% is below threshold ${ALERT_THRESHOLD}%"
            exit 1
        else
            log_warning "Some health checks failed but success rate is acceptable"
        fi
    else
        log_success "All health checks passed!"
    fi
}

# Main function
main() {
    log_info "Starting QuantStream Analytics Platform health check"
    log_info "Environment: $ENVIRONMENT"
    log_info "Cloud Provider: $CLOUD_PROVIDER"
    
    # Run health checks
    check_aws_infrastructure
    check_azure_infrastructure
    check_gcp_infrastructure
    
    # Output results
    case "$OUTPUT_FORMAT" in
        "json")
            output_json
            ;;
        "table")
            output_table
            ;;
        *)
            log_error "Invalid output format: $OUTPUT_FORMAT"
            exit 1
            ;;
    esac
    
    save_report
    display_summary
}

# Parse arguments and run main function
parse_args "$@"
main