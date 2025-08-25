#!/bin/bash
# QuantStream Analytics Platform - Infrastructure Deployment Script
# This script handles the complete deployment of infrastructure across environments

set -euo pipefail

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

# Default values
ENVIRONMENT=""
CLOUD_PROVIDER="aws"
AUTO_APPROVE="false"
INIT_BACKEND="true"
VALIDATE_ONLY="false"
DRY_RUN="false"
DESTROY_MODE="false"
TERRAFORM_DIR=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

Deploy QuantStream Analytics Platform infrastructure

OPTIONS:
    -e, --environment ENV       Environment to deploy (dev, staging, prod) [REQUIRED]
    -c, --cloud PROVIDER        Cloud provider (aws, azure, gcp, all) [default: aws]
    -a, --auto-approve          Auto-approve Terraform apply
    -n, --no-init              Skip Terraform backend initialization
    -v, --validate-only         Only validate configuration, don't deploy
    -d, --dry-run              Show planned changes without applying
    --destroy                   Destroy infrastructure (DANGEROUS)
    -h, --help                  Show this help message

EXAMPLES:
    # Deploy to development environment on AWS
    $0 -e dev -c aws

    # Deploy to production with auto-approve
    $0 -e prod -c all --auto-approve

    # Validate configuration only
    $0 -e staging --validate-only

    # Show planned changes (dry run)
    $0 -e dev --dry-run

    # Destroy infrastructure (BE CAREFUL!)
    $0 -e dev --destroy

PREREQUISITES:
    - Terraform >= 1.5.0
    - AWS CLI (if using AWS)
    - Azure CLI (if using Azure)
    - gcloud CLI (if using GCP)
    - Appropriate cloud provider credentials configured

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
            -a|--auto-approve)
                AUTO_APPROVE="true"
                shift
                ;;
            -n|--no-init)
                INIT_BACKEND="false"
                shift
                ;;
            -v|--validate-only)
                VALIDATE_ONLY="true"
                shift
                ;;
            -d|--dry-run)
                DRY_RUN="true"
                shift
                ;;
            --destroy)
                DESTROY_MODE="true"
                shift
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

    # Validate cloud provider
    if [[ ! "$CLOUD_PROVIDER" =~ ^(aws|azure|gcp|all)$ ]]; then
        log_error "Invalid cloud provider: $CLOUD_PROVIDER. Must be one of: aws, azure, gcp, all"
        exit 1
    fi

    TERRAFORM_DIR="${PROJECT_ROOT}/infrastructure/environments/${ENVIRONMENT}"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    # Check if Terraform is installed
    if ! command -v terraform &> /dev/null; then
        log_error "Terraform is not installed or not in PATH"
        exit 1
    fi

    # Check Terraform version
    TERRAFORM_VERSION=$(terraform version -json | jq -r '.terraform_version' 2>/dev/null || echo "unknown")
    log_info "Terraform version: $TERRAFORM_VERSION"

    # Check if environment directory exists
    if [[ ! -d "$TERRAFORM_DIR" ]]; then
        log_error "Environment directory does not exist: $TERRAFORM_DIR"
        exit 1
    fi

    # Check cloud provider CLIs based on selection
    if [[ "$CLOUD_PROVIDER" == "aws" || "$CLOUD_PROVIDER" == "all" ]]; then
        if ! command -v aws &> /dev/null; then
            log_warning "AWS CLI is not installed. Required for AWS deployments."
        else
            log_info "AWS CLI version: $(aws --version 2>&1 | cut -d/ -f2 | cut -d' ' -f1)"
        fi
    fi

    if [[ "$CLOUD_PROVIDER" == "azure" || "$CLOUD_PROVIDER" == "all" ]]; then
        if ! command -v az &> /dev/null; then
            log_warning "Azure CLI is not installed. Required for Azure deployments."
        else
            log_info "Azure CLI version: $(az version --query '\"azure-cli\"' -o tsv 2>/dev/null)"
        fi
    fi

    if [[ "$CLOUD_PROVIDER" == "gcp" || "$CLOUD_PROVIDER" == "all" ]]; then
        if ! command -v gcloud &> /dev/null; then
            log_warning "Google Cloud CLI is not installed. Required for GCP deployments."
        else
            log_info "gcloud CLI version: $(gcloud version --format='value(Google Cloud SDK)' 2>/dev/null)"
        fi
    fi
}

# Initialize Terraform backend
init_terraform() {
    if [[ "$INIT_BACKEND" == "false" ]]; then
        log_info "Skipping Terraform backend initialization"
        return 0
    fi

    log_info "Initializing Terraform backend for $ENVIRONMENT environment..."
    
    cd "$TERRAFORM_DIR"
    
    # Create backend configuration based on environment
    BACKEND_CONFIG_FILE="backend-${ENVIRONMENT}.hcl"
    
    if [[ ! -f "$BACKEND_CONFIG_FILE" ]]; then
        log_warning "Backend configuration file not found: $BACKEND_CONFIG_FILE"
        log_info "Creating default backend configuration..."
        
        cat > "$BACKEND_CONFIG_FILE" << EOF
# Backend configuration for $ENVIRONMENT environment
bucket = "quantstream-terraform-state-${ENVIRONMENT}"
key    = "infrastructure/${ENVIRONMENT}/terraform.tfstate"
region = "us-west-2"
encrypt = true
dynamodb_table = "terraform-lock-${ENVIRONMENT}"
EOF
    fi
    
    terraform init -backend-config="$BACKEND_CONFIG_FILE" -upgrade
    
    if [[ $? -ne 0 ]]; then
        log_error "Terraform initialization failed"
        exit 1
    fi
    
    log_success "Terraform backend initialized successfully"
}

# Validate Terraform configuration
validate_terraform() {
    log_info "Validating Terraform configuration..."
    
    cd "$TERRAFORM_DIR"
    terraform validate
    
    if [[ $? -ne 0 ]]; then
        log_error "Terraform validation failed"
        exit 1
    fi
    
    log_success "Terraform configuration is valid"
}

# Plan Terraform deployment
plan_terraform() {
    log_info "Creating Terraform deployment plan..."
    
    cd "$TERRAFORM_DIR"
    
    # Create variable files based on cloud provider selection
    TF_VAR_FILE="terraform.tfvars"
    PLAN_FILE="terraform-${ENVIRONMENT}-${TIMESTAMP}.plan"
    
    # Set cloud provider variables
    TERRAFORM_VARS=""
    case "$CLOUD_PROVIDER" in
        "aws")
            TERRAFORM_VARS="-var 'enable_aws=true' -var 'enable_azure=false' -var 'enable_gcp=false'"
            ;;
        "azure")
            TERRAFORM_VARS="-var 'enable_aws=false' -var 'enable_azure=true' -var 'enable_gcp=false'"
            ;;
        "gcp")
            TERRAFORM_VARS="-var 'enable_aws=false' -var 'enable_azure=false' -var 'enable_gcp=true'"
            ;;
        "all")
            TERRAFORM_VARS="-var 'enable_aws=true' -var 'enable_azure=true' -var 'enable_gcp=true'"
            ;;
    esac
    
    if [[ "$DESTROY_MODE" == "true" ]]; then
        eval "terraform plan -destroy -var-file='$TF_VAR_FILE' $TERRAFORM_VARS -out='$PLAN_FILE'"
    else
        eval "terraform plan -var-file='$TF_VAR_FILE' $TERRAFORM_VARS -out='$PLAN_FILE'"
    fi
    
    if [[ $? -ne 0 ]]; then
        log_error "Terraform planning failed"
        exit 1
    fi
    
    log_success "Terraform plan created: $PLAN_FILE"
    echo "PLAN_FILE=$PLAN_FILE" # Export for use in apply function
}

# Apply Terraform deployment
apply_terraform() {
    if [[ "$VALIDATE_ONLY" == "true" ]]; then
        log_info "Validation-only mode - skipping apply"
        return 0
    fi
    
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "Dry-run mode - skipping apply"
        return 0
    fi
    
    cd "$TERRAFORM_DIR"
    
    # Get plan file from plan function
    PLAN_FILE="terraform-${ENVIRONMENT}-${TIMESTAMP}.plan"
    
    if [[ ! -f "$PLAN_FILE" ]]; then
        log_error "Plan file not found: $PLAN_FILE"
        exit 1
    fi
    
    if [[ "$DESTROY_MODE" == "true" ]]; then
        log_warning "DESTROY MODE - This will delete all infrastructure!"
        if [[ "$AUTO_APPROVE" != "true" ]]; then
            read -p "Are you sure you want to destroy the infrastructure? Type 'yes' to confirm: " confirm
            if [[ "$confirm" != "yes" ]]; then
                log_info "Destroy cancelled by user"
                exit 0
            fi
        fi
    fi
    
    log_info "Applying Terraform deployment..."
    
    if [[ "$AUTO_APPROVE" == "true" ]]; then
        terraform apply -auto-approve "$PLAN_FILE"
    else
        terraform apply "$PLAN_FILE"
    fi
    
    if [[ $? -ne 0 ]]; then
        log_error "Terraform apply failed"
        exit 1
    fi
    
    # Clean up plan file after successful apply
    rm -f "$PLAN_FILE"
    
    if [[ "$DESTROY_MODE" == "true" ]]; then
        log_success "Infrastructure destroyed successfully"
    else
        log_success "Infrastructure deployed successfully"
    fi
}

# Save deployment information
save_deployment_info() {
    if [[ "$DESTROY_MODE" == "true" || "$VALIDATE_ONLY" == "true" || "$DRY_RUN" == "true" ]]; then
        return 0
    fi
    
    log_info "Saving deployment information..."
    
    cd "$TERRAFORM_DIR"
    
    DEPLOYMENT_INFO_FILE="deployment-${ENVIRONMENT}-${TIMESTAMP}.json"
    
    cat > "$DEPLOYMENT_INFO_FILE" << EOF
{
  "timestamp": "$TIMESTAMP",
  "environment": "$ENVIRONMENT",
  "cloud_provider": "$CLOUD_PROVIDER",
  "terraform_version": "$TERRAFORM_VERSION",
  "deployment_status": "completed",
  "outputs": $(terraform output -json 2>/dev/null || echo '{}')
}
EOF
    
    log_success "Deployment information saved: $DEPLOYMENT_INFO_FILE"
}

# Main deployment function
main() {
    log_info "Starting QuantStream Analytics Platform deployment"
    log_info "Environment: $ENVIRONMENT"
    log_info "Cloud Provider: $CLOUD_PROVIDER"
    log_info "Timestamp: $TIMESTAMP"
    
    if [[ "$DESTROY_MODE" == "true" ]]; then
        log_warning "DESTROY MODE ENABLED - This will delete infrastructure!"
    fi
    
    check_prerequisites
    init_terraform
    validate_terraform
    plan_terraform
    apply_terraform
    save_deployment_info
    
    if [[ "$DESTROY_MODE" != "true" ]]; then
        log_success "QuantStream Analytics Platform deployment completed successfully!"
        log_info "You can now access your infrastructure using the endpoints provided in the Terraform outputs."
    fi
}

# Parse arguments and run main function
parse_args "$@"
main