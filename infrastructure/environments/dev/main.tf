# Development Environment Configuration for QuantStream Analytics Platform

terraform {
  required_version = ">= 1.5.0"
  
  backend "s3" {
    # Backend configuration will be provided via backend config file
    # Example: terraform init -backend-config="backend-dev.hcl"
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.11"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.29"
    }
  }
}

# Local values for development environment
locals {
  environment = "dev"
  
  # Development-specific configurations
  config = {
    # Cost-optimized instance types for development
    kubernetes_node_type    = "t3.medium"
    database_instance_class = "db.t3.micro"
    redis_node_type        = "cache.t3.micro"
    kafka_instance_type    = "kafka.t3.small"
    
    # Minimal scaling for cost optimization
    min_nodes = 1
    max_nodes = 3
    desired_nodes = 1
    
    # Reduced retention periods
    backup_retention_days = 7
    log_retention_days = 7
    
    # Single AZ deployment for cost savings
    multi_az_enabled = false
    
    # Disable expensive features in dev
    enable_enhanced_monitoring = false
    enable_performance_insights = false
    enable_cross_region_backup = false
  }
  
  # Common tags for development environment
  dev_tags = {
    Environment = local.environment
    Purpose     = "development"
    AutoShutdown = "true"  # Enable automatic shutdown for cost savings
    Backup      = "minimal"
  }
}

# Common Resources Module
module "common_resources" {
  source = "../../shared/common-resources"
  
  project_name    = var.project_name
  environment     = local.environment
  enable_aws      = var.enable_aws
  enable_azure    = var.enable_azure
  enable_gcp      = var.enable_gcp
  
  aws_region      = var.aws_region
  azure_location  = var.azure_location
  gcp_region      = var.gcp_region
  gcp_project_id  = var.gcp_project_id
  
  common_tags     = merge(var.common_tags, local.dev_tags)
}

# Networking Module
module "networking" {
  source = "../../modules/networking"
  
  project_name    = var.project_name
  environment     = local.environment
  resource_prefix = module.common_resources.resource_prefix
  
  # Cloud provider enablement
  enable_aws   = var.enable_aws
  enable_azure = var.enable_azure  
  enable_gcp   = var.enable_gcp
  
  # Network configuration
  aws_vpc_cidr   = var.aws_vpc_cidr
  azure_vnet_cidr = var.azure_vnet_cidr
  gcp_vpc_cidr   = var.gcp_vpc_cidr
  
  aws_region     = var.aws_region
  azure_location = var.azure_location
  gcp_region     = var.gcp_region
  
  # Development-specific networking
  enable_nat_gateway = false  # Cost optimization
  enable_vpn_gateway = false  # Not needed in dev
  
  common_tags = merge(var.common_tags, local.dev_tags)
  
  depends_on = [module.common_resources]
}

# Security Module
module "security" {
  source = "../../modules/security"
  
  project_name    = var.project_name
  environment     = local.environment
  resource_prefix = module.common_resources.resource_prefix
  
  enable_aws   = var.enable_aws
  enable_azure = var.enable_azure
  enable_gcp   = var.enable_gcp
  
  # Development security settings (relaxed for ease of development)
  kms_key_rotation_enabled = false
  secrets_rotation_days    = 90
  
  common_tags = merge(var.common_tags, local.dev_tags)
  
  depends_on = [module.common_resources]
}

# Compute Module
module "compute" {
  source = "../../modules/compute"
  
  project_name    = var.project_name
  environment     = local.environment
  resource_prefix = module.common_resources.resource_prefix
  
  enable_aws   = var.enable_aws
  enable_azure = var.enable_azure
  enable_gcp   = var.enable_gcp
  
  # Networking inputs
  aws_vpc_id          = module.networking.aws_vpc_id
  aws_subnet_ids      = module.networking.aws_private_subnet_ids
  azure_vnet_id       = module.networking.azure_vnet_id
  azure_subnet_ids    = module.networking.azure_private_subnet_ids
  gcp_vpc_id          = module.networking.gcp_vpc_id
  gcp_subnet_ids      = module.networking.gcp_private_subnet_ids
  
  # Security inputs
  aws_security_group_ids   = module.security.aws_security_group_ids
  azure_nsg_ids           = module.security.azure_nsg_ids
  gcp_firewall_rules      = module.security.gcp_firewall_rules
  
  # Development-specific compute configuration
  kubernetes_node_type = local.config.kubernetes_node_type
  min_nodes           = local.config.min_nodes
  max_nodes           = local.config.max_nodes
  desired_nodes       = local.config.desired_nodes
  
  common_tags = merge(var.common_tags, local.dev_tags)
  
  depends_on = [module.networking, module.security]
}

# Data Infrastructure Module
module "data" {
  source = "../../modules/data"
  
  project_name    = var.project_name
  environment     = local.environment
  resource_prefix = module.common_resources.resource_prefix
  
  enable_aws   = var.enable_aws
  enable_azure = var.enable_azure
  enable_gcp   = var.enable_gcp
  
  # Networking inputs
  aws_vpc_id          = module.networking.aws_vpc_id
  aws_subnet_ids      = module.networking.aws_database_subnet_ids
  azure_vnet_id       = module.networking.azure_vnet_id
  azure_subnet_ids    = module.networking.azure_database_subnet_ids
  gcp_vpc_id          = module.networking.gcp_vpc_id
  gcp_subnet_ids      = module.networking.gcp_database_subnet_ids
  
  # Security inputs
  kms_key_ids = module.security.kms_key_ids
  
  # Development-specific data configuration
  database_instance_class = local.config.database_instance_class
  redis_node_type        = local.config.redis_node_type
  kafka_instance_type    = local.config.kafka_instance_type
  backup_retention_days  = local.config.backup_retention_days
  multi_az_enabled       = local.config.multi_az_enabled
  
  common_tags = merge(var.common_tags, local.dev_tags)
  
  depends_on = [module.networking, module.security]
}

# Monitoring Module
module "monitoring" {
  source = "../../modules/monitoring"
  
  project_name    = var.project_name
  environment     = local.environment
  resource_prefix = module.common_resources.resource_prefix
  
  enable_aws   = var.enable_aws
  enable_azure = var.enable_azure
  enable_gcp   = var.enable_gcp
  
  # Kubernetes cluster information
  kubernetes_clusters = module.compute.kubernetes_clusters
  
  # Development-specific monitoring configuration
  prometheus_retention_days = local.config.log_retention_days
  grafana_admin_password   = "dev-admin-password"  # Use secrets in production
  elasticsearch_instance_type = "t3.small.elasticsearch"
  
  # Reduced monitoring features for cost optimization
  enable_enhanced_monitoring = local.config.enable_enhanced_monitoring
  
  common_tags = merge(var.common_tags, local.dev_tags)
  
  depends_on = [module.compute]
}