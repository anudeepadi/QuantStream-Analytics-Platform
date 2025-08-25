# Global Variables for QuantStream Analytics Platform Infrastructure

# Project and Environment Configuration
variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "quantstream-analytics"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "cost_center" {
  description = "Cost center for billing and resource allocation"
  type        = string
  default     = "data-analytics"
}

variable "owner_team" {
  description = "Team responsible for the infrastructure"
  type        = string
  default     = "platform-engineering"
}

# AWS Configuration
variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-west-2"
}

variable "aws_availability_zones" {
  description = "AWS availability zones"
  type        = list(string)
  default     = ["us-west-2a", "us-west-2b", "us-west-2c"]
}

# Azure Configuration
variable "azure_subscription_id" {
  description = "Azure subscription ID"
  type        = string
}

variable "azure_location" {
  description = "Azure region for resources"
  type        = string
  default     = "West US 2"
}

# Google Cloud Configuration
variable "gcp_project_id" {
  description = "Google Cloud project ID"
  type        = string
}

variable "gcp_region" {
  description = "Google Cloud region"
  type        = string
  default     = "us-west2"
}

variable "gcp_zone" {
  description = "Google Cloud zone"
  type        = string
  default     = "us-west2-a"
}

# Networking Configuration
variable "vpc_cidr_aws" {
  description = "CIDR block for AWS VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "vpc_cidr_azure" {
  description = "CIDR block for Azure VNet"
  type        = string
  default     = "10.1.0.0/16"
}

variable "vpc_cidr_gcp" {
  description = "CIDR block for GCP VPC"
  type        = string
  default     = "10.2.0.0/16"
}

# Kubernetes Configuration
variable "k8s_host" {
  description = "Kubernetes cluster endpoint"
  type        = string
  default     = ""
}

variable "k8s_cluster_ca_certificate" {
  description = "Kubernetes cluster CA certificate"
  type        = string
  default     = ""
  sensitive   = true
}

variable "k8s_token" {
  description = "Kubernetes authentication token"
  type        = string
  default     = ""
  sensitive   = true
}

variable "k8s_exec" {
  description = "Kubernetes exec configuration for authentication"
  type = object({
    api_version = string
    command     = string
    args        = list(string)
  })
  default = null
}

# Databricks Configuration
variable "databricks_host_aws" {
  description = "Databricks workspace URL for AWS"
  type        = string
  default     = ""
}

variable "databricks_token_aws" {
  description = "Databricks access token for AWS"
  type        = string
  default     = ""
  sensitive   = true
}

variable "databricks_host_azure" {
  description = "Databricks workspace URL for Azure"
  type        = string
  default     = ""
}

variable "databricks_token_azure" {
  description = "Databricks access token for Azure"
  type        = string
  default     = ""
  sensitive   = true
}

variable "databricks_host_gcp" {
  description = "Databricks workspace URL for GCP"
  type        = string
  default     = ""
}

variable "databricks_token_gcp" {
  description = "Databricks access token for GCP"
  type        = string
  default     = ""
  sensitive   = true
}

# Database Configuration
variable "db_instance_class" {
  description = "Database instance class"
  type        = string
  default     = "db.r5.xlarge"
}

variable "db_allocated_storage" {
  description = "Database allocated storage in GB"
  type        = number
  default     = 100
}

variable "db_max_allocated_storage" {
  description = "Database maximum allocated storage in GB"
  type        = number
  default     = 1000
}

# Redis Configuration
variable "redis_node_type" {
  description = "Redis cache node type"
  type        = string
  default     = "cache.r6g.large"
}

variable "redis_num_cache_nodes" {
  description = "Number of Redis cache nodes"
  type        = number
  default     = 3
}

# Kafka Configuration
variable "kafka_instance_type" {
  description = "Kafka broker instance type"
  type        = string
  default     = "kafka.m5.large"
}

variable "kafka_number_of_broker_nodes" {
  description = "Number of Kafka broker nodes"
  type        = number
  default     = 3
}

# Monitoring Configuration
variable "enable_prometheus" {
  description = "Enable Prometheus monitoring"
  type        = bool
  default     = true
}

variable "enable_grafana" {
  description = "Enable Grafana dashboards"
  type        = bool
  default     = true
}

variable "enable_elasticsearch" {
  description = "Enable Elasticsearch for logging"
  type        = bool
  default     = true
}

# Security Configuration
variable "enable_encryption" {
  description = "Enable encryption at rest and in transit"
  type        = bool
  default     = true
}

variable "kms_key_deletion_window" {
  description = "KMS key deletion window in days"
  type        = number
  default     = 30
}

# Auto-scaling Configuration
variable "min_cluster_size" {
  description = "Minimum cluster size for auto-scaling"
  type        = number
  default     = 1
}

variable "max_cluster_size" {
  description = "Maximum cluster size for auto-scaling"
  type        = number
  default     = 10
}

variable "desired_cluster_size" {
  description = "Desired cluster size"
  type        = number
  default     = 3
}

# Disaster Recovery Configuration
variable "enable_cross_region_backup" {
  description = "Enable cross-region backup for disaster recovery"
  type        = bool
  default     = true
}

variable "backup_retention_days" {
  description = "Backup retention period in days"
  type        = number
  default     = 30
}

# Additional Tags
variable "additional_tags" {
  description = "Additional tags to apply to resources"
  type        = map(string)
  default     = {}
}