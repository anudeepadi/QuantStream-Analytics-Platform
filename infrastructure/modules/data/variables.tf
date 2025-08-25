# Variables for Data Infrastructure Module

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

variable "resource_prefix" {
  description = "Resource naming prefix"
  type        = string
}

# Cloud Provider Configuration
variable "enable_aws" {
  description = "Enable AWS data infrastructure"
  type        = bool
  default     = true
}

variable "enable_azure" {
  description = "Enable Azure data infrastructure"
  type        = bool
  default     = false
}

variable "enable_gcp" {
  description = "Enable Google Cloud data infrastructure"
  type        = bool
  default     = false
}

# Database Configuration
variable "database_name" {
  description = "Name of the database"
  type        = string
  default     = "quantstream_db"
}

variable "database_username" {
  description = "Database administrator username"
  type        = string
  default     = "postgres"
}

variable "postgresql_version" {
  description = "PostgreSQL engine version"
  type        = string
  default     = "14.9"
}

variable "database_instance_class" {
  description = "Database instance class"
  type        = string
  default     = ""
}

variable "backup_retention_days" {
  description = "Number of days to retain database backups"
  type        = number
  default     = 0  # 0 means use environment default
  
  validation {
    condition     = var.backup_retention_days >= 0 && var.backup_retention_days <= 35
    error_message = "Backup retention days must be between 0 and 35."
  }
}

variable "multi_az_enabled" {
  description = "Enable Multi-AZ deployment for database"
  type        = bool
  default     = null  # null means use environment default
}

# Redis Configuration
variable "redis_node_type" {
  description = "Redis cache node type"
  type        = string
  default     = ""
}

variable "redis_num_cache_nodes" {
  description = "Number of Redis cache nodes"
  type        = number
  default     = 0  # 0 means use environment default
  
  validation {
    condition     = var.redis_num_cache_nodes >= 0
    error_message = "Redis cache nodes must be non-negative."
  }
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
  
  validation {
    condition     = var.kafka_number_of_broker_nodes >= 3
    error_message = "Kafka cluster must have at least 3 broker nodes."
  }
}

# AWS-specific Variables
variable "aws_vpc_id" {
  description = "AWS VPC ID"
  type        = string
  default     = ""
}

variable "aws_subnet_ids" {
  description = "AWS subnet IDs for data infrastructure"
  type        = list(string)
  default     = []
}

variable "aws_db_subnet_group_name" {
  description = "AWS DB subnet group name"
  type        = string
  default     = ""
}

variable "aws_security_group_ids" {
  description = "AWS security group IDs"
  type        = list(string)
  default     = []
}

# Azure-specific Variables
variable "azure_resource_group_name" {
  description = "Azure resource group name"
  type        = string
  default     = ""
}

variable "azure_location" {
  description = "Azure location"
  type        = string
  default     = "West US 2"
}

variable "azure_vnet_id" {
  description = "Azure VNet ID"
  type        = string
  default     = ""
}

variable "azure_subnet_ids" {
  description = "Azure subnet IDs for data infrastructure"
  type        = list(string)
  default     = []
}

# GCP-specific Variables
variable "gcp_project_id" {
  description = "Google Cloud project ID"
  type        = string
  default     = ""
}

variable "gcp_region" {
  description = "Google Cloud region"
  type        = string
  default     = "us-west2"
}

variable "gcp_vpc_id" {
  description = "GCP VPC network ID"
  type        = string
  default     = ""
}

variable "gcp_vpc_self_link" {
  description = "GCP VPC network self link"
  type        = string
  default     = ""
}

variable "gcp_subnet_ids" {
  description = "GCP subnet IDs for data infrastructure"
  type        = list(string)
  default     = []
}

# Security Configuration
variable "kms_key_ids" {
  description = "KMS key IDs for encryption by cloud provider"
  type        = map(string)
  default     = {}
}

variable "enable_encryption" {
  description = "Enable encryption at rest and in transit"
  type        = bool
  default     = true
}

variable "enable_deletion_protection" {
  description = "Enable deletion protection for production databases"
  type        = bool
  default     = null  # null means use environment default
}

# Storage Configuration
variable "enable_intelligent_tiering" {
  description = "Enable intelligent storage tiering for cost optimization"
  type        = bool
  default     = true
}

variable "data_retention_days" {
  description = "Number of days to retain data in hot storage"
  type        = number
  default     = 90
  
  validation {
    condition     = var.data_retention_days > 0
    error_message = "Data retention days must be positive."
  }
}

# Performance Configuration
variable "enable_performance_insights" {
  description = "Enable database performance insights"
  type        = bool
  default     = null  # null means use environment default
}

variable "enable_enhanced_monitoring" {
  description = "Enable enhanced monitoring for databases"
  type        = bool
  default     = null  # null means use environment default
}

# High Availability Configuration
variable "enable_cross_region_backup" {
  description = "Enable cross-region backup for disaster recovery"
  type        = bool
  default     = false
}

variable "enable_read_replicas" {
  description = "Enable read replicas for database scaling"
  type        = bool
  default     = null  # null means use environment default
}

# Monitoring Configuration
variable "enable_detailed_monitoring" {
  description = "Enable detailed monitoring and logging"
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "Number of days to retain logs"
  type        = number
  default     = 30
  
  validation {
    condition     = var.log_retention_days > 0
    error_message = "Log retention days must be positive."
  }
}

# Common Tags
variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}