# Global Outputs for QuantStream Analytics Platform Infrastructure

# Environment Information
output "project_name" {
  description = "Name of the project"
  value       = var.project_name
}

output "environment" {
  description = "Current environment"
  value       = var.environment
}

# AWS Outputs
output "aws_region" {
  description = "AWS region used for resources"
  value       = var.aws_region
}

# Azure Outputs  
output "azure_location" {
  description = "Azure location used for resources"
  value       = var.azure_location
}

# GCP Outputs
output "gcp_project_id" {
  description = "Google Cloud project ID"
  value       = var.gcp_project_id
}

output "gcp_region" {
  description = "Google Cloud region used for resources"
  value       = var.gcp_region
}

# Networking Outputs
output "vpc_cidr_blocks" {
  description = "VPC CIDR blocks for all cloud providers"
  value = {
    aws   = var.vpc_cidr_aws
    azure = var.vpc_cidr_azure
    gcp   = var.vpc_cidr_gcp
  }
}

# Configuration Status
output "features_enabled" {
  description = "Enabled features configuration"
  value = {
    prometheus     = var.enable_prometheus
    grafana        = var.enable_grafana
    elasticsearch  = var.enable_elasticsearch
    encryption     = var.enable_encryption
    cross_region_backup = var.enable_cross_region_backup
  }
}

# Auto-scaling Configuration
output "cluster_sizing" {
  description = "Cluster sizing configuration"
  value = {
    min_size     = var.min_cluster_size
    max_size     = var.max_cluster_size
    desired_size = var.desired_cluster_size
  }
}