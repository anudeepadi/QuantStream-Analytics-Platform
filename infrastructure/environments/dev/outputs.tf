# Outputs for Development Environment

# Common Resource Outputs
output "deployment_id" {
  description = "Unique deployment identifier"
  value       = module.common_resources.deployment_id
}

output "resource_prefix" {
  description = "Resource naming prefix"
  value       = module.common_resources.resource_prefix
}

output "enabled_providers" {
  description = "List of enabled cloud providers"
  value       = module.common_resources.enabled_providers
}

# Networking Outputs
output "networking_info" {
  description = "Networking configuration information"
  value = {
    aws = var.enable_aws ? {
      vpc_id              = module.networking.aws_vpc_id
      private_subnet_ids  = module.networking.aws_private_subnet_ids
      public_subnet_ids   = module.networking.aws_public_subnet_ids
      database_subnet_ids = module.networking.aws_database_subnet_ids
    } : null
    
    azure = var.enable_azure ? {
      vnet_id             = module.networking.azure_vnet_id
      private_subnet_ids  = module.networking.azure_private_subnet_ids
      public_subnet_ids   = module.networking.azure_public_subnet_ids
      database_subnet_ids = module.networking.azure_database_subnet_ids
    } : null
    
    gcp = var.enable_gcp ? {
      vpc_id              = module.networking.gcp_vpc_id
      private_subnet_ids  = module.networking.gcp_private_subnet_ids
      public_subnet_ids   = module.networking.gcp_public_subnet_ids
      database_subnet_ids = module.networking.gcp_database_subnet_ids
    } : null
  }
}

# Security Outputs
output "security_info" {
  description = "Security configuration information"
  value = {
    kms_key_ids = module.security.kms_key_ids
    aws_security_groups = var.enable_aws ? module.security.aws_security_group_ids : null
    azure_nsgs = var.enable_azure ? module.security.azure_nsg_ids : null
    gcp_firewall_rules = var.enable_gcp ? module.security.gcp_firewall_rules : null
  }
  sensitive = true
}

# Compute Outputs
output "compute_info" {
  description = "Compute infrastructure information"
  value = {
    kubernetes_clusters = module.compute.kubernetes_clusters
    cluster_endpoints = module.compute.cluster_endpoints
  }
}

# Data Infrastructure Outputs
output "data_infrastructure_info" {
  description = "Data infrastructure information"
  value = {
    database_endpoints = module.data.database_endpoints
    redis_endpoints = module.data.redis_endpoints
    kafka_endpoints = module.data.kafka_endpoints
    storage_buckets = module.data.storage_buckets
  }
  sensitive = true
}

# Monitoring Outputs
output "monitoring_info" {
  description = "Monitoring infrastructure information"
  value = {
    prometheus_endpoints = module.monitoring.prometheus_endpoints
    grafana_endpoints = module.monitoring.grafana_endpoints
    elasticsearch_endpoints = module.monitoring.elasticsearch_endpoints
    kibana_endpoints = module.monitoring.kibana_endpoints
  }
}

# Environment Summary
output "environment_summary" {
  description = "Summary of development environment"
  value = {
    environment = "dev"
    enabled_providers = module.common_resources.enabled_providers
    deployment_id = module.common_resources.deployment_id
    cost_optimized = true
    multi_az = false
    auto_shutdown_enabled = true
  }
}