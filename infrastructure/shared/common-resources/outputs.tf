# Outputs for Common Resources Module

output "deployment_id" {
  description = "Unique deployment identifier"
  value       = random_id.deployment.hex
}

output "resource_prefix" {
  description = "Common resource prefix for naming"
  value       = local.resource_prefix
}

output "enabled_providers" {
  description = "List of enabled cloud providers"
  value       = local.enabled_providers
}

output "cloud_provider_configs" {
  description = "Configuration for each cloud provider"
  value       = local.cloud_providers
}

# AWS Outputs
output "aws_account_id" {
  description = "AWS Account ID"
  value       = var.enable_aws ? data.aws_caller_identity.current[0].account_id : null
}

output "aws_region" {
  description = "AWS region being used"
  value       = var.enable_aws ? var.aws_region : null
}

# Azure Outputs
output "azure_subscription_id" {
  description = "Azure Subscription ID"
  value       = var.enable_azure ? data.azurerm_client_config.current[0].subscription_id : null
}

output "azure_tenant_id" {
  description = "Azure Tenant ID"
  value       = var.enable_azure ? data.azurerm_client_config.current[0].tenant_id : null
}

output "azure_location" {
  description = "Azure location being used"
  value       = var.enable_azure ? var.azure_location : null
}

# GCP Outputs
output "gcp_project_id" {
  description = "Google Cloud project ID"
  value       = var.enable_gcp ? var.gcp_project_id : null
}

output "gcp_region" {
  description = "Google Cloud region being used"
  value       = var.enable_gcp ? var.gcp_region : null
}