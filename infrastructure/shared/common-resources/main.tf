# Common Resources for Multi-Cloud QuantStream Analytics Platform
# This module provides cloud-agnostic resource definitions

terraform {
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
    random = {
      source  = "hashicorp/random"
      version = "~> 3.4"
    }
  }
}

# Random ID for unique resource naming
resource "random_id" "deployment" {
  byte_length = 4
}

# Local values for common configurations
locals {
  deployment_id = random_id.deployment.hex
  
  # Common naming convention
  resource_prefix = "${var.project_name}-${var.environment}-${local.deployment_id}"
  
  # Cloud provider configurations
  cloud_providers = {
    aws = {
      enabled = var.enable_aws
      region  = var.aws_region
    }
    azure = {
      enabled = var.enable_azure
      region  = var.azure_location
    }
    gcp = {
      enabled = var.enable_gcp
      region  = var.gcp_region
    }
  }
  
  # Enabled cloud providers
  enabled_providers = [
    for provider, config in local.cloud_providers : provider if config.enabled
  ]
}

# Data sources for existing resources
data "aws_caller_identity" "current" {
  count = var.enable_aws ? 1 : 0
}

data "azurerm_client_config" "current" {
  count = var.enable_azure ? 1 : 0
}

data "google_client_config" "current" {
  count = var.enable_gcp ? 1 : 0
}