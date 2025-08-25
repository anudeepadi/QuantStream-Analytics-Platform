# Variables for Common Resources Module

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

# Cloud Provider Enablement
variable "enable_aws" {
  description = "Enable AWS resources"
  type        = bool
  default     = true
}

variable "enable_azure" {
  description = "Enable Azure resources"
  type        = bool
  default     = false
}

variable "enable_gcp" {
  description = "Enable Google Cloud resources"
  type        = bool
  default     = false
}

# AWS Configuration
variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

# Azure Configuration
variable "azure_location" {
  description = "Azure location"
  type        = string
  default     = "West US 2"
}

# GCP Configuration
variable "gcp_region" {
  description = "Google Cloud region"
  type        = string
  default     = "us-west2"
}

variable "gcp_project_id" {
  description = "Google Cloud project ID"
  type        = string
  default     = ""
}

# Common Tags
variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}