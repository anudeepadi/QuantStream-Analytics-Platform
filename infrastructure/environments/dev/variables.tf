# Variables for Development Environment

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "quantstream-analytics"
}

# Cloud Provider Configuration
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
  description = "AWS region for resources"
  type        = string
  default     = "us-west-2"
}

variable "aws_vpc_cidr" {
  description = "CIDR block for AWS VPC"
  type        = string
  default     = "10.0.0.0/16"
}

# Azure Configuration
variable "azure_location" {
  description = "Azure location for resources"
  type        = string
  default     = "West US 2"
}

variable "azure_vnet_cidr" {
  description = "CIDR block for Azure VNet"
  type        = string
  default     = "10.1.0.0/16"
}

# GCP Configuration
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

variable "gcp_vpc_cidr" {
  description = "CIDR block for GCP VPC"
  type        = string
  default     = "10.2.0.0/16"
}

# Common Tags
variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default = {
    Project   = "QuantStream Analytics"
    ManagedBy = "Terraform"
    Owner     = "Platform Engineering"
  }
}