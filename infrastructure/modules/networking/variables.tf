# Variables for Networking Module

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
  description = "Enable AWS networking resources"
  type        = bool
  default     = true
}

variable "enable_azure" {
  description = "Enable Azure networking resources"
  type        = bool
  default     = false
}

variable "enable_gcp" {
  description = "Enable Google Cloud networking resources"
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
  
  validation {
    condition     = can(cidrhost(var.aws_vpc_cidr, 0))
    error_message = "AWS VPC CIDR must be a valid IPv4 CIDR block."
  }
}

# Azure Configuration
variable "azure_location" {
  description = "Azure location for resources"
  type        = string
  default     = "West US 2"
}

variable "azure_vnet_cidr" {
  description = "CIDR block for Azure Virtual Network"
  type        = string
  default     = "10.1.0.0/16"
  
  validation {
    condition     = can(cidrhost(var.azure_vnet_cidr, 0))
    error_message = "Azure VNet CIDR must be a valid IPv4 CIDR block."
  }
}

# GCP Configuration
variable "gcp_region" {
  description = "Google Cloud region for resources"
  type        = string
  default     = "us-west2"
}

variable "gcp_vpc_cidr" {
  description = "CIDR block for GCP VPC"
  type        = string
  default     = "10.2.0.0/16"
  
  validation {
    condition     = can(cidrhost(var.gcp_vpc_cidr, 0))
    error_message = "GCP VPC CIDR must be a valid IPv4 CIDR block."
  }
}

# Networking Features
variable "enable_nat_gateway" {
  description = "Enable NAT Gateway for private subnet internet access"
  type        = bool
  default     = true
}

variable "enable_vpn_gateway" {
  description = "Enable VPN Gateway for on-premises connectivity"
  type        = bool
  default     = false
}

variable "enable_flow_logs" {
  description = "Enable VPC Flow Logs for network monitoring"
  type        = bool
  default     = true
}

variable "enable_dns_hostnames" {
  description = "Enable DNS hostnames in VPC"
  type        = bool
  default     = true
}

variable "enable_dns_support" {
  description = "Enable DNS support in VPC"
  type        = bool
  default     = true
}

# Security Configuration
variable "allowed_cidr_blocks" {
  description = "List of CIDR blocks allowed for external access"
  type        = list(string)
  default     = []
}

variable "restricted_ports" {
  description = "List of ports that should be restricted"
  type        = list(number)
  default     = [22, 3389, 5432, 6379, 9092]
}

# Common Tags
variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}