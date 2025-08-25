# Variables for Compute Module

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
  description = "Enable AWS compute resources"
  type        = bool
  default     = true
}

variable "enable_azure" {
  description = "Enable Azure compute resources"
  type        = bool
  default     = false
}

variable "enable_gcp" {
  description = "Enable Google Cloud compute resources"
  type        = bool
  default     = false
}

# Kubernetes Configuration
variable "kubernetes_version" {
  description = "Kubernetes version to use"
  type        = string
  default     = "1.28"
}

variable "kubernetes_node_type" {
  description = "Instance type for Kubernetes nodes"
  type        = string
  default     = ""
}

# Cluster Sizing
variable "min_nodes" {
  description = "Minimum number of nodes in the cluster"
  type        = number
  default     = 1
  
  validation {
    condition     = var.min_nodes >= 1
    error_message = "Minimum nodes must be at least 1."
  }
}

variable "max_nodes" {
  description = "Maximum number of nodes in the cluster"
  type        = number
  default     = 10
  
  validation {
    condition     = var.max_nodes >= var.min_nodes
    error_message = "Maximum nodes must be greater than or equal to minimum nodes."
  }
}

variable "desired_nodes" {
  description = "Desired number of nodes in the cluster"
  type        = number
  default     = 3
  
  validation {
    condition     = var.desired_nodes >= var.min_nodes && var.desired_nodes <= var.max_nodes
    error_message = "Desired nodes must be between min_nodes and max_nodes."
  }
}

# AWS-specific Variables
variable "aws_vpc_id" {
  description = "AWS VPC ID for the EKS cluster"
  type        = string
  default     = ""
}

variable "aws_subnet_ids" {
  description = "AWS subnet IDs for the EKS cluster"
  type        = list(string)
  default     = []
}

variable "aws_public_subnet_ids" {
  description = "AWS public subnet IDs for the EKS cluster"
  type        = list(string)
  default     = []
}

variable "aws_security_group_ids" {
  description = "AWS security group IDs for the EKS cluster"
  type        = list(string)
  default     = []
}

variable "kms_key_arn_aws" {
  description = "AWS KMS key ARN for EKS encryption"
  type        = string
  default     = ""
}

variable "ssh_key_name" {
  description = "AWS SSH key pair name for EC2 instances"
  type        = string
  default     = ""
}

# Azure-specific Variables
variable "azure_resource_group_name" {
  description = "Azure resource group name"
  type        = string
  default     = ""
}

variable "azure_location" {
  description = "Azure location for resources"
  type        = string
  default     = "West US 2"
}

variable "azure_vnet_id" {
  description = "Azure VNet ID for the AKS cluster"
  type        = string
  default     = ""
}

variable "azure_subnet_ids" {
  description = "Azure subnet IDs for the AKS cluster"
  type        = list(string)
  default     = []
}

variable "azure_nsg_ids" {
  description = "Azure Network Security Group IDs"
  type        = list(string)
  default     = []
}

variable "log_analytics_workspace_id" {
  description = "Azure Log Analytics workspace ID for AKS monitoring"
  type        = string
  default     = ""
}

variable "enable_data_node_pool" {
  description = "Enable dedicated node pool for data workloads"
  type        = bool
  default     = false
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
  description = "GCP VPC ID for the GKE cluster"
  type        = string
  default     = ""
}

variable "gcp_vpc_name" {
  description = "GCP VPC name for the GKE cluster"
  type        = string
  default     = ""
}

variable "gcp_subnet_ids" {
  description = "GCP subnet IDs for the GKE cluster"
  type        = list(string)
  default     = []
}

variable "gcp_subnet_names" {
  description = "GCP subnet names for the GKE cluster"
  type        = list(string)
  default     = []
}

variable "gcp_firewall_rules" {
  description = "GCP firewall rule names"
  type        = list(string)
  default     = []
}

# Additional Features
variable "enable_cluster_autoscaler" {
  description = "Enable cluster autoscaler"
  type        = bool
  default     = true
}

variable "enable_vertical_pod_autoscaler" {
  description = "Enable vertical pod autoscaler"
  type        = bool
  default     = true
}

variable "enable_network_policy" {
  description = "Enable network policy enforcement"
  type        = bool
  default     = true
}

variable "enable_pod_security_policy" {
  description = "Enable pod security policy"
  type        = bool
  default     = true
}

variable "enable_logging" {
  description = "Enable cluster logging"
  type        = bool
  default     = true
}

variable "enable_monitoring" {
  description = "Enable cluster monitoring"
  type        = bool
  default     = true
}

# Common Tags
variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}