# Outputs for Networking Module

#######################
# AWS Networking Outputs
#######################

output "aws_vpc_id" {
  description = "ID of the AWS VPC"
  value       = var.enable_aws ? aws_vpc.main[0].id : null
}

output "aws_vpc_cidr" {
  description = "CIDR block of the AWS VPC"
  value       = var.enable_aws ? aws_vpc.main[0].cidr_block : null
}

output "aws_internet_gateway_id" {
  description = "ID of the AWS Internet Gateway"
  value       = var.enable_aws ? aws_internet_gateway.main[0].id : null
}

output "aws_public_subnet_ids" {
  description = "IDs of the AWS public subnets"
  value       = var.enable_aws ? aws_subnet.public[*].id : []
}

output "aws_private_subnet_ids" {
  description = "IDs of the AWS private subnets"
  value       = var.enable_aws ? aws_subnet.private[*].id : []
}

output "aws_database_subnet_ids" {
  description = "IDs of the AWS database subnets"
  value       = var.enable_aws ? aws_subnet.database[*].id : []
}

output "aws_db_subnet_group_name" {
  description = "Name of the AWS DB subnet group"
  value       = var.enable_aws ? aws_db_subnet_group.main[0].name : null
}

output "aws_nat_gateway_ids" {
  description = "IDs of the AWS NAT Gateways"
  value       = var.enable_aws && var.enable_nat_gateway ? aws_nat_gateway.main[*].id : []
}

output "aws_public_route_table_id" {
  description = "ID of the AWS public route table"
  value       = var.enable_aws ? aws_route_table.public[0].id : null
}

output "aws_private_route_table_ids" {
  description = "IDs of the AWS private route tables"
  value       = var.enable_aws ? aws_route_table.private[*].id : []
}

output "aws_availability_zones" {
  description = "Available AWS availability zones"
  value       = local.aws_azs
}

# AWS Security Group Outputs
output "aws_security_group_web_id" {
  description = "ID of the AWS web security group"
  value       = var.enable_aws ? aws_security_group.web[0].id : null
}

output "aws_security_group_app_id" {
  description = "ID of the AWS application security group"
  value       = var.enable_aws ? aws_security_group.app[0].id : null
}

output "aws_security_group_database_id" {
  description = "ID of the AWS database security group"
  value       = var.enable_aws ? aws_security_group.database[0].id : null
}

output "aws_security_group_messaging_id" {
  description = "ID of the AWS messaging security group"
  value       = var.enable_aws ? aws_security_group.messaging[0].id : null
}

#######################
# Azure Networking Outputs
#######################

output "azure_resource_group_name" {
  description = "Name of the Azure resource group"
  value       = var.enable_azure ? azurerm_resource_group.main[0].name : null
}

output "azure_resource_group_location" {
  description = "Location of the Azure resource group"
  value       = var.enable_azure ? azurerm_resource_group.main[0].location : null
}

output "azure_vnet_id" {
  description = "ID of the Azure Virtual Network"
  value       = var.enable_azure ? azurerm_virtual_network.main[0].id : null
}

output "azure_vnet_name" {
  description = "Name of the Azure Virtual Network"
  value       = var.enable_azure ? azurerm_virtual_network.main[0].name : null
}

output "azure_vnet_cidr" {
  description = "CIDR blocks of the Azure Virtual Network"
  value       = var.enable_azure ? azurerm_virtual_network.main[0].address_space : []
}

output "azure_public_subnet_ids" {
  description = "IDs of the Azure public subnets"
  value       = var.enable_azure ? azurerm_subnet.public[*].id : []
}

output "azure_private_subnet_ids" {
  description = "IDs of the Azure private subnets"
  value       = var.enable_azure ? azurerm_subnet.private[*].id : []
}

output "azure_database_subnet_ids" {
  description = "IDs of the Azure database subnets"
  value       = var.enable_azure ? azurerm_subnet.database[*].id : []
}

# Azure Network Security Group Outputs
output "azure_nsg_web_id" {
  description = "ID of the Azure web network security group"
  value       = var.enable_azure ? azurerm_network_security_group.web[0].id : null
}

output "azure_nsg_app_id" {
  description = "ID of the Azure application network security group"
  value       = var.enable_azure ? azurerm_network_security_group.app[0].id : null
}

output "azure_nsg_database_id" {
  description = "ID of the Azure database network security group"
  value       = var.enable_azure ? azurerm_network_security_group.database[0].id : null
}

#######################
# GCP Networking Outputs
#######################

output "gcp_vpc_id" {
  description = "ID of the GCP VPC network"
  value       = var.enable_gcp ? google_compute_network.main[0].id : null
}

output "gcp_vpc_name" {
  description = "Name of the GCP VPC network"
  value       = var.enable_gcp ? google_compute_network.main[0].name : null
}

output "gcp_vpc_self_link" {
  description = "Self link of the GCP VPC network"
  value       = var.enable_gcp ? google_compute_network.main[0].self_link : null
}

output "gcp_public_subnet_ids" {
  description = "IDs of the GCP public subnets"
  value       = var.enable_gcp ? google_compute_subnetwork.public[*].id : []
}

output "gcp_private_subnet_ids" {
  description = "IDs of the GCP private subnets"
  value       = var.enable_gcp ? google_compute_subnetwork.private[*].id : []
}

output "gcp_database_subnet_ids" {
  description = "IDs of the GCP database subnets"
  value       = var.enable_gcp ? google_compute_subnetwork.database[*].id : []
}

output "gcp_private_subnet_names" {
  description = "Names of the GCP private subnets"
  value       = var.enable_gcp ? google_compute_subnetwork.private[*].name : []
}

#######################
# Cross-Cloud Networking Summary
#######################

output "networking_summary" {
  description = "Summary of networking configuration across all clouds"
  value = {
    aws = var.enable_aws ? {
      vpc_id                 = aws_vpc.main[0].id
      vpc_cidr               = aws_vpc.main[0].cidr_block
      public_subnets_count   = length(aws_subnet.public)
      private_subnets_count  = length(aws_subnet.private)
      database_subnets_count = length(aws_subnet.database)
      nat_gateways_count     = var.enable_nat_gateway ? length(aws_nat_gateway.main) : 0
      availability_zones     = local.aws_azs
    } : null
    
    azure = var.enable_azure ? {
      vnet_id                = azurerm_virtual_network.main[0].id
      vnet_cidr              = azurerm_virtual_network.main[0].address_space
      resource_group_name    = azurerm_resource_group.main[0].name
      location               = azurerm_resource_group.main[0].location
      public_subnets_count   = length(azurerm_subnet.public)
      private_subnets_count  = length(azurerm_subnet.private)
      database_subnets_count = length(azurerm_subnet.database)
    } : null
    
    gcp = var.enable_gcp ? {
      vpc_id                 = google_compute_network.main[0].id
      vpc_name               = google_compute_network.main[0].name
      region                 = var.gcp_region
      public_subnets_count   = length(google_compute_subnetwork.public)
      private_subnets_count  = length(google_compute_subnetwork.private)
      database_subnets_count = length(google_compute_subnetwork.database)
    } : null
  }
}

# Subnet CIDR Mappings for Reference
output "subnet_cidrs" {
  description = "CIDR blocks for all subnets across cloud providers"
  value = {
    aws = var.enable_aws ? {
      public   = local.aws_subnets.public
      private  = local.aws_subnets.private
      database = local.aws_subnets.database
    } : null
    
    azure = var.enable_azure ? {
      public   = local.azure_subnets.public
      private  = local.azure_subnets.private
      database = local.azure_subnets.database
    } : null
    
    gcp = var.enable_gcp ? {
      public   = local.gcp_subnets.public
      private  = local.gcp_subnets.private
      database = local.gcp_subnets.database
    } : null
  }
}