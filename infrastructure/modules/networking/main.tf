# Networking Module for QuantStream Analytics Platform
# Provides multi-cloud VPC, subnet, and networking infrastructure

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
  }
}

# Local values for networking configuration
locals {
  # Calculate subnet CIDRs for each cloud provider
  aws_subnets = var.enable_aws ? {
    private = [
      cidrsubnet(var.aws_vpc_cidr, 8, 1),
      cidrsubnet(var.aws_vpc_cidr, 8, 2),
      cidrsubnet(var.aws_vpc_cidr, 8, 3)
    ]
    public = [
      cidrsubnet(var.aws_vpc_cidr, 8, 101),
      cidrsubnet(var.aws_vpc_cidr, 8, 102),
      cidrsubnet(var.aws_vpc_cidr, 8, 103)
    ]
    database = [
      cidrsubnet(var.aws_vpc_cidr, 8, 201),
      cidrsubnet(var.aws_vpc_cidr, 8, 202),
      cidrsubnet(var.aws_vpc_cidr, 8, 203)
    ]
  } : null

  azure_subnets = var.enable_azure ? {
    private = [
      cidrsubnet(var.azure_vnet_cidr, 8, 1),
      cidrsubnet(var.azure_vnet_cidr, 8, 2),
      cidrsubnet(var.azure_vnet_cidr, 8, 3)
    ]
    public = [
      cidrsubnet(var.azure_vnet_cidr, 8, 101),
      cidrsubnet(var.azure_vnet_cidr, 8, 102),
      cidrsubnet(var.azure_vnet_cidr, 8, 103)
    ]
    database = [
      cidrsubnet(var.azure_vnet_cidr, 8, 201),
      cidrsubnet(var.azure_vnet_cidr, 8, 202),
      cidrsubnet(var.azure_vnet_cidr, 8, 203)
    ]
  } : null

  gcp_subnets = var.enable_gcp ? {
    private = [
      cidrsubnet(var.gcp_vpc_cidr, 8, 1),
      cidrsubnet(var.gcp_vpc_cidr, 8, 2),
      cidrsubnet(var.gcp_vpc_cidr, 8, 3)
    ]
    public = [
      cidrsubnet(var.gcp_vpc_cidr, 8, 101),
      cidrsubnet(var.gcp_vpc_cidr, 8, 102),
      cidrsubnet(var.gcp_vpc_cidr, 8, 103)
    ]
    database = [
      cidrsubnet(var.gcp_vpc_cidr, 8, 201),
      cidrsubnet(var.gcp_vpc_cidr, 8, 202),
      cidrsubnet(var.gcp_vpc_cidr, 8, 203)
    ]
  } : null

  # Availability zones
  aws_azs = var.enable_aws ? data.aws_availability_zones.available[0].names : []
}

# AWS Data Sources
data "aws_availability_zones" "available" {
  count = var.enable_aws ? 1 : 0
  state = "available"
  
  filter {
    name   = "region-name"
    values = [var.aws_region]
  }
}

#######################
# AWS Networking Resources
#######################

# AWS VPC
resource "aws_vpc" "main" {
  count = var.enable_aws ? 1 : 0
  
  cidr_block           = var.aws_vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-vpc"
    Type = "vpc"
    Cloud = "aws"
  })
}

# AWS Internet Gateway
resource "aws_internet_gateway" "main" {
  count = var.enable_aws ? 1 : 0
  
  vpc_id = aws_vpc.main[0].id
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-igw"
    Type = "internet-gateway"
    Cloud = "aws"
  })
}

# AWS Public Subnets
resource "aws_subnet" "public" {
  count = var.enable_aws ? length(local.aws_subnets.public) : 0
  
  vpc_id                  = aws_vpc.main[0].id
  cidr_block              = local.aws_subnets.public[count.index]
  availability_zone       = local.aws_azs[count.index % length(local.aws_azs)]
  map_public_ip_on_launch = true
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-public-subnet-${count.index + 1}"
    Type = "public-subnet"
    Cloud = "aws"
    Tier = "public"
    AZ = local.aws_azs[count.index % length(local.aws_azs)]
  })
}

# AWS Private Subnets
resource "aws_subnet" "private" {
  count = var.enable_aws ? length(local.aws_subnets.private) : 0
  
  vpc_id            = aws_vpc.main[0].id
  cidr_block        = local.aws_subnets.private[count.index]
  availability_zone = local.aws_azs[count.index % length(local.aws_azs)]
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-private-subnet-${count.index + 1}"
    Type = "private-subnet"
    Cloud = "aws"
    Tier = "private"
    AZ = local.aws_azs[count.index % length(local.aws_azs)]
  })
}

# AWS Database Subnets
resource "aws_subnet" "database" {
  count = var.enable_aws ? length(local.aws_subnets.database) : 0
  
  vpc_id            = aws_vpc.main[0].id
  cidr_block        = local.aws_subnets.database[count.index]
  availability_zone = local.aws_azs[count.index % length(local.aws_azs)]
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-database-subnet-${count.index + 1}"
    Type = "database-subnet"
    Cloud = "aws"
    Tier = "database"
    AZ = local.aws_azs[count.index % length(local.aws_azs)]
  })
}

# AWS Database Subnet Group
resource "aws_db_subnet_group" "main" {
  count = var.enable_aws ? 1 : 0
  
  name       = "${var.resource_prefix}-db-subnet-group"
  subnet_ids = aws_subnet.database[*].id
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-db-subnet-group"
    Type = "database-subnet-group"
    Cloud = "aws"
  })
}

# AWS Elastic IPs for NAT Gateways
resource "aws_eip" "nat" {
  count = var.enable_aws && var.enable_nat_gateway ? length(aws_subnet.public) : 0
  
  domain = "vpc"
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-nat-eip-${count.index + 1}"
    Type = "elastic-ip"
    Cloud = "aws"
  })
  
  depends_on = [aws_internet_gateway.main]
}

# AWS NAT Gateways
resource "aws_nat_gateway" "main" {
  count = var.enable_aws && var.enable_nat_gateway ? length(aws_subnet.public) : 0
  
  allocation_id = aws_eip.nat[count.index].id
  subnet_id     = aws_subnet.public[count.index].id
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-nat-gateway-${count.index + 1}"
    Type = "nat-gateway"
    Cloud = "aws"
  })
  
  depends_on = [aws_internet_gateway.main]
}

# AWS Route Table for Public Subnets
resource "aws_route_table" "public" {
  count = var.enable_aws ? 1 : 0
  
  vpc_id = aws_vpc.main[0].id
  
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main[0].id
  }
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-public-rt"
    Type = "route-table"
    Cloud = "aws"
    Tier = "public"
  })
}

# AWS Route Table Associations for Public Subnets
resource "aws_route_table_association" "public" {
  count = var.enable_aws ? length(aws_subnet.public) : 0
  
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public[0].id
}

# AWS Route Tables for Private Subnets
resource "aws_route_table" "private" {
  count = var.enable_aws ? length(aws_subnet.private) : 0
  
  vpc_id = aws_vpc.main[0].id
  
  dynamic "route" {
    for_each = var.enable_nat_gateway ? [1] : []
    content {
      cidr_block     = "0.0.0.0/0"
      nat_gateway_id = aws_nat_gateway.main[count.index % length(aws_nat_gateway.main)].id
    }
  }
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-private-rt-${count.index + 1}"
    Type = "route-table"
    Cloud = "aws"
    Tier = "private"
  })
}

# AWS Route Table Associations for Private Subnets
resource "aws_route_table_association" "private" {
  count = var.enable_aws ? length(aws_subnet.private) : 0
  
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[count.index].id
}

#######################
# Azure Networking Resources
#######################

# Azure Resource Group
resource "azurerm_resource_group" "main" {
  count = var.enable_azure ? 1 : 0
  
  name     = "${var.resource_prefix}-rg"
  location = var.azure_location
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-rg"
    Type = "resource-group"
    Cloud = "azure"
  })
}

# Azure Virtual Network
resource "azurerm_virtual_network" "main" {
  count = var.enable_azure ? 1 : 0
  
  name                = "${var.resource_prefix}-vnet"
  location            = azurerm_resource_group.main[0].location
  resource_group_name = azurerm_resource_group.main[0].name
  address_space       = [var.azure_vnet_cidr]
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-vnet"
    Type = "virtual-network"
    Cloud = "azure"
  })
}

# Azure Public Subnets
resource "azurerm_subnet" "public" {
  count = var.enable_azure ? length(local.azure_subnets.public) : 0
  
  name                 = "${var.resource_prefix}-public-subnet-${count.index + 1}"
  resource_group_name  = azurerm_resource_group.main[0].name
  virtual_network_name = azurerm_virtual_network.main[0].name
  address_prefixes     = [local.azure_subnets.public[count.index]]
}

# Azure Private Subnets
resource "azurerm_subnet" "private" {
  count = var.enable_azure ? length(local.azure_subnets.private) : 0
  
  name                 = "${var.resource_prefix}-private-subnet-${count.index + 1}"
  resource_group_name  = azurerm_resource_group.main[0].name
  virtual_network_name = azurerm_virtual_network.main[0].name
  address_prefixes     = [local.azure_subnets.private[count.index]]
}

# Azure Database Subnets
resource "azurerm_subnet" "database" {
  count = var.enable_azure ? length(local.azure_subnets.database) : 0
  
  name                 = "${var.resource_prefix}-database-subnet-${count.index + 1}"
  resource_group_name  = azurerm_resource_group.main[0].name
  virtual_network_name = azurerm_virtual_network.main[0].name
  address_prefixes     = [local.azure_subnets.database[count.index]]
  
  delegation {
    name = "Microsoft.DBforPostgreSQL/flexibleServers"
    
    service_delegation {
      name    = "Microsoft.DBforPostgreSQL/flexibleServers"
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
    }
  }
}

#######################
# GCP Networking Resources
#######################

# GCP VPC Network
resource "google_compute_network" "main" {
  count = var.enable_gcp ? 1 : 0
  
  name                    = "${var.resource_prefix}-vpc"
  auto_create_subnetworks = false
  mtu                     = 1460
  
  depends_on = [
    google_project_service.compute[0]
  ]
}

# GCP Compute API
resource "google_project_service" "compute" {
  count = var.enable_gcp ? 1 : 0
  
  service = "compute.googleapis.com"
  
  disable_dependent_services = true
}

# GCP Private Subnets
resource "google_compute_subnetwork" "private" {
  count = var.enable_gcp ? length(local.gcp_subnets.private) : 0
  
  name                     = "${var.resource_prefix}-private-subnet-${count.index + 1}"
  region                   = var.gcp_region
  network                  = google_compute_network.main[0].id
  ip_cidr_range           = local.gcp_subnets.private[count.index]
  private_ip_google_access = true
  
  secondary_ip_range {
    range_name    = "services-range"
    ip_cidr_range = cidrsubnet(local.gcp_subnets.private[count.index], 4, 1)
  }
  
  secondary_ip_range {
    range_name    = "pod-ranges"
    ip_cidr_range = cidrsubnet(local.gcp_subnets.private[count.index], 4, 2)
  }
}

# GCP Public Subnets
resource "google_compute_subnetwork" "public" {
  count = var.enable_gcp ? length(local.gcp_subnets.public) : 0
  
  name          = "${var.resource_prefix}-public-subnet-${count.index + 1}"
  region        = var.gcp_region
  network       = google_compute_network.main[0].id
  ip_cidr_range = local.gcp_subnets.public[count.index]
}

# GCP Database Subnets
resource "google_compute_subnetwork" "database" {
  count = var.enable_gcp ? length(local.gcp_subnets.database) : 0
  
  name                     = "${var.resource_prefix}-database-subnet-${count.index + 1}"
  region                   = var.gcp_region
  network                  = google_compute_network.main[0].id
  ip_cidr_range           = local.gcp_subnets.database[count.index]
  private_ip_google_access = true
}