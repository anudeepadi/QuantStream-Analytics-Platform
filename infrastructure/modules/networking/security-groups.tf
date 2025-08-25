# Security Groups and Firewall Rules for Multi-Cloud Networking

#######################
# AWS Security Groups
#######################

# Default Security Group for AWS VPC
resource "aws_default_security_group" "main" {
  count = var.enable_aws ? 1 : 0
  
  vpc_id = aws_vpc.main[0].id
  
  # Remove all default rules
  ingress = []
  egress  = []
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-default-sg"
    Type = "security-group"
    Cloud = "aws"
  })
}

# Security Group for Web Tier (Load Balancers)
resource "aws_security_group" "web" {
  count = var.enable_aws ? 1 : 0
  
  name_prefix = "${var.resource_prefix}-web-"
  description = "Security group for web tier and load balancers"
  vpc_id      = aws_vpc.main[0].id
  
  # HTTP access
  ingress {
    description = "HTTP"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }
  
  # HTTPS access
  ingress {
    description = "HTTPS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }
  
  # All outbound traffic
  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-web-sg"
    Type = "security-group"
    Cloud = "aws"
    Tier = "web"
  })
  
  lifecycle {
    create_before_destroy = true
  }
}

# Security Group for Application Tier (Kubernetes)
resource "aws_security_group" "app" {
  count = var.enable_aws ? 1 : 0
  
  name_prefix = "${var.resource_prefix}-app-"
  description = "Security group for application tier (Kubernetes)"
  vpc_id      = aws_vpc.main[0].id
  
  # Kubernetes API Server
  ingress {
    description     = "Kubernetes API Server"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [aws_security_group.web[0].id]
  }
  
  # Node communication
  ingress {
    description = "Node communication"
    from_port   = 10250
    to_port     = 10250
    protocol    = "tcp"
    self        = true
  }
  
  # Pod communication
  ingress {
    description = "Pod communication"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    self        = true
  }
  
  # All outbound traffic
  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-app-sg"
    Type = "security-group"
    Cloud = "aws"
    Tier = "application"
  })
  
  lifecycle {
    create_before_destroy = true
  }
}

# Security Group for Database Tier
resource "aws_security_group" "database" {
  count = var.enable_aws ? 1 : 0
  
  name_prefix = "${var.resource_prefix}-database-"
  description = "Security group for database tier"
  vpc_id      = aws_vpc.main[0].id
  
  # PostgreSQL access from application tier
  ingress {
    description     = "PostgreSQL"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.app[0].id]
  }
  
  # Redis access from application tier
  ingress {
    description     = "Redis"
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [aws_security_group.app[0].id]
  }
  
  # Database replication
  ingress {
    description = "Database replication"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    self        = true
  }
  
  # Minimal outbound traffic for updates only
  egress {
    description = "HTTPS for updates"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-database-sg"
    Type = "security-group"
    Cloud = "aws"
    Tier = "database"
  })
  
  lifecycle {
    create_before_destroy = true
  }
}

# Security Group for Kafka/Messaging
resource "aws_security_group" "messaging" {
  count = var.enable_aws ? 1 : 0
  
  name_prefix = "${var.resource_prefix}-messaging-"
  description = "Security group for Kafka and messaging services"
  vpc_id      = aws_vpc.main[0].id
  
  # Kafka broker communication
  ingress {
    description     = "Kafka brokers"
    from_port       = 9092
    to_port         = 9092
    protocol        = "tcp"
    security_groups = [aws_security_group.app[0].id]
  }
  
  # Kafka inter-broker communication
  ingress {
    description = "Kafka inter-broker"
    from_port   = 9093
    to_port     = 9093
    protocol    = "tcp"
    self        = true
  }
  
  # Zookeeper
  ingress {
    description = "Zookeeper"
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    self        = true
  }
  
  # All outbound traffic
  egress {
    description = "All outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-messaging-sg"
    Type = "security-group"
    Cloud = "aws"
    Tier = "messaging"
  })
  
  lifecycle {
    create_before_destroy = true
  }
}

#######################
# Azure Network Security Groups
#######################

# Network Security Group for Web Tier
resource "azurerm_network_security_group" "web" {
  count = var.enable_azure ? 1 : 0
  
  name                = "${var.resource_prefix}-web-nsg"
  location            = azurerm_resource_group.main[0].location
  resource_group_name = azurerm_resource_group.main[0].name
  
  security_rule {
    name                       = "HTTP"
    priority                   = 1001
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "80"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
  
  security_rule {
    name                       = "HTTPS"
    priority                   = 1002
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "443"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-web-nsg"
    Type = "network-security-group"
    Cloud = "azure"
    Tier = "web"
  })
}

# Network Security Group for Application Tier
resource "azurerm_network_security_group" "app" {
  count = var.enable_azure ? 1 : 0
  
  name                = "${var.resource_prefix}-app-nsg"
  location            = azurerm_resource_group.main[0].location
  resource_group_name = azurerm_resource_group.main[0].name
  
  security_rule {
    name                       = "KubernetesAPI"
    priority                   = 1001
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "443"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "*"
  }
  
  security_rule {
    name                       = "NodeCommunication"
    priority                   = 1002
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "10250"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "*"
  }
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-app-nsg"
    Type = "network-security-group"
    Cloud = "azure"
    Tier = "application"
  })
}

# Network Security Group for Database Tier
resource "azurerm_network_security_group" "database" {
  count = var.enable_azure ? 1 : 0
  
  name                = "${var.resource_prefix}-database-nsg"
  location            = azurerm_resource_group.main[0].location
  resource_group_name = azurerm_resource_group.main[0].name
  
  security_rule {
    name                       = "PostgreSQL"
    priority                   = 1001
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "5432"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "*"
  }
  
  security_rule {
    name                       = "Redis"
    priority                   = 1002
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "6379"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "*"
  }
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-database-nsg"
    Type = "network-security-group"
    Cloud = "azure"
    Tier = "database"
  })
}

# Associate NSGs with Subnets
resource "azurerm_subnet_network_security_group_association" "public" {
  count = var.enable_azure ? length(azurerm_subnet.public) : 0
  
  subnet_id                 = azurerm_subnet.public[count.index].id
  network_security_group_id = azurerm_network_security_group.web[0].id
}

resource "azurerm_subnet_network_security_group_association" "private" {
  count = var.enable_azure ? length(azurerm_subnet.private) : 0
  
  subnet_id                 = azurerm_subnet.private[count.index].id
  network_security_group_id = azurerm_network_security_group.app[0].id
}

resource "azurerm_subnet_network_security_group_association" "database" {
  count = var.enable_azure ? length(azurerm_subnet.database) : 0
  
  subnet_id                 = azurerm_subnet.database[count.index].id
  network_security_group_id = azurerm_network_security_group.database[0].id
}

#######################
# GCP Firewall Rules
#######################

# Firewall rule for web traffic
resource "google_compute_firewall" "web" {
  count = var.enable_gcp ? 1 : 0
  
  name    = "${var.resource_prefix}-web-firewall"
  network = google_compute_network.main[0].name
  
  allow {
    protocol = "tcp"
    ports    = ["80", "443"]
  }
  
  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["web"]
}

# Firewall rule for application tier
resource "google_compute_firewall" "app" {
  count = var.enable_gcp ? 1 : 0
  
  name    = "${var.resource_prefix}-app-firewall"
  network = google_compute_network.main[0].name
  
  allow {
    protocol = "tcp"
    ports    = ["443", "10250"]
  }
  
  source_tags = ["web"]
  target_tags = ["app"]
}

# Firewall rule for database access
resource "google_compute_firewall" "database" {
  count = var.enable_gcp ? 1 : 0
  
  name    = "${var.resource_prefix}-database-firewall"
  network = google_compute_network.main[0].name
  
  allow {
    protocol = "tcp"
    ports    = ["5432", "6379"]
  }
  
  source_tags = ["app"]
  target_tags = ["database"]
}

# Firewall rule for internal communication
resource "google_compute_firewall" "internal" {
  count = var.enable_gcp ? 1 : 0
  
  name    = "${var.resource_prefix}-internal-firewall"
  network = google_compute_network.main[0].name
  
  allow {
    protocol = "tcp"
  }
  
  allow {
    protocol = "udp"
  }
  
  allow {
    protocol = "icmp"
  }
  
  source_ranges = [var.gcp_vpc_cidr]
}

# Deny all other traffic (implicit in GCP but made explicit)
resource "google_compute_firewall" "deny_all" {
  count = var.enable_gcp ? 1 : 0
  
  name    = "${var.resource_prefix}-deny-all-firewall"
  network = google_compute_network.main[0].name
  
  deny {
    protocol = "all"
  }
  
  source_ranges = ["0.0.0.0/0"]
  priority      = 65534
}