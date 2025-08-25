# Data Infrastructure Module for QuantStream Analytics Platform
# Provides multi-cloud databases, caching, messaging, and storage infrastructure

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

# Random password generation for databases
resource "random_password" "db_password" {
  count   = var.enable_aws || var.enable_azure || var.enable_gcp ? 1 : 0
  length  = 16
  special = true
  
  lifecycle {
    ignore_changes = [special]
  }
}

# Local values for data configuration
locals {
  # Environment-based sizing
  database_configs = {
    dev = {
      instance_class     = "db.t3.micro"
      allocated_storage  = 20
      max_allocated_storage = 100
      backup_retention_days = 7
      multi_az          = false
      deletion_protection = false
    }
    staging = {
      instance_class     = "db.r5.large"
      allocated_storage  = 100
      max_allocated_storage = 500
      backup_retention_days = 14
      multi_az          = true
      deletion_protection = false
    }
    prod = {
      instance_class     = "db.r5.2xlarge"
      allocated_storage  = 500
      max_allocated_storage = 2000
      backup_retention_days = 30
      multi_az          = true
      deletion_protection = true
    }
  }
  
  redis_configs = {
    dev = {
      node_type         = "cache.t3.micro"
      num_cache_nodes   = 1
      parameter_group   = "default.redis7"
      port             = 6379
    }
    staging = {
      node_type         = "cache.r6g.large"
      num_cache_nodes   = 2
      parameter_group   = "default.redis7.cluster.on"
      port             = 6379
    }
    prod = {
      node_type         = "cache.r6g.xlarge"
      num_cache_nodes   = 3
      parameter_group   = "default.redis7.cluster.on"
      port             = 6379
    }
  }
  
  current_db_config    = local.database_configs[var.environment]
  current_redis_config = local.redis_configs[var.environment]
}

#######################
# AWS Data Infrastructure
#######################

# AWS RDS PostgreSQL Primary Instance
resource "aws_db_instance" "postgresql_primary" {
  count = var.enable_aws ? 1 : 0
  
  identifier = "${var.resource_prefix}-postgresql-primary"
  
  # Engine configuration
  engine              = "postgres"
  engine_version      = var.postgresql_version
  instance_class      = var.database_instance_class != "" ? var.database_instance_class : local.current_db_config.instance_class
  
  # Storage configuration
  allocated_storage     = local.current_db_config.allocated_storage
  max_allocated_storage = local.current_db_config.max_allocated_storage
  storage_type         = "gp3"
  storage_encrypted    = true
  kms_key_id          = var.kms_key_ids["aws"]
  
  # Database configuration
  db_name  = var.database_name
  username = var.database_username
  password = random_password.db_password[0].result
  port     = 5432
  
  # High availability
  multi_az               = var.multi_az_enabled != null ? var.multi_az_enabled : local.current_db_config.multi_az
  availability_zone      = var.multi_az_enabled ? null : data.aws_availability_zones.available[0].names[0]
  
  # Network configuration
  db_subnet_group_name   = var.aws_db_subnet_group_name
  vpc_security_group_ids = var.aws_security_group_ids
  publicly_accessible    = false
  
  # Backup configuration
  backup_retention_period = var.backup_retention_days != 0 ? var.backup_retention_days : local.current_db_config.backup_retention_days
  backup_window          = "03:00-04:00"
  maintenance_window     = "sun:04:00-sun:05:00"
  
  # Monitoring and performance
  monitoring_interval                   = 60
  monitoring_role_arn                  = aws_iam_role.rds_monitoring[0].arn
  performance_insights_enabled         = var.environment != "dev"
  performance_insights_retention_period = var.environment == "prod" ? 731 : 7
  
  # Security and compliance
  deletion_protection      = local.current_db_config.deletion_protection
  skip_final_snapshot     = var.environment == "dev"
  final_snapshot_identifier = var.environment != "dev" ? "${var.resource_prefix}-postgresql-final-snapshot" : null
  
  # Enable automated minor version upgrades
  auto_minor_version_upgrade = true
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-postgresql-primary"
    Type = "database"
    Engine = "postgresql"
    Cloud = "aws"
    Role = "primary"
  })
  
  depends_on = [aws_iam_role.rds_monitoring]
}

# AWS RDS Read Replica
resource "aws_db_instance" "postgresql_read_replica" {
  count = var.enable_aws && var.environment != "dev" ? 2 : 0
  
  identifier = "${var.resource_prefix}-postgresql-read-${count.index + 1}"
  
  # Replica configuration
  replicate_source_db = aws_db_instance.postgresql_primary[0].identifier
  instance_class      = var.database_instance_class != "" ? var.database_instance_class : local.current_db_config.instance_class
  
  # Place replicas in different AZs
  availability_zone = data.aws_availability_zones.available[0].names[(count.index + 1) % length(data.aws_availability_zones.available[0].names)]
  
  # Network configuration
  publicly_accessible = false
  
  # Monitoring
  monitoring_interval = 60
  monitoring_role_arn = aws_iam_role.rds_monitoring[0].arn
  
  # Performance insights
  performance_insights_enabled         = var.environment == "prod"
  performance_insights_retention_period = var.environment == "prod" ? 731 : 7
  
  # Security
  skip_final_snapshot = true
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-postgresql-read-${count.index + 1}"
    Type = "database"
    Engine = "postgresql"
    Cloud = "aws"
    Role = "read-replica"
  })
}

# RDS Monitoring IAM Role
resource "aws_iam_role" "rds_monitoring" {
  count = var.enable_aws ? 1 : 0
  
  name = "${var.resource_prefix}-rds-monitoring-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "monitoring.rds.amazonaws.com"
        }
      }
    ]
  })
  
  tags = var.common_tags
}

resource "aws_iam_role_policy_attachment" "rds_monitoring" {
  count = var.enable_aws ? 1 : 0
  
  role       = aws_iam_role.rds_monitoring[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}

# AWS ElastiCache Redis Subnet Group
resource "aws_elasticache_subnet_group" "redis" {
  count = var.enable_aws ? 1 : 0
  
  name       = "${var.resource_prefix}-redis-subnet-group"
  subnet_ids = var.aws_subnet_ids
  
  tags = var.common_tags
}

# AWS ElastiCache Redis Cluster
resource "aws_elasticache_replication_group" "redis" {
  count = var.enable_aws ? 1 : 0
  
  replication_group_id       = "${var.resource_prefix}-redis"
  description                = "Redis cluster for ${var.project_name}"
  port                       = local.current_redis_config.port
  parameter_group_name       = local.current_redis_config.parameter_group
  
  # Node configuration
  node_type               = var.redis_node_type != "" ? var.redis_node_type : local.current_redis_config.node_type
  num_cache_clusters      = local.current_redis_config.num_cache_nodes
  
  # Engine configuration
  engine_version          = "7.0"
  
  # Network configuration
  subnet_group_name       = aws_elasticache_subnet_group.redis[0].name
  security_group_ids      = var.aws_security_group_ids
  
  # Security
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  auth_token                 = random_password.db_password[0].result
  
  # Backup and maintenance
  snapshot_retention_limit = var.environment == "prod" ? 7 : 1
  snapshot_window         = "03:00-05:00"
  maintenance_window      = "sun:05:00-sun:07:00"
  
  # Automatic failover
  automatic_failover_enabled = var.environment != "dev"
  multi_az_enabled          = var.environment != "dev"
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-redis"
    Type = "cache"
    Engine = "redis"
    Cloud = "aws"
  })
}

# AWS MSK (Kafka) Cluster Configuration
resource "aws_msk_configuration" "kafka" {
  count = var.enable_aws ? 1 : 0
  
  kafka_versions = ["2.8.1"]
  name          = "${var.resource_prefix}-kafka-config"
  
  server_properties = <<PROPERTIES
auto.create.topics.enable=false
default.replication.factor=3
min.insync.replicas=2
num.partitions=3
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
log.retention.hours=168
PROPERTIES
}

# AWS MSK Kafka Cluster
resource "aws_msk_cluster" "kafka" {
  count = var.enable_aws ? 1 : 0
  
  cluster_name           = "${var.resource_prefix}-kafka"
  kafka_version         = "2.8.1"
  number_of_broker_nodes = var.kafka_number_of_broker_nodes
  
  broker_node_group_info {
    instance_type   = var.kafka_instance_type
    client_subnets  = var.aws_subnet_ids
    storage_info {
      ebs_storage_info {
        volume_size = var.environment == "prod" ? 1000 : 500
      }
    }
    security_groups = var.aws_security_group_ids
  }
  
  configuration_info {
    arn      = aws_msk_configuration.kafka[0].arn
    revision = aws_msk_configuration.kafka[0].latest_revision
  }
  
  encryption_info {
    encryption_at_rest_kms_key_id = var.kms_key_ids["aws"]
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }
  
  client_authentication {
    tls {
      certificate_authority_arns = []
    }
    sasl {
      scram = true
    }
  }
  
  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk[0].name
      }
      s3 {
        enabled = true
        bucket  = aws_s3_bucket.data_lake[0].id
        prefix  = "kafka-logs/"
      }
    }
  }
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-kafka"
    Type = "messaging"
    Engine = "kafka"
    Cloud = "aws"
  })
}

# CloudWatch Log Group for MSK
resource "aws_cloudwatch_log_group" "msk" {
  count = var.enable_aws ? 1 : 0
  
  name              = "/aws/msk/${var.resource_prefix}-kafka"
  retention_in_days = var.environment == "prod" ? 90 : 30
  kms_key_id       = var.kms_key_ids["aws"]
  
  tags = var.common_tags
}

# AWS S3 Data Lake Bucket
resource "aws_s3_bucket" "data_lake" {
  count = var.enable_aws ? 1 : 0
  
  bucket = "${var.resource_prefix}-data-lake-${random_password.db_password[0].result}"
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-data-lake"
    Type = "storage"
    Purpose = "data-lake"
    Cloud = "aws"
  })
}

resource "aws_s3_bucket_versioning" "data_lake" {
  count = var.enable_aws ? 1 : 0
  
  bucket = aws_s3_bucket.data_lake[0].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  count = var.enable_aws ? 1 : 0
  
  bucket = aws_s3_bucket.data_lake[0].id
  
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = var.kms_key_ids["aws"]
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  count = var.enable_aws ? 1 : 0
  
  bucket = aws_s3_bucket.data_lake[0].id
  
  rule {
    id     = "data-lifecycle"
    status = "Enabled"
    
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    
    transition {
      days          = 365
      storage_class = "DEEP_ARCHIVE"
    }
    
    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "STANDARD_IA"
    }
    
    noncurrent_version_expiration {
      noncurrent_days = 365
    }
  }
}

# AWS Availability Zones Data Source
data "aws_availability_zones" "available" {
  count = var.enable_aws ? 1 : 0
  
  state = "available"
}

#######################
# Azure Data Infrastructure
#######################

# Azure PostgreSQL Flexible Server
resource "azurerm_postgresql_flexible_server" "main" {
  count = var.enable_azure ? 1 : 0
  
  name                   = "${var.resource_prefix}-postgresql"
  resource_group_name    = var.azure_resource_group_name
  location              = var.azure_location
  version               = "14"
  
  delegated_subnet_id    = var.azure_subnet_ids[0]
  private_dns_zone_id    = azurerm_private_dns_zone.postgres[0].id
  
  administrator_login    = var.database_username
  administrator_password = random_password.db_password[0].result
  
  zone                   = "1"
  
  storage_mb             = local.current_db_config.allocated_storage * 1024
  
  sku_name               = var.environment == "dev" ? "B_Standard_B1ms" : "GP_Standard_D4s_v3"
  
  backup_retention_days  = local.current_db_config.backup_retention_days
  
  high_availability {
    mode                      = var.environment != "dev" ? "ZoneRedundant" : "Disabled"
    standby_availability_zone = var.environment != "dev" ? "2" : null
  }
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-postgresql"
    Type = "database"
    Engine = "postgresql"
    Cloud = "azure"
  })
  
  depends_on = [azurerm_private_dns_zone_virtual_network_link.postgres]
}

# Azure Private DNS Zone for PostgreSQL
resource "azurerm_private_dns_zone" "postgres" {
  count = var.enable_azure ? 1 : 0
  
  name                = "${var.resource_prefix}-postgres.private.postgres.database.azure.com"
  resource_group_name = var.azure_resource_group_name
  
  tags = var.common_tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "postgres" {
  count = var.enable_azure ? 1 : 0
  
  name                  = "${var.resource_prefix}-postgres-vnet-link"
  private_dns_zone_name = azurerm_private_dns_zone.postgres[0].name
  resource_group_name   = var.azure_resource_group_name
  virtual_network_id    = var.azure_vnet_id
  
  tags = var.common_tags
}

# Azure Cache for Redis
resource "azurerm_redis_cache" "main" {
  count = var.enable_azure ? 1 : 0
  
  name                = "${var.resource_prefix}-redis"
  location            = var.azure_location
  resource_group_name = var.azure_resource_group_name
  
  capacity            = var.environment == "dev" ? 0 : 2
  family              = var.environment == "dev" ? "C" : "P"
  sku_name            = var.environment == "dev" ? "Basic" : "Premium"
  
  enable_non_ssl_port = false
  minimum_tls_version = "1.2"
  
  redis_configuration {
    enable_authentication = true
  }
  
  public_network_access_enabled = false
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-redis"
    Type = "cache"
    Engine = "redis"
    Cloud = "azure"
  })
}

# Azure Event Hubs Namespace (Kafka)
resource "azurerm_eventhub_namespace" "kafka" {
  count = var.enable_azure ? 1 : 0
  
  name                = "${var.resource_prefix}-eventhub"
  location            = var.azure_location
  resource_group_name = var.azure_resource_group_name
  
  sku      = var.environment == "dev" ? "Basic" : "Standard"
  capacity = var.environment == "prod" ? 2 : 1
  
  auto_inflate_enabled     = var.environment != "dev"
  maximum_throughput_units = var.environment == "prod" ? 20 : 10
  
  kafka_enabled = true
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-eventhub"
    Type = "messaging"
    Engine = "kafka"
    Cloud = "azure"
  })
}

# Azure Event Hub (Kafka Topic)
resource "azurerm_eventhub" "data_stream" {
  count = var.enable_azure ? 1 : 0
  
  name                = "data-stream"
  namespace_name      = azurerm_eventhub_namespace.kafka[0].name
  resource_group_name = var.azure_resource_group_name
  
  partition_count   = var.environment == "prod" ? 32 : 8
  message_retention = var.environment == "prod" ? 7 : 1
}

# Azure Storage Account for Data Lake
resource "azurerm_storage_account" "data_lake" {
  count = var.enable_azure ? 1 : 0
  
  name                     = "${replace(var.resource_prefix, "-", "")}datalake"
  resource_group_name      = var.azure_resource_group_name
  location                 = var.azure_location
  account_tier             = "Standard"
  account_replication_type = var.environment == "prod" ? "ZRS" : "LRS"
  account_kind             = "StorageV2"
  
  is_hns_enabled = true  # Hierarchical namespace for Data Lake Gen2
  
  blob_properties {
    versioning_enabled = true
    change_feed_enabled = true
    
    container_delete_retention_policy {
      days = var.environment == "prod" ? 30 : 7
    }
    
    delete_retention_policy {
      days = var.environment == "prod" ? 30 : 7
    }
  }
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-data-lake"
    Type = "storage"
    Purpose = "data-lake"
    Cloud = "azure"
  })
}

resource "azurerm_storage_data_lake_gen2_filesystem" "data" {
  count = var.enable_azure ? 1 : 0
  
  name               = "data"
  storage_account_id = azurerm_storage_account.data_lake[0].id
}

#######################
# GCP Data Infrastructure
#######################

# GCP Cloud SQL PostgreSQL Instance
resource "google_sql_database_instance" "postgresql" {
  count = var.enable_gcp ? 1 : 0
  
  name             = "${var.resource_prefix}-postgresql"
  database_version = "POSTGRES_14"
  region           = var.gcp_region
  
  deletion_protection = local.current_db_config.deletion_protection
  
  settings {
    tier              = var.environment == "dev" ? "db-f1-micro" : "db-custom-4-15360"
    availability_type = var.environment != "dev" ? "REGIONAL" : "ZONAL"
    disk_type         = "PD_SSD"
    disk_size         = local.current_db_config.allocated_storage
    
    backup_configuration {
      enabled                        = true
      start_time                     = "03:00"
      point_in_time_recovery_enabled = var.environment != "dev"
      backup_retention_settings {
        retained_backups = local.current_db_config.backup_retention_days
      }
    }
    
    ip_configuration {
      ipv4_enabled                                  = false
      private_network                               = var.gcp_vpc_self_link
      enable_private_path_for_google_cloud_services = true
    }
    
    database_flags {
      name  = "log_statement"
      value = "all"
    }
    
    maintenance_window {
      day  = 7
      hour = 4
    }
  }
  
  depends_on = [google_service_networking_connection.private_vpc_connection]
}

# GCP Private Service Connection
resource "google_compute_global_address" "private_ip_address" {
  count = var.enable_gcp ? 1 : 0
  
  name          = "${var.resource_prefix}-private-ip-address"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = var.gcp_vpc_self_link
}

resource "google_service_networking_connection" "private_vpc_connection" {
  count = var.enable_gcp ? 1 : 0
  
  network                 = var.gcp_vpc_self_link
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip_address[0].name]
}

# GCP Cloud SQL Database
resource "google_sql_database" "database" {
  count = var.enable_gcp ? 1 : 0
  
  name     = var.database_name
  instance = google_sql_database_instance.postgresql[0].name
}

# GCP Cloud SQL User
resource "google_sql_user" "user" {
  count = var.enable_gcp ? 1 : 0
  
  name     = var.database_username
  instance = google_sql_database_instance.postgresql[0].name
  password = random_password.db_password[0].result
}

# GCP Memorystore Redis Instance
resource "google_redis_instance" "main" {
  count = var.enable_gcp ? 1 : 0
  
  name           = "${var.resource_prefix}-redis"
  region         = var.gcp_region
  memory_size_gb = var.environment == "dev" ? 1 : 5
  
  tier                    = var.environment == "dev" ? "BASIC" : "STANDARD_HA"
  redis_version          = "REDIS_7_0"
  display_name           = "${var.resource_prefix} Redis Instance"
  
  authorized_network     = var.gcp_vpc_self_link
  connect_mode          = "PRIVATE_SERVICE_ACCESS"
  
  redis_configs = {
    maxmemory-policy = "allkeys-lru"
  }
  
  labels = var.common_tags
}

# GCP Cloud Storage Bucket for Data Lake
resource "google_storage_bucket" "data_lake" {
  count = var.enable_gcp ? 1 : 0
  
  name          = "${var.resource_prefix}-data-lake-${random_password.db_password[0].result}"
  location      = var.gcp_region
  storage_class = "STANDARD"
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }
  
  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type          = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
  }
  
  encryption {
    default_kms_key_name = var.kms_key_ids["gcp"]
  }
  
  labels = var.common_tags
}

# GCP Pub/Sub Topics (Kafka alternative)
resource "google_pubsub_topic" "data_stream" {
  count = var.enable_gcp ? 1 : 0
  
  name = "${var.resource_prefix}-data-stream"
  
  labels = var.common_tags
}

resource "google_pubsub_subscription" "data_stream_subscription" {
  count = var.enable_gcp ? 1 : 0
  
  name  = "${var.resource_prefix}-data-stream-subscription"
  topic = google_pubsub_topic.data_stream[0].name
  
  ack_deadline_seconds = 20
  
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
  
  labels = var.common_tags
}