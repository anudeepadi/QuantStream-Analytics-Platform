# Outputs for Data Infrastructure Module

#######################
# AWS Data Infrastructure Outputs
#######################

# Database Outputs
output "aws_rds_endpoint" {
  description = "AWS RDS PostgreSQL primary endpoint"
  value       = var.enable_aws ? aws_db_instance.postgresql_primary[0].endpoint : null
}

output "aws_rds_port" {
  description = "AWS RDS PostgreSQL port"
  value       = var.enable_aws ? aws_db_instance.postgresql_primary[0].port : null
}

output "aws_rds_database_name" {
  description = "AWS RDS database name"
  value       = var.enable_aws ? aws_db_instance.postgresql_primary[0].db_name : null
}

output "aws_rds_username" {
  description = "AWS RDS database username"
  value       = var.enable_aws ? aws_db_instance.postgresql_primary[0].username : null
}

output "aws_rds_read_replica_endpoints" {
  description = "AWS RDS read replica endpoints"
  value       = var.enable_aws && var.environment != "dev" ? aws_db_instance.postgresql_read_replica[*].endpoint : []
}

# Redis Outputs
output "aws_redis_endpoint" {
  description = "AWS ElastiCache Redis primary endpoint"
  value       = var.enable_aws ? aws_elasticache_replication_group.redis[0].primary_endpoint_address : null
}

output "aws_redis_port" {
  description = "AWS ElastiCache Redis port"
  value       = var.enable_aws ? aws_elasticache_replication_group.redis[0].port : null
}

output "aws_redis_reader_endpoint" {
  description = "AWS ElastiCache Redis reader endpoint"
  value       = var.enable_aws ? aws_elasticache_replication_group.redis[0].reader_endpoint_address : null
}

# Kafka Outputs
output "aws_msk_bootstrap_brokers" {
  description = "AWS MSK Kafka bootstrap brokers"
  value       = var.enable_aws ? aws_msk_cluster.kafka[0].bootstrap_brokers_tls : null
  sensitive   = true
}

output "aws_msk_zookeeper_connect_string" {
  description = "AWS MSK Zookeeper connection string"
  value       = var.enable_aws ? aws_msk_cluster.kafka[0].zookeeper_connect_string : null
  sensitive   = true
}

output "aws_msk_cluster_arn" {
  description = "AWS MSK cluster ARN"
  value       = var.enable_aws ? aws_msk_cluster.kafka[0].arn : null
}

# Storage Outputs
output "aws_s3_data_lake_bucket" {
  description = "AWS S3 data lake bucket name"
  value       = var.enable_aws ? aws_s3_bucket.data_lake[0].id : null
}

output "aws_s3_data_lake_arn" {
  description = "AWS S3 data lake bucket ARN"
  value       = var.enable_aws ? aws_s3_bucket.data_lake[0].arn : null
}

#######################
# Azure Data Infrastructure Outputs
#######################

# Database Outputs
output "azure_postgresql_fqdn" {
  description = "Azure PostgreSQL Flexible Server FQDN"
  value       = var.enable_azure ? azurerm_postgresql_flexible_server.main[0].fqdn : null
}

output "azure_postgresql_username" {
  description = "Azure PostgreSQL username"
  value       = var.enable_azure ? azurerm_postgresql_flexible_server.main[0].administrator_login : null
}

output "azure_postgresql_database_name" {
  description = "Azure PostgreSQL database name"
  value       = var.enable_azure ? var.database_name : null
}

# Redis Outputs
output "azure_redis_hostname" {
  description = "Azure Cache for Redis hostname"
  value       = var.enable_azure ? azurerm_redis_cache.main[0].hostname : null
}

output "azure_redis_port" {
  description = "Azure Cache for Redis port"
  value       = var.enable_azure ? azurerm_redis_cache.main[0].port : null
}

output "azure_redis_ssl_port" {
  description = "Azure Cache for Redis SSL port"
  value       = var.enable_azure ? azurerm_redis_cache.main[0].ssl_port : null
}

output "azure_redis_primary_access_key" {
  description = "Azure Cache for Redis primary access key"
  value       = var.enable_azure ? azurerm_redis_cache.main[0].primary_access_key : null
  sensitive   = true
}

# Event Hubs (Kafka) Outputs
output "azure_eventhub_namespace_name" {
  description = "Azure Event Hubs namespace name"
  value       = var.enable_azure ? azurerm_eventhub_namespace.kafka[0].name : null
}

output "azure_eventhub_connection_string" {
  description = "Azure Event Hubs connection string"
  value       = var.enable_azure ? azurerm_eventhub_namespace.kafka[0].default_primary_connection_string : null
  sensitive   = true
}

# Storage Outputs
output "azure_storage_account_name" {
  description = "Azure Storage Account name for data lake"
  value       = var.enable_azure ? azurerm_storage_account.data_lake[0].name : null
}

output "azure_storage_account_primary_endpoint" {
  description = "Azure Storage Account primary endpoint"
  value       = var.enable_azure ? azurerm_storage_account.data_lake[0].primary_dfs_endpoint : null
}

output "azure_data_lake_filesystem_name" {
  description = "Azure Data Lake Gen2 filesystem name"
  value       = var.enable_azure ? azurerm_storage_data_lake_gen2_filesystem.data[0].name : null
}

#######################
# GCP Data Infrastructure Outputs
#######################

# Database Outputs
output "gcp_sql_connection_name" {
  description = "GCP Cloud SQL connection name"
  value       = var.enable_gcp ? google_sql_database_instance.postgresql[0].connection_name : null
}

output "gcp_sql_private_ip" {
  description = "GCP Cloud SQL private IP address"
  value       = var.enable_gcp ? google_sql_database_instance.postgresql[0].private_ip_address : null
}

output "gcp_sql_database_name" {
  description = "GCP Cloud SQL database name"
  value       = var.enable_gcp ? google_sql_database.database[0].name : null
}

output "gcp_sql_username" {
  description = "GCP Cloud SQL username"
  value       = var.enable_gcp ? google_sql_user.user[0].name : null
}

# Redis Outputs
output "gcp_redis_host" {
  description = "GCP Memorystore Redis host"
  value       = var.enable_gcp ? google_redis_instance.main[0].host : null
}

output "gcp_redis_port" {
  description = "GCP Memorystore Redis port"
  value       = var.enable_gcp ? google_redis_instance.main[0].port : null
}

output "gcp_redis_auth_string" {
  description = "GCP Memorystore Redis auth string"
  value       = var.enable_gcp ? google_redis_instance.main[0].auth_string : null
  sensitive   = true
}

# Pub/Sub Outputs
output "gcp_pubsub_topic_name" {
  description = "GCP Pub/Sub topic name"
  value       = var.enable_gcp ? google_pubsub_topic.data_stream[0].name : null
}

output "gcp_pubsub_subscription_name" {
  description = "GCP Pub/Sub subscription name"
  value       = var.enable_gcp ? google_pubsub_subscription.data_stream_subscription[0].name : null
}

# Storage Outputs
output "gcp_storage_bucket_name" {
  description = "GCP Storage bucket name for data lake"
  value       = var.enable_gcp ? google_storage_bucket.data_lake[0].name : null
}

output "gcp_storage_bucket_url" {
  description = "GCP Storage bucket URL"
  value       = var.enable_gcp ? google_storage_bucket.data_lake[0].url : null
}

#######################
# Cross-Cloud Database Summary
#######################

output "database_endpoints" {
  description = "Database endpoints across all cloud providers"
  value = {
    aws = var.enable_aws ? {
      primary_endpoint = aws_db_instance.postgresql_primary[0].endpoint
      port             = aws_db_instance.postgresql_primary[0].port
      database_name    = aws_db_instance.postgresql_primary[0].db_name
      username         = aws_db_instance.postgresql_primary[0].username
      read_replicas    = var.environment != "dev" ? aws_db_instance.postgresql_read_replica[*].endpoint : []
    } : null
    
    azure = var.enable_azure ? {
      fqdn          = azurerm_postgresql_flexible_server.main[0].fqdn
      port          = 5432
      database_name = var.database_name
      username      = azurerm_postgresql_flexible_server.main[0].administrator_login
    } : null
    
    gcp = var.enable_gcp ? {
      connection_name = google_sql_database_instance.postgresql[0].connection_name
      private_ip      = google_sql_database_instance.postgresql[0].private_ip_address
      port           = 5432
      database_name  = google_sql_database.database[0].name
      username       = google_sql_user.user[0].name
    } : null
  }
}

output "redis_endpoints" {
  description = "Redis endpoints across all cloud providers"
  value = {
    aws = var.enable_aws ? {
      primary_endpoint = aws_elasticache_replication_group.redis[0].primary_endpoint_address
      reader_endpoint  = aws_elasticache_replication_group.redis[0].reader_endpoint_address
      port            = aws_elasticache_replication_group.redis[0].port
    } : null
    
    azure = var.enable_azure ? {
      hostname = azurerm_redis_cache.main[0].hostname
      port     = azurerm_redis_cache.main[0].port
      ssl_port = azurerm_redis_cache.main[0].ssl_port
    } : null
    
    gcp = var.enable_gcp ? {
      host = google_redis_instance.main[0].host
      port = google_redis_instance.main[0].port
    } : null
  }
}

output "kafka_endpoints" {
  description = "Kafka/messaging endpoints across all cloud providers"
  value = {
    aws = var.enable_aws ? {
      bootstrap_brokers    = aws_msk_cluster.kafka[0].bootstrap_brokers_tls
      zookeeper_connect   = aws_msk_cluster.kafka[0].zookeeper_connect_string
      cluster_arn         = aws_msk_cluster.kafka[0].arn
    } : null
    
    azure = var.enable_azure ? {
      namespace_name      = azurerm_eventhub_namespace.kafka[0].name
      connection_string   = azurerm_eventhub_namespace.kafka[0].default_primary_connection_string
      kafka_enabled       = azurerm_eventhub_namespace.kafka[0].kafka_enabled
    } : null
    
    gcp = var.enable_gcp ? {
      topic_name          = google_pubsub_topic.data_stream[0].name
      subscription_name   = google_pubsub_subscription.data_stream_subscription[0].name
    } : null
  }
  sensitive = true
}

output "storage_buckets" {
  description = "Storage bucket information across all cloud providers"
  value = {
    aws = var.enable_aws ? {
      bucket_name = aws_s3_bucket.data_lake[0].id
      bucket_arn  = aws_s3_bucket.data_lake[0].arn
      region      = aws_s3_bucket.data_lake[0].region
    } : null
    
    azure = var.enable_azure ? {
      account_name       = azurerm_storage_account.data_lake[0].name
      primary_endpoint   = azurerm_storage_account.data_lake[0].primary_dfs_endpoint
      filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.data[0].name
    } : null
    
    gcp = var.enable_gcp ? {
      bucket_name = google_storage_bucket.data_lake[0].name
      bucket_url  = google_storage_bucket.data_lake[0].url
      location    = google_storage_bucket.data_lake[0].location
    } : null
  }
}

# Database Password (sensitive)
output "database_password" {
  description = "Generated database password"
  value       = (var.enable_aws || var.enable_azure || var.enable_gcp) ? random_password.db_password[0].result : null
  sensitive   = true
}

# Configuration Summary
output "data_infrastructure_summary" {
  description = "Summary of data infrastructure configuration"
  value = {
    environment = var.environment
    enabled_providers = [
      for provider in ["aws", "azure", "gcp"] : provider
      if (provider == "aws" && var.enable_aws) ||
         (provider == "azure" && var.enable_azure) ||
         (provider == "gcp" && var.enable_gcp)
    ]
    database_engine = "PostgreSQL"
    cache_engine    = "Redis"
    messaging_systems = {
      aws   = var.enable_aws ? "MSK (Kafka)" : null
      azure = var.enable_azure ? "Event Hubs (Kafka)" : null
      gcp   = var.enable_gcp ? "Pub/Sub" : null
    }
    storage_types = {
      aws   = var.enable_aws ? "S3" : null
      azure = var.enable_azure ? "Data Lake Gen2" : null
      gcp   = var.enable_gcp ? "Cloud Storage" : null
    }
  }
}