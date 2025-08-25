# Local Values for QuantStream Analytics Platform Infrastructure

locals {
  # Common naming conventions
  name_prefix = "${var.project_name}-${var.environment}"
  
  # Standard tags applied to all resources
  common_tags = merge(
    {
      Project      = var.project_name
      Environment  = var.environment
      ManagedBy    = "Terraform"
      Platform     = "QuantStream"
      CreatedBy    = "Infrastructure-Team"
      CostCenter   = var.cost_center
      OwnerTeam    = var.owner_team
      Timestamp    = formatdate("YYYY-MM-DD", timestamp())
    },
    var.additional_tags
  )

  # Environment-specific configurations
  env_config = {
    dev = {
      instance_types = {
        k8s_node     = "t3.medium"
        db_instance  = "db.t3.micro"
        redis_node   = "cache.t3.micro"
        kafka_broker = "kafka.t3.small"
      }
      min_capacity = 1
      max_capacity = 3
      desired_capacity = 1
      multi_az = false
      backup_retention = 7
    }
    
    staging = {
      instance_types = {
        k8s_node     = "t3.large"
        db_instance  = "db.r5.large"
        redis_node   = "cache.r6g.large"
        kafka_broker = "kafka.m5.large"
      }
      min_capacity = 2
      max_capacity = 6
      desired_capacity = 3
      multi_az = true
      backup_retention = 14
    }
    
    prod = {
      instance_types = {
        k8s_node     = "m5.xlarge"
        db_instance  = "db.r5.2xlarge"
        redis_node   = "cache.r6g.xlarge"
        kafka_broker = "kafka.m5.xlarge"
      }
      min_capacity = 3
      max_capacity = 20
      desired_capacity = 6
      multi_az = true
      backup_retention = 30
    }
  }

  # Current environment configuration
  current_env = local.env_config[var.environment]

  # Subnet configurations for multi-cloud
  subnet_configs = {
    aws = {
      private_subnets = [
        cidrsubnet(var.vpc_cidr_aws, 8, 1),
        cidrsubnet(var.vpc_cidr_aws, 8, 2),
        cidrsubnet(var.vpc_cidr_aws, 8, 3)
      ]
      public_subnets = [
        cidrsubnet(var.vpc_cidr_aws, 8, 101),
        cidrsubnet(var.vpc_cidr_aws, 8, 102),
        cidrsubnet(var.vpc_cidr_aws, 8, 103)
      ]
      database_subnets = [
        cidrsubnet(var.vpc_cidr_aws, 8, 201),
        cidrsubnet(var.vpc_cidr_aws, 8, 202),
        cidrsubnet(var.vpc_cidr_aws, 8, 203)
      ]
    }
    
    azure = {
      private_subnets = [
        cidrsubnet(var.vpc_cidr_azure, 8, 1),
        cidrsubnet(var.vpc_cidr_azure, 8, 2),
        cidrsubnet(var.vpc_cidr_azure, 8, 3)
      ]
      public_subnets = [
        cidrsubnet(var.vpc_cidr_azure, 8, 101),
        cidrsubnet(var.vpc_cidr_azure, 8, 102),
        cidrsubnet(var.vpc_cidr_azure, 8, 103)
      ]
      database_subnets = [
        cidrsubnet(var.vpc_cidr_azure, 8, 201),
        cidrsubnet(var.vpc_cidr_azure, 8, 202),
        cidrsubnet(var.vpc_cidr_azure, 8, 203)
      ]
    }
    
    gcp = {
      private_subnets = [
        cidrsubnet(var.vpc_cidr_gcp, 8, 1),
        cidrsubnet(var.vpc_cidr_gcp, 8, 2),
        cidrsubnet(var.vpc_cidr_gcp, 8, 3)
      ]
      public_subnets = [
        cidrsubnet(var.vpc_cidr_gcp, 8, 101),
        cidrsubnet(var.vpc_cidr_gcp, 8, 102),
        cidrsubnet(var.vpc_cidr_gcp, 8, 103)
      ]
      database_subnets = [
        cidrsubnet(var.vpc_cidr_gcp, 8, 201),
        cidrsubnet(var.vpc_cidr_gcp, 8, 202),
        cidrsubnet(var.vpc_cidr_gcp, 8, 203)
      ]
    }
  }

  # Monitoring configuration
  monitoring_config = {
    prometheus_retention = var.environment == "prod" ? "30d" : "15d"
    grafana_admin_user = "admin"
    elasticsearch_version = "7.17"
    kibana_version = "7.17"
    log_retention_days = var.environment == "prod" ? 90 : 30
  }

  # Security configuration
  security_config = {
    allowed_cidr_blocks = var.environment == "prod" ? [] : ["0.0.0.0/0"]  # Restrict in prod
    ssl_policy = "TLS-1-2"
    encryption_algorithm = "AES256"
    key_rotation_enabled = true
  }

  # Databricks configuration
  databricks_config = {
    unity_catalog_name = "${local.name_prefix}-unity-catalog"
    cluster_policy_name = "${local.name_prefix}-cluster-policy"
    job_cluster_config = {
      spark_version = "13.3.x-scala2.12"
      node_type_id = local.current_env.instance_types.k8s_node
      driver_node_type_id = local.current_env.instance_types.k8s_node
      num_workers = local.current_env.desired_capacity
      autoscale = {
        min_workers = local.current_env.min_capacity
        max_workers = local.current_env.max_capacity
      }
      enable_photon = var.environment == "prod" ? true : false
    }
  }
}