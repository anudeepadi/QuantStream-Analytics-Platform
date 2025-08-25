# AWS Provider Configuration
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
      Platform    = "QuantStream"
      CreatedBy   = "Infrastructure-Team"
      CostCenter  = var.cost_center
    }
  }
}

# Azure Provider Configuration
provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = true
      recover_soft_deleted_key_vaults = true
    }
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
  
  subscription_id = var.azure_subscription_id
}

# Google Cloud Provider Configuration
provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
  zone    = var.gcp_zone
}

provider "google-beta" {
  project = var.gcp_project_id
  region  = var.gcp_region
  zone    = var.gcp_zone
}

# Kubernetes Provider Configuration
provider "kubernetes" {
  host                   = var.k8s_host
  cluster_ca_certificate = base64decode(var.k8s_cluster_ca_certificate)
  token                  = var.k8s_token

  dynamic "exec" {
    for_each = var.k8s_exec != null ? [var.k8s_exec] : []
    content {
      api_version = exec.value.api_version
      command     = exec.value.command
      args        = exec.value.args
    }
  }
}

# Helm Provider Configuration
provider "helm" {
  kubernetes {
    host                   = var.k8s_host
    cluster_ca_certificate = base64decode(var.k8s_cluster_ca_certificate)
    token                  = var.k8s_token

    dynamic "exec" {
      for_each = var.k8s_exec != null ? [var.k8s_exec] : []
      content {
        api_version = exec.value.api_version
        command     = exec.value.command
        args        = exec.value.args
      }
    }
  }
}

# Databricks Provider Configuration
provider "databricks" {
  alias = "aws"
  host  = var.databricks_host_aws
  token = var.databricks_token_aws
}

provider "databricks" {
  alias = "azure"
  host  = var.databricks_host_azure
  token = var.databricks_token_azure
}

provider "databricks" {
  alias = "gcp"
  host  = var.databricks_host_gcp
  token = var.databricks_token_gcp
}