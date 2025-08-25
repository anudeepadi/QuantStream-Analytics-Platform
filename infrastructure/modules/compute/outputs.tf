# Outputs for Compute Module

#######################
# AWS EKS Outputs
#######################

output "aws_eks_cluster_id" {
  description = "ID of the AWS EKS cluster"
  value       = var.enable_aws ? aws_eks_cluster.main[0].id : null
}

output "aws_eks_cluster_arn" {
  description = "ARN of the AWS EKS cluster"
  value       = var.enable_aws ? aws_eks_cluster.main[0].arn : null
}

output "aws_eks_cluster_endpoint" {
  description = "Endpoint for AWS EKS control plane"
  value       = var.enable_aws ? aws_eks_cluster.main[0].endpoint : null
}

output "aws_eks_cluster_version" {
  description = "The Kubernetes version for the EKS cluster"
  value       = var.enable_aws ? aws_eks_cluster.main[0].version : null
}

output "aws_eks_cluster_certificate_authority_data" {
  description = "Base64 encoded certificate data required to communicate with the cluster"
  value       = var.enable_aws ? aws_eks_cluster.main[0].certificate_authority[0].data : null
  sensitive   = true
}

output "aws_eks_cluster_security_group_id" {
  description = "Security group ID attached to the EKS cluster"
  value       = var.enable_aws ? aws_eks_cluster.main[0].vpc_config[0].cluster_security_group_id : null
}

output "aws_eks_cluster_iam_role_arn" {
  description = "IAM role ARN of the EKS cluster"
  value       = var.enable_aws ? aws_eks_cluster.main[0].role_arn : null
}

output "aws_eks_node_group_arn" {
  description = "ARN of the EKS node group"
  value       = var.enable_aws ? aws_eks_node_group.main[0].arn : null
}

output "aws_eks_node_group_status" {
  description = "Status of the EKS node group"
  value       = var.enable_aws ? aws_eks_node_group.main[0].status : null
}

output "aws_eks_cluster_oidc_issuer_url" {
  description = "The URL on the EKS cluster OIDC Issuer"
  value       = var.enable_aws ? aws_eks_cluster.main[0].identity[0].oidc[0].issuer : null
}

#######################
# Azure AKS Outputs
#######################

output "azure_aks_cluster_id" {
  description = "ID of the Azure AKS cluster"
  value       = var.enable_azure ? azurerm_kubernetes_cluster.main[0].id : null
}

output "azure_aks_cluster_name" {
  description = "Name of the Azure AKS cluster"
  value       = var.enable_azure ? azurerm_kubernetes_cluster.main[0].name : null
}

output "azure_aks_cluster_fqdn" {
  description = "FQDN of the Azure AKS cluster"
  value       = var.enable_azure ? azurerm_kubernetes_cluster.main[0].fqdn : null
}

output "azure_aks_cluster_endpoint" {
  description = "Endpoint for Azure AKS control plane"
  value       = var.enable_azure ? azurerm_kubernetes_cluster.main[0].kube_config[0].host : null
}

output "azure_aks_cluster_ca_certificate" {
  description = "Base64 encoded certificate data for Azure AKS cluster"
  value       = var.enable_azure ? azurerm_kubernetes_cluster.main[0].kube_config[0].cluster_ca_certificate : null
  sensitive   = true
}

output "azure_aks_client_certificate" {
  description = "Base64 encoded client certificate for Azure AKS cluster"
  value       = var.enable_azure ? azurerm_kubernetes_cluster.main[0].kube_config[0].client_certificate : null
  sensitive   = true
}

output "azure_aks_client_key" {
  description = "Base64 encoded client key for Azure AKS cluster"
  value       = var.enable_azure ? azurerm_kubernetes_cluster.main[0].kube_config[0].client_key : null
  sensitive   = true
}

output "azure_aks_kube_config_raw" {
  description = "Raw kubeconfig for Azure AKS cluster"
  value       = var.enable_azure ? azurerm_kubernetes_cluster.main[0].kube_config_raw : null
  sensitive   = true
}

output "azure_aks_node_resource_group" {
  description = "Resource group containing AKS cluster nodes"
  value       = var.enable_azure ? azurerm_kubernetes_cluster.main[0].node_resource_group : null
}

output "azure_aks_principal_id" {
  description = "Principal ID of the system assigned identity for AKS cluster"
  value       = var.enable_azure ? azurerm_kubernetes_cluster.main[0].identity[0].principal_id : null
}

#######################
# GCP GKE Outputs
#######################

output "gcp_gke_cluster_id" {
  description = "ID of the GCP GKE cluster"
  value       = var.enable_gcp ? google_container_cluster.main[0].id : null
}

output "gcp_gke_cluster_name" {
  description = "Name of the GCP GKE cluster"
  value       = var.enable_gcp ? google_container_cluster.main[0].name : null
}

output "gcp_gke_cluster_endpoint" {
  description = "Endpoint for GCP GKE control plane"
  value       = var.enable_gcp ? google_container_cluster.main[0].endpoint : null
}

output "gcp_gke_cluster_ca_certificate" {
  description = "Base64 encoded certificate data for GCP GKE cluster"
  value       = var.enable_gcp ? google_container_cluster.main[0].master_auth[0].cluster_ca_certificate : null
  sensitive   = true
}

output "gcp_gke_cluster_location" {
  description = "Location of the GCP GKE cluster"
  value       = var.enable_gcp ? google_container_cluster.main[0].location : null
}

output "gcp_gke_cluster_zone" {
  description = "Zone of the GCP GKE cluster (for zonal clusters)"
  value       = var.enable_gcp ? google_container_cluster.main[0].location : null
}

output "gcp_gke_cluster_self_link" {
  description = "Self link of the GCP GKE cluster"
  value       = var.enable_gcp ? google_container_cluster.main[0].self_link : null
}

output "gcp_gke_node_pool_name" {
  description = "Name of the GCP GKE node pool"
  value       = var.enable_gcp ? google_container_node_pool.main[0].name : null
}

output "gcp_gke_service_account_email" {
  description = "Email address of the GKE node service account"
  value       = var.enable_gcp ? google_service_account.gke_nodes[0].email : null
}

#######################
# Cross-Cloud Kubernetes Summary
#######################

output "kubernetes_clusters" {
  description = "Summary of all Kubernetes clusters"
  value = {
    aws = var.enable_aws ? {
      cluster_id   = aws_eks_cluster.main[0].id
      cluster_name = aws_eks_cluster.main[0].name
      endpoint     = aws_eks_cluster.main[0].endpoint
      version      = aws_eks_cluster.main[0].version
      status       = aws_eks_cluster.main[0].status
      oidc_issuer  = aws_eks_cluster.main[0].identity[0].oidc[0].issuer
    } : null
    
    azure = var.enable_azure ? {
      cluster_id   = azurerm_kubernetes_cluster.main[0].id
      cluster_name = azurerm_kubernetes_cluster.main[0].name
      endpoint     = azurerm_kubernetes_cluster.main[0].kube_config[0].host
      version      = azurerm_kubernetes_cluster.main[0].kubernetes_version
      fqdn         = azurerm_kubernetes_cluster.main[0].fqdn
      principal_id = azurerm_kubernetes_cluster.main[0].identity[0].principal_id
    } : null
    
    gcp = var.enable_gcp ? {
      cluster_id   = google_container_cluster.main[0].id
      cluster_name = google_container_cluster.main[0].name
      endpoint     = google_container_cluster.main[0].endpoint
      location     = google_container_cluster.main[0].location
      status       = google_container_cluster.main[0].status
      self_link    = google_container_cluster.main[0].self_link
    } : null
  }
}

output "cluster_endpoints" {
  description = "Kubernetes cluster endpoints for all cloud providers"
  value = {
    aws   = var.enable_aws ? aws_eks_cluster.main[0].endpoint : null
    azure = var.enable_azure ? azurerm_kubernetes_cluster.main[0].kube_config[0].host : null
    gcp   = var.enable_gcp ? google_container_cluster.main[0].endpoint : null
  }
}

output "cluster_ca_certificates" {
  description = "Base64 encoded CA certificates for all clusters"
  value = {
    aws   = var.enable_aws ? aws_eks_cluster.main[0].certificate_authority[0].data : null
    azure = var.enable_azure ? azurerm_kubernetes_cluster.main[0].kube_config[0].cluster_ca_certificate : null
    gcp   = var.enable_gcp ? google_container_cluster.main[0].master_auth[0].cluster_ca_certificate : null
  }
  sensitive = true
}

# Node Configuration Summary
output "node_configuration" {
  description = "Node configuration summary across all clusters"
  value = {
    scaling = {
      min_nodes     = var.min_nodes
      max_nodes     = var.max_nodes
      desired_nodes = var.desired_nodes
    }
    instance_types = {
      aws_node_type    = var.enable_aws ? local.current_instance_sizes.eks_node_type : null
      azure_node_size  = var.enable_azure ? local.current_instance_sizes.aks_node_size : null
      gcp_machine_type = var.enable_gcp ? local.current_instance_sizes.gke_machine_type : null
    }
    features = {
      cluster_autoscaler = var.enable_cluster_autoscaler
      network_policy     = var.enable_network_policy
      logging           = var.enable_logging
      monitoring        = var.enable_monitoring
    }
  }
}