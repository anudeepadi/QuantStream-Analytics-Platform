# Compute Module for QuantStream Analytics Platform
# Provides multi-cloud Kubernetes clusters and auto-scaling infrastructure

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
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
  }
}

# Local values for compute configuration
locals {
  # Environment-based instance sizing
  instance_sizes = {
    dev = {
      eks_node_type    = var.kubernetes_node_type != "" ? var.kubernetes_node_type : "t3.medium"
      aks_node_size    = "Standard_B2ms"
      gke_machine_type = "e2-standard-2"
    }
    staging = {
      eks_node_type    = var.kubernetes_node_type != "" ? var.kubernetes_node_type : "t3.large"
      aks_node_size    = "Standard_D2s_v3"
      gke_machine_type = "e2-standard-4"
    }
    prod = {
      eks_node_type    = var.kubernetes_node_type != "" ? var.kubernetes_node_type : "m5.xlarge"
      aks_node_size    = "Standard_D4s_v3"
      gke_machine_type = "n2-standard-4"
    }
  }
  
  current_instance_sizes = local.instance_sizes[var.environment]
  
  # Common labels for all Kubernetes resources
  common_labels = {
    "app.kubernetes.io/managed-by" = "terraform"
    "platform"                     = "quantstream"
    "environment"                  = var.environment
  }
}

#######################
# AWS EKS Cluster
#######################

# EKS Cluster IAM Role
resource "aws_iam_role" "eks_cluster" {
  count = var.enable_aws ? 1 : 0
  
  name = "${var.resource_prefix}-eks-cluster-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "eks.amazonaws.com"
        }
      }
    ]
  })
  
  tags = var.common_tags
}

# EKS Cluster IAM Role Policy Attachments
resource "aws_iam_role_policy_attachment" "eks_cluster_policy" {
  count = var.enable_aws ? 1 : 0
  
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
  role       = aws_iam_role.eks_cluster[0].name
}

resource "aws_iam_role_policy_attachment" "eks_vpc_resource_controller" {
  count = var.enable_aws ? 1 : 0
  
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSVPCResourceController"
  role       = aws_iam_role.eks_cluster[0].name
}

# EKS Cluster
resource "aws_eks_cluster" "main" {
  count = var.enable_aws ? 1 : 0
  
  name     = "${var.resource_prefix}-eks"
  role_arn = aws_iam_role.eks_cluster[0].arn
  version  = var.kubernetes_version
  
  vpc_config {
    subnet_ids              = concat(var.aws_subnet_ids, var.aws_public_subnet_ids)
    endpoint_private_access = true
    endpoint_public_access  = var.environment == "prod" ? false : true
    public_access_cidrs    = var.environment == "prod" ? [] : ["0.0.0.0/0"]
    security_group_ids     = var.aws_security_group_ids
  }
  
  enabled_cluster_log_types = ["api", "audit", "authenticator", "controllerManager", "scheduler"]
  
  encryption_config {
    provider {
      key_arn = var.kms_key_arn_aws
    }
    resources = ["secrets"]
  }
  
  depends_on = [
    aws_iam_role_policy_attachment.eks_cluster_policy,
    aws_iam_role_policy_attachment.eks_vpc_resource_controller
  ]
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-eks"
    Type = "kubernetes-cluster"
    Cloud = "aws"
  })
}

# EKS Node Group IAM Role
resource "aws_iam_role" "eks_node_group" {
  count = var.enable_aws ? 1 : 0
  
  name = "${var.resource_prefix}-eks-node-group-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
  
  tags = var.common_tags
}

# EKS Node Group IAM Role Policy Attachments
resource "aws_iam_role_policy_attachment" "eks_worker_node_policy" {
  count = var.enable_aws ? 1 : 0
  
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
  role       = aws_iam_role.eks_node_group[0].name
}

resource "aws_iam_role_policy_attachment" "eks_cni_policy" {
  count = var.enable_aws ? 1 : 0
  
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
  role       = aws_iam_role.eks_node_group[0].name
}

resource "aws_iam_role_policy_attachment" "eks_container_registry_read_only" {
  count = var.enable_aws ? 1 : 0
  
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
  role       = aws_iam_role.eks_node_group[0].name
}

# EKS Node Group
resource "aws_eks_node_group" "main" {
  count = var.enable_aws ? 1 : 0
  
  cluster_name    = aws_eks_cluster.main[0].name
  node_group_name = "${var.resource_prefix}-node-group"
  node_role_arn   = aws_iam_role.eks_node_group[0].arn
  subnet_ids      = var.aws_subnet_ids
  instance_types  = [local.current_instance_sizes.eks_node_type]
  
  scaling_config {
    desired_size = var.desired_nodes
    max_size     = var.max_nodes
    min_size     = var.min_nodes
  }
  
  update_config {
    max_unavailable_percentage = 25
  }
  
  remote_access {
    ec2_ssh_key = var.ssh_key_name
    source_security_group_ids = var.aws_security_group_ids
  }
  
  labels = local.common_labels
  
  depends_on = [
    aws_iam_role_policy_attachment.eks_worker_node_policy,
    aws_iam_role_policy_attachment.eks_cni_policy,
    aws_iam_role_policy_attachment.eks_container_registry_read_only
  ]
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-node-group"
    Type = "kubernetes-node-group"
    Cloud = "aws"
  })
}

# EKS Addons
resource "aws_eks_addon" "vpc_cni" {
  count = var.enable_aws ? 1 : 0
  
  cluster_name = aws_eks_cluster.main[0].name
  addon_name   = "vpc-cni"
  resolve_conflicts = "OVERWRITE"
  
  tags = var.common_tags
}

resource "aws_eks_addon" "kube_proxy" {
  count = var.enable_aws ? 1 : 0
  
  cluster_name = aws_eks_cluster.main[0].name
  addon_name   = "kube-proxy"
  resolve_conflicts = "OVERWRITE"
  
  tags = var.common_tags
}

resource "aws_eks_addon" "coredns" {
  count = var.enable_aws ? 1 : 0
  
  cluster_name = aws_eks_cluster.main[0].name
  addon_name   = "coredns"
  resolve_conflicts = "OVERWRITE"
  
  tags = var.common_tags
}

#######################
# Azure AKS Cluster
#######################

# Azure Kubernetes Service
resource "azurerm_kubernetes_cluster" "main" {
  count = var.enable_azure ? 1 : 0
  
  name                = "${var.resource_prefix}-aks"
  location            = var.azure_location
  resource_group_name = var.azure_resource_group_name
  dns_prefix          = "${var.resource_prefix}-aks"
  kubernetes_version  = var.kubernetes_version
  
  private_cluster_enabled = var.environment == "prod" ? true : false
  
  default_node_pool {
    name                = "default"
    node_count          = var.desired_nodes
    vm_size             = local.current_instance_sizes.aks_node_size
    vnet_subnet_id      = var.azure_subnet_ids[0]
    enable_auto_scaling = true
    min_count          = var.min_nodes
    max_count          = var.max_nodes
    
    upgrade_settings {
      max_surge = "10%"
    }
    
    tags = var.common_tags
  }
  
  identity {
    type = "SystemAssigned"
  }
  
  network_profile {
    network_plugin    = "azure"
    load_balancer_sku = "standard"
    outbound_type     = "loadBalancer"
  }
  
  monitor_metrics {
    annotations_allowed = null
    labels_allowed      = null
  }
  
  oms_agent {
    log_analytics_workspace_id = var.log_analytics_workspace_id
  }
  
  key_vault_secrets_provider {
    secret_rotation_enabled = true
  }
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-aks"
    Type = "kubernetes-cluster"
    Cloud = "azure"
  })
}

# AKS Additional Node Pool for Data Workloads
resource "azurerm_kubernetes_cluster_node_pool" "data_pool" {
  count = var.enable_azure && var.enable_data_node_pool ? 1 : 0
  
  name                  = "datapool"
  kubernetes_cluster_id = azurerm_kubernetes_cluster.main[0].id
  vm_size               = "Standard_D4s_v3"
  node_count            = 2
  vnet_subnet_id        = var.azure_subnet_ids[0]
  
  enable_auto_scaling = true
  min_count          = 1
  max_count          = 5
  
  node_taints = ["workload=data:NoSchedule"]
  
  node_labels = merge(local.common_labels, {
    "workload" = "data"
    "node-type" = "compute-optimized"
  })
  
  tags = var.common_tags
}

#######################
# GCP GKE Cluster
#######################

# GKE Cluster
resource "google_container_cluster" "main" {
  count = var.enable_gcp ? 1 : 0
  
  name     = "${var.resource_prefix}-gke"
  location = var.gcp_region
  
  # We can't create a cluster with no node pool defined, but we want to only use
  # separately managed node pools. So we create the smallest possible default
  # node pool and immediately delete it.
  remove_default_node_pool = true
  initial_node_count       = 1
  
  network    = var.gcp_vpc_name
  subnetwork = var.gcp_subnet_names[0]
  
  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = var.environment == "prod" ? true : false
    master_ipv4_cidr_block  = "172.16.0.0/28"
  }
  
  ip_allocation_policy {
    cluster_secondary_range_name  = "pod-ranges"
    services_secondary_range_name = "services-range"
  }
  
  # Enable network policy for security
  network_policy {
    enabled = true
  }
  
  # Enable Workload Identity
  workload_identity_config {
    workload_pool = "${var.gcp_project_id}.svc.id.goog"
  }
  
  # Enable logging and monitoring
  logging_service    = "logging.googleapis.com/kubernetes"
  monitoring_service = "monitoring.googleapis.com/kubernetes"
  
  master_auth {
    client_certificate_config {
      issue_client_certificate = false
    }
  }
  
  cluster_autoscaling {
    enabled = true
    resource_limits {
      resource_type = "cpu"
      minimum       = var.min_nodes * 2
      maximum       = var.max_nodes * 4
    }
    resource_limits {
      resource_type = "memory"
      minimum       = var.min_nodes * 4
      maximum       = var.max_nodes * 16
    }
  }
  
  maintenance_policy {
    recurring_window {
      start_time = "2023-01-01T00:00:00Z"
      end_time   = "2023-01-01T04:00:00Z"
      recurrence = "FREQ=WEEKLY;BYDAY=SA"
    }
  }
}

# GKE Node Pool
resource "google_container_node_pool" "main" {
  count = var.enable_gcp ? 1 : 0
  
  name       = "${var.resource_prefix}-node-pool"
  location   = var.gcp_region
  cluster    = google_container_cluster.main[0].name
  node_count = var.desired_nodes
  
  autoscaling {
    min_node_count = var.min_nodes
    max_node_count = var.max_nodes
  }
  
  management {
    auto_repair  = true
    auto_upgrade = true
  }
  
  node_config {
    preemptible     = var.environment != "prod"
    machine_type    = local.current_instance_sizes.gke_machine_type
    service_account = google_service_account.gke_nodes[0].email
    
    oauth_scopes = [
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring",
      "https://www.googleapis.com/auth/devstorage.read_only"
    ]
    
    labels = local.common_labels
    
    tags = ["gke-node", "${var.resource_prefix}-gke"]
    
    metadata = {
      disable-legacy-endpoints = "true"
    }
  }
  
  upgrade_settings {
    max_surge       = 1
    max_unavailable = 0
  }
}

# GKE Node Service Account
resource "google_service_account" "gke_nodes" {
  count = var.enable_gcp ? 1 : 0
  
  account_id   = "${var.resource_prefix}-gke-nodes"
  display_name = "GKE Nodes Service Account"
}

# GKE Node Service Account IAM bindings
resource "google_project_iam_member" "gke_nodes" {
  count = var.enable_gcp ? length(local.gke_node_permissions) : 0
  
  project = var.gcp_project_id
  role    = local.gke_node_permissions[count.index]
  member  = "serviceAccount:${google_service_account.gke_nodes[0].email}"
}

locals {
  gke_node_permissions = var.enable_gcp ? [
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/monitoring.viewer",
    "roles/stackdriver.resourceMetadata.writer"
  ] : []
}