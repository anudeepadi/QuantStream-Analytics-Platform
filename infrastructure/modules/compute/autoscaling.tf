# Auto-scaling Configuration for Multi-Cloud Kubernetes

#######################
# AWS Auto Scaling
#######################

# EKS Fargate Profile for serverless workloads
resource "aws_eks_fargate_profile" "data_workloads" {
  count = var.enable_aws && var.environment == "prod" ? 1 : 0
  
  cluster_name           = aws_eks_cluster.main[0].name
  fargate_profile_name   = "${var.resource_prefix}-fargate-data"
  pod_execution_role_arn = aws_iam_role.fargate_pod_execution_role[0].arn
  subnet_ids            = var.aws_subnet_ids
  
  selector {
    namespace = "data-processing"
    labels = {
      workload = "data-intensive"
    }
  }
  
  selector {
    namespace = "batch-jobs"
    labels = {
      compute-type = "batch"
    }
  }
  
  tags = merge(var.common_tags, {
    Name = "${var.resource_prefix}-fargate-profile"
    Type = "fargate-profile"
    Cloud = "aws"
  })
  
  depends_on = [aws_iam_role_policy_attachment.fargate_pod_execution_role_policy]
}

# Fargate Pod Execution Role
resource "aws_iam_role" "fargate_pod_execution_role" {
  count = var.enable_aws && var.environment == "prod" ? 1 : 0
  
  name = "${var.resource_prefix}-fargate-pod-execution-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "eks-fargate-pods.amazonaws.com"
        }
      }
    ]
  })
  
  tags = var.common_tags
}

resource "aws_iam_role_policy_attachment" "fargate_pod_execution_role_policy" {
  count = var.enable_aws && var.environment == "prod" ? 1 : 0
  
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSFargatePodExecutionRolePolicy"
  role       = aws_iam_role.fargate_pod_execution_role[0].name
}

# EKS Cluster Autoscaler IAM Role
resource "aws_iam_role" "cluster_autoscaler" {
  count = var.enable_aws && var.enable_cluster_autoscaler ? 1 : 0
  
  name = "${var.resource_prefix}-cluster-autoscaler-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Federated = "arn:aws:iam::${data.aws_caller_identity.current[0].account_id}:oidc-provider/${replace(aws_eks_cluster.main[0].identity[0].oidc[0].issuer, "https://", "")}"
        }
        Condition = {
          StringEquals = {
            "${replace(aws_eks_cluster.main[0].identity[0].oidc[0].issuer, "https://", "")}:sub" = "system:serviceaccount:kube-system:cluster-autoscaler"
            "${replace(aws_eks_cluster.main[0].identity[0].oidc[0].issuer, "https://", "")}:aud" = "sts.amazonaws.com"
          }
        }
      }
    ]
  })
  
  tags = var.common_tags
}

data "aws_caller_identity" "current" {
  count = var.enable_aws ? 1 : 0
}

# Cluster Autoscaler IAM Policy
resource "aws_iam_role_policy" "cluster_autoscaler" {
  count = var.enable_aws && var.enable_cluster_autoscaler ? 1 : 0
  
  name = "${var.resource_prefix}-cluster-autoscaler-policy"
  role = aws_iam_role.cluster_autoscaler[0].id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "autoscaling:DescribeAutoScalingGroups",
          "autoscaling:DescribeAutoScalingInstances",
          "autoscaling:DescribeLaunchConfigurations",
          "autoscaling:DescribeTags",
          "autoscaling:SetDesiredCapacity",
          "autoscaling:TerminateInstanceInAutoScalingGroup",
          "ec2:DescribeLaunchTemplateVersions",
          "eks:DescribeNodegroup"
        ]
        Resource = "*"
      }
    ]
  })
}

#######################
# Azure Auto Scaling
#######################

# Azure Monitor Autoscale Settings for AKS
resource "azurerm_monitor_autoscale_setting" "aks_autoscale" {
  count = var.enable_azure && var.enable_cluster_autoscaler ? 1 : 0
  
  name                = "${var.resource_prefix}-aks-autoscale"
  resource_group_name = var.azure_resource_group_name
  location            = var.azure_location
  target_resource_id  = azurerm_kubernetes_cluster.main[0].id
  
  profile {
    name = "default"
    
    capacity {
      default = var.desired_nodes
      minimum = var.min_nodes
      maximum = var.max_nodes
    }
    
    rule {
      metric_trigger {
        metric_name        = "Percentage CPU"
        metric_resource_id = azurerm_kubernetes_cluster.main[0].id
        time_grain         = "PT1M"
        statistic          = "Average"
        time_window        = "PT5M"
        time_aggregation   = "Average"
        operator           = "GreaterThan"
        threshold          = 75
      }
      
      scale_action {
        direction = "Increase"
        type      = "ChangeCount"
        value     = "1"
        cooldown  = "PT5M"
      }
    }
    
    rule {
      metric_trigger {
        metric_name        = "Percentage CPU"
        metric_resource_id = azurerm_kubernetes_cluster.main[0].id
        time_grain         = "PT1M"
        statistic          = "Average"
        time_window        = "PT5M"
        time_aggregation   = "Average"
        operator           = "LessThan"
        threshold          = 25
      }
      
      scale_action {
        direction = "Decrease"
        type      = "ChangeCount"
        value     = "1"
        cooldown  = "PT5M"
      }
    }
  }
  
  notification {
    email {
      send_to_subscription_administrator    = false
      send_to_subscription_co_administrator = false
      custom_emails                         = []
    }
  }
  
  tags = var.common_tags
}

#######################
# GCP Auto Scaling
#######################

# GCP Regional Persistent Disks for stateful workloads
resource "google_compute_disk" "ssd_persistent" {
  count = var.enable_gcp ? 3 : 0
  
  name = "${var.resource_prefix}-persistent-disk-${count.index}"
  type = "pd-ssd"
  zone = "${var.gcp_region}-${substr("abc", count.index, 1)}"
  size = var.environment == "prod" ? 100 : 50
  
  labels = merge(var.common_tags, {
    environment = var.environment
    usage       = "persistent-storage"
  })
}

# GCP Node Pool for High-Memory workloads
resource "google_container_node_pool" "high_memory" {
  count = var.enable_gcp && var.environment == "prod" ? 1 : 0
  
  name       = "${var.resource_prefix}-high-memory-pool"
  location   = var.gcp_region
  cluster    = google_container_cluster.main[0].name
  node_count = 1
  
  autoscaling {
    min_node_count = 0
    max_node_count = 5
  }
  
  management {
    auto_repair  = true
    auto_upgrade = true
  }
  
  node_config {
    preemptible  = false
    machine_type = "n2-highmem-4"
    
    oauth_scopes = [
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring",
      "https://www.googleapis.com/auth/devstorage.read_only"
    ]
    
    labels = merge(local.common_labels, {
      "workload-type" = "high-memory"
      "node-pool"     = "high-memory"
    })
    
    taint {
      key    = "workload"
      value  = "high-memory"
      effect = "NO_SCHEDULE"
    }
    
    metadata = {
      disable-legacy-endpoints = "true"
    }
  }
  
  upgrade_settings {
    max_surge       = 1
    max_unavailable = 0
  }
}

#######################
# Horizontal Pod Autoscaler (HPA) Configuration
#######################

# Create namespace for autoscaling components
resource "kubernetes_namespace" "autoscaling" {
  count = (var.enable_aws || var.enable_azure || var.enable_gcp) ? 1 : 0
  
  metadata {
    name = "autoscaling-system"
    
    labels = {
      "name"                     = "autoscaling-system"
      "app.kubernetes.io/name"   = "autoscaling-system"
      "app.kubernetes.io/managed-by" = "terraform"
    }
  }
  
  depends_on = [
    aws_eks_cluster.main,
    azurerm_kubernetes_cluster.main,
    google_container_cluster.main
  ]
}

# Vertical Pod Autoscaler (VPA) RBAC
resource "kubernetes_service_account" "vpa_recommender" {
  count = (var.enable_aws || var.enable_azure || var.enable_gcp) && var.enable_vertical_pod_autoscaler ? 1 : 0
  
  metadata {
    name      = "vpa-recommender"
    namespace = kubernetes_namespace.autoscaling[0].metadata[0].name
  }
  
  depends_on = [kubernetes_namespace.autoscaling]
}

resource "kubernetes_cluster_role" "vpa_recommender" {
  count = (var.enable_aws || var.enable_azure || var.enable_gcp) && var.enable_vertical_pod_autoscaler ? 1 : 0
  
  metadata {
    name = "vpa-recommender"
  }
  
  rule {
    api_groups = [""]
    resources  = ["pods", "nodes"]
    verbs      = ["get", "list", "watch"]
  }
  
  rule {
    api_groups = ["apps"]
    resources  = ["deployments", "replicasets"]
    verbs      = ["get", "list", "watch"]
  }
  
  rule {
    api_groups = ["metrics.k8s.io"]
    resources  = ["pods", "nodes"]
    verbs      = ["get", "list"]
  }
}

resource "kubernetes_cluster_role_binding" "vpa_recommender" {
  count = (var.enable_aws || var.enable_azure || var.enable_gcp) && var.enable_vertical_pod_autoscaler ? 1 : 0
  
  metadata {
    name = "vpa-recommender"
  }
  
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.vpa_recommender[0].metadata[0].name
  }
  
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.vpa_recommender[0].metadata[0].name
    namespace = kubernetes_namespace.autoscaling[0].metadata[0].name
  }
}