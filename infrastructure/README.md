# QuantStream Analytics Platform - Infrastructure as Code

This directory contains comprehensive Infrastructure as Code (IaC) using Terraform for the QuantStream Analytics Platform, providing scalable, secure, and cost-effective multi-cloud infrastructure for streaming data analytics workloads.

## 🏗️ Architecture Overview

The QuantStream Analytics Platform infrastructure is designed as a multi-cloud solution supporting AWS, Azure, and Google Cloud Platform. The infrastructure includes:

- **Compute**: Kubernetes clusters (EKS/AKS/GKE) with auto-scaling capabilities
- **Data**: PostgreSQL databases, Redis caching, Kafka messaging, and data lakes
- **Networking**: VPCs, subnets, security groups, and load balancers
- **Security**: IAM roles, secrets management, and encryption
- **Monitoring**: Prometheus, Grafana, and centralized logging
- **Databricks**: Workspace provisioning with Unity Catalog integration

## 📁 Infrastructure Structure

```
infrastructure/
├── environments/          # Environment-specific configurations
│   ├── dev/              # Development environment (cost-optimized)
│   ├── staging/          # Staging environment (production-like)
│   └── prod/             # Production environment (full features)
├── modules/              # Reusable Terraform modules
│   ├── networking/       # VPC, subnets, security groups
│   ├── compute/          # Kubernetes clusters, auto-scaling
│   ├── data/            # Databases, caching, messaging
│   ├── security/        # IAM, secrets, encryption
│   └── monitoring/      # Observability infrastructure
├── shared/              # Shared resources and configurations
│   ├── common-resources/ # Cross-environment resources
│   └── cross-cutting-concerns/
├── scripts/             # Deployment and maintenance scripts
│   ├── deployment/      # Automated deployment scripts
│   └── maintenance/     # Health checks and maintenance
└── config/              # Global Terraform configurations
    └── terraform/       # Provider and backend configs
```

## 🚀 Quick Start

### Prerequisites

1. **Tools Required:**
   - Terraform >= 1.5.0
   - AWS CLI (for AWS deployments)
   - Azure CLI (for Azure deployments) 
   - gcloud CLI (for GCP deployments)
   - kubectl (for Kubernetes management)

2. **Cloud Provider Setup:**
   - AWS: Configure credentials using `aws configure`
   - Azure: Login using `az login`
   - GCP: Authenticate using `gcloud auth login`

### Environment Deployment

#### Development Environment (AWS Only)

```bash
# Navigate to development environment
cd environments/dev

# Initialize Terraform
terraform init

# Plan deployment
terraform plan -var-file="terraform.tfvars"

# Apply deployment
terraform apply -var-file="terraform.tfvars"
```

#### Using Deployment Scripts

For automated deployments, use the provided deployment script:

```bash
# Deploy to development environment
./scripts/deployment/deploy.sh -e dev -c aws

# Deploy to production with all cloud providers
./scripts/deployment/deploy.sh -e prod -c all --auto-approve

# Perform dry-run to see planned changes
./scripts/deployment/deploy.sh -e staging --dry-run
```

## 🔧 Module Documentation

### Networking Module (`modules/networking/`)

Provides multi-cloud networking infrastructure:

**Features:**
- VPCs with public, private, and database subnets
- Security groups with least-privilege access
- NAT gateways for outbound connectivity
- Load balancers for high availability
- Cross-cloud networking patterns

**Key Resources:**
- AWS: VPC, Subnets, Security Groups, NAT Gateways
- Azure: VNet, Subnets, NSGs, Application Gateways
- GCP: VPC, Subnets, Firewall Rules, Load Balancers

### Compute Module (`modules/compute/`)

Manages Kubernetes clusters and compute resources:

**Features:**
- Multi-cloud Kubernetes clusters (EKS, AKS, GKE)
- Auto-scaling node groups
- Fargate/Container Instances for serverless workloads
- Cluster autoscaler configuration
- Horizontal and vertical pod autoscaling

**Key Resources:**
- AWS: EKS clusters, Node Groups, Fargate Profiles
- Azure: AKS clusters, Node Pools, Container Instances
- GCP: GKE clusters, Node Pools, Cloud Run

### Data Module (`modules/data/`)

Provides comprehensive data infrastructure:

**Features:**
- PostgreSQL databases with read replicas
- Redis clusters for high-performance caching
- Kafka clusters for stream processing
- Object storage for data lakes with lifecycle management
- Automated backup and disaster recovery

**Key Resources:**
- **Databases:** RDS PostgreSQL, Azure Database, Cloud SQL
- **Caching:** ElastiCache Redis, Azure Cache, Memorystore
- **Messaging:** MSK, Event Hubs, Pub/Sub
- **Storage:** S3, Azure Storage, Cloud Storage

### Security Module (`modules/security/`)

Implements enterprise-grade security:

**Features:**
- IAM roles and policies with least privilege
- Secrets management integration
- Encryption at rest and in transit
- Network security policies
- Compliance configurations (SOC2, PCI-DSS)

### Monitoring Module (`modules/monitoring/`)

Comprehensive observability infrastructure:

**Features:**
- Prometheus for metrics collection
- Grafana for visualization
- ElasticSearch and Kibana for logging
- AlertManager for notifications
- Custom dashboards and alerts

## 🌍 Environment Configuration

### Development Environment
- **Purpose:** Cost-optimized for development and testing
- **Instance Types:** t3.medium, db.t3.micro, cache.t3.micro
- **Features:** Single AZ, minimal retention, auto-shutdown enabled
- **Cloud Providers:** AWS only (default)

### Staging Environment  
- **Purpose:** Pre-production testing and validation
- **Instance Types:** t3.large, db.r5.large, cache.r6g.large
- **Features:** Multi-AZ, production-like sizing, enhanced monitoring
- **Cloud Providers:** AWS + Azure

### Production Environment
- **Purpose:** Live production workloads
- **Instance Types:** m5.xlarge, db.r5.2xlarge, cache.r6g.xlarge  
- **Features:** Full multi-AZ, enterprise sizing, comprehensive monitoring
- **Cloud Providers:** AWS + Azure + GCP

## 🔍 Health Checks and Monitoring

### Automated Health Checks

Use the health check script to verify infrastructure status:

```bash
# Check all infrastructure
./scripts/maintenance/health-check.sh -e prod

# Check specific cloud provider
./scripts/maintenance/health-check.sh -e staging -c aws

# Generate JSON report
./scripts/maintenance/health-check.sh -e prod -f json --save-report
```

### Monitoring Endpoints

After deployment, access monitoring through:
- **Grafana:** `https://grafana.<environment>.quantstream.com`
- **Prometheus:** `https://prometheus.<environment>.quantstream.com`  
- **Kibana:** `https://kibana.<environment>.quantstream.com`

## 🔐 Security Best Practices

### Network Security
- All databases in private subnets
- Security groups with minimal required access
- Network ACLs for additional protection
- VPN/Private Link for secure access

### Data Protection  
- Encryption at rest using customer-managed keys
- Encryption in transit with TLS 1.2+
- Regular security patching and updates
- Automated vulnerability scanning

### Access Management
- Role-based access control (RBAC)
- Multi-factor authentication required
- Regular access reviews and auditing
- Principle of least privilege

## 💰 Cost Optimization

### Development Environment
- T3.micro/B1ms instance types
- Single AZ deployment
- Minimal backup retention (7 days)
- Auto-shutdown capabilities

### Production Environment  
- Reserved instances for predictable workloads
- Spot instances for batch processing
- Intelligent storage tiering
- Auto-scaling to optimize utilization

### Cost Monitoring
- Budget alerts and notifications
- Resource tagging for cost allocation
- Regular cost optimization reviews
- Automated resource cleanup

## 🔄 Disaster Recovery

### Backup Strategy
- Automated daily database backups
- Cross-region backup replication
- Point-in-time recovery capability
- Regular backup testing and validation

### High Availability
- Multi-AZ deployment for critical components
- Load balancing across availability zones
- Automated failover capabilities  
- Health checks and auto-recovery

### Recovery Procedures
- **RTO:** < 1 hour for critical services
- **RPO:** < 15 minutes for data loss
- Documented recovery playbooks
- Regular disaster recovery testing

## 📊 Scaling and Performance

### Auto-Scaling Configuration
- Horizontal Pod Autoscaler (HPA) for applications
- Cluster Autoscaler for Kubernetes nodes  
- Database read replicas for read scaling
- Caching layers for performance optimization

### Performance Targets
- **Database:** < 100ms query response time
- **Cache:** < 5ms response time
- **API:** < 200ms response time
- **Auto-scaling:** < 2 minutes scale-out time

## 🛠️ Deployment Scripts

### Main Deployment Script (`scripts/deployment/deploy.sh`)

Comprehensive deployment automation with features:
- Multi-environment support (dev, staging, prod)
- Multi-cloud deployment (AWS, Azure, GCP, all)
- Terraform state management
- Plan validation and dry-run mode
- Auto-approve for CI/CD integration
- Infrastructure destroy capabilities

**Usage Examples:**
```bash
# Deploy development environment
./deploy.sh -e dev -c aws

# Deploy all clouds with auto-approve  
./deploy.sh -e prod -c all --auto-approve

# Validate configuration only
./deploy.sh -e staging --validate-only

# Destroy infrastructure (careful!)
./deploy.sh -e dev --destroy
```

### Health Check Script (`scripts/maintenance/health-check.sh`)

Automated infrastructure health validation:
- Cross-cloud connectivity checks
- Service availability verification
- Performance threshold monitoring
- JSON/table output formats
- Report generation and alerting

**Usage Examples:**
```bash
# Basic health check
./health-check.sh -e prod

# Detailed report with JSON output
./health-check.sh -e prod -f json --save-report

# Check only AWS infrastructure
./health-check.sh -e staging -c aws
```

## 🔧 Advanced Configuration

### Backend Configuration

Create environment-specific backend configurations:

**backend-dev.hcl:**
```hcl
bucket = "quantstream-terraform-state-dev"
key    = "infrastructure/dev/terraform.tfstate"
region = "us-west-2"
encrypt = true
dynamodb_table = "terraform-lock-dev"
```

### Multi-Cloud Variables

**terraform.tfvars for production:**
```hcl
# Project Configuration
project_name = "quantstream-analytics"

# Multi-Cloud Configuration
enable_aws   = true
enable_azure = true  
enable_gcp   = true

# Cloud-Specific Settings
aws_region      = "us-west-2"
azure_location  = "West US 2"
gcp_region      = "us-west2"

# Environment Tags
common_tags = {
  Environment = "prod"
  Platform    = "QuantStream"
  CostCenter  = "platform-engineering"
}
```

## 📚 Troubleshooting

### Common Issues

1. **Terraform State Lock:**
   ```bash
   terraform force-unlock <lock-id>
   ```

2. **Provider Authentication:**
   ```bash
   # AWS
   aws configure
   
   # Azure  
   az login
   
   # GCP
   gcloud auth login
   ```

3. **Resource Conflicts:**
   ```bash
   terraform import <resource> <id>
   ```

### Debug Mode

Enable debug logging for detailed troubleshooting:
```bash
export TF_LOG=DEBUG
terraform apply
```

## 🔄 Maintenance

### Regular Maintenance Tasks
- Security patches and updates
- Kubernetes version upgrades  
- Database maintenance windows
- Certificate rotation
- Cost optimization reviews

### Monitoring and Alerting
- 24/7 infrastructure monitoring
- Automated alert escalation
- Performance threshold monitoring
- Capacity planning alerts

## 📖 Additional Documentation

- **Module Documentation:** See individual module README files
- **Security Guidelines:** [Security Best Practices](./docs/security.md)
- **Troubleshooting:** [Common Issues](./docs/troubleshooting.md)
- **Change Management:** [Deployment Procedures](./docs/deployment.md)

## 🏷️ Versioning

This infrastructure uses semantic versioning:
- **Major:** Breaking changes to infrastructure
- **Minor:** New features and capabilities  
- **Patch:** Bug fixes and improvements

## ⚠️ Important Notes

- Always run `terraform plan` before `terraform apply`
- Use the provided scripts for consistent deployments
- Test infrastructure changes in development first
- Follow security guidelines for production deployments
- Monitor costs regularly, especially in development environments

---

For questions or support regarding infrastructure, please contact the Platform Engineering team or open an issue in the repository.