# Terraform State Backend Configuration
# This file configures remote state storage for different cloud providers

# AWS S3 Backend Configuration (Default)
terraform {
  backend "s3" {
    # Configuration will be provided via backend config file or CLI
    # bucket         = "quantstream-terraform-state"
    # key            = "infrastructure/terraform.tfstate"
    # region         = "us-west-2"
    # encrypt        = true
    # dynamodb_table = "terraform-lock"
  }
}

# Alternative Azure Backend Configuration
# terraform {
#   backend "azurerm" {
#     resource_group_name  = "rg-quantstream-terraform"
#     storage_account_name = "quantstreamterraformstate"
#     container_name       = "tfstate"
#     key                  = "infrastructure.tfstate"
#   }
# }

# Alternative GCS Backend Configuration
# terraform {
#   backend "gcs" {
#     bucket = "quantstream-terraform-state"
#     prefix = "infrastructure/state"
#   }
# }