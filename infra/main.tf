terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  # Uncomment to enable remote state:
  # backend "s3" {
  #   bucket         = "your-terraform-state-bucket"
  #   key            = "auradata/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "terraform-locks"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "AuraData"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}
