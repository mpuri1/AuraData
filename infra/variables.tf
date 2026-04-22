variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "ecr_repo_name" {
  description = "Name of the ECR repository for the AuraData Docker image"
  type        = string
  default     = "auradata"
}

variable "claims_bucket_name" {
  description = "S3 bucket for claims data input and refined output"
  type        = string
  default     = "auradata-claims"
}

variable "image_retention_count" {
  description = "Number of Docker image versions to retain in ECR"
  type        = number
  default     = 10
}
