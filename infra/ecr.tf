# ---------------------------------------------------------------------------
# ECR — Container Registry for NexusData Docker Image
# Stores the Docker image built from NexusData's Dockerfile.
# The image is tagged and pushed here for deployment to ECS/App Runner/EC2.
#
# Build and push:
#   docker build -t nexusdata .
#   docker tag nexusdata:latest <ecr_repo_url>:latest
#   docker push <ecr_repo_url>:latest
# ---------------------------------------------------------------------------

resource "aws_ecr_repository" "nexusdata" {
  name                 = "${var.ecr_repo_name}-${var.environment}"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true # Automated vulnerability scanning on every push
  }

  encryption_configuration {
    encryption_type = "AES256"
  }
}

# Lifecycle policy: retain only the last N tagged images to control storage costs
resource "aws_ecr_lifecycle_policy" "nexusdata" {
  repository = aws_ecr_repository.nexusdata.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last ${var.image_retention_count} tagged images"
        selection = {
          tagStatus   = "tagged"
          tagPrefixList = ["v", "latest", "prod"]
          countType   = "imageCountMoreThan"
          countNumber = var.image_retention_count
        }
        action = { type = "expire" }
      },
      {
        rulePriority = 2
        description  = "Expire untagged images after 7 days"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 7
        }
        action = { type = "expire" }
      }
    ]
  })
}
