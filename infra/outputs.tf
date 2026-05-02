output "ecr_repository_url" {
  description = "ECR repository URL — use in docker push and container image config"
  value       = aws_ecr_repository.nexusdata.repository_url
}

output "claims_bucket_name" {
  description = "S3 bucket for claims input/refined data — set as AWS_S3_CLAIMS_BUCKET in .env"
  value       = aws_s3_bucket.claims.id
}

output "claims_bucket_arn" {
  description = "Full ARN of the claims S3 bucket"
  value       = aws_s3_bucket.claims.arn
}

output "runtime_role_arn" {
  description = "IAM role ARN for the NexusData runtime — attach to ECS task definition or EC2 instance profile"
  value       = aws_iam_role.nexusdata_runtime.arn
}
