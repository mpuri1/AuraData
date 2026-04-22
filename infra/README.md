# AuraData — AWS Infrastructure (Terraform)

This module provisions the production AWS infrastructure for the AuraData autonomous claims correction pipeline.

## What It Provisions

| Resource | Type | Purpose |
|---|---|---|
| `auradata-{env}` | ECR Repository | Docker image registry (built from `Dockerfile`) |
| ECR Lifecycle Policy | ECR | Retain last 10 tagged images; expire untagged after 7d |
| `auradata-claims-{env}` | S3 Bucket | Claims input + refined output storage |
| S3 Lifecycle Rules | S3 | Raw: 90d → Refined: STANDARD_IA → GLACIER (7yr insurance compliance) |
| `auradata-runtime-{env}` | IAM Role | Bedrock invoke + S3 claims + ECR pull permissions |

## Dual LLM Support

This Terraform module provisions the IAM role that enables `LLM_PROVIDER=bedrock` in `llm_provider.py`. The role grants `bedrock:InvokeModel` for:
- `anthropic.claude-3-haiku-20240307-v1:0` (default — cost-optimized)
- `anthropic.claude-3-5-sonnet-20240620-v1:0` (upgrade path)

## How to Use

```bash
cd infra/
terraform init
terraform plan -var="environment=dev"
terraform apply -var="environment=dev"
```

## Docker Push to ECR

```bash
# After terraform apply:
ECR_URL=$(terraform output -raw ecr_repository_url)
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $ECR_URL
docker build -t auradata:latest ..
docker tag auradata:latest $ECR_URL:latest
docker push $ECR_URL:latest
```

## Wiring to Application

After `terraform apply`, update `.env`:

```bash
AWS_S3_CLAIMS_BUCKET=$(terraform output -raw claims_bucket_name)
LLM_PROVIDER=bedrock   # enables Bedrock in llm_provider.py
```

## Variables

| Variable | Default | Description |
|---|---|---|
| `aws_region` | `us-east-1` | AWS region |
| `environment` | `dev` | dev / staging / prod |
| `ecr_repo_name` | `auradata` | ECR repository name prefix |
| `claims_bucket_name` | `auradata-claims` | S3 bucket name prefix |
| `image_retention_count` | `10` | ECR image versions to keep |
