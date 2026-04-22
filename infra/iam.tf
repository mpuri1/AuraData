# ---------------------------------------------------------------------------
# IAM — AuraData Runtime Role
# Grants the application (container on ECS/App Runner) permission to:
#   1. Call Amazon Bedrock (for LLM_PROVIDER=bedrock in llm_provider.py)
#   2. Read/write the claims S3 bucket
#   3. Pull images from ECR
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "auradata_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com", "ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "auradata_runtime" {
  name               = "auradata-runtime-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.auradata_assume_role.json
  description        = "Runtime role for AuraData — grants Bedrock LLM + S3 claims + ECR access"
}

# Bedrock access: allows ChatBedrock in llm_provider.py to invoke Anthropic Claude
data "aws_iam_policy_document" "bedrock_invoke" {
  statement {
    effect  = "Allow"
    actions = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"]
    # Anthropic Claude 3 Haiku (default) and Sonnet (upgrade path)
    resources = [
      "arn:aws:bedrock:*::foundation-model/anthropic.claude-3-haiku-20240307-v1:0",
      "arn:aws:bedrock:*::foundation-model/anthropic.claude-3-5-sonnet-20240620-v1:0",
    ]
  }
}

resource "aws_iam_policy" "bedrock_invoke" {
  name        = "auradata-bedrock-invoke-${var.environment}"
  description = "Allows AuraData to call Bedrock LLM models (Claude Haiku/Sonnet)"
  policy      = data.aws_iam_policy_document.bedrock_invoke.json
}

resource "aws_iam_role_policy_attachment" "auradata_bedrock" {
  role       = aws_iam_role.auradata_runtime.name
  policy_arn = aws_iam_policy.bedrock_invoke.arn
}

# S3 access: scoped to the claims bucket only
data "aws_iam_policy_document" "s3_claims" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.claims.arn,
      "${aws_s3_bucket.claims.arn}/*",
    ]
  }
}

resource "aws_iam_policy" "s3_claims" {
  name        = "auradata-s3-claims-${var.environment}"
  description = "Least-privilege S3 access for AuraData to read/write claims data"
  policy      = data.aws_iam_policy_document.s3_claims.json
}

resource "aws_iam_role_policy_attachment" "auradata_s3" {
  role       = aws_iam_role.auradata_runtime.name
  policy_arn = aws_iam_policy.s3_claims.arn
}

# ECR read access: allows the container runtime to pull images
resource "aws_iam_role_policy_attachment" "auradata_ecr" {
  role       = aws_iam_role.auradata_runtime.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}
