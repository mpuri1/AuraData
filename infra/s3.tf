# ---------------------------------------------------------------------------
# S3 — Claims Data Storage
# Replaces local claims_data.csv and refined_claims.db with cloud-native storage.
# Input prefix:   s3://nexusdata-claims-{env}/input/
# Refined prefix: s3://nexusdata-claims-{env}/refined/
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "claims" {
  bucket = "${var.claims_bucket_name}-${var.environment}"
}

resource "aws_s3_bucket_versioning" "claims" {
  bucket = aws_s3_bucket.claims.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "claims" {
  bucket = aws_s3_bucket.claims.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "claims" {
  bucket = aws_s3_bucket.claims.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Retention: raw claims input kept 90 days; refined (audited) records kept 7 years
# for regulatory compliance with insurance industry data retention requirements.
resource "aws_s3_bucket_lifecycle_configuration" "claims" {
  bucket = aws_s3_bucket.claims.id

  rule {
    id     = "raw-input-retention"
    status = "Enabled"

    filter { prefix = "input/" }

    expiration { days = 90 }
  }

  rule {
    id     = "refined-long-term"
    status = "Enabled"

    filter { prefix = "refined/" }

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 365
      storage_class = "GLACIER"
    }
    # No expiration — insurance refined records kept indefinitely
  }
}
