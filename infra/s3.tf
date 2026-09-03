# ---------------------------------------------------------------------------------------
# S3 data lake — CONFIGURATION ONLY
#
# There is deliberately no `resource "aws_s3_bucket"` in this file. The bucket predates the
# POC and holds 4.5 GB across bronze/, silver/, gold/, quarantine/ and manifests/. Declaring
# it as a resource would put "destroy this bucket" one `terraform destroy` away, and the
# operating contract forbids that outcome existing at all. Managing the sub-resources gives
# Terraform everything it needs to enforce the spec §8 posture and nothing it needs to lose
# the data.
#
# COST — S3 Standard, us-east-1, $0.023/GB-month.
#   Measured 2026-09-03: bronze 4.41 GB, silver 88.3 MB, gold 10.2 MB, quarantine 314 KB,
#   manifests 1.05 MB  =  4.51 GB  ->  ~$0.10/month.
#   Request charges are one-off and rounding error: ~1,900 PUTs is under $0.01.
#   Versioning is on, so noncurrent versions accrue too; the lifecycle rule below reaps them.
# ---------------------------------------------------------------------------------------

data "aws_s3_bucket" "lake" {
  bucket = var.s3_bucket
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = data.aws_s3_bucket.lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Spec §8: public access blocked. The live bucket has all four settings OFF; this is the one
# change in the whole configuration that alters existing behaviour rather than recording it.
resource "aws_s3_bucket_public_access_block" "lake" {
  bucket = data.aws_s3_bucket.lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = data.aws_s3_bucket.lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256" # SSE-S3; no KMS key, so no $1/month key charge
    }
    bucket_key_enabled = true
  }
}

# Every rule is scoped to an AIMternet prefix. Nothing here can reach another tenant's data
# if this bucket is ever shared.
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = data.aws_s3_bucket.lake.id

  # Bronze telemetry: 4.2 GB of the 4.4 GB Bronze layer, written once and read once by the
  # Silver build. Standard-IA at $0.0125/GB-month roughly halves its storage cost.
  rule {
    id     = "bronze-telemetry-to-ia"
    status = "Enabled"

    filter {
      prefix = "${var.s3_prefixes.bronze}/workstation_telemetry/"
    }

    transition {
      days          = var.bronze_telemetry_transition_days
      storage_class = "STANDARD_IA"
    }
  }

  # Quarantine holds rejected records and validation reports — evidence, not an archive.
  rule {
    id     = "quarantine-expiry"
    status = "Enabled"

    filter {
      prefix = "${var.s3_prefixes.quarantine}/"
    }

    expiration {
      days = var.quarantine_expiration_days
    }
  }

  # Silver and Gold are rebuilt on every curate run, so overwritten versions pile up.
  dynamic "rule" {
    for_each = toset([var.s3_prefixes.silver, var.s3_prefixes.gold])

    content {
      id     = "reap-noncurrent-${rule.value}"
      status = "Enabled"

      filter {
        prefix = "${rule.value}/"
      }

      noncurrent_version_expiration {
        noncurrent_days = var.noncurrent_version_expiration_days
      }
    }
  }

  # Incomplete multipart uploads are invisible in the console and billed like storage.
  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {} # whole bucket: an aborted upload has no useful prefix

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}
