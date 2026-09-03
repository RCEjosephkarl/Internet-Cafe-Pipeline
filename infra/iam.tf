# ---------------------------------------------------------------------------------------
# Least-privilege IAM (spec §8). No cost — IAM roles and policies are free.
#
# The policy DOCUMENTS are always rendered, so `terraform plan` and `terraform console` show
# exactly what the pipeline is allowed to do. The ROLES are gated behind var.manage_iam,
# which defaults to false, because this POC runs in a shared account whose EC2 instance
# profile already exists and whose Redshift cluster belongs to someone else. Creating a
# second role would be noise; attaching one to a shared cluster would be a change to
# infrastructure this configuration has no business touching.
#
# Set manage_iam = true in a clean account to have Terraform own them.
# ---------------------------------------------------------------------------------------

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  bucket_arn = "arn:${data.aws_partition.current.partition}:s3:::${var.s3_bucket}"

  # Only the prefixes this project owns, so a shared bucket stays shared.
  managed_prefixes = [
    var.s3_prefixes.bronze,
    var.s3_prefixes.silver,
    var.s3_prefixes.gold,
    var.s3_prefixes.quarantine,
    var.s3_prefixes.manifests,
  ]

  object_arns = [for p in local.managed_prefixes : "${local.bucket_arn}/${p}/*"]

  table_arns = [
    aws_dynamodb_table.events.arn,
    "${aws_dynamodb_table.events.arn}/index/*",
    aws_dynamodb_table.telemetry.arn,
  ]
}

# --- EC2 pipeline role: what Airflow, the loaders and the APIs actually need --------------

data "aws_iam_policy_document" "ec2_pipeline" {
  statement {
    sid       = "ListOnlyManagedPrefixes"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:ListBucketVersions", "s3:GetBucketLocation"]
    resources = [local.bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = concat(local.managed_prefixes, [for p in local.managed_prefixes : "${p}/*"])
    }
  }

  statement {
    sid       = "ReadWriteManagedObjects"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:AbortMultipartUpload"]
    resources = local.object_arns
  }

  # Deliberately absent: s3:DeleteObject, s3:DeleteBucket*, s3:PutBucketPolicy. The pipeline
  # only ever adds objects. Bronze in particular is append-only by design (§3).

  statement {
    sid    = "DynamoDBDataPlane"
    effect = "Allow"
    actions = [
      "dynamodb:PutItem",
      "dynamodb:BatchWriteItem",
      "dynamodb:GetItem",
      "dynamodb:BatchGetItem",
      "dynamodb:Query",
      "dynamodb:DescribeTable",
      "dynamodb:DescribeTimeToLive",
    ]
    resources = local.table_arns
  }

  # Scan is not granted: nothing in the operational path scans these tables, and a scan over
  # 6.3M telemetry items is a bill, not a query.

  statement {
    sid       = "CreateTablesOnFirstBootstrap"
    effect    = "Allow"
    actions   = ["dynamodb:CreateTable", "dynamodb:UpdateTimeToLive"]
    resources = local.table_arns
  }
}

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ec2_pipeline" {
  count = var.manage_iam ? 1 : 0

  name               = "aimternet-ec2-pipeline"
  description        = "Airflow, loaders and the APIs on the EC2 host"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

resource "aws_iam_role_policy" "ec2_pipeline" {
  count = var.manage_iam ? 1 : 0

  name   = "aimternet-ec2-pipeline"
  role   = aws_iam_role.ec2_pipeline[0].id
  policy = data.aws_iam_policy_document.ec2_pipeline.json
}

resource "aws_iam_instance_profile" "ec2_pipeline" {
  count = var.manage_iam ? 1 : 0

  name = "aimternet-ec2-pipeline"
  role = aws_iam_role.ec2_pipeline[0].name
}

# --- Redshift COPY role: read Gold, nothing else -----------------------------------------
#
# The cluster this POC uses has no default IAM role, which is why the Redshift loader falls
# back to batched INSERT (documented in poc_policy as REDSHIFT_LOADS_VIA_INSERT). This role
# is what would make COPY work: create it, attach it to the cluster, and set
# AIMTERNET_REDSHIFT_COPY_IAM_ROLE to its ARN — the loader probes for that at run time and
# switches strategy on its own.

data "aws_iam_policy_document" "redshift_copy" {
  statement {
    sid       = "ReadGoldObjects"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${local.bucket_arn}/${var.s3_prefixes.gold}/*"]
  }

  statement {
    sid       = "ListGoldPrefix"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [local.bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.s3_prefixes.gold}/*"]
    }
  }
}

data "aws_iam_policy_document" "redshift_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }

    # Confused-deputy guard: only this account's Redshift may assume it.
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "redshift_copy" {
  count = var.manage_iam || var.manage_redshift_copy_role ? 1 : 0

  name               = "aimternet-redshift-copy"
  description        = "Lets Redshift COPY read the Gold prefix"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume.json
}

resource "aws_iam_role_policy" "redshift_copy" {
  count = var.manage_iam || var.manage_redshift_copy_role ? 1 : 0

  name   = "aimternet-redshift-copy"
  role   = aws_iam_role.redshift_copy[0].id
  policy = data.aws_iam_policy_document.redshift_copy.json
}
