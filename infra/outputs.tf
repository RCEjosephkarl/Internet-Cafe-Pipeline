output "s3_bucket" {
  description = "Bucket whose configuration this stack manages."
  value       = data.aws_s3_bucket.lake.id
}

output "s3_layer_uris" {
  description = "Where each layer lives, for .env and the runbook."
  value = {
    for layer, prefix in var.s3_prefixes : layer => "s3://${var.s3_bucket}/${prefix}/"
  }
}

output "dynamodb_tables" {
  description = "Table names and ARNs for the two event stores."
  value = {
    events = {
      name    = aws_dynamodb_table.events.name
      arn     = aws_dynamodb_table.events.arn
      indexes = ["GSI1", "GSI2"]
    }
    telemetry = {
      name = aws_dynamodb_table.telemetry.name
      arn  = aws_dynamodb_table.telemetry.arn
      ttl  = var.ddb_ttl_enabled ? "enabled on expires_at" : "disabled (finding F2)"
    }
  }
}

output "redshift_copy_role_arn" {
  description = "Set AIMTERNET_REDSHIFT_COPY_IAM_ROLE to this to switch the loader from batched INSERT to COPY."
  value = (
    var.manage_iam || var.manage_redshift_copy_role
    ? aws_iam_role.redshift_copy[0].arn
    : null
  )
}

output "estimated_monthly_cost_usd" {
  description = <<-EOT
    What this configuration is responsible for, per month. RDS and Redshift are NOT in this
    stack (see README) and are the real line items — Redshift especially.
  EOT
  value = {
    s3_standard_4_51_gb                      = "0.10"
    dynamodb_storage_2_93_gb                 = "0.73"
    iam                                      = "0.00"
    total_managed_here                       = "0.83"
    one_off_dynamodb_writes_already_incurred = "8.09"
  }
}
