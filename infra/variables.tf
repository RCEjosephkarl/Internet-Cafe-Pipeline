# Every name this configuration touches is an input. Nothing here hardcodes an account id, a
# bucket name, a table name or a host (spec §8). Supply them through terraform.tfvars — see
# terraform.tfvars.example — or the matching TF_VAR_* environment variables.

variable "aws_region" {
  description = "Region holding the bucket, the DynamoDB tables, RDS and Redshift."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment tag applied to every resource."
  type        = string
  default     = "poc"
}

variable "s3_bucket" {
  description = <<-EOT
    Name of the EXISTING data-lake bucket. This configuration manages the bucket's
    configuration only — versioning, public access, encryption and lifecycle. It never
    declares an aws_s3_bucket resource, so no plan it produces can delete the bucket or its
    4.5 GB of objects.
  EOT
  type        = string

  validation {
    condition     = length(var.s3_bucket) > 2
    error_message = "s3_bucket must be the real bucket name; there is no default on purpose."
  }
}

variable "s3_prefixes" {
  description = "Layer prefixes inside the bucket. Lifecycle rules are scoped to these, so a shared bucket's other tenants are untouched."
  type = object({
    bronze     = string
    silver     = string
    gold       = string
    quarantine = string
    manifests  = string
  })
  default = {
    bronze     = "bronze"
    silver     = "silver"
    gold       = "gold"
    quarantine = "quarantine"
    manifests  = "manifests"
  }
}

variable "ddb_events_table" {
  description = "DynamoDB table holding workstation lifecycle and alert events."
  type        = string
  default     = "aimternet_workstation_events"
}

variable "ddb_telemetry_table" {
  description = "DynamoDB table holding per-workstation telemetry samples."
  type        = string
  default     = "aimternet_workstation_telemetry"
}

variable "ddb_ttl_enabled" {
  description = <<-EOT
    Enable DynamoDB TTL on the telemetry table's `expires_at` attribute.

    DEFAULT false, and that is a deliberate finding, not an oversight (F2). The source sets
    `expires_at = timestamp + 7 days` and the simulated window (2026-07-01…08-31) is already
    in the past, so switching this on deletes roughly 61 of the 62 days within ~48 hours.
    The attribute is written verbatim either way; only expiry is off.
  EOT
  type        = bool
  default     = false
}

variable "bronze_telemetry_transition_days" {
  description = "Days before Bronze telemetry moves to a colder storage class. Telemetry is 4.2 GB of the 4.4 GB Bronze layer and is read once, by the Silver build."
  type        = number
  default     = 30
}

variable "quarantine_expiration_days" {
  description = "Days a quarantined record and its validation report are kept before deletion."
  type        = number
  default     = 90
}

variable "noncurrent_version_expiration_days" {
  description = "Versioning is on, so overwritten objects linger as noncurrent versions and keep costing money. This reaps them."
  type        = number
  default     = 30
}

variable "manage_iam" {
  description = <<-EOT
    Create the two least-privilege IAM roles from spec §8 (EC2 pipeline role, Redshift COPY
    role).

    DEFAULT false. The POC runs against a pre-existing shared account whose EC2 instance
    profile and Redshift cluster were provisioned by someone else; creating roles here would
    duplicate them, and attaching the COPY role would mean modifying a shared cluster. The
    policy documents are still rendered on every plan, so they can be reviewed — and adopted
    — without this flag.
  EOT
  type        = bool
  default     = false
}

variable "adopt_existing_dynamodb_tables" {
  description = <<-EOT
    Import the two already-created DynamoDB tables into state instead of planning to create
    them. `make bootstrap` creates the tables through boto3 (ensure_tables) so the pipeline
    can run without Terraform at all; this flag is how Terraform takes ownership afterwards
    without a create/destroy cycle. Turn it off only for a genuinely empty account.
  EOT
  type        = bool
  default     = true
}
