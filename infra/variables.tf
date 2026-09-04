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

variable "manage_redshift_copy_role" {
  description = <<-EOT
    Create the Redshift COPY role on its own, without the EC2 pipeline role.

    Split out from manage_iam because the two roles have different answers. The EC2 role
    duplicates an instance profile that already exists in this shared account, so it stays
    off. The COPY role duplicates nothing: it is the missing half of the
    REDSHIFT_LOADS_VIA_INSERT gap, and creating it is entirely inside what Terraform owns
    here.

    Creating it is not enough to switch the loader to COPY. The role must then be ATTACHED to
    the cluster, which Terraform deliberately cannot do (invariant 10: Redshift is not ours to
    modify, and the IAM user cannot call redshift:DescribeClusters in any case). Attach it in
    the console, set AIMTERNET_REDSHIFT_COPY_IAM_ROLE to the ARN this outputs, and the loader
    probes for it and switches strategy on the next run. See docs/runbook.md.
  EOT
  type        = bool
  default     = false
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
