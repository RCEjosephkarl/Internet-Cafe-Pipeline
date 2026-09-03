# ---------------------------------------------------------------------------------------
# DynamoDB — the two tables from docs/dynamodb.md, which is the design of record.
#
# `make bootstrap` creates these through boto3 (`ensure_tables`) so the pipeline never
# depends on Terraform being installed. The import blocks below let Terraform adopt what is
# already there instead of planning a create; the key schemas here and the ones in
# src/aimternet/pipeline/loaders/dynamodb.py::_table_definitions must stay identical, and
# tests/unit/test_infra_terraform.py asserts that they do.
#
# COST — on-demand (PAY_PER_REQUEST), us-east-1.
#   Storage $0.25/GB-month:  telemetry 2.90 GB + events 29.2 MB = 2.93 GB -> ~$0.73/month.
#   Writes $1.25/million WRU, already incurred and one-off:
#     telemetry  6,300,000 items x 1 WRU (~460 B, no GSI)      = 6.30M
#     events        58,311 items x 3 WRU (~500 B, two GSIs)    = 0.17M
#                                                     total    ~ $8.09
#   Reads are trivial: the dashboard's telemetry panel is a handful of Query calls.
#   On-demand is the right mode here — the load is one enormous burst then near-idle, which
#   is exactly the shape provisioned capacity bills you for and never uses.
# ---------------------------------------------------------------------------------------

resource "aws_dynamodb_table" "events" {
  name         = var.ddb_events_table
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "PK"
  range_key    = "SK"

  # PK  WS#<workstation_id>
  # SK  EVT#<event_timestamp_utc>#<event_id>   — UTC ISO-8601 sorts lexicographically
  attribute {
    name = "PK"
    type = "S"
  }
  attribute {
    name = "SK"
    type = "S"
  }
  attribute {
    name = "GSI1PK"
    type = "S"
  }
  attribute {
    name = "GSI1SK"
    type = "S"
  }
  attribute {
    name = "GSI2PK"
    type = "S"
  }
  attribute {
    name = "GSI2SK"
    type = "S"
  }

  # Both ends of one rental: GSI1PK = SESSION#<session_id>
  global_secondary_index {
    name            = "GSI1"
    hash_key        = "GSI1PK"
    range_key       = "GSI1SK"
    projection_type = "ALL"
  }

  # Alert triage without a scan: GSI2PK = TYPE#<event_type>
  global_secondary_index {
    name            = "GSI2"
    hash_key        = "GSI2PK"
    range_key       = "GSI2SK"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = false # POC; the source of record is S3 Bronze, which is versioned
  }

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    Name    = var.ddb_events_table
    Dataset = "workstation_events"
  }
}

resource "aws_dynamodb_table" "telemetry" {
  name         = var.ddb_telemetry_table
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "PK"
  range_key    = "SK"

  # PK  WS#<workstation_id>   — 175 partitions, which is what stops a 6.3M-item load
  # SK  TS#<timestamp_utc>      hot-spotting on one key
  attribute {
    name = "PK"
    type = "S"
  }
  attribute {
    name = "SK"
    type = "S"
  }

  # Written verbatim from the source; expiry left off by default. See var.ddb_ttl_enabled
  # and finding F2 — the data's expires_at values are already in the past.
  ttl {
    attribute_name = "expires_at"
    enabled        = var.ddb_ttl_enabled
  }

  point_in_time_recovery {
    enabled = false
  }

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    Name    = var.ddb_telemetry_table
    Dataset = "workstation_telemetry"
  }
}

# Adopt the tables the bootstrap already created rather than planning a create-then-conflict.
import {
  for_each = var.adopt_existing_dynamodb_tables ? toset(["events"]) : toset([])
  to       = aws_dynamodb_table.events
  id       = var.ddb_events_table
}

import {
  for_each = var.adopt_existing_dynamodb_tables ? toset(["telemetry"]) : toset([])
  to       = aws_dynamodb_table.telemetry
  id       = var.ddb_telemetry_table
}
