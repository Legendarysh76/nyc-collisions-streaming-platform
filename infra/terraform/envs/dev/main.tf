locals {
  name_prefix = "${var.project}-${var.env}"
  tags = {
    Project     = var.project
    Environment = var.env
    ManagedBy   = "terraform"
  }
}

# --- S3 buckets (system of record + curated + logs) ---
resource "aws_s3_bucket" "raw" {
  bucket = "${local.name_prefix}-raw"
  tags   = local.tags
}

resource "aws_s3_bucket" "curated" {
  bucket = "${local.name_prefix}-curated"
  tags   = local.tags
}

resource "aws_s3_bucket" "logs" {
  bucket = "${local.name_prefix}-logs"
  tags   = local.tags
}

# --- Kinesis data stream (streaming ingestion) ---
resource "aws_kinesis_stream" "collisions" {
  name             = "${local.name_prefix}-stream"
  shard_count      = 1
  retention_period = 24
  tags             = local.tags
}
