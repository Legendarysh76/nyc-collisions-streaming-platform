variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project name"
  type        = string
  default     = "nyc-collisions"
}

variable "env" {
  description = "Environment name"
  type        = string
  default     = "dev"
}
