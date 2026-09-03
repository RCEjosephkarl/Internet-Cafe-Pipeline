# Terraform and provider pins. See infra/README.md for what this configuration does and,
# just as importantly, what it deliberately refuses to manage.

terraform {
  required_version = ">= 1.5.0" # `import` blocks need 1.5

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  # Credentials come from the environment or the EC2 instance role — never from this file
  # (spec §8). There is no `access_key`/`secret_key` here on purpose.

  default_tags {
    tags = {
      Project     = "AIMternet-Cafe"
      ManagedBy   = "terraform"
      Environment = var.environment
    }
  }
}
