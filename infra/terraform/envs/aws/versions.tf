terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Uncomment for shared state:
  # backend "s3" {
  #   bucket = "my-tfstate-bucket"
  #   key    = "rtdaas/aws/terraform.tfstate"
  #   region = "us-west-2"
  # }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project   = "rtdaas"
      ManagedBy = "terraform"
    }
  }
}
