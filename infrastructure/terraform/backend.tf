terraform {
  backend "s3" {
    bucket         = "vehicle-analytics-tf-state"
    key            = "terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "vehicle-analytics-tf-lock"
  }
}