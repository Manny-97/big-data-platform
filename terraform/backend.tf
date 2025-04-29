terraform {
  backend "s3" {
    bucket         = "emr-serverless-terraform-backend223"
    key            = "us-east-1/state-files/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    # kms_key_id     = "THE_ID_OF_THE_KMS_KEY"
    # dynamodb_table = "component_terraform_kys"
  }
}
