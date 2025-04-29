resource "aws_s3_bucket" "company_emr_bucket" {
  bucket = "company-pyspark-scripts223"
}

resource "aws_s3_bucket" "source_bucket" {
  bucket = "source-bucket-telecos-223"
}

resource "aws_s3_bucket" "cleaned_bucket" {
  bucket = "cleaned-bucket-telecos-223"
}
