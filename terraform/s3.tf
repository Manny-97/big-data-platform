resource "aws_s3_bucket" "company_emr_bucket" {
  bucket = "company-pyspark-scripts-223"
}

resource "aws_s3_bucket" "source_bucket" {
  bucket = "source-bucket-telecos-223"
}

resource "aws_s3_bucket" "cleaned_bucket" {
  bucket = "cleaned-bucket-telecos-223"
}

resource "aws_s3_object" "upload_file" {
  bucket = "company-pyspark-scripts-223"
  key    = "uploads/scripts/spark2.py"
  source = "../spark_code/spark2.py"

  # The filemd5() function is available in Terraform 0.11.12 and later
  # For Terraform 0.11.11 and earlier, use the md5() function and the file() function:
  # etag = "${md5(file("path/to/file"))}"
  etag = filemd5("../spark_code/spark2.py")
  depends_on = [aws_s3_bucket.company_emr_bucket]
}
