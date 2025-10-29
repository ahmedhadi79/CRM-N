###########################################################
# AWS Glue Job: CSV TO PARQUET
###########################################################
resource "aws_s3_object" "csv_to_parquet" {
  bucket = local.glue_assets_bucket_name
  key    = "${local.project_name}/scripts/glue_csv_to_parquet.py"
  source = "../src/glue/csv_to_parquet/main.py"

  etag = filemd5("../src/glue/csv_to_parquet/main.py")
}

resource "aws_glue_job" "csv_to_parquet" {
  name        = "${local.prefix}-csv-to-parquet"
  description = "AWS Glue Job to convert csv to parquet"
  role_arn    = aws_iam_role.iam_for_glue.arn
  max_retries = "0"
  timeout     = var.sf_timeout_python_glue_job
  command {
    name            = "pythonshell"
    script_location = "s3://${local.glue_assets_bucket_name}/${aws_s3_object.csv_to_parquet.key}"
    python_version  = "3.9"
  }
  execution_property {
    max_concurrent_runs = 100
  }
  default_arguments = {
    "--BUCKET_NAME"     = local.raw_datalake_bucket_name,
    "--TABLE_NAME"      = "table_name",
    "--ARCHIVE_ENABLED" = "true",
  }
  max_capacity = 1
}
