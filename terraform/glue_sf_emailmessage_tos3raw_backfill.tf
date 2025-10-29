###########################################################
# AWS Glue Job: Salesforce emailmessage to S3 Raw Back Fill
###########################################################
resource "aws_s3_object" "sf_emailmessage_tos3raw_backfill" {
  bucket = local.glue_assets_bucket_name
  key    = "${local.project_name}/scripts/sf_emailmessage_tos3raw_backfill.py"
  source = "../src/glue/sf_emailmessage_tos3raw_backfill.py"

  etag = filemd5("../src/glue/sf_emailmessage_tos3raw_backfill.py")
}

resource "aws_s3_object" "sf_emailmessage_tos3raw_backfill_data_catalog" {
  bucket = local.glue_assets_bucket_name
  key    = "${local.project_name}/scripts/sf_emailmessage_tos3raw_backfill/data_catalog.py"
  source = "../src/lambdas/salesforce_lambda_to_s3_raw/data_catalog.py"

  etag = filemd5("../src/lambdas/salesforce_lambda_to_s3_raw/data_catalog.py")
}

resource "aws_glue_job" "sf_emailmessage_tos3raw_backfill" {
  name        = "${local.prefix}-sf-emailmessage-tos3raw-backfill"
  description = "AWS Glue Job to transfer Salesforce emailmessage to S3 raw backfill"
  role_arn    = aws_iam_role.iam_for_glue.arn
  max_retries = "0"
  timeout     = var.sf_timeout_python_glue_job
  command {
    name            = "pythonshell"
    script_location = "s3://${local.glue_assets_bucket_name}/${aws_s3_object.sf_emailmessage_tos3raw_backfill.key}"
    python_version  = "3.9"
  }
  execution_property {
    max_concurrent_runs = 100
  }
  default_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${local.glue_assets_bucket_name}/temporary/"
    "--S3_RAW"                           = local.raw_datalake_bucket_name
    "--enable-auto-scaling"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-glue-datacatalog"          = "true"
    "--extra-py-files" = join(",", [
      "s3://${local.glue_assets_bucket_name}/${aws_s3_object.glue_api_client.key}",
      "s3://${local.glue_assets_bucket_name}/${aws_s3_object.glue_custom_functions.key}",
      "s3://${local.glue_assets_bucket_name}/${aws_s3_object.glue_salesforce_queries.key}",
      "s3://${local.glue_assets_bucket_name}/${aws_s3_object.sf_emailmessage_tos3raw_backfill_data_catalog.key}",
    ])
    "--additional-python-modules" = "flatten_json==0.1.14, awswrangler==3.9.0, requests==2.32.3"
    "--ENV"                       = var.bespoke_account
    "--TOKEN_URL"                 = var.sf_token_url
    "--SALESFORCE_AUTH_DETAILS"   = var.sf_auth_details
    "--SALESFORCE_API_VERSION"    = var.sf_api_version
  }
  max_capacity = 1
}
