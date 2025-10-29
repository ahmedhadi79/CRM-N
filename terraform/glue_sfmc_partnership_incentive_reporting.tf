###########################################################
# AWS Glue Job: sfmc partnership incentive reporting
###########################################################
resource "aws_s3_object" "sfmc_partnership_incentive_reporting" {
  bucket = local.glue_assets_bucket_name
  key    = "${local.project_name}/scripts/sfmc_partnership_incentive_reporting.py"
  source = "../src/glue/sfmc_partnership_incentive_reporting.py"

  etag = filemd5("../src/glue/sfmc_partnership_incentive_reporting.py")
}

resource "aws_cloudwatch_log_group" "sfmc_partnership_incentive_reportin" {
  name              = "/aws-glue/jobs/datalake-${var.bespoke_account}-sfmc-partnership-incentive-reporting"
  retention_in_days = 14
}


resource "aws_glue_job" "sfmc_partnership_incentive_reporting" {
  name              = "${local.prefix}-sfmc-partnership-incentive-reporting"
  description       = "AWS Glue Job sfmc partnership incentive reporting"
  role_arn          = aws_iam_role.iam_for_glue_sfmc.arn
  glue_version      = "4.0"
  number_of_workers = 10
  worker_type       = "G.1X"
  max_retries       = "0"
  timeout           = var.sf_timeout_python_glue_job
  command {
    name            = "glueetl"
    script_location = "s3://${local.glue_assets_bucket_name}/${aws_s3_object.sfmc_partnership_incentive_reporting.key}"
    python_version  = 3
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${local.glue_assets_bucket_name}/temporary/"
    "--S3_RAW"                           = local.raw_datalake_bucket_name
    "--s3bucket"                         = data.aws_ssm_parameter.s3_sfmc_data_collection_name.value
    "--enable-auto-scaling"              = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.sfmc_partnership_incentive_reportin.name
    "--cloudwatch-log-stream-prefix"     = "sfmc_partnership_incentive_reportin"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--enable-glue-datacatalog"          = "true"
    "--spark-event-logs-path"            = "s3://${local.glue_assets_bucket_name}/sparkHistoryLogs/"
    "--TempDir"                          = "s3://${local.glue_assets_bucket_name}/temporary/"
    "--ENV"                              = var.bespoke_account

  }
}


###########################################################
# AWS Glue Triggers - sfmc partnership incentive reporting
###########################################################
resource "aws_glue_trigger" "sfmc_partnership_incentive_reporting_trigger" {
  name     = "sfmc_partnership_incentive_reporting_trigger"
  schedule = "cron(00 02 * * ? *)"
  type     = "SCHEDULED"
  enabled  = "true"

  actions {
    job_name = aws_glue_job.sfmc_partnership_incentive_reporting.name
  }
}
