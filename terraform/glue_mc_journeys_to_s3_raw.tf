###########################################################
# AWS Glue Job: to daily request and paginate SFMC Journeys
###########################################################
resource "aws_s3_object" "mc_journeys_to_s3_raw" {
  bucket = local.glue_assets_bucket_name
  key    = "${local.project_name}/scripts/glue_mc_journeys_to_s3_raw.py"
  source = "../src/glue/mc_journeys_to_s3_raw/main.py"

  etag = filemd5("../src/glue/mc_journeys_to_s3_raw/main.py")
}

resource "aws_glue_job" "mc_journeys_to_s3_raw" {
  name        = "${local.prefix}-mc-journeys-to-s3-raw"
  description = "AWS Glue Job to daily request and paginate SFMC journeys"
  role_arn    = aws_iam_role.iam_for_glue.arn
  max_retries = "0"
  timeout     = 360
  command {
    name            = "pythonshell"
    script_location = "s3://${local.glue_assets_bucket_name}/${aws_s3_object.mc_journeys_to_s3_raw.key}"
    python_version  = "3.9"
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
    ])
    "--additional-python-modules" = "flatten_json==0.1.14"
    "--ENV"                       = var.bespoke_account
    "--AUTH_PATH"                 = var.mc_auth_path
    "--LOGIN_URL"                 = var.mc_login_url
  }
  max_capacity = 1
}

###########################################################
# AWS Glue Triggers - mc_journeys_to_s3_raw
###########################################################
resource "aws_glue_trigger" "mc_journeys_to_s3_raw_trigger" {
  name     = "mc_journeys_to_s3_raw_trigger"
  schedule = "cron(45 20 * * ? *)"
  type     = "SCHEDULED"
  enabled  = var.mc_journeys_to_s3_raw_trigger_enabled

  actions {
    job_name = aws_glue_job.mc_journeys_to_s3_raw.name
  }
}
