###########################################################
# AWS Lambda function: ga_data_view_3 to S3 Raw
###########################################################
module "lambda_ga_data_view_3_to_s3_raw" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-ga-data-view-3-to-s3-raw"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  memory_size   = 2048
  layers        = [local.lambda_layer_aws_wrangler_arn]

  source_path = [
    "${path.module}/../src/common/custom_functions.py",
    "${path.module}/../src/common/api_client.py",
    {
      path = "${path.module}/../src/lambdas/ga_data_view_factory_to_s3_raw",
      commands = [
        ":zip",
        "cd `mktemp -d`",
        "python3.12 -m pip install --target=. -r ${abspath(path.module)}/../src/lambdas/ga_data_view_factory_to_s3_raw/requirements.txt --implementation cp --platform manylinux2014_x86_64 --only-binary=:all:",
        ":zip .",
      ],
      patterns = ["!README.md"],
    },
  ]

  environment_variables = {
    ENV                 = var.bespoke_account,
    GA_VIEW_ID          = "3",
    GA_APP_ID           = "278396662",
    REPORT_DIMENSIONS   = "['date', 'country', 'firstUserCustomChannelGroup:9877601687']",
    REPORT_METRICS      = "['totalUsers', 'activeUsers', 'newUsers', 'Sessions', 'engagedSessions', 'engagementRate', 'userEngagementDuration', 'eventCount', 'keyEvents', 'userKeyEventRate']",
    AUTH_PATH           = "sls/data/google-analytics-service-account",
    WRANGLER_WRITE_MODE = "overwrite_partitions",
    START_DATE          = "None", //in format %Y-%m-%d
    END_DATE            = "None", //in format %Y-%m-%d
  }

  hash_extra   = "${local.prefix}-ga-data-view-3-to-s3-raw"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"
}

###########################################################
# AWS Event Bridge Rule
###########################################################

resource "aws_cloudwatch_event_rule" "schedule_ga_data_view_3_to_s3_raw" {
  name                = module.lambda_ga_data_view_3_to_s3_raw.lambda_function_name
  description         = "Schedule Lambda function execution from GA4 Reporting API to S3"
  schedule_expression = "cron(00 07 * * ? *)"
  state               = var.lambda_ga_view_3_cron_enable
}

resource "aws_cloudwatch_event_target" "ga_data_view_3_to_s3_raw_lambdaexecution" {
  arn  = module.lambda_ga_data_view_3_to_s3_raw.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_ga_data_view_3_to_s3_raw.name
}

###########################################################
# AWS Lambda Event Bridge Permission
###########################################################
resource "aws_lambda_permission" "ga_data_view_3_to_s3_raw_allow_cloudwatch_event_rule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_ga_data_view_3_to_s3_raw.lambda_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_ga_data_view_3_to_s3_raw.arn
}
