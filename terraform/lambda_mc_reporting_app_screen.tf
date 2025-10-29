###########################################################
# AWS Lambda function: App Screen data csv reporting
###########################################################
module "lambda_mc_reporting_app_screen" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-mc-reporting-app-screen"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  memory_size   = 2048
  layers        = [local.lambda_layer_aws_wrangler_arn]


  source_path = [
    "../src/lambdas/mc_reporting_app_screen"
  ]

  environment_variables = {
    S3_BUCKET = data.aws_ssm_parameter.s3_sfmc_data_collection_name.value
  }

  hash_extra   = "${local.prefix}-mc-reporting-app-screen"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"
}

resource "aws_cloudwatch_event_rule" "schedule_mc_reporting_app_screen" {
  name                = module.lambda_mc_reporting_app_screen.lambda_function_name
  description         = "Schedule Lambda function execution from datalake to sfmc s3 landing zone"
  schedule_expression = "cron(0 3,9,15,21 * * ? *)"
  state               = var.lambda_mc_reporting_ias_cron_enable
}

resource "aws_cloudwatch_event_target" "mc_reporting_app_screen_lambdaexecution" {
  arn  = module.lambda_mc_reporting_app_screen.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_mc_reporting_app_screen.name
}

###########################################################
# AWS Lambda Trigger
###########################################################
resource "aws_lambda_permission" "mc_reporting_app_screen_allow_cloudwatch_event_rule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_mc_reporting_app_screen.lambda_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_mc_reporting_app_screen.arn
}
