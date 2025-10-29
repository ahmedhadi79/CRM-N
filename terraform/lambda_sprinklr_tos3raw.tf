######################################################################
# AWS Lambda function: Sprinklr to S3 Raw
######################################################################
module "lambda_sprinklr_tos3raw" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-sprinklr-tos3raw"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  memory_size   = 1024
  layers        = [local.lambda_layer_aws_wrangler_arn]

  source_path = [
    "../src/common/custom_functions.py",
    "../src/common/api_client.py",
    {
      path             = "../src/lambdas/sprinklr_tos3raw"
      pip_requirements = true,
      patterns         = ["!README.md"]
    }
  ]

  environment_variables = {
    ENV       = var.bespoke_account,
    S3_RAW    = local.raw_datalake_bucket_name,
    AUTH_PATH = "sls/data/sprinklr",
  }

  hash_extra   = "${local.prefix}-sprinklr-tos3raw"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda_sprinklr.arn
  tracing_mode = "Active"
}

###########################################################
# AWS Event Bridge Rule
###########################################################
resource "aws_cloudwatch_event_rule" "schedule_sprinklr_tos3raw" {
  name                = module.lambda_sprinklr_tos3raw.lambda_function_name
  description         = "Schedule Lambda function execution from sprinklr to S3 Raw"
  schedule_expression = "cron(0 05 * * ? *)"
  state               = var.lambda_sprinklr_cron_enable
}

resource "aws_cloudwatch_event_target" "sprinklr_tos3raw_lambdaexecution" {
  arn  = module.lambda_sprinklr_tos3raw.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_sprinklr_tos3raw.name
}

###########################################################
# AWS Lambda Trigger
###########################################################
resource "aws_lambda_permission" "sprinklr_tos3raw_allow_cloudwatch_event_rule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_sprinklr_tos3raw.lambda_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_sprinklr_tos3raw.arn
}
