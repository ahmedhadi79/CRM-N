###########################################################
# AWS Lambda function: marketing cloud Journeys to S3 Raw
###########################################################
module "lambda_mc_journeys_to_s3_raw" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-mc-journeys-to-s3-raw"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  memory_size   = 2048
  layers        = [local.lambda_layer_aws_wrangler_arn]

  source_path = [
    "../src/common/custom_functions.py",
    "../src/common/api_client.py",
    {
      path             = "../src/lambdas/mc_journeys_to_s3_raw"
      pip_requirements = true,
      patterns         = ["!README.md"]
    }
  ]

  environment_variables = {
    ENV       = var.bespoke_account,
    AUTH_PATH = var.mc_auth_path,
    LOGIN_URL = var.mc_login_url,
  }

  hash_extra   = "${local.prefix}-mc-journeys-to-s3-raw"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"
}

###########################################################
# AWS Event Bridge Rule
###########################################################
resource "aws_cloudwatch_event_rule" "schedule_mc_journeys_to_s3_raw" {
  name                = module.lambda_mc_journeys_to_s3_raw.lambda_function_name
  description         = "Schedule Lambda function execution from mc journeys to S3"
  schedule_expression = "cron(45 20 * * ? *)"
  state               = "DISABLED" // Obsolete
}

resource "aws_cloudwatch_event_target" "mc_journeys_to_s3_raw_lambdaexecution" {
  arn  = module.lambda_mc_journeys_to_s3_raw.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_mc_journeys_to_s3_raw.name
}

###########################################################
# AWS Lambda Trigger
###########################################################
resource "aws_lambda_permission" "mc_journeys_to_s3_raw_allow_cloudwatch_event_rule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_mc_journeys_to_s3_raw.lambda_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_mc_journeys_to_s3_raw.arn
}
