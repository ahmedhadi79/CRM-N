######################################################################
# AWS Lambda function: Nice Skills Summary Report to S3 Raw
######################################################################
module "lambda_nice_skillssummary_tos3raw" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-nice-skillssummary-tos3raw"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  memory_size   = 1024
  layers        = [local.lambda_layer_aws_wrangler_arn]

  source_path = [
    "../src/common/custom_functions.py",
    "../src/common/api_client.py",
    {
      path             = "../src/lambdas/nice_skillssummary_tos3raw"
      pip_requirements = true,
      patterns         = ["!README.md"]
    }
  ]

  environment_variables = {
    ENV    = var.bespoke_account,
    S3_RAW = local.raw_datalake_bucket_name,
  }

  hash_extra   = "${local.prefix}-nice-skillssummary-tos3raw"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"
}

###########################################################
# AWS Event Bridge Rule
###########################################################
resource "aws_cloudwatch_event_rule" "schedule_nice_skillssummary_tos3raw" {
  name                = module.lambda_nice_skillssummary_tos3raw.lambda_function_name
  description         = "Schedule Lambda function execution from Nice Skills Summary to S3 Raw"
  schedule_expression = "cron(0 * * * ? *)"
}

resource "aws_cloudwatch_event_target" "nice_skillssummary_tos3raw_lambdaexecution" {
  arn  = module.lambda_nice_skillssummary_tos3raw.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_nice_skillssummary_tos3raw.name
}

###########################################################
# AWS Lambda Trigger
###########################################################
resource "aws_lambda_permission" "nice_skillssummary_tos3raw_allow_cloudwatch_event_rule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_nice_skillssummary_tos3raw.lambda_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_nice_skillssummary_tos3raw.arn
}
