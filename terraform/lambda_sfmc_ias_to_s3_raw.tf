###########################################################
# AWS Lambda function: SFMC IAS to S3 Raw
###########################################################
module "lambda_sfmc_ias_to_s3_raw" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-sfmc-ias-data-to-s3"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  memory_size   = 2048
  layers        = [local.lambda_layer_aws_wrangler_arn]


  source_path = [
    "../src/lambdas/sfmc_ias_to_s3_raw"
  ]

  environment_variables = {
    S3_BUCKET = data.aws_ssm_parameter.s3_sfmc_data_collection_name.value,
    S3_RAW    = local.raw_datalake_bucket_name
  }

  hash_extra   = "${local.prefix}-sfmc-ias-data-to-s3"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"
}

resource "aws_cloudwatch_event_rule" "schedule_sfmc_ias_to_s3" {
  name                = module.lambda_sfmc_ias_to_s3_raw.lambda_function_name
  description         = "Schedule Lambda function execution from sfmc ias to S3"
  schedule_expression = "rate(12 hours)"
}

resource "aws_cloudwatch_event_target" "sfmc_ias_to_s3_lambdaexecution" {
  arn  = module.lambda_sfmc_ias_to_s3_raw.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_sfmc_ias_to_s3.name
}

###########################################################
# AWS Lambda Trigger
###########################################################
resource "aws_lambda_permission" "sfmc_ias_tos3_allow_cloudwatch_event_rule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_sfmc_ias_to_s3_raw.lambda_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_sfmc_ias_to_s3.arn
}
