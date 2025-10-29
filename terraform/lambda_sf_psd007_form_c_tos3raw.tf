###########################################################
# AWS Lambda function: psd007 to S3 Raw
###########################################################
module "lambda_sf_psd007_form_c_tos3raw" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-sf-psd007-form-c-tos3raw"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  memory_size   = 2048
  layers        = [local.lambda_layer_aws_wrangler_arn]


  source_path = [
    "../src/lambdas/sf_psd007_form_tos3raw"
  ]

  environment_variables = {
    S3_RAW = local.raw_datalake_bucket_name,
  }

  hash_extra   = "${local.prefix}-sf-psd007-form-c-tos3raw"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"
}

resource "aws_cloudwatch_event_rule" "schedule_sf_psd007_form_c_tos3raw" {
  name                = module.lambda_sf_psd007_form_c_tos3raw.lambda_function_name
  description         = "Schedule Lambda function execution from sfmc to S3"
  schedule_expression = "cron(45 00 * * ? *)"
  state               = "DISABLED"
}

resource "aws_cloudwatch_event_target" "sf_psd007_form_c_tos3raw_lambdaexecution" {
  arn  = module.lambda_sf_psd007_form_c_tos3raw.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_sf_psd007_form_c_tos3raw.name
}

###########################################################
# AWS Lambda Trigger
###########################################################
resource "aws_lambda_permission" "sf_psd007_form_c_tos3raw_allow_cloudwatch_event_rule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_sf_psd007_form_c_tos3raw.lambda_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_sf_psd007_form_c_tos3raw.arn
}
