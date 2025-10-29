###########################################################
# AWS Lambda function: salesforce outbound call mi activity
###########################################################
module "lambda_sf_outboundcall_mi_report_to_s3_curated" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-sf-outboundcall-rprt-to-s3-curated"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  memory_size   = 10240
  layers        = [local.lambda_layer_aws_wrangler_arn]

  source_path = [
    "../src/lambdas/sf_outboundcall_mi_report"
  ]

  environment_variables = {
    S3_CURATED = local.curated_datalake_bucket_name
    IS_SANDBOX = true
  }

  hash_extra   = "${local.prefix}-sf-outboundcall-rprt-to-s3-curated"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"
}

###########################################################
# AWS Event Bridge Rule
###########################################################
resource "aws_cloudwatch_event_rule" "schedule_sf_outboundcall_report" {
  name                = module.lambda_sf_outboundcall_mi_report_to_s3_curated.lambda_function_name
  description         = "Schedule Lambda function execution for outbound call mi"
  schedule_expression = "cron(0 04 * * ? *)"
}

resource "aws_cloudwatch_event_target" "sf_outboundcall_lambdaexecution" {
  arn  = module.lambda_sf_outboundcall_mi_report_to_s3_curated.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_sf_outboundcall_report.name
}

###########################################################
# AWS Lambda Trigger
###########################################################
resource "aws_lambda_permission" "sf_outboundcall_report_allow_cloudwatch_event_rule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_sf_outboundcall_mi_report_to_s3_curated.lambda_function_arn
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_sf_outboundcall_report.arn
}
