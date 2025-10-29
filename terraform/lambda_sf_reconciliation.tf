###########################################################
# AWS Lambda function: Salesforce Recon
###########################################################
module "lambda_sf_reconciliation" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-sf-reconciliation"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  memory_size   = 10240
  layers        = [local.lambda_layer_aws_wrangler_arn]

  source_path = [
    "../src/lambdas/sf_datalake_reconciliation"
  ]

  environment_variables = {
    S3_RAW                  = local.raw_datalake_bucket_name,
    S3_RECON                = local.recon_datalake_bucket_name,
    SALESFORCE_AUTH_DETAILS = var.sf_auth_details,
    TOKEN_URL               = var.sf_token_url,
    SALESFORCE_API_VERSION  = var.sf_api_version,
  }

  hash_extra   = "${local.prefix}-sf-datalake-reconciliation"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"
}

# ###########################################################
# # AWS Event Bridge Rule
# ###########################################################
# resource "aws_cloudwatch_event_rule" "schedule_sf_datalake_reconciliation" {
#   name                = module.lambda_sf_reconciliation.lambda_function_name
#   description         = "Schedule Lambda function execution for Salesforce and Datalake Reconciliation"
#   schedule_expression = "cron(0 01 * * ? *)"
# }

# resource "aws_cloudwatch_event_target" "sf_datalake_recon_lambdaexecution" {
#   arn  = module.lambda_sf_reconciliation.lambda_function_arn
#   rule = aws_cloudwatch_event_rule.schedule_sf_datalake_reconciliation.name
# }

# ###########################################################
# # AWS Lambda Trigger
# ###########################################################
# resource "aws_lambda_permission" "sf_datalake_recon_allow_cloudwatch_event_rule" {
#   statement_id  = "AllowExecutionFromCloudWatch"
#   action        = "lambda:InvokeFunction"
#   function_name = module.lambda_sf_reconciliation.lambda_function_name
#   principal     = "events.amazonaws.com"
#   source_arn    = aws_cloudwatch_event_rule.schedule_sf_datalake_reconciliation.arn
# }
