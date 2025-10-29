###########################################################
# AWS Lambda function: property valuation s3 to salesforce
###########################################################
module "lambda_property_valuation_s3_to_sf" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.11.0"

  function_name = "${local.prefix}-property-valuation-s3-to-sf"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = "900"
  memory_size   = var.lambda_sf_memory_size
  layers        = [local.lambda_layer_aws_wrangler_arn]

  source_path = [
    "../src/lambdas/property_valuation_s3_to_sf",
  ]

  environment_variables = {
    ENV                     = var.bespoke_account,
    S3_CURATED              = local.curated_datalake_bucket_name
    SALESFORCE_AUTH_DETAILS = var.sf_auth_details,
    TOKEN_URL               = var.sf_token_url,
    SALESFORCE_INSTANCE_URL = var.sf_instance_url,
    SALESFORCE_API_VERSION  = var.sf_api_version,
  }

  hash_extra   = "${local.prefix}-property-valuation-s3-to-sf"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda_to_sf.arn
  tracing_mode = "Active"
}

# ###########################################################
# # AWS Event Bridge Rule
# ###########################################################
resource "aws_cloudwatch_event_rule" "schedule_property_valuation_s3_to_sf" {
  name                = module.lambda_property_valuation_s3_to_sf.lambda_function_name
  description         = "Schedule Lambda function execution from Prop valueation(s3) to Salesforce"
  schedule_expression = "cron(30 04 * * ? *)"
  state               = var.lambda_property_valuation_s3_to_sf_enable
}

resource "aws_cloudwatch_event_target" "prop_valuation_s3_to_sf_lambdaexecution" {
  arn  = module.lambda_property_valuation_s3_to_sf.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_property_valuation_s3_to_sf.name
}

# ###########################################################
# # AWS Lambda Trigger
# ###########################################################
resource "aws_lambda_permission" "prop_valuation_s3_to_sf_allow_cloudwatch_event_rule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_property_valuation_s3_to_sf.lambda_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_property_valuation_s3_to_sf.arn
}
