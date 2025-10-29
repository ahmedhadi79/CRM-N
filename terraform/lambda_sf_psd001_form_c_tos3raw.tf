###########################################################
# AWS Lambda function: Salesforce psd001 Form C to S3 Raw
###########################################################
module "lambda_sf_psd001_form_c_tos3raw" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-sf-psd001-form-c-tos3raw"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = "900"
  memory_size   = var.lambda_sf_memory_size
  layers        = [local.lambda_layer_aws_wrangler_arn]

  source_path = [
    "../src/common/custom_functions.py",
    "../src/common/salesforce_queries.py",
    "../src/common/api_client.py",
    {
      path             = "../src/lambdas/sf_psd001_form_tos3raw"
      pip_requirements = true,
      patterns         = ["!README.md"]
    }
  ]

  environment_variables = {
    ENV                     = var.bespoke_account,
    S3_RAW                  = local.raw_datalake_bucket_name,
    SALESFORCE_AUTH_DETAILS = var.sf_auth_details,
    TOKEN_URL               = var.sf_token_url,
    SALESFORCE_API_VERSION  = var.sf_api_version,
  }

  hash_extra   = "${local.prefix}-sf-psd001-form-c-tos3raw"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"
}

# ###########################################################
# # AWS Event Bridge Rule
# ###########################################################
# resource "aws_cloudwatch_event_rule" "schedule_sf_psd001_form_c_tos3raw" {
#   name                = module.lambda_sf_psd001_form_c_tos3raw.lambda_function_name
#   description         = "Schedule Lambda function execution from Salesforce psd001 Form C to S3"
#   schedule_expression = "cron(30 00 * * ? *)"
#   state               = "DISABLED"
# }

# resource "aws_cloudwatch_event_target" "sf_psd001_form_c_tos3raw_lambdaexecution" {
#   arn  = module.lambda_sf_psd001_form_c_tos3raw.lambda_function_arn
#   rule = aws_cloudwatch_event_rule.schedule_sf_psd001_form_c_tos3raw.name
# }

# ###########################################################
# # AWS Lambda Trigger
# ###########################################################
# resource "aws_lambda_permission" "sf_psd001_form_c_tos3raw_allow_cloudwatch_event_rule" {
#   statement_id  = "AllowExecutionFromCloudWatch"
#   action        = "lambda:InvokeFunction"
#   function_name = module.lambda_sf_psd001_form_c_tos3raw.lambda_function_name
#   principal     = "events.amazonaws.com"
#   source_arn    = aws_cloudwatch_event_rule.schedule_sf_psd001_form_c_tos3raw.arn
# }
