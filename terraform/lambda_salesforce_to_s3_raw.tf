###########################################################
# AWS Lambda function: Salesforce to S3 Raw
###########################################################
module "lambda_salesforce_to_s3_raw" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-salesforce-to-s3-raw"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = "900"
  memory_size   = var.lambda_sf_memory_size
  layers        = [local.lambda_layer_aws_wrangler_arn]

  source_path = [
    {
      path             = "../src/lambdas/salesforce_lambda_to_s3_raw"
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

  hash_extra   = "${local.prefix}-salesforce-to-s3-raw"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"
}

# ###########################################################
# # AWS Event Bridge Rules
# ###########################################################

resource "aws_cloudwatch_event_rule" "schedule_salesforce_events" {
  for_each            = var.salesforce_events
  name                = "datalake_raw_${each.key}_trigger"
  description         = "Schedule Lambda function execution for ${each.key}"
  schedule_expression = each.value.schedule
  state               = each.value.state
}

resource "aws_cloudwatch_event_target" "salesforce_cloudwatch_events" {
  for_each = var.salesforce_events

  arn  = module.lambda_salesforce_to_s3_raw.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_salesforce_events[each.key].name

  input = jsonencode({
    table_name = each.key
    cdc_field  = each.value.cdc_field
  })
}

resource "aws_lambda_permission" "allow_cloudwatch_event_rule_api_client" {
  for_each = var.salesforce_events

  statement_id  = "AllowExecutionFromCloudWatch_${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_salesforce_to_s3_raw.lambda_function_arn
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_salesforce_events[each.key].arn
}
