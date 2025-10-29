###########################################################
# AWS Lambda function: Wallet Activation data csv reporting
###########################################################
module "lambda_mc_reporting_wallet_activation" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.9.0"

  function_name = "${local.prefix}-mc-reporting-wallet-activation"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"
  timeout       = 900
  memory_size   = 2048
  layers        = [local.lambda_layer_aws_wrangler_arn]


  source_path = [
    "../src/lambdas/mc_reporting_wallet_activation"
  ]

  environment_variables = {
    S3_BUCKET = data.aws_ssm_parameter.s3_sfmc_data_collection_name.value
  }

  hash_extra   = "${local.prefix}-mc-reporting-wallet-activation"
  create_role  = false
  lambda_role  = aws_iam_role.iam_for_lambda.arn
  tracing_mode = "Active"
}

resource "aws_cloudwatch_event_rule" "schedule_mc_reporting_wallet_activation" {
  name                = module.lambda_mc_reporting_wallet_activation.lambda_function_name
  description         = "Schedule Lambda function execution from datalake to sfmc s3 landing zone"
  schedule_expression = "cron(0 3,9,15,21 * * ? *)"
  state               = var.lambda_mc_reporting_ias_cron_enable
}

resource "aws_cloudwatch_event_target" "mc_reporting_wallet_activation_lambdaexecution" {
  arn  = module.lambda_mc_reporting_wallet_activation.lambda_function_arn
  rule = aws_cloudwatch_event_rule.schedule_mc_reporting_wallet_activation.name
}

###########################################################
# AWS Lambda Trigger
###########################################################
resource "aws_lambda_permission" "mc_reporting_wallet_activation_allow_cloudwatch_event_rule" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = module.lambda_mc_reporting_wallet_activation.lambda_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_mc_reporting_wallet_activation.arn
}
