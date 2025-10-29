locals {
  project_name                  = "crm-data-integration"
  prefix                        = "datalake-${local.project_name}"
  lambda_layer_aws_wrangler_arn = "arn:aws:lambda:${var.region}:336392948345:layer:AWSSDKPandas-Python312:8"
  raw_datalake_bucket_name      = "bb2-${var.bespoke_account}-datalake-raw"
  curated_datalake_bucket_name  = "bb2-${var.bespoke_account}-datalake-curated"
  recon_datalake_bucket_name    = "bb2-${var.bespoke_account}-datalake-reconciliation"
  athena_results_bucket_name    = "bb2-${var.bespoke_account}-datalake-athena-results"
  glue_assets_bucket_name       = "aws-glue-assets-${var.aws_account_id}-${var.region}"
}
