variable "bespoke_account" {
  description = "bespoke account to deploy (sandbox, nfrt, alpha, beta, production)"
  type        = string
}

variable "resource_management_iam_role" {
  description = "Name of the role TF uses to manage resources in AWS accounts."
  type        = string
}

variable "external_id" {
  description = "External identifier to use when assuming the role."
  type        = string
}

variable "aws_account_id" {
  type        = string
  description = "AWS Account ID which may be operated on by this template"
}

variable "project_url" {
  description = "URL of the gitlab project that owns the resources"
  default     = "http://localhost"
  type        = string
}

variable "region" {
  type        = string
  default     = "eu-west-2"
  description = "AWS Region the S3 bucket should reside in"
}

variable "mc_auth_path" {
  type        = string
  description = "Path to the Marketing Cloud secret path in Secrets Manager"
}

variable "mc_login_url" {
  type        = string
  description = "Marketing Cloud login URL"
}

variable "lambda_sf_memory_size" {
  type        = number
  description = "Memory size for this lambda function."
}

variable "sf_auth_details" {
  type        = string
  description = "Salesforce Authentication"
  default     = "sls/etl/salesforceAuthDetails"
}

variable "sf_token_url" {
  type        = string
  description = "Salesforce Token URL"
}

variable "sf_api_version" {
  type        = string
  description = "Salesforce API Version"
  default     = "v53.0"
}

variable "lambda_ga_view_1_cron_enable" {
  type        = string
  description = "Event bridge state rule"
  default     = "DISABLED"
}

variable "lambda_ga_view_2_cron_enable" {
  type        = string
  description = "Event bridge rule enabled bool"
  default     = "DISABLED"
}

variable "lambda_ga_view_3_cron_enable" {
  type        = string
  description = "Event bridge rule enabled bool"
  default     = "DISABLED"
}

variable "lambda_mc_campaigns_cron_enable" {
  type        = string
  description = "Event bridge state rule"
  default     = "DISABLED"
}

variable "lambda_mc_email_clicked_cron_enable" {
  type        = string
  description = "Event bridge state rule"
  default     = "DISABLED"
}

variable "lambda_mc_email_opened_cron_enable" {
  type        = string
  description = "Event bridge state rule"
  default     = "DISABLED"
}

variable "lambda_mc_journeys_cron_enable" {
  type        = string
  description = "Event bridge state rule"
  default     = "DISABLED"
}

variable "lambda_mc_person_accounts_cron_enable" {
  type        = string
  description = "Event bridge state rule"
  default     = "DISABLED"
}

variable "lambda_mc_reporting_ftd_cron_enable" {
  type        = string
  description = "Event bridge state rule"
  default     = "DISABLED"
}

variable "lambda_mc_reporting_ias_cron_enable" {
  type        = string
  description = "Event bridge state rule"
  default     = "DISABLED"
}

variable "lambda_mc_reporting_wallet_cron_enable" {
  type        = string
  description = "Event bridge state rule"
  default     = "DISABLED"
}

variable "lambda_mc_affluency_cron_enable" {
  type        = string
  description = "Event bridge state rule"
  default     = "DISABLED"
}

variable "lambda_sprinklr_cron_enable" {
  type        = string
  description = "Event bridge state rule"
  default     = "DISABLED"
}

variable "sf_timeout_python_glue_job" {
  type        = number
  description = "timeout for python glue job."
}

variable "mc_activities_sent_to_s3_raw_trigger_enabled" {
  type        = string
  description = "Glue trigger enabled bool"
  default     = "false"
}

variable "sf_instance_url" {
  type        = string
  description = "Salesforce End point URL"
}

variable "lambda_property_valuation_s3_to_sf_enable" {
  type        = string
  description = "Event bridge state rule"
  default     = "DISABLED"
}

variable "lambda_sf_valuation_files_case_enable" {
  type        = string
  description = "Event bridge state rule"
  default     = "DISABLED"
}

variable "mc_journeys_to_s3_raw_trigger_enabled" {
  type        = string
  description = "Glue trigger enabled bool"
  default     = "false"
}

variable "salesforce_events" {
  default = {}
}
