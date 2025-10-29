data "aws_ssm_parameter" "s3_sfmc_data_collection_name" {
  name = "sls-s3-sfmc-data-name"
}

data "aws_ssm_parameter" "s3_sfmc_data_collection_arn" {
  name = "sls-s3-sfmc-data-arn"
}
