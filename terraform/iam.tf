###########################################################
# IAM Role for AWS Lambda
###########################################################
resource "aws_iam_role" "iam_for_lambda" {
  name = "${local.prefix}-lambda"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = [
            "lambda.amazonaws.com"
          ]
        }
      },
    ]
  })

  inline_policy {
    name   = "s3"
    policy = data.aws_iam_policy_document.s3.json
  }

  inline_policy {
    name   = "athena"
    policy = data.aws_iam_policy_document.athena.json
  }

  inline_policy {
    name   = "glue"
    policy = data.aws_iam_policy_document.glue.json
  }

  inline_policy {
    name   = "secretsmanager"
    policy = data.aws_iam_policy_document.secretsmanager.json
  }

  inline_policy {
    name   = "translate"
    policy = data.aws_iam_policy_document.translate.json
  }

  inline_policy {
    name   = "logs"
    policy = data.aws_iam_policy_document.logs.json
  }

  inline_policy {
    name   = "xray"
    policy = data.aws_iam_policy_document.xray.json
  }
}

###########################################################
# IAM Role for AWS Lambda Sprinklr
###########################################################
resource "aws_iam_role" "iam_for_lambda_sprinklr" {
  name = "${local.prefix}-lambda-sprinklr"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = [
            "lambda.amazonaws.com"
          ]
        }
      },
    ]
  })

  inline_policy {
    name   = "s3"
    policy = data.aws_iam_policy_document.s3.json
  }

  inline_policy {
    name   = "athena"
    policy = data.aws_iam_policy_document.athena.json
  }

  inline_policy {
    name   = "glue"
    policy = data.aws_iam_policy_document.glue.json
  }

  inline_policy {
    name   = "sprinklr_secretsmanager_update"
    policy = data.aws_iam_policy_document.sprinklr_secretsmanager_update.json
  }

  inline_policy {
    name   = "logs"
    policy = data.aws_iam_policy_document.logs.json
  }

  inline_policy {
    name   = "xray"
    policy = data.aws_iam_policy_document.xray.json
  }
}

###########################################################
# IAM Role for AWS Lambda s3 to Service Salesforce
###########################################################
resource "aws_iam_role" "iam_for_lambda_to_sf" {
  name = "${local.prefix}-lambda-to-salesforce"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = [
            "lambda.amazonaws.com"
          ]
        }
      },
    ]
  })

  inline_policy {
    name   = "sf_s3"
    policy = data.aws_iam_policy_document.sf_s3.json
  }

  inline_policy {
    name   = "sf_secretsmanager_update"
    policy = data.aws_iam_policy_document.sf_secretsmanager_update.json
  }

  inline_policy {
    name   = "logs"
    policy = data.aws_iam_policy_document.logs.json
  }

  inline_policy {
    name   = "xray"
    policy = data.aws_iam_policy_document.xray.json
  }
}

data "aws_iam_policy_document" "s3" {
  statement {
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListObjects",
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:DeleteObject",
      "s3:DeleteObjectVersion",
      "s3:PutObject",
    ]
    effect = "Allow"
    resources = [
      "arn:aws:s3:::${local.raw_datalake_bucket_name}",
      "arn:aws:s3:::${local.raw_datalake_bucket_name}/*",
      "arn:aws:s3:::${local.curated_datalake_bucket_name}",
      "arn:aws:s3:::${local.curated_datalake_bucket_name}/*",
      "arn:aws:s3:::${local.recon_datalake_bucket_name}",
      "arn:aws:s3:::${local.recon_datalake_bucket_name}/*",
      "arn:aws:s3:::${local.athena_results_bucket_name}",
      "arn:aws:s3:::${local.athena_results_bucket_name}/*",
      data.aws_ssm_parameter.s3_sfmc_data_collection_arn.value,
      "${data.aws_ssm_parameter.s3_sfmc_data_collection_arn.value}/*",
    ]
  }
}

data "aws_iam_policy_document" "sf_s3" {
  statement {
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListObjects",
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:DeleteObject",
      "s3:DeleteObjectVersion",
      "s3:PutObject",
    ]
    effect = "Allow"
    resources = [
      "arn:aws:s3:::${local.curated_datalake_bucket_name}",
      "arn:aws:s3:::${local.curated_datalake_bucket_name}/*",
    ]
  }
}

data "aws_iam_policy_document" "s3_sfmc" {
  statement {
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListObjects",
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:DeleteObject",
      "s3:DeleteObjectVersion",
      "s3:PutObject",
    ]
    effect = "Allow"
    resources = [
      "arn:aws:s3:::${local.raw_datalake_bucket_name}",
      "arn:aws:s3:::${local.raw_datalake_bucket_name}/*",
      "arn:aws:s3:::${local.recon_datalake_bucket_name}",
      "arn:aws:s3:::${local.recon_datalake_bucket_name}/*",
      "arn:aws:s3:::${local.athena_results_bucket_name}",
      "arn:aws:s3:::${local.athena_results_bucket_name}/*",
      data.aws_ssm_parameter.s3_sfmc_data_collection_arn.value,
      "${data.aws_ssm_parameter.s3_sfmc_data_collection_arn.value}/*",
    ]
  }
}


data "aws_iam_policy_document" "athena" {
  statement {
    actions   = ["athena:*"]
    effect    = "Allow"
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "glue" {
  statement {
    actions = [
      "glue:CreateDatabase",
      "glue:DeleteDatabase",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:UpdateDatabase",
      "glue:CreateTable",
      "glue:DeleteTable",
      "glue:BatchDeleteTable",
      "glue:UpdateTable",
      "glue:GetTable",
      "glue:GetTables",
      "glue:BatchCreatePartition",
      "glue:CreatePartition",
      "glue:DeletePartition",
      "glue:BatchDeletePartition",
      "glue:UpdatePartition",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchGetPartition"
    ]
    effect    = "Allow"
    resources = ["*"]
  }

  statement {
    actions = [
      "lakeformation:*",
    ]
    effect    = "Allow"
    resources = ["*"]
  }

}


data "aws_iam_policy_document" "secretsmanager" {
  statement {
    actions = [
      "secretsmanager:GetSecretValue"
    ]
    effect = "Allow"
    resources = [
      "arn:aws:secretsmanager:${var.region}:${var.aws_account_id}:secret:${var.mc_auth_path}-*",
      "arn:aws:secretsmanager:${var.region}:${var.aws_account_id}:secret:${var.sf_auth_details}-*",
      "arn:aws:secretsmanager:${var.region}:${var.aws_account_id}:secret:sls/data/niceAuthDetails-*",
      "arn:aws:secretsmanager:${var.region}:${var.aws_account_id}:secret:sls/data/google-analytics-service-account-*",
    ]
  }
}

data "aws_iam_policy_document" "sprinklr_secretsmanager_update" {
  statement {
    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:PutSecretValue",
      "secretsmanager:UpdateSecret"
    ]
    effect = "Allow"
    resources = [
      "arn:aws:secretsmanager:${var.region}:${var.aws_account_id}:secret:sls/data/sprinklr-*",
    ]
  }
}

data "aws_iam_policy_document" "sf_secretsmanager_update" {
  statement {
    actions = [
      "secretsmanager:GetSecretValue"
    ]
    effect = "Allow"
    resources = [
      "arn:aws:secretsmanager:${var.region}:${var.aws_account_id}:secret:${var.sf_auth_details}-*",
    ]
  }
}


data "aws_iam_policy_document" "translate" {
  statement {
    effect    = "Allow"
    actions   = ["translate:TranslateText"]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "logs" {
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "xray" {
  statement {
    effect = "Allow"
    actions = [
      "xray:PutTraceSegments",
      "xray:PutTelemetryRecords"
    ]
    resources = ["*"]
  }
}

# Datalake raw permissions
resource "aws_lakeformation_permissions" "lambda_datalake_raw_database" {
  principal   = aws_iam_role.iam_for_lambda.arn
  permissions = ["CREATE_TABLE"]

  database {
    name = "datalake_raw"
  }
}

resource "aws_lakeformation_permissions" "lambda_datalake_raw_tables" {
  permissions = [
    "SELECT",
    "DESCRIBE",
    "INSERT",
    "DELETE",
    "INSERT",
    "ALTER",
  ]

  principal = aws_iam_role.iam_for_lambda.arn

  table {
    database_name = "datalake_raw"
    wildcard      = true
  }

  lifecycle {
    ignore_changes = [permissions]
  }
}

# Datalake Curated permissions
resource "aws_lakeformation_permissions" "lambda_datalake_curated_database" {
  principal   = aws_iam_role.iam_for_lambda.arn
  permissions = ["CREATE_TABLE"]

  database {
    name = "datalake_curated"
  }
}

resource "aws_lakeformation_permissions" "lambda_datalake_curated_tables" {
  permissions = [
    "SELECT",
    "DESCRIBE",
    "INSERT",
    "DELETE",
    "INSERT",
    "ALTER",
  ]

  principal = aws_iam_role.iam_for_lambda.arn

  table {
    database_name = "datalake_curated"
    wildcard      = true
  }

  lifecycle {
    ignore_changes = [permissions]
  }
}

# Datalake Reconciliation permissions
resource "aws_lakeformation_permissions" "lambda_datalake_reconciliation_database" {
  principal   = aws_iam_role.iam_for_lambda.arn
  permissions = ["CREATE_TABLE"]

  database {
    name = "datalake_reconciliation"
  }
}

resource "aws_lakeformation_permissions" "lambda_datalake_reconciliation_tables" {
  permissions = [
    "SELECT",
    "DESCRIBE",
    "INSERT",
    "DELETE",
    "INSERT",
    "ALTER",
  ]

  principal = aws_iam_role.iam_for_lambda.arn

  table {
    database_name = "datalake_reconciliation"
    wildcard      = true
  }

  lifecycle {
    ignore_changes = [permissions]
  }
}

###########################################################
# IAM Role for AWS Glue spark sfmc
###########################################################

resource "aws_iam_role" "iam_for_glue_sfmc" {
  name = "${local.prefix}-glue-sfmc"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  inline_policy {
    name   = "s3_sfmc"
    policy = data.aws_iam_policy_document.s3_sfmc.json
  }

  inline_policy {
    name   = "glue"
    policy = data.aws_iam_policy_document.glue.json
  }

  inline_policy {
    name   = "athena"
    policy = data.aws_iam_policy_document.athena.json
  }
}

resource "aws_iam_role_policy_attachment" "glue_sfmc_role_policy" {
  role       = aws_iam_role.iam_for_glue_sfmc.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

###########################################################
# IAM Role for AWS Glue
###########################################################

resource "aws_iam_role" "iam_for_glue" {
  name = "${local.prefix}-glue"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  inline_policy {
    name   = "glue_secretsmanager"
    policy = data.aws_iam_policy_document.glue_secretsmanager.json
  }

  inline_policy {
    name   = "glue_s3"
    policy = data.aws_iam_policy_document.glue_s3.json
  }

  inline_policy {
    name   = "glue"
    policy = data.aws_iam_policy_document.glue.json
  }

  inline_policy {
    name   = "athena"
    policy = data.aws_iam_policy_document.athena.json
  }
}

resource "aws_iam_role_policy_attachment" "glue_role_policy" {
  role       = aws_iam_role.iam_for_glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_secretsmanager" {
  statement {
    actions = [
      "secretsmanager:GetSecretValue"
    ]
    effect = "Allow"
    resources = [
      "arn:aws:secretsmanager:${var.region}:${var.aws_account_id}:secret:${var.sf_auth_details}-*",
      "arn:aws:secretsmanager:${var.region}:${var.aws_account_id}:secret:${var.mc_auth_path}-*",
    ]
  }
}

data "aws_iam_policy_document" "glue_s3" {
  statement {
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListObjects",
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:DeleteObject",
      "s3:DeleteObjectVersion",
      "s3:PutObject",
    ]
    effect = "Allow"
    resources = [
      "arn:aws:s3:::${local.raw_datalake_bucket_name}",
      "arn:aws:s3:::${local.raw_datalake_bucket_name}/*"
    ]
  }
}

# AWS Lakeformation permissions
resource "aws_lakeformation_permissions" "glue_datalake_curated_database" {
  principal   = aws_iam_role.iam_for_glue.arn
  permissions = ["CREATE_TABLE"]

  database {
    name = "datalake_raw"
  }
}

resource "aws_lakeformation_permissions" "glue_datalake_raw_tables" {
  permissions = [
    "SELECT",
    "DESCRIBE",
    "INSERT",
    "DELETE",
    "INSERT",
    "ALTER",
    "DROP",
  ]

  principal = aws_iam_role.iam_for_glue.arn

  table {
    database_name = "datalake_raw"
    wildcard      = true
  }

  lifecycle {
    ignore_changes = [permissions]
  }
}
