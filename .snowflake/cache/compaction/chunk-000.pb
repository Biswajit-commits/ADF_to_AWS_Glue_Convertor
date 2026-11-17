
�m
&ADF_to_AWS_Glue_Convertor/converter.py�m"""
ADF to AWS Glue Converter Engine
Converts Azure Data Factory artifacts (Dataflows, Datasets, Linked Services)
into AWS Glue compatible PySpark scripts and configurations.
"""
import json
import re


# --- Type Mapping ---
ADF_TO_SPARK_TYPES = {
    "string": "StringType()",
    "integer": "IntegerType()",
    "int": "IntegerType()",
    "long": "LongType()",
    "double": "DoubleType()",
    "float": "FloatType()",
    "boolean": "BooleanType()",
    "date": "DateType()",
    "timestamp": "TimestampType()",
    "binary": "BinaryType()",
    "decimal": "DecimalType()",
    "short": "ShortType()",
    "byte": "ByteType()",
}


def convert_linked_service(ls_json: dict) -> dict:
    """Convert an ADF Linked Service to Glue Connection config."""
    name = ls_json.get("name", "unknown")
    props = ls_json.get("properties", {})
    ls_type = props.get("type", "")
    type_props = props.get("typeProperties", {})

    result = {
        "name": name,
        "adf_type": ls_type,
        "glue_connection": {},
        "notes": [],
    }

    if "Snowflake" in ls_type:
        result["glue_connection"] = {
            "Type": "JDBC",
            "ConnectionProperties": {
                "JDBC_CONNECTION_URL": f"jdbc:snowflake://{type_props.get('host', '<HOST>')}/",
                "USERNAME": type_props.get("user", "<USERNAME>"),
                "PASSWORD": "{{resolve:secretsmanager:snowflake-credentials:SecretString:password}}",
                "DATABASE": type_props.get("database", "<DATABASE>"),
                "WAREHOUSE": type_props.get("warehouse", "<WAREHOUSE>"),
            },
        }
        result["notes"].append("Store Snowflake credentials in AWS Secrets Manager")

    elif "AzureBlobStorage" in ls_type:
        conn_str = type_props.get("connectionString", "")
        account_match = re.search(r"AccountName=([^;]+)", conn_str)
        account_name = account_match.group(1) if account_match else "<STORAGE_ACCOUNT>"
        result["glue_connection"] = {
            "Type": "S3",
            "Description": f"Migrated from Azure Blob: {account_name}",
            "ConnectionProperties": {
                "S3_BUCKET": f"s3://<YOUR-BUCKET-NAME>/  # was Azure Blob account: {account_name}",
            },
        }
        result["notes"].append(
            f"Azure Blob '{account_name}' must be migrated to S3 or accessed via S3-compatible gateway"
        )

    elif "AzureSqlDatabase" in ls_type or "SqlServer" in ls_type:
        result["glue_connection"] = {
            "Type": "JDBC",
            "ConnectionProperties": {
                "JDBC_CONNECTION_URL": "jdbc:sqlserver://<HOST>:<PORT>;databaseName=<DB>",
                "USERNAME": "{{resolve:secretsmanager:sql-credentials:SecretString:username}}",
                "PASSWORD": "{{resolve:secretsmanager:sql-credentials:SecretString:password}}",
            },
        }
        result["notes"].append("Migrate SQL connection details to Secrets Manager")

    else:
        result["glue_connection"] = {
            "Type": "CUSTOM",
            "Description": f"Manual migration needed for ADF type: {ls_type}",
        }
        result["notes"].append(f"No automatic mapping for ADF type '{ls_type}' — manual review required")

    return result


def convert_dataset(ds_json: dict, linked_services: dict) -> dict:
    """Convert an ADF Dataset to Glue Catalog Table / source config."""
    name = ds_json.get("name", "unknown")
    props = ds_json.get("properties", {})
    ds_type = props.get("type", "")
    type_props = props.get("typeProperties", {})
    ls_ref = props.get("linkedServiceName", {}).get("referenceName", "")

    result = {
        "name": name,
        "adf_type": ds_type,
        "glue_catalog_entry": {},
        "glue_code_snippet": "",
        "notes": [],
    }

    if ds_type == "DelimitedText":
        location = type_props.get("location", {})
        container = location.get("container", "<container>")
        file_name = location.get("fileName", "*")
        delimiter = type_props.get("columnDelimiter", ",")
        has_header = type_props.get("firstRowAsHeader", True)

        result["glue_catalog_entry"] = {
            "DatabaseName": "<GLUE_DATABASE>",
            "TableInput": {
                "Name": name.lower(),
                "StorageDescriptor": {
                    "Location": f"s3://<YOUR-BUCKET>/{container}/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
                        "Parameters": {"separatorChar": delimiter},
                    },
                },
                "Parameters": {"skip.header.line.count": "1" if has_header else "0"},
            },
        }
        result["glue_code_snippet"] = (
            f'# Read source: {name}\n'
            f'df_{name} = glueContext.create_dynamic_frame.from_options(\n'
            f'    connection_type="s3",\n'
            f'    format="csv",\n'
            f'    format_options={{"withHeader": {has_header}, "separator": "{delimiter}"}},\n'
            f'    connection_options={{"paths": ["s3://<YOUR-BUCKET>/{container}/{file_name}"]}},\n'
            f'    transformation_ctx="{name}_ctx"\n'
            f')\n'
        )

    elif "Snowflake" in ds_type:
        schema = type_props.get("schema", "public")
        table = type_props.get("table", "<TABLE>")
        result["glue_catalog_entry"] = {
            "DatabaseName": "<GLUE_DATABASE>",
            "TableInput": {
                "Name": name.lower(),
                "StorageDescriptor": {
                    "Location": f"jdbc:snowflake (schema={schema}, table={table})",
                },
            },
        }
        result["glue_code_snippet"] = (
            f'# Sink: {name} (Snowflake via JDBC)\n'
            f'df_{name} = glueContext.write_dynamic_frame.from_options(\n'
            f'    frame=<SOURCE_FRAME>,\n'
            f'    connection_type="custom.jdbc",\n'
            f'    connection_options={{\n'
            f'        "connectionName": "{ls_ref}",\n'
            f'        "dbtable": "{schema}.{table}",\n'
            f'    }},\n'
            f'    transformation_ctx="{name}_sink_ctx"\n'
            f')\n'
        )
        result["notes"].append("Ensure Snowflake JDBC driver is added to Glue job jars")

    else:
        result["glue_code_snippet"] = f"# TODO: Manual migration needed for dataset type: {ds_type}\n"
        result["notes"].append(f"Dataset type '{ds_type}' requires manual conversion")

    return result


def _parse_dsl_columns(script_lines: list) -> list:
    """Extract column definitions from ADF dataflow script."""
    columns = []
    in_output = False
    for line in script_lines:
        stripped = line.strip().rstrip(",")
        if "output(" in stripped:
            in_output = True
            continue
        if in_output and ")" in stripped and "as" not in stripped:
            in_output = False
            continue
        if in_output:
            match = re.match(r"(\w+)\s+as\s+(\w+)", stripped)
            if match:
                columns.append({"name": match.group(1), "type": match.group(2)})
    return columns


def _parse_filter_expression(script_lines: list) -> str:
    """Extract filter expressions from ADF dataflow script."""
    for line in script_lines:
        if "filter(" in line:
            match = re.search(r"filter\((.+?)\)\s*~>", line)
            if match:
                expr = match.group(1)
                expr = expr.replace("==", "==").replace("&&", " & ").replace("||", " | ")
                return expr
    return ""


def convert_dataflow(df_json: dict, datasets: dict) -> dict:
    """Convert an ADF Mapping Data Flow to a Glue PySpark ETL script."""
    name = df_json.get("name", "unknown")
    props = df_json.get("properties", {})
    type_props = props.get("typeProperties", {})

    sources = type_props.get("sources", [])
    sinks = type_props.get("sinks", [])
    script_lines = type_props.get("scriptLines", [])
    transformations = type_props.get("transformations", [])

    columns = _parse_dsl_columns(script_lines)
    filter_expr = _parse_filter_expression(script_lines)

    # Build the Glue PySpark script
    lines = []
    lines.append('"""')
    lines.append(f"AWS Glue ETL Job: {name}")
    lines.append(f"Converted from ADF Mapping Data Flow: {name}")
    lines.append('"""')
    lines.append("import sys")
    lines.append("from awsglue.transforms import *")
    lines.append("from awsglue.utils import getResolvedOptions")
    lines.append("from pyspark.context import SparkContext")
    lines.append("from awsglue.context import GlueContext")
    lines.append("from awsglue.job import Job")
    lines.append("from pyspark.sql import functions as F")
    lines.append("from pyspark.sql.types import *")
    lines.append("")
    lines.append("# Initialize Glue context")
    lines.append('args = getResolvedOptions(sys.argv, ["JOB_NAME"])')
    lines.append("sc = SparkContext()")
    lines.append("glueContext = GlueContext(sc)")
    lines.append("spark = glueContext.spark_session")
    lines.append('job = Job(glueContext)')
    lines.append('job.init(args["JOB_NAME"], args)')
    lines.append("")

    # Schema definition
    if columns:
        lines.append("# Schema definition (from ADF dataflow)")
        lines.append("schema = StructType([")
        for col in columns:
            spark_type = ADF_TO_SPARK_TYPES.get(col["type"], "StringType()")
            lines.append(f'    StructField("{col["name"]}", {spark_type}, True),')
        lines.append("])")
        lines.append("")

    # Sources
    for src in sources:
        ds_ref = src.get("dataset", {}).get("referenceName", "unknown_source")
        src_name = src.get("name", "source")
        ds_info = datasets.get(ds_ref, {})
        ds_type = ds_info.get("properties", {}).get("type", "")

        if ds_type == "DelimitedText":
            loc = ds_info.get("properties", {}).get("typeProperties", {}).get("location", {})
            container = loc.get("container", "<container>")
            file_name = loc.get("fileName", "*")
            delimiter = ds_info.get("properties", {}).get("typeProperties", {}).get("columnDelimiter", ",")
            lines.append(f"# Source: {src_name} (from ADF dataset: {ds_ref})")
            lines.append(f'{src_name}_dyf = glueContext.create_dynamic_frame.from_options(')
            lines.append(f'    connection_type="s3",')
            lines.append(f'    format="csv",')
            lines.append(f'    format_options={{"withHeader": True, "separator": "{delimiter}"}},')
            lines.append(f'    connection_options={{"paths": ["s3://<YOUR-BUCKET>/{container}/{file_name}"]}},')
            lines.append(f'    transformation_ctx="{src_name}_ctx"')
            lines.append(f")")
            lines.append(f"{src_name}_df = {src_name}_dyf.toDF()")
        else:
            lines.append(f"# Source: {src_name} (from dataset: {ds_ref})")
            lines.append(f"# TODO: Configure source connection for type '{ds_type}'")
            lines.append(f'{src_name}_df = spark.read.format("jdbc").load()  # Configure manually')
        lines.append("")

    # Transformations (filters)
    if filter_expr:
        lines.append("# Transformation: Filter")
        lines.append(f'filtered_df = {sources[0]["name"]}_df.filter("{filter_expr}")')
        lines.append("")
        last_df = "filtered_df"
    elif transformations:
        for t in transformations:
            t_name = t.get("name", "transform")
            lines.append(f"# Transformation: {t_name}")
            lines.append(f"# TODO: Implement transformation logic for '{t_name}'")
        lines.append("")
        last_df = f"{sources[0]['name']}_df" if sources else "source_df"
    else:
        last_df = f"{sources[0]['name']}_df" if sources else "source_df"

    # Sinks
    for sink in sinks:
        ds_ref = sink.get("dataset", {}).get("referenceName", "unknown_sink")
        sink_name = sink.get("name", "sink")
        ds_info = datasets.get(ds_ref, {})
        ds_type = ds_info.get("properties", {}).get("type", "")

        if "Snowflake" in ds_type:
            tp = ds_info.get("properties", {}).get("typeProperties", {})
            schema = tp.get("schema", "public")
            table = tp.get("table", "<TABLE>")
            lines.append(f"# Sink: {sink_name} -> Snowflake ({schema}.{table})")
            lines.append(f"output_dyf = DynamicFrame.fromDF({last_df}, glueContext, '{sink_name}_dyf')")
            lines.append(f"glueContext.write_dynamic_frame.from_options(")
            lines.append(f'    frame=output_dyf,')
            lines.append(f'    connection_type="custom.jdbc",')
            lines.append(f"    connection_options={{")
            lines.append(f'        "connectionName": "<GLUE_SNOWFLAKE_CONNECTION>",')
            lines.append(f'        "dbtable": "{schema}.{table}",')
            lines.append(f"    }},")
            lines.append(f'    transformation_ctx="{sink_name}_ctx"')
            lines.append(f")")
        else:
            lines.append(f"# Sink: {sink_name} (type: {ds_type})")
            lines.append(f"output_dyf = DynamicFrame.fromDF({last_df}, glueContext, '{sink_name}_dyf')")
            lines.append(f"glueContext.write_dynamic_frame.from_catalog(")
            lines.append(f'    database="<GLUE_DATABASE>",')
            lines.append(f'    table_name="{ds_ref.lower()}",')
            lines.append(f'    transformation_ctx="{sink_name}_ctx"')
            lines.append(f")")
        lines.append("")

    lines.append("job.commit()")

    return {
        "name": name,
        "glue_script": "\n".join(lines),
        "sources": [s.get("name") for s in sources],
        "sinks": [s.get("name") for s in sinks],
        "transformations": [t.get("name") for t in transformations],
        "notes": [],
    }
*
���� 8dd8e22240743aaf9cf0b43574ea5059
��
,ADF_to_AWS_Glue_Convertor/glue_deployment.py��"""
AWS Glue Deployment Bundle Generator
Produces IAM policies, roles, and CloudFormation/Terraform templates
for deploying converted Glue jobs.
"""
import json


def generate_iam_policy(job_name: str, s3_buckets: list, region: str = "us-east-1", account_id: str = "<AWS_ACCOUNT_ID>") -> dict:
    """Generate IAM policy for the Glue job."""
    s3_resources = []
    for bucket in s3_buckets:
        bucket_name = bucket.strip("s3://").split("/")[0] if bucket.startswith("s3://") else bucket
        s3_resources.append(f"arn:aws:s3:::{bucket_name}")
        s3_resources.append(f"arn:aws:s3:::{bucket_name}/*")

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "GlueServiceAccess",
                "Effect": "Allow",
                "Action": [
                    "glue:*",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "logs:GetLogEvents",
                ],
                "Resource": [
                    f"arn:aws:glue:{region}:{account_id}:catalog",
                    f"arn:aws:glue:{region}:{account_id}:database/*",
                    f"arn:aws:glue:{region}:{account_id}:table/*",
                    f"arn:aws:glue:{region}:{account_id}:job/{job_name}*",
                    f"arn:aws:logs:{region}:{account_id}:log-group:/aws-glue/*",
                ],
            },
            {
                "Sid": "S3DataAccess",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation",
                ],
                "Resource": s3_resources if s3_resources else ["arn:aws:s3:::<YOUR-BUCKET>", "arn:aws:s3:::<YOUR-BUCKET>/*"],
            },
            {
                "Sid": "GlueScriptsBucket",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                ],
                "Resource": [
                    f"arn:aws:s3:::{job_name.lower()}-glue-scripts/*",
                ],
            },
            {
                "Sid": "SecretsManagerAccess",
                "Effect": "Allow",
                "Action": [
                    "secretsmanager:GetSecretValue",
                    "secretsmanager:DescribeSecret",
                ],
                "Resource": [
                    f"arn:aws:secretsmanager:{region}:{account_id}:secret:*",
                ],
            },
            {
                "Sid": "CloudWatchMetrics",
                "Effect": "Allow",
                "Action": [
                    "cloudwatch:PutMetricData",
                ],
                "Resource": "*",
                "Condition": {
                    "StringEquals": {
                        "cloudwatch:namespace": "Glue"
                    }
                },
            },
        ],
    }
    return policy


def generate_iam_role(job_name: str, account_id: str = "<AWS_ACCOUNT_ID>") -> dict:
    """Generate IAM Role with trust policy for Glue."""
    role = {
        "RoleName": f"{job_name}-GlueRole",
        "AssumeRolePolicyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "glue.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        },
        "ManagedPolicyArns": [
            "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
        ],
        "Description": f"IAM role for Glue job: {job_name} (migrated from ADF)",
    }
    return role


def generate_glue_job_config(job_name: str, script_location: str, connections: list = None, extra_jars: list = None) -> dict:
    """Generate AWS Glue Job configuration."""
    job_config = {
        "Name": job_name,
        "Role": f"{job_name}-GlueRole",
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": script_location,
            "PythonVersion": "3",
        },
        "DefaultArguments": {
            "--job-language": "python",
            "--job-bookmark-option": "job-bookmark-enable",
            "--TempDir": f"s3://{job_name.lower()}-glue-scripts/temp/",
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-spark-ui": "true",
            "--spark-event-logs-path": f"s3://{job_name.lower()}-glue-scripts/spark-logs/",
        },
        "MaxRetries": 1,
        "GlueVersion": "4.0",
        "NumberOfWorkers": 2,
        "WorkerType": "G.1X",
        "Timeout": 60,
    }

    if connections:
        job_config["Connections"] = {"Connections": connections}

    if extra_jars:
        job_config["DefaultArguments"]["--extra-jars"] = ",".join(extra_jars)

    return job_config


def generate_cloudformation_template(job_name: str, policy: dict, role: dict, job_config: dict) -> dict:
    """Generate a CloudFormation template for the Glue deployment."""
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": f"CloudFormation stack for Glue ETL job: {job_name} (migrated from ADF)",
        "Parameters": {
            "Environment": {
                "Type": "String",
                "Default": "dev",
                "AllowedValues": ["dev", "staging", "prod"],
            },
            "S3BucketName": {
                "Type": "String",
                "Description": "S3 bucket for data and scripts",
            },
        },
        "Resources": {
            "GlueJobRole": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "RoleName": {"Fn::Sub": f"{job_name}-${{Environment}}-GlueRole"},
                    "AssumeRolePolicyDocument": role["AssumeRolePolicyDocument"],
                    "ManagedPolicyArns": role["ManagedPolicyArns"],
                },
            },
            "GlueJobPolicy": {
                "Type": "AWS::IAM::Policy",
                "Properties": {
                    "PolicyName": {"Fn::Sub": f"{job_name}-${{Environment}}-Policy"},
                    "PolicyDocument": policy,
                    "Roles": [{"Ref": "GlueJobRole"}],
                },
            },
            "GlueDatabase": {
                "Type": "AWS::Glue::Database",
                "Properties": {
                    "CatalogId": {"Ref": "AWS::AccountId"},
                    "DatabaseInput": {
                        "Name": {"Fn::Sub": f"{job_name.lower()}_${{Environment}}"},
                        "Description": f"Glue database for {job_name} (migrated from ADF)",
                    },
                },
            },
            "GlueJob": {
                "Type": "AWS::Glue::Job",
                "DependsOn": ["GlueJobRole", "GlueJobPolicy"],
                "Properties": {
                    "Name": {"Fn::Sub": f"{job_name}-${{Environment}}"},
                    "Role": {"Fn::GetAtt": ["GlueJobRole", "Arn"]},
                    "Command": {
                        "Name": "glueetl",
                        "ScriptLocation": {
                            "Fn::Sub": f"s3://${{S3BucketName}}/scripts/{job_name}.py"
                        },
                        "PythonVersion": "3",
                    },
                    "GlueVersion": "4.0",
                    "NumberOfWorkers": 2,
                    "WorkerType": "G.1X",
                    "DefaultArguments": job_config.get("DefaultArguments", {}),
                },
            },
            "ScriptsBucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {
                    "BucketName": {"Fn::Sub": f"{job_name.lower()}-glue-scripts-${{Environment}}"},
                    "VersioningConfiguration": {"Status": "Enabled"},
                },
            },
        },
        "Outputs": {
            "GlueJobName": {
                "Value": {"Ref": "GlueJob"},
                "Description": "Name of the created Glue job",
            },
            "GlueRoleArn": {
                "Value": {"Fn::GetAtt": ["GlueJobRole", "Arn"]},
                "Description": "ARN of the Glue IAM role",
            },
            "ScriptsBucketName": {
                "Value": {"Ref": "ScriptsBucket"},
                "Description": "S3 bucket for Glue scripts",
            },
        },
    }
    return template


def generate_terraform_template(job_name: str, region: str = "us-east-1") -> str:
    """Generate Terraform HCL for the Glue deployment."""
    tf = f'''# Terraform configuration for Glue ETL: {job_name}
# Migrated from Azure Data Factory

terraform {{
  required_providers {{
    aws = {{
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }}
  }}
}}

provider "aws" {{
  region = "{region}"
}}

variable "environment" {{
  type    = string
  default = "dev"
}}

variable "s3_bucket_name" {{
  type        = string
  description = "S3 bucket for source data"
}}

# IAM Role for Glue
resource "aws_iam_role" "glue_role" {{
  name = "{job_name}-${{var.environment}}-GlueRole"

  assume_role_policy = jsonencode({{
    Version = "2012-10-17"
    Statement = [{{
      Effect    = "Allow"
      Principal = {{ Service = "glue.amazonaws.com" }}
      Action    = "sts:AssumeRole"
    }}]
  }})
}}

resource "aws_iam_role_policy_attachment" "glue_service" {{
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}}

resource "aws_iam_role_policy" "glue_s3_access" {{
  name = "{job_name}-S3Access"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({{
    Version = "2012-10-17"
    Statement = [
      {{
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${{var.s3_bucket_name}}",
          "arn:aws:s3:::${{var.s3_bucket_name}}/*",
          "${{aws_s3_bucket.scripts.arn}}",
          "${{aws_s3_bucket.scripts.arn}}/*"
        ]
      }},
      {{
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = "*"
      }}
    ]
  }})
}}

# Scripts bucket
resource "aws_s3_bucket" "scripts" {{
  bucket = "{job_name.lower()}-glue-scripts-${{var.environment}}"
}}

resource "aws_s3_bucket_versioning" "scripts" {{
  bucket = aws_s3_bucket.scripts.id
  versioning_configuration {{
    status = "Enabled"
  }}
}}

# Glue Catalog Database
resource "aws_glue_catalog_database" "main" {{
  name = "{job_name.lower()}_${{var.environment}}"
}}

# Glue Job
resource "aws_glue_job" "etl" {{
  name     = "{job_name}-${{var.environment}}"
  role_arn = aws_iam_role.glue_role.arn

  command {{
    name            = "glueetl"
    script_location = "s3://${{aws_s3_bucket.scripts.id}}/scripts/{job_name}.py"
    python_version  = "3"
  }}

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 60
  max_retries       = 1

  default_arguments = {{
    "--job-language"                          = "python"
    "--job-bookmark-option"                   = "job-bookmark-enable"
    "--TempDir"                               = "s3://${{aws_s3_bucket.scripts.id}}/temp/"
    "--enable-metrics"                        = "true"
    "--enable-continuous-cloudwatch-log"       = "true"
    "--enable-spark-ui"                       = "true"
    "--spark-event-logs-path"                 = "s3://${{aws_s3_bucket.scripts.id}}/spark-logs/"
  }}
}}

output "glue_job_name" {{
  value = aws_glue_job.etl.name
}}

output "glue_role_arn" {{
  value = aws_iam_role.glue_role.arn
}}

output "scripts_bucket" {{
  value = aws_s3_bucket.scripts.id
}}
'''
    return tf


def generate_full_deployment_bundle(job_name: str, s3_buckets: list = None, region: str = "us-east-1", account_id: str = "<AWS_ACCOUNT_ID>") -> dict:
    """Generate the complete deployment bundle (flat dict)."""
    if not s3_buckets:
        s3_buckets = ["<YOUR-BUCKET>"]

    policy = generate_iam_policy(job_name, s3_buckets, region, account_id)
    role = generate_iam_role(job_name, account_id)
    job_config = generate_glue_job_config(
        job_name,
        f"s3://{job_name.lower()}-glue-scripts/scripts/{job_name}.py",
    )
    cfn_template = generate_cloudformation_template(job_name, policy, role, job_config)
    tf_template = generate_terraform_template(job_name, region)

    return {
        "iam_policy.json": json.dumps(policy, indent=2),
        "iam_role.json": json.dumps(role, indent=2),
        "glue_job_config.json": json.dumps(job_config, indent=2),
        "cloudformation.json": json.dumps(cfn_template, indent=2),
        "main.tf": tf_template,
    }


def _generate_cfn_yaml(job_name: str, script_name: str, s3_bucket: str, region: str, account_id: str, worker_type: str = "G.1X", num_workers: int = 2, timeout: int = 60) -> str:
    """Generate CloudFormation YAML (human-readable) for a Glue job deployment."""
    return f"""AWSTemplateFormatVersion: '2010-09-09'
Description: >
  AWS Glue ETL Job: {job_name}
  Migrated from Azure Data Factory

Parameters:
  Environment:
    Type: String
    Default: dev
    AllowedValues: [dev, staging, prod]
  S3BucketName:
    Type: String
    Default: {s3_bucket}
    Description: S3 bucket containing scripts and data
  GlueJobName:
    Type: String
    Default: {job_name}

Resources:
  # S3 Bucket for Glue scripts and data
  GlueS3Bucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${{S3BucketName}}-${{Environment}}"
      VersioningConfiguration:
        Status: Enabled
      Tags:
        - Key: Project
          Value: !Ref GlueJobName
        - Key: MigratedFrom
          Value: AzureDataFactory

  # IAM Role for Glue Job
  GlueJobRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub "${{GlueJobName}}-${{Environment}}-role"
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: glue.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
      Policies:
        - PolicyName: !Sub "${{GlueJobName}}-s3-access"
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
              - Sid: S3DataAccess
                Effect: Allow
                Action:
                  - s3:GetObject
                  - s3:PutObject
                  - s3:DeleteObject
                  - s3:ListBucket
                  - s3:GetBucketLocation
                Resource:
                  - !Sub "arn:aws:s3:::${{S3BucketName}}-${{Environment}}"
                  - !Sub "arn:aws:s3:::${{S3BucketName}}-${{Environment}}/*"
              - Sid: SecretsManagerAccess
                Effect: Allow
                Action:
                  - secretsmanager:GetSecretValue
                  - secretsmanager:DescribeSecret
                Resource:
                  - !Sub "arn:aws:secretsmanager:${{AWS::Region}}:${{AWS::AccountId}}:secret:*"
              - Sid: CloudWatchLogs
                Effect: Allow
                Action:
                  - logs:CreateLogGroup
                  - logs:CreateLogStream
                  - logs:PutLogEvents
                Resource:
                  - !Sub "arn:aws:logs:${{AWS::Region}}:${{AWS::AccountId}}:log-group:/aws-glue/*"

  # Glue Catalog Database
  GlueCatalogDatabase:
    Type: AWS::Glue::Database
    Properties:
      CatalogId: !Ref AWS::AccountId
      DatabaseInput:
        Name: !Sub "${{GlueJobName}}_${{Environment}}"
        Description: !Sub "Glue database for ${{GlueJobName}} (migrated from ADF)"

  # Glue ETL Job
  GlueETLJob:
    Type: AWS::Glue::Job
    DependsOn:
      - GlueJobRole
    Properties:
      Name: !Sub "${{GlueJobName}}-${{Environment}}"
      Role: !GetAtt GlueJobRole.Arn
      Command:
        Name: glueetl
        ScriptLocation: !Sub "s3://${{S3BucketName}}-${{Environment}}/scripts/{script_name}"
        PythonVersion: '3'
      GlueVersion: '4.0'
      NumberOfWorkers: {num_workers}
      WorkerType: {worker_type}
      Timeout: {timeout}
      MaxRetries: 1
      DefaultArguments:
        "--job-language": python
        "--job-bookmark-option": job-bookmark-enable
        "--TempDir": !Sub "s3://${{S3BucketName}}-${{Environment}}/temp/"
        "--enable-metrics": "true"
        "--enable-continuous-cloudwatch-log": "true"
        "--enable-spark-ui": "true"
        "--spark-event-logs-path": !Sub "s3://${{S3BucketName}}-${{Environment}}/spark-logs/"
      Tags:
        Project: !Ref GlueJobName
        MigratedFrom: AzureDataFactory

Outputs:
  GlueJobName:
    Value: !Ref GlueETLJob
    Description: Name of the deployed Glue job
  GlueRoleArn:
    Value: !GetAtt GlueJobRole.Arn
    Description: ARN of the Glue IAM role
  S3BucketName:
    Value: !Ref GlueS3Bucket
    Description: S3 bucket for scripts and data
  GlueDatabaseName:
    Value: !Ref GlueCatalogDatabase
    Description: Glue Catalog database name
"""


def _generate_readme(job_name: str, script_name: str, data_files: list, s3_bucket: str, region: str, worker_type: str = "G.1X", num_workers: int = 2) -> str:
    """Generate README.md for the deployment bundle."""
    data_section = ""
    if data_files:
        file_list = "\n".join(f"  - `{f}`" for f in data_files)
        data_section = f"""
## 2. Upload Data to S3

Upload the data files from `data/` to S3:

```bash
aws s3 cp data/ s3://{s3_bucket}/data/ --recursive
```

Data files included:
{file_list}
"""
    else:
        data_section = """
## 2. Data Sources

No local data files were detected. Configure your data source connections
in the Glue job or Glue Catalog as needed.
"""

    return f"""# AWS Glue Job: {job_name}

> Migrated from Azure Data Factory using ADF-to-AWS-Glue Convertor

## Folder Structure

```
aws-glue-{job_name.lower().replace("_", "-")}/
|
├── scripts/
│   └── {script_name}           # PySpark Glue ETL script
│
├── data/
│   └── (source data files)     # Data files for S3 upload
│
├── iam/
│   └── glue_trust_policy.json  # IAM trust policy & permissions
│
├── cloudformation/
│   └── {job_name.lower()}-glue-job.yaml  # CloudFormation template
│
└── README.md                   # This file
```

## Deployment Steps

### 1. Create S3 Bucket

```bash
aws s3 mb s3://{s3_bucket} --region {region}
aws s3api put-bucket-versioning \\
    --bucket {s3_bucket} \\
    --versioning-configuration Status=Enabled
```
{data_section}
### 3. Upload Glue Script to S3

```bash
aws s3 cp scripts/{script_name} s3://{s3_bucket}/scripts/{script_name}
```

### 4. Create IAM Role and Attach Permissions

```bash
# Create the IAM role with trust policy
aws iam create-role \\
    --role-name {job_name}-GlueRole \\
    --assume-role-policy-document file://iam/glue_trust_policy.json

# Attach AWS managed Glue service policy
aws iam attach-role-policy \\
    --role-name {job_name}-GlueRole \\
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# Attach custom S3 + Secrets Manager policy (inline)
aws iam put-role-policy \\
    --role-name {job_name}-GlueRole \\
    --policy-name {job_name}-DataAccessPolicy \\
    --policy-document file://iam/glue_trust_policy.json
```

### 5. Deploy via CloudFormation (Recommended)

```bash
aws cloudformation deploy \\
    --template-file cloudformation/{job_name.lower()}-glue-job.yaml \\
    --stack-name {job_name.lower()}-stack \\
    --parameter-overrides \\
        Environment=dev \\
        S3BucketName={s3_bucket} \\
        GlueJobName={job_name} \\
    --capabilities CAPABILITY_NAMED_IAM \\
    --region {region}
```

### 6. Run the Glue Job

```bash
aws glue start-job-run --job-name {job_name}-dev --region {region}
```

## Job Configuration

| Parameter | Value |
|-----------|-------|
| Glue Version | 4.0 |
| Python Version | 3 |
| Worker Type | {worker_type} |
| Number of Workers | {num_workers} |
| Timeout (min) | 60 |
| Job Bookmarks | Enabled |
| Spark UI | Enabled |
| CloudWatch Logs | Enabled |

## Notes

- Replace `<YOUR-BUCKET>` placeholders in the script with actual S3 paths
- Store database credentials in AWS Secrets Manager before running
- The CloudFormation template creates all resources including the S3 bucket
- For production: increase workers, adjust timeout, enable job bookmarks
"""


def generate_job_folder_bundle(
    job_name: str,
    glue_script: str,
    data_files: dict = None,
    s3_buckets: list = None,
    region: str = "us-east-1",
    account_id: str = "<AWS_ACCOUNT_ID>",
    worker_type: str = "G.1X",
    num_workers: int = 2,
    timeout: int = 60,
) -> dict:
    """
    Generate a complete AWS Glue deployment bundle in the folder structure:

    aws-glue-<job>/
    ├── scripts/<job>.py
    ├── data/<files...>
    ├── iam/glue_trust_policy.json
    ├── cloudformation/<job>-glue-job.yaml
    └── README.md

    Returns a dict of {relative_path: file_content} for ZIP assembly.
    """
    if not s3_buckets:
        s3_buckets = ["<YOUR-BUCKET>"]
    if not data_files:
        data_files = {}

    s3_bucket = s3_buckets[0] if s3_buckets else f"{job_name.lower()}-glue-data"
    folder_name = f"aws-glue-{job_name.lower().replace('_', '-')}"
    script_name = f"{job_name}.py"

    # IAM: Combined trust policy + permissions document
    trust_and_permissions = {
        "Version": "2012-10-17",
        "RoleName": f"{job_name}-GlueRole",
        "TrustPolicy": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "glue.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        },
        "PermissionsPolicy": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "GlueServiceAccess",
                    "Effect": "Allow",
                    "Action": [
                        "glue:GetJob",
                        "glue:GetJobs",
                        "glue:GetJobRun",
                        "glue:GetJobRuns",
                        "glue:StartJobRun",
                        "glue:BatchStopJobRun",
                        "glue:GetDatabase",
                        "glue:GetDatabases",
                        "glue:GetTable",
                        "glue:GetTables",
                        "glue:CreateTable",
                        "glue:UpdateTable",
                        "glue:GetPartitions",
                        "glue:BatchCreatePartition",
                    ],
                    "Resource": [
                        f"arn:aws:glue:{region}:{account_id}:catalog",
                        f"arn:aws:glue:{region}:{account_id}:database/*",
                        f"arn:aws:glue:{region}:{account_id}:table/*",
                        f"arn:aws:glue:{region}:{account_id}:job/{job_name}*",
                    ],
                },
                {
                    "Sid": "S3DataAccess",
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket",
                        "s3:GetBucketLocation",
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{s3_bucket}",
                        f"arn:aws:s3:::{s3_bucket}/*",
                    ],
                },
                {
                    "Sid": "SecretsManagerAccess",
                    "Effect": "Allow",
                    "Action": [
                        "secretsmanager:GetSecretValue",
                        "secretsmanager:DescribeSecret",
                    ],
                    "Resource": [
                        f"arn:aws:secretsmanager:{region}:{account_id}:secret:*",
                    ],
                },
                {
                    "Sid": "CloudWatchLogs",
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                        "logs:GetLogEvents",
                    ],
                    "Resource": [
                        f"arn:aws:logs:{region}:{account_id}:log-group:/aws-glue/*",
                    ],
                },
                {
                    "Sid": "CloudWatchMetrics",
                    "Effect": "Allow",
                    "Action": ["cloudwatch:PutMetricData"],
                    "Resource": "*",
                    "Condition": {"StringEquals": {"cloudwatch:namespace": "Glue"}},
                },
            ],
        },
        "ManagedPolicyArns": [
            "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
        ],
        "WorkerConfiguration": {
            "WorkerType": worker_type,
            "NumberOfWorkers": num_workers,
            "GlueVersion": "4.0",
            "PythonVersion": "3",
            "Timeout": timeout,
            "MaxRetries": 1,
            "JobBookmarks": "Enabled",
        },
    }

    # CloudFormation YAML
    cfn_yaml = _generate_cfn_yaml(
        job_name, script_name, s3_bucket, region, account_id,
        worker_type, num_workers, timeout,
    )

    # README
    readme = _generate_readme(
        job_name, script_name, list(data_files.keys()),
        s3_bucket, region, worker_type, num_workers,
    )

    # Assemble bundle
    bundle = {}
    bundle[f"{folder_name}/scripts/{script_name}"] = glue_script
    bundle[f"{folder_name}/iam/glue_trust_policy.json"] = json.dumps(trust_and_permissions, indent=2)
    bundle[f"{folder_name}/cloudformation/{job_name.lower()}-glue-job.yaml"] = cfn_yaml
    bundle[f"{folder_name}/README.md"] = readme

    # Data files
    for filename, content in data_files.items():
        bundle[f"{folder_name}/data/{filename}"] = content

    # If no data files, add a placeholder .gitkeep
    if not data_files:
        bundle[f"{folder_name}/data/.gitkeep"] = "# Place source data files here before uploading to S3\n"

    return bundle
*
���� 69147c64490d5ad0fec7672acc5f66e1
�\
*ADF_to_AWS_Glue_Convertor/streamlit_app.py�\"""
ADF to AWS Glue Convertor
A Streamlit application to upload Azure Data Factory artifacts and convert them
to AWS Glue compatible PySpark scripts with deployment bundles.
"""
import io
import json
import zipfile
import streamlit as st

from converter import convert_linked_service, convert_dataset, convert_dataflow
from glue_deployment import generate_job_folder_bundle

st.set_page_config(page_title="ADF to AWS Glue Convertor", page_icon="🔄", layout="wide")

st.title("ADF to AWS Glue Convertor")
st.markdown("Upload Azure Data Factory JSON files and convert them to AWS Glue PySpark ETL scripts with full deployment bundles (IAM, CloudFormation, Terraform).")

st.divider()

# --- Session State Initialization ---
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {"dataflow": {}, "dataset": {}, "linkedService": {}, "data": {}}
if "conversion_results" not in st.session_state:
    st.session_state.conversion_results = None
if "conversion_done" not in st.session_state:
    st.session_state.conversion_done = False


# --- Sidebar: Upload Section ---
with st.sidebar:
    st.header("Upload ADF Files")
    st.markdown("Upload JSON files from your ADF repository folders.")

    st.subheader("Linked Services")
    ls_files = st.file_uploader(
        "Upload Linked Service JSON files",
        type=["json"],
        accept_multiple_files=True,
        key="ls_upload",
    )
    if ls_files:
        for f in ls_files:
            content = json.loads(f.read())
            f.seek(0)
            st.session_state.uploaded_files["linkedService"][f.name] = content
        st.success(f"{len(ls_files)} linked service(s) uploaded")

    st.subheader("Datasets")
    ds_files = st.file_uploader(
        "Upload Dataset JSON files",
        type=["json"],
        accept_multiple_files=True,
        key="ds_upload",
    )
    if ds_files:
        for f in ds_files:
            content = json.loads(f.read())
            f.seek(0)
            st.session_state.uploaded_files["dataset"][f.name] = content
        st.success(f"{len(ds_files)} dataset(s) uploaded")

    st.subheader("Dataflows")
    df_files = st.file_uploader(
        "Upload Dataflow JSON files",
        type=["json"],
        accept_multiple_files=True,
        key="df_upload",
    )
    if df_files:
        for f in df_files:
            content = json.loads(f.read())
            f.seek(0)
            st.session_state.uploaded_files["dataflow"][f.name] = content
        st.success(f"{len(df_files)} dataflow(s) uploaded")

    st.subheader("Data Files (Optional)")
    data_files_upload = st.file_uploader(
        "Upload source data files (CSV, JSON, etc.)",
        type=["csv", "json", "parquet", "txt", "tsv"],
        accept_multiple_files=True,
        key="data_upload",
    )
    if data_files_upload:
        for f in data_files_upload:
            st.session_state.uploaded_files["data"][f.name] = f.read().decode("utf-8", errors="replace")
            f.seek(0)
        st.success(f"{len(data_files_upload)} data file(s) uploaded")

    st.divider()
    total = sum(len(v) for v in st.session_state.uploaded_files.values())
    st.metric("Total Files Uploaded", total)


# --- Main Area: File Overview ---
col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Linked Services")
    if st.session_state.uploaded_files["linkedService"]:
        for fname, content in st.session_state.uploaded_files["linkedService"].items():
            ls_type = content.get("properties", {}).get("type", "Unknown")
            st.markdown(f"- **{fname}** (`{ls_type}`)")
    else:
        st.info("No linked services uploaded yet")

with col2:
    st.subheader("Datasets")
    if st.session_state.uploaded_files["dataset"]:
        for fname, content in st.session_state.uploaded_files["dataset"].items():
            ds_type = content.get("properties", {}).get("type", "Unknown")
            st.markdown(f"- **{fname}** (`{ds_type}`)")
    else:
        st.info("No datasets uploaded yet")

with col3:
    st.subheader("Dataflows")
    if st.session_state.uploaded_files["dataflow"]:
        for fname, content in st.session_state.uploaded_files["dataflow"].items():
            df_type = content.get("properties", {}).get("type", "Unknown")
            st.markdown(f"- **{fname}** (`{df_type}`)")
    else:
        st.info("No dataflows uploaded yet")

st.divider()

# --- Conversion Engine ---
st.subheader("Run Conversion Engine")

col_btn, col_region, col_account = st.columns([1, 1, 1])
with col_region:
    aws_region = st.text_input("AWS Region", value="us-east-1")
with col_account:
    aws_account_id = st.text_input("AWS Account ID", value="<AWS_ACCOUNT_ID>")

col_bucket, col_worker, col_workers = st.columns([1, 1, 1])
with col_bucket:
    s3_bucket_name = st.text_input("S3 Bucket Name", value="<YOUR-BUCKET>")
with col_worker:
    worker_type = st.selectbox("Worker Type", ["G.1X", "G.2X", "G.4X", "G.8X", "Z.2X"], index=0)
with col_workers:
    num_workers = st.number_input("Number of Workers", min_value=2, max_value=100, value=2)

st.write("")
run_conversion = st.button("Run Conversion Engine", type="primary", use_container_width=True)

if run_conversion:
    uploaded = st.session_state.uploaded_files
    if not any(uploaded[k] for k in ["dataflow", "dataset", "linkedService"]):
        st.error("Please upload at least one ADF file before running conversion.")
    else:
        with st.spinner("Converting ADF artifacts to AWS Glue..."):
            results = {
                "linked_services": [],
                "datasets": [],
                "dataflows": [],
                "deployment_bundles": {},
            }

            # Convert linked services
            for fname, content in uploaded["linkedService"].items():
                converted = convert_linked_service(content)
                results["linked_services"].append(converted)

            # Build dataset lookup
            dataset_lookup = {}
            for fname, content in uploaded["dataset"].items():
                dataset_lookup[content.get("name", fname.replace(".json", ""))] = content

            # Convert datasets
            for fname, content in uploaded["dataset"].items():
                converted = convert_dataset(content, uploaded["linkedService"])
                results["datasets"].append(converted)

            # Convert dataflows and generate folder-structured bundles
            for fname, content in uploaded["dataflow"].items():
                converted = convert_dataflow(content, dataset_lookup)
                results["dataflows"].append(converted)

                # Generate deployment folder bundle per dataflow
                bundle = generate_job_folder_bundle(
                    job_name=converted["name"],
                    glue_script=converted["glue_script"],
                    data_files=uploaded.get("data", {}),
                    s3_buckets=[s3_bucket_name],
                    region=aws_region,
                    account_id=aws_account_id,
                    worker_type=worker_type,
                    num_workers=num_workers,
                )
                results["deployment_bundles"][converted["name"]] = bundle

            st.session_state.conversion_results = results
            st.session_state.conversion_done = True

        st.success("Conversion complete!")

# --- Results Display ---
if st.session_state.conversion_done and st.session_state.conversion_results:
    results = st.session_state.conversion_results
    st.divider()
    st.header("Conversion Results")

    # Tabs for different output types
    tab_bundle, tab_scripts, tab_connections, tab_datasets, tab_download = st.tabs(
        ["Deployment Bundle", "Glue ETL Scripts", "Connections", "Datasets", "Download All"]
    )

    with tab_bundle:
        for job_name, bundle in results["deployment_bundles"].items():
            folder_name = f"aws-glue-{job_name.lower().replace('_', '-')}"
            st.subheader(f"Bundle: `{folder_name}/`")

            # Show folder tree
            st.markdown("**Folder Structure:**")
            tree_lines = sorted(bundle.keys())
            tree_display = "\n".join(f"  {p}" for p in tree_lines)
            st.code(tree_display, language="text")

            # Show each file in expandable sections
            for filepath, content in sorted(bundle.items()):
                filename = filepath.split("/")[-1]
                with st.expander(f"{filepath}"):
                    if filename.endswith(".py"):
                        st.code(content, language="python")
                    elif filename.endswith(".json"):
                        st.code(content, language="json")
                    elif filename.endswith(".yaml") or filename.endswith(".yml"):
                        st.code(content, language="yaml")
                    elif filename == "README.md":
                        st.markdown(content)
                    else:
                        st.text(content)

    with tab_scripts:
        for df_result in results["dataflows"]:
            st.subheader(f"Job: {df_result['name']}")
            st.code(df_result["glue_script"], language="python")
            if df_result["notes"]:
                for note in df_result["notes"]:
                    st.warning(note)

    with tab_connections:
        for ls_result in results["linked_services"]:
            st.subheader(f"Connection: {ls_result['name']}")
            st.markdown(f"**ADF Type:** `{ls_result['adf_type']}`")
            st.json(ls_result["glue_connection"])
            if ls_result["notes"]:
                for note in ls_result["notes"]:
                    st.info(note)

    with tab_datasets:
        for ds_result in results["datasets"]:
            st.subheader(f"Dataset: {ds_result['name']}")
            st.markdown(f"**ADF Type:** `{ds_result['adf_type']}`")
            if ds_result["glue_code_snippet"]:
                st.code(ds_result["glue_code_snippet"], language="python")
            if ds_result["glue_catalog_entry"]:
                st.json(ds_result["glue_catalog_entry"])
            if ds_result["notes"]:
                for note in ds_result["notes"]:
                    st.info(note)

    with tab_download:
        st.subheader("Download Deployment Bundles")
        st.markdown("Each bundle is a self-contained folder ready for your AWS teammate to deploy.")

        for job_name, bundle in results["deployment_bundles"].items():
            folder_name = f"aws-glue-{job_name.lower().replace('_', '-')}"
            st.markdown(f"### {folder_name}/")

            # Build ZIP in memory
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                for filepath, content in bundle.items():
                    zf.writestr(filepath, content)

            zip_buffer.seek(0)
            st.download_button(
                label=f"Download {folder_name}.zip",
                data=zip_buffer.getvalue(),
                file_name=f"{folder_name}.zip",
                mime="application/zip",
                type="primary",
                use_container_width=True,
                key=f"dl_{job_name}",
            )

            st.markdown(f"""
```
{folder_name}/
│
├── scripts/
│   └── {job_name}.py                    # PySpark Glue ETL script
│
├── data/
│   └── (source data files)              # Upload to S3 before running
│
├── iam/
│   └── glue_trust_policy.json           # IAM role + trust + permissions
│
├── cloudformation/
│   └── {job_name.lower()}-glue-job.yaml # Full CloudFormation stack
│
└── README.md                            # Step-by-step deployment guide
```
            """)
            st.divider()
*
���� f70b0e6c66672a9d5b81471ffe819f70
�9
,ADF_to_AWS_Glue_Convertor/test_conversion.py�8"""
Test script: Validates ADF to AWS Glue conversion using sample workspace files.
"""
import sys
sys.path.insert(0, "/workspace/ADF_to_AWS_Glue_Convertor")

import json
from converter import convert_linked_service, convert_dataset, convert_dataflow
from glue_deployment import generate_full_deployment_bundle

# Load sample ADF files from workspace
with open("/workspace/linkedService/Snowflake_Link.json") as f:
    ls_snowflake = json.load(f)

with open("/workspace/linkedService/ls_blob.json") as f:
    ls_blob = json.load(f)

with open("/workspace/dataset/ds_customers_src.json") as f:
    ds_customers = json.load(f)

with open("/workspace/dataset/SNF_TGT.json") as f:
    ds_snf_tgt = json.load(f)

with open("/workspace/dataflow/File_2_Snowflake.json") as f:
    df_file2snow = json.load(f)

print("=" * 70)
print("  TEST: ADF to AWS Glue Conversion Validation")
print("=" * 70)

# --- Test 1: Linked Services ---
print("\n" + "=" * 70)
print("  TEST 1: Linked Service Conversion")
print("=" * 70)

result_ls_snow = convert_linked_service(ls_snowflake)
print(f"\n[Snowflake_Link] ADF Type: {result_ls_snow['adf_type']}")
print(f"  Glue Connection Type: {result_ls_snow['glue_connection'].get('Type')}")
conn_props = result_ls_snow['glue_connection'].get('ConnectionProperties', {})
print(f"  JDBC URL: {conn_props.get('JDBC_CONNECTION_URL', 'N/A')}")
print(f"  Username: {conn_props.get('USERNAME', 'N/A')}")
print(f"  Database: {conn_props.get('DATABASE', 'N/A')}")
print(f"  Warehouse: {conn_props.get('WAREHOUSE', 'N/A')}")
print(f"  Notes: {result_ls_snow['notes']}")
assert result_ls_snow['glue_connection']['Type'] == 'JDBC', "FAIL: Snowflake should map to JDBC"
assert 'PE39900' in conn_props.get('JDBC_CONNECTION_URL', ''), "FAIL: Host not in JDBC URL"
print("  ✓ PASSED")

result_ls_blob = convert_linked_service(ls_blob)
print(f"\n[ls_blob] ADF Type: {result_ls_blob['adf_type']}")
print(f"  Glue Connection Type: {result_ls_blob['glue_connection'].get('Type')}")
print(f"  Notes: {result_ls_blob['notes']}")
assert result_ls_blob['glue_connection']['Type'] == 'S3', "FAIL: Blob should map to S3"
assert 'adfgluedemo' in result_ls_blob['notes'][0], "FAIL: Account name not extracted"
print("  ✓ PASSED")

# --- Test 2: Datasets ---
print("\n" + "=" * 70)
print("  TEST 2: Dataset Conversion")
print("=" * 70)

result_ds_csv = convert_dataset(ds_customers, {})
print(f"\n[ds_customers_src] ADF Type: {result_ds_csv['adf_type']}")
print(f"  Glue Catalog Table: {result_ds_csv['glue_catalog_entry'].get('TableInput', {}).get('Name')}")
print(f"  Code snippet preview:")
for line in result_ds_csv['glue_code_snippet'].split('\n')[:5]:
    print(f"    {line}")
assert result_ds_csv['adf_type'] == 'DelimitedText', "FAIL: Type mismatch"
assert 'source-data' in result_ds_csv['glue_code_snippet'], "FAIL: Container not in code"
assert 'customers.csv' in result_ds_csv['glue_code_snippet'], "FAIL: Filename not in code"
print("  ✓ PASSED")

result_ds_snf = convert_dataset(ds_snf_tgt, {})
print(f"\n[SNF_TGT] ADF Type: {result_ds_snf['adf_type']}")
print(f"  Code snippet preview:")
for line in result_ds_snf['glue_code_snippet'].split('\n')[:5]:
    print(f"    {line}")
assert 'SnowflakeV2Table' in result_ds_snf['adf_type'], "FAIL: Type mismatch"
assert 'DEV.CUST' in result_ds_snf['glue_code_snippet'], "FAIL: Schema.Table not in code"
print("  ✓ PASSED")

# --- Test 3: Dataflow (end-to-end ETL script) ---
print("\n" + "=" * 70)
print("  TEST 3: Dataflow Conversion (Full ETL Script)")
print("=" * 70)

# Build dataset lookup as the app would
dataset_lookup = {
    "ds_customers_src": ds_customers,
    "SNF_TGT": ds_snf_tgt,
}

result_df = convert_dataflow(df_file2snow, dataset_lookup)
print(f"\n[File_2_Snowflake] Sources: {result_df['sources']}")
print(f"  Sinks: {result_df['sinks']}")
print(f"  Transformations: {result_df['transformations']}")

script = result_df['glue_script']
print(f"\n  Script length: {len(script)} characters")
print(f"  Script preview (first 30 lines):")
for i, line in enumerate(script.split('\n')[:30]):
    print(f"    {i+1:3}: {line}")

# Validate script contents
assert 'from awsglue.transforms import *' in script, "FAIL: Missing Glue imports"
assert 'GlueContext' in script, "FAIL: Missing GlueContext"
assert 'job.init' in script, "FAIL: Missing job init"
assert 'job.commit()' in script, "FAIL: Missing job.commit()"
assert 'source-data' in script, "FAIL: S3 path missing container"
assert 'customers.csv' in script, "FAIL: Source filename missing"
assert 'CUSTOMER_ID' in script, "FAIL: Schema columns missing"
assert 'filter' in script.lower(), "FAIL: Filter transformation missing"
assert 'C0011' in script, "FAIL: Filter value missing"
assert 'DEV.CUST' in script, "FAIL: Snowflake sink target missing"
print("\n  ✓ ALL SCRIPT ASSERTIONS PASSED")

# --- Test 4: Deployment Bundle ---
print("\n" + "=" * 70)
print("  TEST 4: Deployment Bundle Generation")
print("=" * 70)

bundle = generate_full_deployment_bundle(
    job_name="File_2_Snowflake",
    s3_buckets=["my-data-bucket"],
    region="us-east-1",
    account_id="123456789012",
)

print(f"\n  Bundle files generated: {list(bundle.keys())}")
for fname, content in bundle.items():
    print(f"  - {fname}: {len(content)} chars")

# Validate IAM policy
policy = json.loads(bundle['iam_policy.json'])
assert policy['Version'] == '2012-10-17', "FAIL: Policy version"
statements = policy['Statement']
assert any(s['Sid'] == 'GlueServiceAccess' for s in statements), "FAIL: Missing Glue access"
assert any(s['Sid'] == 'S3DataAccess' for s in statements), "FAIL: Missing S3 access"
assert any(s['Sid'] == 'SecretsManagerAccess' for s in statements), "FAIL: Missing Secrets access"
assert any('my-data-bucket' in str(s.get('Resource', '')) for s in statements), "FAIL: Bucket not in policy"
print("  ✓ IAM Policy: PASSED")

# Validate IAM role
role = json.loads(bundle['iam_role.json'])
assert 'glue.amazonaws.com' in str(role['AssumeRolePolicyDocument']), "FAIL: Trust policy missing Glue"
print("  ✓ IAM Role: PASSED")

# Validate CloudFormation
cfn = json.loads(bundle['cloudformation.json'])
assert 'GlueJob' in cfn['Resources'], "FAIL: Missing Glue job resource"
assert 'GlueJobRole' in cfn['Resources'], "FAIL: Missing role resource"
assert 'GlueDatabase' in cfn['Resources'], "FAIL: Missing database resource"
assert 'ScriptsBucket' in cfn['Resources'], "FAIL: Missing S3 bucket resource"
print("  ✓ CloudFormation: PASSED")

# Validate Terraform
tf = bundle['main.tf']
assert 'aws_glue_job' in tf, "FAIL: Missing glue_job resource"
assert 'aws_iam_role' in tf, "FAIL: Missing iam_role resource"
assert 'aws_s3_bucket' in tf, "FAIL: Missing s3_bucket resource"
assert 'aws_glue_catalog_database' in tf, "FAIL: Missing catalog database"
print("  ✓ Terraform: PASSED")

# Validate Glue job config
job_cfg = json.loads(bundle['glue_job_config.json'])
assert job_cfg['GlueVersion'] == '4.0', "FAIL: Wrong Glue version"
assert job_cfg['Command']['PythonVersion'] == '3', "FAIL: Wrong Python version"
assert job_cfg['WorkerType'] == 'G.1X', "FAIL: Wrong worker type"
print("  ✓ Glue Job Config: PASSED")

print("\n" + "=" * 70)
print("  ALL TESTS PASSED ✓")
print("=" * 70)
*
���� 57641bb6f255f7c3dbcefb4fbadf845a
�"
	README.md�"# ADF_to_AWS_Glue_Convertor v1

A Streamlit in Snowflake app that converts **Azure Data Factory (ADF)** artifacts — Linked Services, Datasets and Mapping Data Flows — into **AWS Glue** PySpark ETL scripts, plus a ready-to-deploy bundle (IAM, CloudFormation, Terraform).

## Workspace layout

```
.
├── ADF_to_AWS_Glue_Convertor/     # Streamlit app
│   ├── streamlit_app.py           # UI: upload ADF JSON, run conversion, preview & download
│   ├── converter.py               # Conversion engine (linked services, datasets, dataflows)
│   ├── glue_deployment.py         # IAM / CloudFormation / Terraform / job-folder bundle generator
│   ├── test_conversion.py         # Validation script using the sample ADF files below
│   ├── snowflake.yml              # Streamlit deployment definition
│   ├── pyproject.toml             # Python dependencies
│   └── .streamlit/config.toml     # Theme & sleep settings
├── linkedService/                 # Sample ADF linked services
│   ├── Snowflake_Link.json        # SnowflakeV2
│   └── ls_blob.json               # AzureBlobStorage
├── dataset/                       # Sample ADF datasets
│   ├── ds_customers_src.json      # DelimitedText (customers.csv in Blob container source-data)
│   └── SNF_TGT.json               # SnowflakeV2Table (DEV.CUST)
└── dataflow/                      # Sample ADF Mapping Data Flow
    ├── File_2_Snowflake.json
    └── File_2_Snowflake.dsl       # Source → filter(CUSTOMER_ID == 'C0011') → Snowflake sink
```

## Conversion mapping

| ADF artifact | AWS Glue output |
|---|---|
| Linked Service – Snowflake | Glue JDBC connection (password via Secrets Manager) |
| Linked Service – Azure Blob Storage | S3 connection placeholder (data must be migrated to S3) |
| Linked Service – Azure SQL / SQL Server | Glue JDBC connection (credentials via Secrets Manager) |
| Linked Service – other | `CUSTOM` placeholder flagged for manual review |
| Dataset – DelimitedText | Glue Catalog table (OpenCSVSerde) + `create_dynamic_frame` read snippet |
| Dataset – Snowflake table | Glue Catalog entry + JDBC write snippet |
| Mapping Data Flow | Full Glue PySpark job: schema from DSL (ADF → Spark types), sources, filter transformations, sinks |

Each conversion also returns **notes** highlighting manual migration steps.

## Deployment bundle

For every dataflow the app produces a downloadable ZIP:

```
aws-glue-<job>/
├── scripts/<job>.py                    # Glue PySpark script
├── data/<files...>                     # Optional uploaded source data
├── iam/glue_trust_policy.json          # Role trust policy + permissions
├── cloudformation/<job>-glue-job.yaml  # CloudFormation stack
└── README.md                           # Deployment instructions
```

`glue_deployment.py` can also generate standalone IAM policies/roles, Glue job configs and Terraform templates. Worker type (default `G.1X`), number of workers (default 2), timeout, region and AWS account ID are configurable.

## Using the app

1. In the sidebar upload Linked Service, Dataset and Dataflow JSON files (optionally source data files: CSV/JSON/Parquet/TXT/TSV).
2. Click **Run Conversion Engine**.
3. Review results in the tabs: **Bundle**, **Scripts**, **Connections**, **Datasets**, **Download**.
4. Download the deployment bundle ZIP and follow its README to deploy to AWS.

## Deployment (Snowflake)

Defined in `ADF_to_AWS_Glue_Convertor/snowflake.yml`:

- Object: `DWH.DEV.ADF_TO_AWS_GLUE_CONVERTOR`
- Query warehouse: `COMPUTE_WH`
- Compute pool: `SYSTEM_COMPUTE_POOL_CPU` (container runtime, `SpcsOnly`)
- Requires Python ≥ 3.11, `streamlit[snowflake]>=1.54.0`

## Testing

Run the validation script against the sample files in this workspace:

```bash
python ADF_to_AWS_Glue_Convertor/test_conversion.py
```

It converts the sample linked services, datasets and the `File_2_Snowflake` dataflow, then generates a full deployment bundle and prints the results.

## Limitations

- Supported dataflow transformations are currently sources, filters and sinks; other transformations need manual conversion.
- Azure Blob data is not moved automatically — copy it to S3 and replace `<YOUR-BUCKET>` placeholders.
- Credentials are never migrated; configure them in AWS Secrets Manager.
,
����""f4c0b77bb0252e69b5964dd729379708"
�
!linkedService/Snowflake_Link.json�{
    "name": "Snowflake_Link",
    "type": "Microsoft.DataFactory/factories/linkedservices",
    "properties": {
        "description": "Connect to Snowflake : PE39900 Account",
        "annotations": [],
        "type": "SnowflakeV2",
        "typeProperties": {
            "authenticationType": "Basic",
            "accountIdentifier": "PE39900",
            "user": "AZ_USER_01",
            "database": "DWH",
            "warehouse": "WH_SMALL",
            "host": "TZPMRCC-PE39900.snowflakecomputing.com",
            "useUtcTimestamps": false,
            "encryptedCredential": "ew0KICAiVmVyc2lvbiI6ICIyMDE3LTExLTMwIiwNCiAgIlByb3RlY3Rpb25Nb2RlIjogIktleSIsDQogICJTZWNyZXRDb250ZW50VHlwZSI6ICJQbGFpbnRleHQiLA0KICAiQ3JlZGVudGlhbElkIjogIkRBVEFGQUNUT1JZQDczODU3REEzLThDMzEtNEEzNS05MkVELURBMTY4QUREMEU0Ml9iOTFiMTU3Zi1jMzc5LTQ5MzctOWEzMS03ZDkxYTUxNjZmNjMiDQp9"
        },
        "version": "1.1"
    }
}*
���� e0704caff2dfebd2a433e985fff2345c
�
linkedService/ls_blob.json�{
    "name": "ls_blob",
    "type": "Microsoft.DataFactory/factories/linkedservices",
    "properties": {
        "annotations": [],
        "type": "AzureBlobStorage",
        "typeProperties": {
            "connectionString": "DefaultEndpointsProtocol=https;AccountName=adfgluedemo;EndpointSuffix=core.windows.net;",
            "encryptedCredential": "ew0KICAiVmVyc2lvbiI6ICIyMDE3LTExLTMwIiwNCiAgIlByb3RlY3Rpb25Nb2RlIjogIktleSIsDQogICJTZWNyZXRDb250ZW50VHlwZSI6ICJQbGFpbnRleHQiLA0KICAiQ3JlZGVudGlhbElkIjogIkRBVEFGQUNUT1JZQDczODU3REEzLThDMzEtNEEzNS05MkVELURBMTY4QUREMEU0Ml80M2FiMjk0Ni00ZjFhLTQ0NzItOGFkNi0wNmIwNGNkOWQ1MjgiDQp9"
        }
    }
}*
���� 9633fe753cc1cf24307429ef8d5cb8b0
�
dataset/SNF_TGT.json�{
    "name": "SNF_TGT",
    "properties": {
        "linkedServiceName": {
            "referenceName": "Snowflake_Link",
            "type": "LinkedServiceReference"
        },
        "annotations": [],
        "type": "SnowflakeV2Table",
        "schema": [],
        "typeProperties": {
            "schema": "DEV",
            "table": "CUST"
        }
    },
    "type": "Microsoft.DataFactory/factories/datasets"
}*
���� be89dd049d6a336eb8630e2ae5c3ceeb
�
dataset/ds_customers_src.json�{
    "name": "ds_customers_src",
    "properties": {
        "linkedServiceName": {
            "referenceName": "ls_blob",
            "type": "LinkedServiceReference"
        },
        "annotations": [],
        "type": "DelimitedText",
        "typeProperties": {
            "location": {
                "type": "AzureBlobStorageLocation",
                "fileName": "customers.csv",
                "container": "source-data"
            },
            "columnDelimiter": ",",
            "escapeChar": "\\",
            "firstRowAsHeader": true,
            "quoteChar": "\""
        },
        "schema": []
    },
    "type": "Microsoft.DataFactory/factories/datasets"
}*
���� 9fd84b45fadb0f37b107b5240882cc5c
�
dataflow/File_2_Snowflake.json�{
    "name": "File_2_Snowflake",
    "properties": {
        "type": "MappingDataFlow",
        "typeProperties": {
            "sources": [
                {
                    "dataset": {
                        "referenceName": "ds_customers_src",
                        "type": "DatasetReference"
                    },
                    "name": "srcCustomers"
                }
            ],
            "sinks": [
                {
                    "dataset": {
                        "referenceName": "SNF_TGT",
                        "type": "DatasetReference"
                    },
                    "name": "Snowflake",
                    "rejectedDataLinkedService": {
                        "referenceName": "ls_blob",
                        "type": "LinkedServiceReference"
                    }
                }
            ],
            "transformations": [
                {
                    "name": "filterstate"
                }
            ],
            "scriptLines": [
                "source(output(",
                "          CUSTOMER_ID as string,",
                "          CUSTOMER_NAME as string,",
                "          EMAIL as string,",
                "          PHONE as long,",
                "          STATE as string,",
                "          CREATED_DATE as date,",
                "          SOURCE_SYSTEM as string",
                "     ),",
                "     allowSchemaDrift: true,",
                "     validateSchema: false,",
                "     ignoreNoFilesFound: false) ~> srcCustomers",
                "srcCustomers filter(CUSTOMER_ID == 'C0011') ~> filterstate",
                "filterstate sink(allowSchemaDrift: true,",
                "     validateSchema: false,",
                "     deletable:false,",
                "     insertable:true,",
                "     updateable:false,",
                "     upsertable:false,",
                "     format: 'table',",
                "     skipDuplicateMapInputs: true,",
                "     skipDuplicateMapOutputs: true,",
                "     saveOrder: 1,",
                "     stageInsert: true) ~> Snowflake"
            ]
        }
    }
}*
���� b9d27e97ed1ace480d4567cbc733cf71
�
'ADF_to_AWS_Glue_Convertor/snowflake.yml�# created-by: streamlit-in-workspaces; skill-version=1.0.0
definition_version: 2
entities:
  streamlit_app:
    type: streamlit
    identifier:
      database: DWH
      schema: DEV
      name: ADF_TO_AWS_GLUE_CONVERTOR
    title: ADF_to_AWS_Glue_Convertor
    query_warehouse: COMPUTE_WH
    compute_pool: SYSTEM_COMPUTE_POOL_CPU
    run_mode: SpcsOnly
    execute_as: OWNER
    main_file: streamlit_app.py
    artifacts:
      - pyproject.toml
      - streamlit_app.py
      - .streamlit/config.toml
      - converter.py
      - glue_deployment.py
*
���� d242be3b8a6740fe9b182b0aa3b80f50